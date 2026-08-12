//! Task spawning and idempotency tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Error, Result, RetryStrategy, Steda, Task, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const IDEMPOTENCY_RETRY: Task<Value, Value> = Task::new("idempotency-retry");

    const DIFFERENT_TASK: Task<Value, Value> = Task::new("different-task");

    const SPAWN_DEFAULTS: Task<Value, Value> = Task::new("spawn-defaults");

    const UNREGISTERED_IDEM: Task<Value, Value> = Task::new("unregistered-idem");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn idempotent_spawn_replays_and_rejects_conflicts(pool: PgPool) -> Result<()> {
        let queue = unique_queue("idempotency");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let key = format!("idem-{}", unique_queue("key"));
        let first =
            app.spawn(UNREGISTERED_IDEM, json!({"value": 42})).idempotency_key(key.clone()).await?;
        let second =
            app.spawn(UNREGISTERED_IDEM, json!({"value": 42})).idempotency_key(key).await?;

        assert!(first.created());
        assert!(!second.created());
        assert_eq!(first.task_id(), second.task_id());

        let conflict_key = format!("conflict-{}", unique_queue("key"));
        app.spawn(UNREGISTERED_IDEM, json!({"value": 1}))
            .idempotency_key(conflict_key.clone())
            .await?;
        assert!(matches!(
            app.spawn(UNREGISTERED_IDEM, json!({"value": 2}))
                .idempotency_key(conflict_key.clone())
                .await,
            Err(Error::IdempotencyConflict)
        ));
        assert!(matches!(
            app.spawn(DIFFERENT_TASK, json!({"value": 1})).idempotency_key(conflict_key).await,
            Err(Error::IdempotencyConflict)
        ));

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn idempotent_replay_survives_manual_retry(pool: PgPool) -> Result<()> {
        let queue = unique_queue("idempotency_retry");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;
        let worker = app
            .worker()
            .task(IDEMPOTENCY_RETRY, async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("expected failure".to_owned()))
            })
            .build()?;
        let key = format!("retry-{}", unique_queue("key"));

        let spawned = app
            .spawn(IDEMPOTENCY_RETRY, json!({}))
            .max_attempts(1)
            .idempotency_key(key.clone())
            .await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        spawned.retry().await?;

        let replay =
            app.spawn(IDEMPOTENCY_RETRY, json!({})).max_attempts(1).idempotency_key(key).await?;
        assert!(!replay.created());
        assert_eq!(replay.task_id(), spawned.task_id());

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn empty_headers_replay_as_absent_headers(pool: PgPool) -> Result<()> {
        let queue = unique_queue("idempotency_headers");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;
        let key = format!("headers-{}", unique_queue("key"));

        let first =
            app.spawn(UNREGISTERED_IDEM, json!({"value": 42})).idempotency_key(key.clone()).await?;
        let replay = app
            .spawn(UNREGISTERED_IDEM, json!({"value": 42}))
            .headers(Default::default())
            .idempotency_key(key)
            .await?;
        assert!(!replay.created());
        assert_eq!(replay.task_id(), first.task_id());

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn spawn_rejects_oversized_identifiers(pool: PgPool) -> Result<()> {
        let queue = unique_queue("idempotency_bounds");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let error = app
            .spawn(UNREGISTERED_IDEM, json!({}))
            .idempotency_key("x".repeat(1025))
            .await
            .expect_err("oversized idempotency key must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let oversized_name = "x".repeat(1025);
        let error =
            sqlx::query("SELECT id FROM steda.spawn_task($1, $2, '{}'::jsonb, '{}'::jsonb)")
                .bind(&queue)
                .bind(&oversized_name)
                .fetch_one(&pool)
                .await
                .expect_err("oversized task name must be rejected by PostgreSQL");
        assert!(error.to_string().contains("task_name must be at most 1024 bytes"));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn spawn_rejects_invalid_public_options(pool: PgPool) -> Result<()> {
        let queue = Steda::from_pool(pool).queue(unique_queue("invalid_spawn_options"))?;

        let error = queue
            .spawn(UNREGISTERED_IDEM, json!({}))
            .max_attempts(0)
            .await
            .expect_err("zero max_attempts must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let error = queue
            .spawn(UNREGISTERED_IDEM, json!({}))
            .max_attempts(u32::MAX)
            .await
            .expect_err("max_attempts outside the PostgreSQL range must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let error = queue
            .spawn(UNREGISTERED_IDEM, json!({}))
            .idempotency_key("   ")
            .await
            .expect_err("blank idempotency key must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        for factor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let error = queue
                .spawn(UNREGISTERED_IDEM, json!({}))
                .retry_strategy(RetryStrategy::exponential(Duration::ZERO, factor, None))
                .await
                .expect_err("invalid exponential retry factor must be rejected");
            assert!(matches!(error, Error::InvalidOptions(_)));
        }

        let excessive_delay = Duration::from_secs(i32::MAX as u64 + 1);
        let error = queue
            .spawn(UNREGISTERED_IDEM, json!({}))
            .retry_strategy(RetryStrategy::fixed(excessive_delay))
            .await
            .expect_err("retry delays outside the PostgreSQL range must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let error = queue
            .spawn(UNREGISTERED_IDEM, json!({}))
            .retry_strategy(RetryStrategy::exponential(Duration::ZERO, 2.0, Some(excessive_delay)))
            .await
            .expect_err("maximum retry delay outside the PostgreSQL range must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn postgres_rejects_noncanonical_cancellation_policies(pool: PgPool) -> Result<()> {
        let queue = unique_queue("invalid_cancellation_sql");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        for cancellation in [
            json!({}),
            json!({"unknown": 1}),
            json!({"max_delay": -1}),
            json!({"max_duration": 1.5}),
            json!({"max_delay": "1"}),
        ] {
            let options = json!({"cancellation": cancellation});
            sqlx::query("SELECT id FROM steda.spawn_task($1, $2, '{}'::jsonb, $3)")
                .bind(&queue)
                .bind(UNREGISTERED_IDEM.name())
                .bind(options)
                .fetch_one(&pool)
                .await
                .expect_err("noncanonical cancellation policy must be rejected by PostgreSQL");
        }

        let invalid_options = json!([]);
        sqlx::query("SELECT id FROM steda.spawn_task($1, $2, '{}'::jsonb, $3)")
            .bind(&queue)
            .bind(UNREGISTERED_IDEM.name())
            .bind(invalid_options)
            .fetch_one(&pool)
            .await
            .expect_err("spawn options must be a JSON object");

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn spawn_respects_default_and_explicit_attempt_budgets(pool: PgPool) -> Result<()> {
        let queue = unique_queue("spawn_attempts");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task(SPAWN_DEFAULTS, {
                let calls = calls.clone();
                move |_params: Value, _ctx| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<Value, Error>(Error::InvalidOptions("expected failure".to_owned()))
                    }
                }
            })
            .build()?;

        let defaulted = app
            .spawn(SPAWN_DEFAULTS, json!({}))
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        for _ in 0..5 {
            run_worker_for_claims(&worker, app.metrics(), 1).await?;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(matches!(defaulted.snapshot().await?, Some(TaskSnapshot::Failed { .. })));

        let explicit = app
            .spawn(SPAWN_DEFAULTS, json!({}))
            .max_attempts(3)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        for _ in 0..3 {
            run_worker_for_claims(&worker, app.metrics(), 1).await?;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 8);
        assert!(matches!(explicit.snapshot().await?, Some(TaskSnapshot::Failed { .. })));

        app.delete().await?;
        Ok(())
    }
}
