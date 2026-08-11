//! Retry policy and manual retry tests.

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
    use sqlx::{AssertSqlSafe, PgPool, Row};
    use steda::{Error, Result, RetryStrategy, RunId, Steda, Task};
    use time::OffsetDateTime;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct ClaimedOnly;

    impl Task for ClaimedOnly {
        const NAME: &'static str = "claimed-only";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct RetryProbe;

    impl Task for RetryProbe {
        const NAME: &'static str = "retry-probe";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct DefaultBackoff;

    impl Task for DefaultBackoff {
        const NAME: &'static str = "default-backoff";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct Flaky;

    impl Task for Flaky {
        const NAME: &'static str = "flaky";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct NeverRetry;

    impl Task for NeverRetry {
        const NAME: &'static str = "never-retry";
        type Input = Value;
        type Output = Value;
    }

    async fn set_fake_now(pool: &PgPool, now: OffsetDateTime) -> Result<()> {
        let mut connections = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            let mut connection = pool.acquire().await?;
            sqlx::query("SELECT set_config('steda.fake_now', $1, false)")
                .bind(now.to_string())
                .execute(&mut *connection)
                .await?;
            connections.push(connection);
        }
        drop(connections);
        Ok(())
    }

    fn assert_sqlstate(error: &sqlx::Error, expected: &str) {
        let sqlx::Error::Database(database_error) = error else {
            panic!("expected database error {expected}, got {error:?}");
        };
        assert_eq!(database_error.code().as_deref(), Some(expected));
    }
    #[sqlx::test(migrations = "./sql/migrations")]
    async fn retry_delay_handles_fixed_none_and_large_exponential_values(pool: PgPool) {
        let capped: f64 = sqlx::query_scalar("SELECT steda.retry_delay_seconds($1::jsonb, $2)")
            .bind(json!({
                "kind": "exponential",
                "base_seconds": 30.0,
                "factor": 2.0,
                "max_seconds": 3600.0
            }))
            .bind(i32::MAX)
            .fetch_one(&pool)
            .await
            .expect("capped exponential retry should remain calculable");
        assert_eq!(capped, 3600.0);

        let database_capped: f64 =
            sqlx::query_scalar("SELECT steda.retry_delay_seconds($1::jsonb, $2)")
                .bind(json!({
                    "kind": "exponential",
                    "base_seconds": 30.0,
                    "factor": 2.0
                }))
                .bind(i32::MAX)
                .fetch_one(&pool)
                .await
                .expect("uncapped exponential retry should saturate at the database limit");
        assert_eq!(database_capped, f64::from(i32::MAX));

        let fixed: f64 = sqlx::query_scalar("SELECT steda.retry_delay_seconds($1::jsonb, $2)")
            .bind(json!({"kind": "fixed", "base_seconds": 12.5}))
            .bind(99_i32)
            .fetch_one(&pool)
            .await
            .expect("fixed retry should use its configured delay");
        assert_eq!(fixed, 12.5);

        let none: f64 = sqlx::query_scalar("SELECT steda.retry_delay_seconds($1::jsonb, $2)")
            .bind(json!({"kind": "none"}))
            .bind(99_i32)
            .fetch_one(&pool)
            .await
            .expect("none retry should have zero delay");
        assert_eq!(none, 0.0);
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn retry_delay_rejects_noncanonical_strategies(pool: PgPool) {
        for strategy in [
            Value::Null,
            json!({"kind": "fixed"}),
            json!({"kind": "exponential", "base_seconds": 30.0}),
            json!({"kind": "none", "base_seconds": 0.0}),
            json!({"kind": "fixed", "base_seconds": 1.0, "factor": 2.0}),
            json!({"kind": "immediate"}),
        ] {
            let result =
                sqlx::query_scalar::<_, f64>("SELECT steda.retry_delay_seconds($1::jsonb, 1)")
                    .bind(strategy)
                    .fetch_one(&pool)
                    .await;

            assert!(result.is_err(), "noncanonical retry strategy was accepted");
        }
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn capped_exponential_accepts_large_attempt_budget(pool: PgPool) -> Result<()> {
        let queue = unique_queue("retry_cap");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await.expect("queue should be created");

        let task = app
            .spawn::<RetryProbe>(json!({}))
            .max_attempts(2_147_483_647)
            .retry_strategy(RetryStrategy::exponential(
                Duration::from_secs(30),
                2.0,
                Some(Duration::from_secs(3_600)),
            ))
            .await
            .expect("capped retry growth should not invalidate a large attempt budget");

        assert!(task.created());
        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn retry_flow(pool: PgPool) -> Result<()> {
        let queue = unique_queue("retry");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let retry_calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task::<Flaky>({
                let retry_calls = retry_calls.clone();
                move |_params: Value, _ctx| {
                    let retry_calls = retry_calls.clone();
                    async move {
                        if retry_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err(Error::InvalidOptions("boom".to_owned()));
                        }
                        Ok(json!({"ok": true}))
                    }
                }
            })
            .build()?;
        let flaky = app
            .spawn::<Flaky>(json!({}))
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        let retry_result = flaky.result_with_timeout(Duration::from_secs(5)).await?;
        assert_eq!(retry_result, json!({"ok": true}));
        assert_eq!(retry_calls.load(Ordering::SeqCst), 2);

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn default_retry_uses_exponential_backoff(pool: PgPool) -> Result<()> {
        let queue = unique_queue("default_retry");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task::<DefaultBackoff>(async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("retry me".to_owned()))
            })
            .build()?;

        let base = OffsetDateTime::from_unix_timestamp(2_100_100_000)
            .map_err(|err| Error::InvalidOptions(err.to_string()))?;
        set_fake_now(&pool, base).await?;

        let spawned = app.spawn::<DefaultBackoff>(json!({})).max_attempts(2).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let query = format!(
            "SELECT state, available_at FROM steda.runs_{queue} WHERE task_id = $1 AND attempt = 2"
        );
        let row = sqlx::query(AssertSqlSafe(query)).bind(spawned.id()).fetch_one(&pool).await?;
        assert_eq!(row.get::<String, _>("state"), "sleeping");
        assert_eq!(
            row.get::<OffsetDateTime, _>("available_at"),
            base + time::Duration::seconds(30)
        );

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn retry_none_is_terminal_after_first_failure(pool: PgPool) -> Result<()> {
        let queue = unique_queue("retry_none");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task::<NeverRetry>({
                let calls = calls.clone();
                move |_params: Value, _ctx| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<Value, Error>(Error::InvalidOptions("terminal failure".to_owned()))
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn::<NeverRetry>(json!({}))
            .max_attempts(5)
            .retry_strategy(RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert!(matches!(
            app.fetch_task_result(spawned.id()).await?,
            Some(steda::TaskResultSnapshot::Failed { .. })
        ));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn historical_failed_run_keeps_failed_state_after_task_cancellation(
        pool: PgPool,
    ) -> Result<()> {
        let queue = unique_queue("failed_attempt_precedence");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let spawned = app
            .spawn::<ClaimedOnly>(json!({}))
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        let failed_run: RunId =
            sqlx::query_scalar("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1")
                .bind(&queue)
                .bind("failed-attempt-worker")
                .bind(30_i32)
                .bind(1_i32)
                .bind(vec![ClaimedOnly::NAME.to_owned()])
                .fetch_one(&pool)
                .await?;

        sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
            .bind(&queue)
            .bind(failed_run)
            .bind(json!({"name": "ExpectedFailure"}))
            .execute(&pool)
            .await?;
        app.cancel_task(spawned.id()).await?;

        let error = sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
            .bind(&queue)
            .bind(failed_run)
            .bind(json!({"name": "DuplicateFailure"}))
            .execute(&pool)
            .await
            .expect_err("historical failed attempt must remain failed after task cancellation");
        assert_sqlstate(&error, "AB002");

        app.delete().await?;
        Ok(())
    }
}
