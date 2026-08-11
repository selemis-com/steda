//! Queue retention and cleanup tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{Error, QueuePolicyOptions, Result, Steda, Task, TaskContext, TaskResultSnapshot};
    use tokio::time::timeout;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct CleanupRetry;

    impl Task for CleanupRetry {
        const NAME: &'static str = "cleanup-retry";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct CleanupTestTask;

    impl Task for CleanupTestTask {
        const NAME: &'static str = "cleanup-test-task";
        type Input = Value;
        type Output = Value;
    }

    fn relation_name(prefix: &str, queue: &str) -> String {
        format!("{prefix}_{queue}")
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cleanup_skips_task_being_retried(pool: PgPool) -> Result<()> {
        let queue = unique_queue("cleanup_retry_race");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;
        let worker = app
            .worker()
            .task::<CleanupRetry>(async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("fail once".to_owned()))
            })
            .build()?;

        let spawned = app.spawn::<CleanupRetry>(json!({})).max_attempts(1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let mut retry_tx = pool.begin().await?;
        let retry_run: uuid::Uuid = sqlx::query_scalar("SELECT steda.retry_task($1, $2)")
            .bind(&queue)
            .bind(spawned.id())
            .fetch_one(&mut *retry_tx)
            .await?;

        app.set_policy(QueuePolicyOptions {
            cleanup_ttl: Some(Duration::ZERO),
            cleanup_limit: Some(100),
        })
        .await?;
        let deleted = timeout(Duration::from_secs(2), app.cleanup())
            .await
            .map_err(|_| Error::Timeout("cleanup blocked on task being retried".to_owned()))??;
        assert_eq!(deleted, 0);

        retry_tx.commit().await?;
        let task = app.fetch_task_result(spawned.id()).await?;
        assert_eq!(task, Some(TaskResultSnapshot::Pending));

        let run_table = relation_name("runs", &queue);
        let query =
            format!("SELECT count(*) FROM steda.{run_table} WHERE id = $1 AND task_id = $2");
        let count: i64 = sqlx::query_scalar(AssertSqlSafe(query))
            .bind(retry_run)
            .bind(spawned.id())
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cleanup_completed_tasks(pool: PgPool) -> Result<()> {
        let queue = unique_queue("task_cleanup");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task::<CleanupTestTask>(async |_params: Value, ctx: TaskContext| {
                ctx.step("cleanup-checkpoint", async || {
                    Ok::<Value, Error>(json!({"status": "done"}))
                })
                .await
            })
            .build()?;

        let spawned = app.spawn::<CleanupTestTask>(json!({})).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert!(matches!(
            app.fetch_task_result(spawned.id()).await?,
            Some(TaskResultSnapshot::Completed { .. })
        ));

        app.set_policy(QueuePolicyOptions {
            cleanup_ttl: Some(Duration::ZERO),
            cleanup_limit: Some(100),
        })
        .await?;
        let tasks_deleted = app.cleanup().await?;
        assert_eq!(tasks_deleted, 1);
        assert!(app.fetch_task_result(spawned.id()).await?.is_none());

        for prefix in ["tasks", "runs", "checkpoints"] {
            let table = relation_name(prefix, &queue);
            let query = format!("SELECT count(*) FROM steda.{table}");
            let count: i64 = sqlx::query_scalar(AssertSqlSafe(query)).fetch_one(&pool).await?;
            assert_eq!(count, 0, "cleanup should cascade through {prefix} storage");
        }

        let tasks_deleted = app.cleanup().await?;
        assert_eq!(tasks_deleted, 0);

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn global_cleanup_uses_each_queue_persisted_policy(pool: PgPool) -> Result<()> {
        let first_name = unique_queue("cleanup_all_first");
        let second_name = unique_queue("cleanup_all_second");
        let steda = Steda::from_pool(pool);
        let first = steda.queue(first_name.clone())?;
        let second = steda.queue(second_name.clone())?;
        first
            .create_with_policy(QueuePolicyOptions {
                cleanup_ttl: Some(Duration::ZERO),
                cleanup_limit: Some(1),
            })
            .await?;
        second
            .create_with_policy(QueuePolicyOptions {
                cleanup_ttl: Some(Duration::ZERO),
                cleanup_limit: Some(100),
            })
            .await?;

        let first_worker = first
            .worker()
            .task::<CleanupTestTask>(async |_params: Value, _ctx| Ok(json!({"done": true})))
            .build()?;
        let second_worker = second
            .worker()
            .task::<CleanupTestTask>(async |_params: Value, _ctx| Ok(json!({"done": true})))
            .build()?;

        let first_tasks = [
            first.spawn::<CleanupTestTask>(json!({})).await?,
            first.spawn::<CleanupTestTask>(json!({})).await?,
        ];
        let second_task = second.spawn::<CleanupTestTask>(json!({})).await?;
        run_worker_for_claims(&first_worker, first.metrics(), 2).await?;
        run_worker_for_claims(&second_worker, second.metrics(), 1).await?;

        let cleanup: HashMap<_, _> = steda
            .cleanup()
            .await?
            .into_iter()
            .map(|entry| (entry.name, entry.tasks_deleted))
            .collect();
        assert_eq!(cleanup.get(&first_name), Some(&1));
        assert_eq!(cleanup.get(&second_name), Some(&1));

        let first_remaining = [
            first.fetch_task_result(first_tasks[0].id()).await?.is_some(),
            first.fetch_task_result(first_tasks[1].id()).await?.is_some(),
        ]
        .into_iter()
        .filter(|remaining| *remaining)
        .count();
        assert_eq!(first_remaining, 1);
        assert_eq!(second.fetch_task_result(second_task.id()).await?, None);

        first.delete().await?;
        second.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cleanup_includes_the_exact_ttl_boundary(pool: PgPool) -> Result<()> {
        let queue = unique_queue("cleanup_boundary");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;
        let spawned = app.spawn::<CleanupTestTask>(json!({})).await?;
        let tasks_table = relation_name("tasks", &queue);
        let runs_table = relation_name("runs", &queue);
        let mut connection = pool.acquire().await?;

        sqlx::query("SELECT set_config('steda.fake_now', '2026-01-01T00:00:00Z', false)")
            .execute(&mut *connection)
            .await?;
        let run_query = format!("SELECT last_attempt_run FROM steda.{tasks_table} WHERE id = $1");
        let run_id: uuid::Uuid = sqlx::query_scalar(AssertSqlSafe(run_query))
            .bind(spawned.id())
            .fetch_one(&mut *connection)
            .await?;
        let complete_run = format!(
            "UPDATE steda.{runs_table} SET state = 'completed', completed_at = steda.current_time() WHERE id = $1"
        );
        sqlx::query(AssertSqlSafe(complete_run)).bind(run_id).execute(&mut *connection).await?;
        let complete_task =
            format!("UPDATE steda.{tasks_table} SET state = 'completed' WHERE id = $1");
        sqlx::query(AssertSqlSafe(complete_task))
            .bind(spawned.id())
            .execute(&mut *connection)
            .await?;
        sqlx::query("SELECT steda.set_queue_policy($1, 0, 100)")
            .bind(&queue)
            .execute(&mut *connection)
            .await?;

        let deleted: i32 = sqlx::query_scalar("SELECT steda.cleanup_tasks($1)")
            .bind(&queue)
            .fetch_one(&mut *connection)
            .await?;
        assert_eq!(deleted, 1);

        drop(connection);
        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cleanup_waits_for_queue_lifecycle_lock(pool: PgPool) -> Result<()> {
        let queue = unique_queue("cleanup_lifecycle_lock");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let mut lifecycle = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('steda.queue'), hashtext($1))")
            .bind(&queue)
            .execute(&mut *lifecycle)
            .await?;

        let mut contender = pool.acquire().await?;
        sqlx::query("SET lock_timeout = '50ms'").execute(&mut *contender).await?;
        let error = sqlx::query("SELECT steda.cleanup_tasks($1)")
            .bind(&queue)
            .execute(&mut *contender)
            .await
            .expect_err("cleanup must wait for the matching queue lifecycle lock");
        let sqlx::Error::Database(database_error) = &error else {
            return Err(error.into());
        };
        assert_eq!(database_error.code().as_deref(), Some("55P03"));
        sqlx::query("SET lock_timeout = '0'").execute(&mut *contender).await?;
        drop(contender);

        lifecycle.commit().await?;
        app.delete().await?;
        Ok(())
    }
}
