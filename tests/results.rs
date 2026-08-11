//! Task result and result-waiting tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Error, Result, Steda, Task, TaskId, TaskRef, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const RESULT_PROBE: Task<Value, Value> = Task::new("result-probe");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_handle_result_reports_failure(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_failure");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let worker = app
            .worker()
            .task(RESULT_PROBE, async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("task result failure".to_owned()))
            })
            .build()?;

        let task = app.spawn(RESULT_PROBE, json!({})).max_attempts(1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert!(matches!(task.result().await, Err(Error::TaskFailed { .. })));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_handle_result_reports_cancellation(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_cancelled");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let task = app.spawn(RESULT_PROBE, json!({})).await?;
        task.cancel().await?;
        assert!(matches!(task.result().await, Err(Error::Cancelled)));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn reattached_task_snapshot_returns_none_for_unknown_task(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_missing");
        let steda = Steda::from_pool(pool);
        let app = steda.queue(queue)?;
        app.create().await?;

        let task_id = TaskId::from_uuid(uuid::Uuid::now_v7());
        let task_ref: TaskRef<Value, Value> = serde_json::from_value(json!({
            "queue_name": app.name(),
            "task_name": RESULT_PROBE.name(),
            "task_id": task_id,
        }))?;
        let task = steda.task(&task_ref)?;
        assert_eq!(task.snapshot().await?, None);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn reattached_task_result_reports_typed_missing_task(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_await_missing");
        let steda = Steda::from_pool(pool);
        let app = steda.queue(queue)?;
        app.create().await?;

        let task_id = TaskId::from_uuid(uuid::Uuid::now_v7());
        let task_ref: TaskRef<Value, Value> = serde_json::from_value(json!({
            "queue_name": app.name(),
            "task_name": RESULT_PROBE.name(),
            "task_id": task_id,
        }))?;
        let task = steda.task(&task_ref)?;
        let error = task
            .result_with_timeout(Duration::ZERO)
            .await
            .expect_err("awaiting an unknown task must fail");
        assert!(
            matches!(error, Error::TaskNotFound(missing_task_id) if missing_task_id == task_id)
        );

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_handle_timeout_preserves_pending_result(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_timeout");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let task = app.spawn(RESULT_PROBE, json!({})).await?;
        assert!(matches!(task.result_with_timeout(Duration::ZERO).await, Err(Error::Timeout(_))));
        assert_eq!(task.snapshot().await?, Some(TaskSnapshot::Pending));

        app.delete().await?;
        Ok(())
    }
}
