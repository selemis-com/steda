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
    use steda::{Error, Result, Steda, Task, TaskResultSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct ResultProbe;

    impl Task for ResultProbe {
        const NAME: &'static str = "result-probe";
        type Input = Value;
        type Output = Value;
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_handle_result_reports_failure(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_failure");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let worker = app
            .worker()
            .task::<ResultProbe>(async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("task result failure".to_owned()))
            })
            .build()?;

        let task = app.spawn::<ResultProbe>(json!({})).max_attempts(1).await?;
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

        let task = app.spawn::<ResultProbe>(json!({})).await?;
        app.cancel_task(task.id()).await?;
        assert!(matches!(task.result().await, Err(Error::Cancelled)));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn fetch_task_result_returns_none_for_unknown_task(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_missing");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        assert_eq!(app.fetch_task_result(uuid::Uuid::now_v7()).await?, None);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn await_task_result_reports_typed_missing_task(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_await_missing");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let task_id = uuid::Uuid::now_v7();
        let error = app
            .await_task_result(task_id, Some(Duration::ZERO))
            .await
            .expect_err("awaiting an unknown task must fail");
        assert!(matches!(error, Error::TaskNotFound(id) if id == task_id));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_handle_timeout_preserves_pending_result(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_timeout");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let task = app.spawn::<ResultProbe>(json!({})).await?;
        assert!(matches!(task.result_with_timeout(Duration::ZERO).await, Err(Error::Timeout(_))));
        assert_eq!(app.fetch_task_result(task.id()).await?, Some(TaskResultSnapshot::Pending));

        app.delete().await?;
        Ok(())
    }
}
