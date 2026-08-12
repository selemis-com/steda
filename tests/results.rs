//! Task result and result-waiting tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Error, Result, Steda, Task, TaskId, TaskRef, TaskSnapshot, TaskState};
    use tokio::{
        sync::{Notify, Semaphore, oneshot},
        time::timeout,
    };

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
    async fn cancellation_can_commit_atomically_with_application_state(pool: PgPool) -> Result<()> {
        let queue = unique_queue("transactional_cancel");
        let app = Steda::from_pool(pool.clone()).queue(queue)?;
        app.create().await?;
        sqlx::query("CREATE TABLE application_cancellations (id integer PRIMARY KEY)")
            .execute(&pool)
            .await?;

        let rolled_back_task = app.spawn(RESULT_PROBE, json!({"change": 1})).await?;
        let mut rolled_back = pool.begin().await?;
        sqlx::query("INSERT INTO application_cancellations (id) VALUES (1)")
            .execute(&mut *rolled_back)
            .await?;
        rolled_back_task.cancel_in(&mut rolled_back).await?;
        rolled_back.rollback().await?;

        let application_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM application_cancellations")
                .fetch_one(&pool)
                .await?;
        assert_eq!(application_rows, 0);
        assert!(matches!(rolled_back_task.snapshot().await?, Some(TaskSnapshot::Pending)));

        let committed_task = app.spawn(RESULT_PROBE, json!({"change": 2})).await?;
        let mut committed = pool.begin().await?;
        sqlx::query("INSERT INTO application_cancellations (id) VALUES (2)")
            .execute(&mut *committed)
            .await?;
        committed_task.cancel_in(&mut committed).await?;
        committed.commit().await?;

        let application_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM application_cancellations")
                .fetch_one(&pool)
                .await?;
        assert_eq!(application_rows, 1);
        assert!(matches!(committed_task.snapshot().await?, Some(TaskSnapshot::Cancelled)));

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
            "queueName": app.name(),
            "taskName": RESULT_PROBE.name(),
            "taskId": task_id,
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
            "queueName": app.name(),
            "taskName": RESULT_PROBE.name(),
            "taskId": task_id,
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
    async fn reattached_task_reference_enforces_persisted_task_name(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_task_name");
        let steda = Steda::from_pool(pool);
        let app = steda.queue(queue)?;
        app.create().await?;

        let spawned = app.spawn(RESULT_PROBE, json!({})).await?;
        let mut encoded = serde_json::to_value(spawned.task_ref())?;
        encoded["taskName"] = json!("different-task");
        let mismatched_ref: TaskRef<Value, Value> = serde_json::from_value(encoded)?;
        let mismatched = steda.task(&mismatched_ref)?;

        for error in [
            mismatched.snapshot().await.expect_err("snapshot must reject mismatched task name"),
            mismatched.cancel().await.expect_err("cancel must reject mismatched task name"),
            mismatched.retry().await.expect_err("retry must reject mismatched task name"),
            mismatched
                .result_with_timeout(Duration::ZERO)
                .await
                .expect_err("result wait must reject mismatched task name"),
        ] {
            assert!(matches!(error, Error::TaskNameMismatch { .. }));
        }

        spawned.cancel().await?;
        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_snapshot_reports_running_state(pool: PgPool) -> Result<()> {
        let queue = unique_queue("result_running");
        let app = Steda::from_pool(pool).queue(queue)?;
        app.create().await?;

        let started = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let runtime = app
            .worker()
            .task(RESULT_PROBE, {
                let started = started.clone();
                let release = release.clone();
                move |_params: Value, _ctx| {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release
                            .acquire()
                            .await
                            .map_err(|_| {
                                Error::Other("running snapshot semaphore closed".to_owned())
                            })?
                            .forget();
                        Ok(json!({"ok": true}))
                    }
                }
            })
            .build()?;

        let task = app.spawn(RESULT_PROBE, json!({})).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            runtime
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        timeout(Duration::from_secs(5), started.notified())
            .await
            .map_err(|_| Error::Timeout("timed out waiting for task to start".to_owned()))?;

        let snapshot = task.snapshot().await?.expect("running task must have a snapshot");
        assert_eq!(snapshot.state(), TaskState::Running);
        assert!(!snapshot.is_terminal());
        assert!(matches!(snapshot, TaskSnapshot::Running));

        release.add_permits(1);
        assert_eq!(task.result_with_timeout(Duration::from_secs(5)).await?, json!({"ok": true}));

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| {
                Error::Timeout("worker did not stop after running snapshot test".to_owned())
            })?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

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
