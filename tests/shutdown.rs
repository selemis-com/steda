//! Worker shutdown behavior tests.

#[cfg(test)]
mod common;

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
    use steda::{Error, Result, Steda, Task, TaskSnapshot};
    use tokio::{
        sync::{Notify, oneshot},
        time::timeout,
    };

    use super::common::unique_queue;

    const DRAINING_TASK: Task<Value, Value> = Task::new("draining-task");

    const QUICK: Task<Value, Value> = Task::new("quick");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_shutdown_returns_when_idle(pool: PgPool) -> Result<()> {
        let queue = unique_queue("idle_shutdown");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let runtime = app
            .worker()
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            runtime
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after idle shutdown".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn resolved_shutdown_does_not_claim_new_work(pool: PgPool) -> Result<()> {
        let queue = unique_queue("resolved_shutdown");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()?;
        let task = app.spawn(QUICK, json!({})).await?;

        worker.run_until(async {}).await?;

        assert_eq!(app.metrics().claimed_runs(), 0);
        assert_eq!(task.snapshot().await?, Some(TaskSnapshot::Pending));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_shutdown_drains_running_task(pool: PgPool) -> Result<()> {
        let queue = unique_queue("drain_shutdown");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let runtime = app
            .worker()
            .task(DRAINING_TASK, {
                let started = started.clone();
                let release = release.clone();
                let completed = completed.clone();
                move |_params: Value, _ctx| {
                    let started = started.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"drained": true}))
                    }
                }
            })
            .build()?;

        let spawned = app.spawn(DRAINING_TASK, json!({})).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let mut worker = tokio::spawn(async move {
            runtime
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        timeout(Duration::from_secs(5), started.notified())
            .await
            .map_err(|_| Error::Timeout("worker did not start task".to_owned()))?;
        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        assert!(
            timeout(Duration::from_millis(25), &mut worker).await.is_err(),
            "worker returned before draining the running task"
        );

        release.notify_one();
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after task release".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert_eq!(spawned.result().await?, json!({"drained": true}));

        app.delete().await?;

        Ok(())
    }
}
