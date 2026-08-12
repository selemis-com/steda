//! Worker runtime, concurrency, and maintenance tests.

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
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{CancellationPolicy, Error, Result, RetryStrategy, Steda, Task};
    use tokio::{
        sync::{Notify, Semaphore, oneshot},
        time::{sleep, timeout},
    };
    use uuid::Uuid;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const CANCEL_WHILE_SATURATED: Task<Value, Value> = Task::new("cancel-while-saturated");

    const CONCURRENT_TASK: Task<Value, Value> = Task::new("concurrent-task");

    const DRAINING_TASK: Task<Value, Value> = Task::new("draining-task");

    const EXPIRE_ONCE: Task<Value, Value> = Task::new("expire-once");

    const HANG: Task<Value, Value> = Task::new("hang");

    const QUICK: Task<Value, Value> = Task::new("quick");

    const RENEW_LEASE: Task<Value, Value> = Task::new("renew-lease");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_claims_only_available_concurrency(pool: PgPool) -> Result<()> {
        let queue = unique_queue("worker_concurrency");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let active = Arc::new(AtomicUsize::new(0));
        let reached_capacity = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = app
            .worker()
            .concurrency(2)
            .task(CONCURRENT_TASK, {
                let active = active.clone();
                let reached_capacity = reached_capacity.clone();
                let release = release.clone();
                move |_params: Value, _ctx| {
                    let active = active.clone();
                    let reached_capacity = reached_capacity.clone();
                    let release = release.clone();
                    async move {
                        if active.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                            reached_capacity.notify_one();
                        }
                        release
                            .acquire()
                            .await
                            .map_err(|_| {
                                Error::Other("concurrency test semaphore closed".to_owned())
                            })?
                            .forget();
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(json!({"ok": true}))
                    }
                }
            })
            .build()?;

        let first = app.spawn(CONCURRENT_TASK, json!({})).await?;
        let second = app.spawn(CONCURRENT_TASK, json!({})).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            worker
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        timeout(Duration::from_secs(5), reached_capacity.notified()).await.map_err(|_| {
            Error::Timeout("worker never reached configured concurrency".to_owned())
        })?;
        assert_eq!(active.load(Ordering::SeqCst), 2);

        release.add_permits(2);
        assert_eq!(first.result_with_timeout(Duration::from_secs(5)).await?, json!({"ok": true}));
        assert_eq!(second.result_with_timeout(Duration::from_secs(5)).await?, json!({"ok": true}));

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after concurrency test".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn invalid_worker_concurrency_is_rejected(pool: PgPool) -> Result<()> {
        let queue = Steda::from_pool(pool).queue(unique_queue("invalid_worker_concurrency"))?;
        let error = queue
            .worker()
            .concurrency(0)
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()
            .expect_err("zero worker concurrency must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_builder_rejects_invalid_configuration(pool: PgPool) -> Result<()> {
        let queue = Steda::from_pool(pool).queue(unique_queue("invalid_worker_configuration"))?;

        let error = queue
            .worker()
            .lease_duration(Duration::ZERO)
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()
            .expect_err("zero lease duration must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let excessive_concurrency = usize::try_from(i32::MAX).expect("i32 must fit usize") + 1;
        let error = queue
            .worker()
            .concurrency(excessive_concurrency)
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()
            .expect_err("concurrency outside the PostgreSQL range must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let error =
            queue.worker().build().expect_err("worker without task capabilities must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let error = queue
            .worker()
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()
            .expect_err("duplicate task registration must be rejected");
        assert!(matches!(error, Error::InvalidOptions(_)));

        let invalid_task: Task<Value, Value> = Task::new("   ");
        let error = queue
            .worker()
            .task(invalid_task, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()
            .expect_err("invalid task names must be rejected during registration");
        assert!(matches!(error, Error::InvalidOptions(_)));

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn terminal_claim_error_stops_claiming_and_drains_running_work(
        pool: PgPool,
    ) -> Result<()> {
        let queue_name = unique_queue("terminal_claim_error");
        let app = Steda::from_pool(pool.clone()).queue(queue_name.clone())?;
        app.create().await?;

        let started = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = app
            .worker()
            .concurrency(2)
            .task(DRAINING_TASK, {
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
                                Error::Other("terminal-error test semaphore closed".to_owned())
                            })?
                            .forget();
                        Ok(json!({"drained": true}))
                    }
                }
            })
            .build()?;

        let spawned = app.spawn(DRAINING_TASK, json!({})).await?;
        let worker = tokio::spawn(async move { worker.run().await });

        timeout(Duration::from_secs(5), started.notified())
            .await
            .map_err(|_| Error::Timeout("worker did not start draining task".to_owned()))?;

        let run_table = format!("runs_{queue_name}");
        let corrupt_storage =
            format!("ALTER TABLE steda.{run_table} DROP COLUMN available_at CASCADE");
        sqlx::query(AssertSqlSafe(corrupt_storage)).execute(&pool).await?;

        // The worker has one free slot, so its next claim observes the permanent
        // storage break. It must stop claiming but retain ownership of current work.
        let metrics = app.metrics();
        timeout(Duration::from_secs(5), async {
            while metrics.claim_errors() == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| Error::Timeout("worker did not observe terminal claim error".to_owned()))?;
        assert!(!worker.is_finished(), "terminal claim error abandoned in-flight work");

        release.add_permits(1);
        assert_eq!(
            spawned.result_with_timeout(Duration::from_secs(5)).await?,
            json!({"drained": true})
        );

        let worker_error = timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not return terminal claim error".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))?
            .expect_err("terminal claim error must stop the worker");
        assert!(matches!(worker_error, Error::Database(_)));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_renews_finite_claim_while_handler_is_healthy(pool: PgPool) -> Result<()> {
        let queue = unique_queue("renew_healthy_lease");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .lease_duration(Duration::from_secs(2))
            .task(RENEW_LEASE, async |_params: Value, _ctx| {
                sleep(Duration::from_millis(2_500)).await;
                Ok(json!({"renewed": true}))
            })
            .build()?;
        let task = app
            .spawn(RENEW_LEASE, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        assert_eq!(task.result().await?, json!({"renewed": true}));
        assert_eq!(app.metrics().lease_lost_executions(), 0);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn saturated_worker_reaps_expired_run_and_retries(pool: PgPool) -> Result<()> {
        let queue = unique_queue("saturated_reap");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let started = Arc::new(Notify::new());
        let attempts = Arc::new(AtomicUsize::new(0));
        let runtime = app
            .worker()
            .task(EXPIRE_ONCE, {
                let started = started.clone();
                let attempts = attempts.clone();
                move |_params: Value, _ctx| {
                    let started = started.clone();
                    let attempts = attempts.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            started.notify_one();
                            std::future::pending::<Result<Value>>().await
                        } else {
                            Ok(json!({"retried": true}))
                        }
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn(EXPIRE_ONCE, json!({}))
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
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
            .map_err(|_| Error::Timeout("worker did not start expiring task".to_owned()))?;

        let task_table = format!("tasks_{queue}");
        let run_id_query = format!("SELECT last_attempt_run FROM steda.{task_table} WHERE id = $1");
        let run_id: Uuid = sqlx::query_scalar(AssertSqlSafe(run_id_query))
            .bind(spawned.task_id())
            .fetch_one(&pool)
            .await?;
        let run_table = format!("runs_{queue}");
        let query = format!(
            "UPDATE steda.{run_table} SET claim_expires_at = steda.current_time() - interval '1 second' WHERE id = $1"
        );
        sqlx::query(AssertSqlSafe(query)).bind(run_id).execute(&pool).await?;

        let result = spawned.result_with_timeout(Duration::from_secs(5)).await?;
        assert_eq!(result, json!({"retried": true}));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after retry".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cancelled_running_task_releases_worker_permit(pool: PgPool) -> Result<()> {
        let queue = unique_queue("cancel_releases_permit");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let hanging_started = Arc::new(Notify::new());
        let runtime = app
            .worker()
            .task(HANG, {
                let hanging_started = hanging_started.clone();
                move |_params: Value, _ctx| {
                    let hanging_started = hanging_started.clone();
                    async move {
                        hanging_started.notify_one();
                        std::future::pending::<Result<Value>>().await
                    }
                }
            })
            .task(QUICK, async |_params: Value, _ctx| Ok(json!({"ok": true})))
            .build()?;

        let hanging = app.spawn(HANG, json!({})).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            runtime
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        timeout(Duration::from_secs(5), hanging_started.notified()).await.map_err(|_| {
            Error::Timeout("timed out waiting for hanging task to start".to_owned())
        })?;

        hanging.cancel().await?;

        let quick = app.spawn(QUICK, json!({})).await?;
        let result = quick.result_with_timeout(Duration::from_secs(5)).await?;
        assert_eq!(result, json!({"ok": true}));

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after cancellation".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn saturated_worker_enforces_max_duration(pool: PgPool) -> Result<()> {
        let queue = unique_queue("saturated_cancel");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let started = Arc::new(Notify::new());
        let runtime = app
            .worker()
            .task(CANCEL_WHILE_SATURATED, {
                let started = started.clone();
                move |_params: Value, _ctx| {
                    let started = started.clone();
                    async move {
                        started.notify_one();
                        std::future::pending::<Result<Value>>().await
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn(CANCEL_WHILE_SATURATED, json!({}))
            .cancellation(CancellationPolicy::new().max_duration(Duration::from_secs(1)))
            .await?;

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
            .map_err(|_| Error::Timeout("worker did not start cancellable task".to_owned()))?;

        assert!(matches!(
            spawned.result_with_timeout(Duration::from_secs(5)).await,
            Err(Error::Cancelled)
        ));

        shutdown_tx
            .send(())
            .map_err(|_| Error::Other("worker shutdown receiver dropped".to_owned()))?;
        timeout(Duration::from_secs(5), worker)
            .await
            .map_err(|_| Error::Timeout("worker did not stop after cancellation".to_owned()))?
            .map_err(|err| Error::Other(format!("worker task join failed: {err}")))??;

        app.delete().await?;
        Ok(())
    }
}
