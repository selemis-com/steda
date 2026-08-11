//! Task execution behavior tests.

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

    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Error, Result, RetryStrategy, Steda, Step, Task, TaskContext, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const DOUBLE_CHECKPOINT: Step<i32> = Step::new("double");

    const FORGED_CANCELLED: Task<Value, Value> = Task::new("forged-cancelled");

    const FORGED_FAILED: Task<Value, Value> = Task::new("forged-failed");

    const FORGED_LEASE_LOST: Task<Value, Value> = Task::new("forged-lease-lost");

    const FORGED_SUSPENDED: Task<Value, Value> = Task::new("forged-suspended");

    const MIXED_FAIL: Task<Value, Value> = Task::new("mixed-fail");

    const MIXED_OK_A: Task<Value, Value> = Task::new("mixed-ok-a");

    const MIXED_OK_B: Task<Value, Value> = Task::new("mixed-ok-b");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn processes_task(pool: PgPool) -> Result<()> {
        #[derive(Debug, Serialize, Deserialize)]
        struct Params {
            value: i32,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Output {
            value: i32,
        }

        const DOUBLE: Task<Params, Output> = Task::new("double");

        let queue = unique_queue("task_exec");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let step_calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task(DOUBLE, {
                let step_calls = step_calls.clone();
                move |params: Params, ctx: TaskContext| {
                    let step_calls = step_calls.clone();
                    async move {
                        let value = ctx
                            .step(DOUBLE_CHECKPOINT, async move || {
                                step_calls.fetch_add(1, Ordering::SeqCst);
                                Ok(params.value * 2)
                            })
                            .await?;
                        Ok(Output { value })
                    }
                }
            })
            .build()?;

        let spawned = app.spawn(DOUBLE, Params { value: 21 }).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let output = spawned.result_with_timeout(Duration::from_secs(5)).await?;
        assert_eq!(output, Output { value: 42 });
        assert_eq!(step_calls.load(Ordering::SeqCst), 1);

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn handler_control_errors_are_persisted_as_failures(pool: PgPool) -> Result<()> {
        let queue = unique_queue("control_errors");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(FORGED_SUSPENDED, async |_params: Value, _ctx| Err::<Value, _>(Error::Suspended))
            .task(FORGED_CANCELLED, async |_params: Value, _ctx| Err::<Value, _>(Error::Cancelled))
            .task(FORGED_FAILED, async |_params: Value, _ctx| Err::<Value, _>(Error::FailedRun))
            .task(FORGED_LEASE_LOST, async |_params: Value, _ctx| Err::<Value, _>(Error::LeaseLost))
            .build()?;

        let suspended = app
            .spawn(FORGED_SUSPENDED, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;
        let cancelled = app
            .spawn(FORGED_CANCELLED, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;
        let failed = app
            .spawn(FORGED_FAILED, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;
        let lease_lost = app
            .spawn(FORGED_LEASE_LOST, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 4).await?;
        for snapshot in [
            suspended.snapshot().await?,
            cancelled.snapshot().await?,
            failed.snapshot().await?,
            lease_lost.snapshot().await?,
        ] {
            assert!(matches!(snapshot, Some(TaskSnapshot::Failed { .. })));
        }

        assert_eq!(app.metrics().lease_lost_executions(), 0);
        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_handles_mixed_success_and_failure(pool: PgPool) -> Result<()> {
        let queue = unique_queue("mixed_batch");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(MIXED_OK_A, async |_params: Value, _ctx| Ok(json!({"ok": "a"})))
            .task(MIXED_OK_B, async |_params: Value, _ctx| Ok(json!({"ok": "b"})))
            .task(MIXED_FAIL, async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("mixed batch failure".to_owned()))
            })
            .build()?;

        let ok_a = app.spawn(MIXED_OK_A, json!({})).await?;
        let fail = app.spawn(MIXED_FAIL, json!({})).max_attempts(1).await?;
        let ok_b = app.spawn(MIXED_OK_B, json!({})).await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        assert!(matches!(ok_a.snapshot().await?, Some(TaskSnapshot::Completed { .. })));
        assert!(matches!(fail.snapshot().await?, Some(TaskSnapshot::Failed { .. })));
        assert!(matches!(ok_b.snapshot().await?, Some(TaskSnapshot::Completed { .. })));

        app.delete().await?;

        Ok(())
    }
}
