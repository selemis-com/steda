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
    use steda::{Error, Result, RetryStrategy, Steda, Task, TaskContext, TaskResultSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct ForgedCancelled;

    impl Task for ForgedCancelled {
        const NAME: &'static str = "forged-cancelled";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct ForgedFailed;

    impl Task for ForgedFailed {
        const NAME: &'static str = "forged-failed";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct ForgedLeaseLost;

    impl Task for ForgedLeaseLost {
        const NAME: &'static str = "forged-lease-lost";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct ForgedSuspended;

    impl Task for ForgedSuspended {
        const NAME: &'static str = "forged-suspended";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct MixedFail;

    impl Task for MixedFail {
        const NAME: &'static str = "mixed-fail";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct MixedOkA;

    impl Task for MixedOkA {
        const NAME: &'static str = "mixed-ok-a";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct MixedOkB;

    impl Task for MixedOkB {
        const NAME: &'static str = "mixed-ok-b";
        type Input = Value;
        type Output = Value;
    }

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

        struct Double;

        impl Task for Double {
            const NAME: &'static str = "double";
            type Input = Params;
            type Output = Output;
        }

        let queue = unique_queue("task_exec");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let step_calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task::<Double>({
                let step_calls = step_calls.clone();
                move |params: Params, ctx: TaskContext| {
                    let step_calls = step_calls.clone();
                    async move {
                        let value = ctx
                            .step("double", async move || {
                                step_calls.fetch_add(1, Ordering::SeqCst);
                                Ok(params.value * 2)
                            })
                            .await?;
                        Ok(Output { value })
                    }
                }
            })
            .build()?;

        let spawned = app.spawn::<Double>(Params { value: 21 }).await?;
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
            .task::<ForgedSuspended>(async |_params: Value, _ctx| Err::<Value, _>(Error::Suspended))
            .task::<ForgedCancelled>(async |_params: Value, _ctx| Err::<Value, _>(Error::Cancelled))
            .task::<ForgedFailed>(async |_params: Value, _ctx| Err::<Value, _>(Error::FailedRun))
            .task::<ForgedLeaseLost>(async |_params: Value, _ctx| Err::<Value, _>(Error::LeaseLost))
            .build()?;

        let spawned_ids = [
            app.spawn::<ForgedSuspended>(json!({}))
                .max_attempts(1)
                .retry_strategy(RetryStrategy::none())
                .await?
                .id(),
            app.spawn::<ForgedCancelled>(json!({}))
                .max_attempts(1)
                .retry_strategy(RetryStrategy::none())
                .await?
                .id(),
            app.spawn::<ForgedFailed>(json!({}))
                .max_attempts(1)
                .retry_strategy(RetryStrategy::none())
                .await?
                .id(),
            app.spawn::<ForgedLeaseLost>(json!({}))
                .max_attempts(1)
                .retry_strategy(RetryStrategy::none())
                .await?
                .id(),
        ];

        for spawned_id in spawned_ids {
            run_worker_for_claims(&worker, app.metrics(), 1).await?;
            assert!(matches!(
                app.fetch_task_result(spawned_id).await?,
                Some(TaskResultSnapshot::Failed { .. })
            ));
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
            .task::<MixedOkA>(async |_params: Value, _ctx| Ok(json!({"ok": "a"})))
            .task::<MixedOkB>(async |_params: Value, _ctx| Ok(json!({"ok": "b"})))
            .task::<MixedFail>(async |_params: Value, _ctx| {
                Err::<Value, Error>(Error::InvalidOptions("mixed batch failure".to_owned()))
            })
            .build()?;

        let ok_a = app.spawn::<MixedOkA>(json!({})).await?;
        let fail = app.spawn::<MixedFail>(json!({})).max_attempts(1).await?;
        let ok_b = app.spawn::<MixedOkB>(json!({})).await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        assert!(matches!(
            app.fetch_task_result(ok_a.id()).await?,
            Some(TaskResultSnapshot::Completed { .. })
        ));
        assert!(matches!(
            app.fetch_task_result(fail.id()).await?,
            Some(TaskResultSnapshot::Failed { .. })
        ));
        assert!(matches!(
            app.fetch_task_result(ok_b.id()).await?,
            Some(TaskResultSnapshot::Completed { .. })
        ));

        app.delete().await?;

        Ok(())
    }
}
