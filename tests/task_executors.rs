//! Task executor extension-point tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use sqlx::PgPool;
    use steda::{Error, Result, RetryStrategy, RunId, Steda, Task, TaskContext, TaskExecutor};
    use tokio::sync::Notify;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    /// Task executed through a reusable executor object rather than an async function.
    #[derive(Clone, Copy, Debug)]
    struct Provisioned;

    impl Task for Provisioned {
        const NAME: &'static str = "provisioned";
        type Input = i64;
        type Output = i64;
    }

    /// Task whose executor remains active until Steda supervision cancels it.
    #[derive(Clone, Copy, Debug)]
    struct Cancellable;

    impl Task for Cancellable {
        const NAME: &'static str = "cancellable-executor";
        type Input = ();
        type Output = ();
    }

    /// Records that dropping the execution future tears down provisioned work.
    #[derive(Debug)]
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Executor that represents external work which only stops when its future is dropped.
    #[derive(Clone, Debug)]
    struct CancellableExecutor {
        started: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }

    impl TaskExecutor<Cancellable> for CancellableExecutor {
        fn execute(
            &self,
            (): (),
            _context: TaskContext,
        ) -> impl Future<Output = Result<()>> + Send {
            let started = Arc::clone(&self.started);
            let dropped = Arc::clone(&self.dropped);
            async move {
                let guard = DropFlag(dropped);
                started.notify_one();
                future::pending::<()>().await;
                drop(guard);
                Ok(())
            }
        }
    }

    /// Records each attempt as if a fresh execution environment were provisioned for it.
    #[derive(Clone, Debug)]
    struct ProvisionedExecutor {
        attempts: Arc<Mutex<Vec<(u32, RunId)>>>,
    }

    impl TaskExecutor<Provisioned> for ProvisionedExecutor {
        fn execute(
            &self,
            input: i64,
            context: TaskContext,
        ) -> impl Future<Output = Result<i64>> + Send {
            let attempts = Arc::clone(&self.attempts);
            async move {
                attempts
                    .lock()
                    .expect("attempt mutex poisoned")
                    .push((context.attempt(), context.run_id()));

                if context.attempt() == 1 {
                    return Err(Error::Other("provisioned runtime exited".to_owned()));
                }

                Ok(input * 2)
            }
        }
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn reusable_executor_uses_the_normal_retry_and_result_path(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("task_executor");
        let steda = Steda::from_pool(pool);
        let queue = steda.queue(queue_name)?;
        queue.create().await?;

        let attempts = Arc::new(Mutex::new(Vec::new()));
        let worker = queue
            .worker()
            .task_executor::<Provisioned>(ProvisionedExecutor { attempts: Arc::clone(&attempts) })
            .build()?;
        let task = queue
            .spawn::<Provisioned>(21)
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;

        run_worker_for_claims(&worker, queue.metrics(), 2).await?;

        assert_eq!(task.result().await?, 42);
        {
            let attempts = attempts.lock().expect("attempt mutex poisoned");
            assert_eq!(attempts.len(), 2);
            assert_eq!(attempts[0].0, 1);
            assert_eq!(attempts[1].0, 2);
            assert_ne!(attempts[0].1, attempts[1].1);
            drop(attempts);
        }

        queue.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn supervision_drops_executor_future_after_cancellation(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("task_executor_cancel");
        let steda = Steda::from_pool(pool);
        let queue = steda.queue(queue_name)?;
        queue.create().await?;

        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let worker = queue
            .worker()
            .task_executor::<Cancellable>(CancellableExecutor {
                started: Arc::clone(&started),
                dropped: Arc::clone(&dropped),
            })
            .build()?;
        let task = queue.spawn::<Cancellable>(()).await?;

        let run_worker = run_worker_for_claims(&worker, queue.metrics(), 1);
        let cancel = async {
            started.notified().await;
            queue.cancel_task(task.id()).await
        };
        let (worker_result, cancel_result) = tokio::join!(run_worker, cancel);
        cancel_result?;
        worker_result?;

        assert!(dropped.load(Ordering::SeqCst), "executor future was not dropped");
        assert!(task.result().await.is_err_and(|error| error.is_cancelled()));

        queue.delete().await?;
        Ok(())
    }
}
