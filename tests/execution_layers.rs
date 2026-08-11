//! Shared execution-layer tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{
        Error, Result, Steda, Task,
        middleware::{Layer, Request, Response, Service},
    };

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    /// Task executed on the first queue.
    #[derive(Clone, Copy, Debug)]
    struct First;

    impl Task for First {
        const NAME: &'static str = "first";
        type Input = Value;
        type Output = Value;
    }

    /// Task executed on the second queue.
    #[derive(Clone, Copy, Debug)]
    struct Second;

    impl Task for Second {
        const NAME: &'static str = "second";
        type Input = Value;
        type Output = Value;
    }

    /// Task whose middleware panics before invoking its handler.
    #[derive(Clone, Copy, Debug)]
    struct LayerPanic;

    impl Task for LayerPanic {
        const NAME: &'static str = "layer-panic";
        type Input = Value;
        type Output = Value;
    }

    /// Echo a JSON task payload unchanged.
    async fn echo(input: Value, _context: steda::TaskContext) -> Result<Value> {
        Ok(input)
    }

    /// Handler that should never run because the execution layer panics first.
    async fn unreachable_handler(_input: Value, _context: steda::TaskContext) -> Result<Value> {
        Ok(json!({"unreachable": true}))
    }

    /// Records the queue and task observed for every handler invocation.
    #[derive(Clone, Debug)]
    struct ObserveLayer {
        seen: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl<S> Layer<S> for ObserveLayer {
        type Service = ObserveService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            ObserveService { inner, seen: Arc::clone(&self.seen) }
        }
    }

    /// Service produced by [`ObserveLayer`].
    #[derive(Clone, Debug)]
    struct ObserveService<S> {
        inner: S,
        seen: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl<S> Service<Request> for ObserveService<S>
    where
        S: Service<Request, Response = Response, Error = Error>,
    {
        type Response = Response;
        type Error = Error;
        type Future = S::Future;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: Request) -> Self::Future {
            self.seen
                .lock()
                .expect("observation mutex poisoned")
                .push((request.queue_name().to_owned(), request.task_name().to_owned()));
            self.inner.call(request)
        }
    }

    /// Layer that panics synchronously from the execution service call.
    #[derive(Clone, Copy, Debug)]
    struct PanicLayer;

    impl<S> Layer<S> for PanicLayer {
        type Service = PanicService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            PanicService { inner }
        }
    }

    /// Service produced by [`PanicLayer`].
    #[derive(Clone, Debug)]
    struct PanicService<S> {
        inner: S,
    }

    impl<S> Service<Request> for PanicService<S>
    where
        S: Service<Request, Response = Response, Error = Error>,
    {
        type Response = Response;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Response>> + Send + 'static>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, _request: Request) -> Self::Future {
            panic!("execution layer exploded before returning a future")
        }
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn one_execution_layer_is_shared_across_queues(pool: PgPool) -> Result<()> {
        let first_queue = unique_queue("layer_first");
        let second_queue = unique_queue("layer_second");
        let seen = Arc::new(Mutex::new(Vec::new()));

        let steda = Steda::builder(pool).layer(ObserveLayer { seen: Arc::clone(&seen) }).build();

        let first = steda.queue(first_queue.clone())?;
        let second = steda.queue(second_queue.clone())?;
        first.create().await?;
        second.create().await?;

        let first_worker = first.worker().task::<First>(echo).build()?;
        let second_worker = second.worker().task::<Second>(echo).build()?;

        let first_task = first.spawn::<First>(json!({"queue": 1})).await?;
        let second_task = second.spawn::<Second>(json!({"queue": 2})).await?;

        run_worker_for_claims(&first_worker, first.metrics(), 1).await?;
        run_worker_for_claims(&second_worker, second.metrics(), 1).await?;

        assert_eq!(first_task.result().await?, json!({"queue": 1}));
        assert_eq!(second_task.result().await?, json!({"queue": 2}));

        let observed = {
            let seen = seen.lock().expect("observation mutex poisoned");
            seen.clone()
        };

        assert_eq!(
            observed,
            vec![
                (first_queue.clone(), First::NAME.to_owned()),
                (second_queue.clone(), Second::NAME.to_owned()),
            ]
        );

        first.delete().await?;
        second.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn execution_layer_panic_is_persisted_as_failure(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("layer_panic");
        let steda = Steda::builder(pool).layer(PanicLayer).build();
        let queue = steda.queue(queue_name)?;
        queue.create().await?;
        let worker = queue.worker().task::<LayerPanic>(unreachable_handler).build()?;
        let task = queue
            .spawn::<LayerPanic>(json!({}))
            .max_attempts(1)
            .retry_strategy(steda::RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, queue.metrics(), 1).await?;
        assert!(matches!(
            queue.fetch_task_result(task.id()).await?,
            Some(steda::TaskResultSnapshot::Failed { .. })
        ));

        queue.delete().await?;
        Ok(())
    }
}
