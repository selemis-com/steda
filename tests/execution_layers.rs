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
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{
        Error, JsonObject, Result, RunId, Steda, Task, TaskId,
        middleware::{Layer, Request, Response, Service},
    };

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    /// Task executed on the first queue.
    const FIRST: Task<Value, Value> = Task::new("first");

    /// Task executed on the second queue.
    const SECOND: Task<Value, Value> = Task::new("second");

    /// Task used to verify execution request metadata.
    const METADATA: Task<Value, Value> = Task::new("metadata");

    /// Task whose middleware panics before invoking its handler.
    const LAYER_PANIC: Task<Value, Value> = Task::new("layer-panic");

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

    /// Metadata observed at the middleware boundary for one execution.
    #[derive(Clone, Debug, PartialEq)]
    struct ExecutionObservation {
        /// Logical task identifier observed on the request.
        task_id: TaskId,
        /// Claimed run identifier observed on the request.
        run_id: RunId,
        /// Task identifier observed through the request's task context.
        context_task_id: TaskId,
        /// Run identifier observed through the request's task context.
        context_run_id: RunId,
        /// Registered task name.
        task_name: String,
        /// Queue containing the task.
        queue_name: String,
        /// Current attempt number.
        attempt: u32,
        /// Application-defined task headers.
        headers: JsonObject,
    }

    /// Records the full public execution-request metadata surface.
    #[derive(Clone, Debug)]
    struct MetadataLayer {
        seen: Arc<Mutex<Vec<ExecutionObservation>>>,
    }

    impl<S> Layer<S> for MetadataLayer {
        type Service = MetadataService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            MetadataService { inner, seen: Arc::clone(&self.seen) }
        }
    }

    /// Service produced by [`MetadataLayer`].
    #[derive(Clone, Debug)]
    struct MetadataService<S> {
        inner: S,
        seen: Arc<Mutex<Vec<ExecutionObservation>>>,
    }

    impl<S> Service<Request> for MetadataService<S>
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
            let context = request.context();
            self.seen.lock().expect("metadata mutex poisoned").push(ExecutionObservation {
                task_id: request.task_id(),
                run_id: request.run_id(),
                context_task_id: context.task_id(),
                context_run_id: context.run_id(),
                task_name: request.task_name().to_owned(),
                queue_name: request.queue_name().to_owned(),
                attempt: request.attempt(),
                headers: request.headers().clone(),
            });
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

        let first_worker = first.worker().task(FIRST, echo).build()?;
        let second_worker = second.worker().task(SECOND, echo).build()?;

        let first_task = first.spawn(FIRST, json!({"queue": 1})).await?;
        let second_task = second.spawn(SECOND, json!({"queue": 2})).await?;

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
                (first_queue.clone(), FIRST.name().to_owned()),
                (second_queue.clone(), SECOND.name().to_owned()),
            ]
        );

        first.delete().await?;
        second.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn execution_layer_receives_complete_request_metadata(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("layer_metadata");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let steda =
            Steda::builder(pool.clone()).layer(MetadataLayer { seen: Arc::clone(&seen) }).build();
        let queue = steda.queue(queue_name.clone())?;
        queue.create().await?;
        let worker = queue.worker().task(METADATA, echo).build()?;

        let mut headers = JsonObject::new();
        headers.insert("trace_id".to_owned(), json!("trace-123"));
        let spawned = queue.spawn(METADATA, json!({"value": 7})).headers(headers.clone()).await?;
        run_worker_for_claims(&worker, queue.metrics(), 1).await?;
        assert_eq!(spawned.result().await?, json!({"value": 7}));

        let task_table = format!("tasks_{queue_name}");
        let query = format!("SELECT last_attempt_run FROM steda.{task_table} WHERE id = $1");
        let persisted_run_id: RunId = sqlx::query_scalar(AssertSqlSafe(query))
            .bind(spawned.task_id())
            .fetch_one(&pool)
            .await?;

        let observed = {
            let seen = seen.lock().expect("metadata mutex poisoned");
            seen.clone()
        };
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0],
            ExecutionObservation {
                task_id: spawned.task_id(),
                run_id: persisted_run_id,
                context_task_id: spawned.task_id(),
                context_run_id: persisted_run_id,
                task_name: METADATA.name().to_owned(),
                queue_name: queue_name.clone(),
                attempt: 1,
                headers,
            }
        );

        queue.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn execution_layer_panic_is_persisted_as_failure(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("layer_panic");
        let steda = Steda::builder(pool).layer(PanicLayer).build();
        let queue = steda.queue(queue_name)?;
        queue.create().await?;
        let worker = queue.worker().task(LAYER_PANIC, unreachable_handler).build()?;
        let task = queue
            .spawn(LAYER_PANIC, json!({}))
            .max_attempts(1)
            .retry_strategy(steda::RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, queue.metrics(), 1).await?;
        assert!(matches!(task.snapshot().await?, Some(steda::TaskSnapshot::Failed { .. })));

        queue.delete().await?;
        Ok(())
    }
}
