//! Shared task execution service boundary.
//!
//! Steda keeps durable scheduling and state transitions in `PostgreSQL`. This
//! module exposes the narrow in-process boundary around one registered task executor
//! invocation so applications can compose standard Tower middleware without
//! moving durable semantics into the worker process.

use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::future::{BoxFuture, poll_fn};
use tokio::sync::Mutex;
use tower_service::Service;

use crate::{
    context::TaskContext,
    error::{Error, Result},
    types::{Json, JsonObject, RunId, TaskId},
    worker::ErasedTaskExecutor,
};

/// Request passed through Steda's shared execution middleware stack.
///
/// The request describes one already-claimed task attempt. Middleware runs
/// inside Steda's durable execution envelope: errors and panics are handled by
/// the executor and persisted through the normal `PostgreSQL` state transitions.
pub struct ExecutionRequest {
    /// Task context available to the executor.
    context: TaskContext,
    /// JSON-erased task input used only at the internal dispatch boundary.
    input: Json,
    /// JSON-erased task executor selected from the worker's frozen registry.
    executor: ErasedTaskExecutor,
}

impl ExecutionRequest {
    /// Build an execution request at the internal dispatch boundary.
    pub(crate) fn new(context: TaskContext, input: Json, executor: ErasedTaskExecutor) -> Self {
        Self { context, input, executor }
    }

    /// Return the logical task identifier.
    pub fn task_id(&self) -> TaskId {
        self.context.id()
    }

    /// Return the claimed run identifier.
    pub fn run_id(&self) -> RunId {
        self.context.run_id()
    }

    /// Return the registered task name.
    pub fn task_name(&self) -> &str {
        self.context.name()
    }

    /// Return the queue containing this task.
    pub fn queue_name(&self) -> &str {
        self.context.queue_name()
    }

    /// Return the current attempt number.
    pub fn attempt(&self) -> i32 {
        self.context.attempt()
    }

    /// Return task headers.
    pub fn headers(&self) -> &JsonObject {
        self.context.headers()
    }

    /// Return the durable task context.
    pub const fn context(&self) -> &TaskContext {
        &self.context
    }

    /// Split the internal dispatch request into its execution parts.
    pub(crate) fn into_parts(self) -> (TaskContext, Json, ErasedTaskExecutor) {
        (self.context, self.input, self.executor)
    }
}

impl fmt::Debug for ExecutionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionRequest")
            .field("task_id", &self.task_id())
            .field("run_id", &self.run_id())
            .field("task_name", &self.task_name())
            .field("queue_name", &self.queue_name())
            .field("attempt", &self.attempt())
            .finish_non_exhaustive()
    }
}

/// Successful response from one task executor invocation.
///
/// The task output remains JSON-erased only at this internal heterogeneous
/// dispatch boundary. Typed producers and task executors never interact with it.
#[derive(Debug)]
pub struct ExecutionResponse {
    /// Serialized task output returned by the executor.
    output: Json,
}

impl ExecutionResponse {
    /// Wrap a serialized executor result.
    const fn new(output: Json) -> Self {
        Self { output }
    }

    /// Recover the serialized executor result for the durable state transition.
    pub(crate) fn into_output(self) -> Json {
        self.output
    }
}

/// Base Tower service for one registered task-executor invocation.
///
/// Applications normally interact with this type only indirectly through
/// [`StedaBuilder::layer`](crate::StedaBuilder::layer). The complete layer
/// stack is assembled once and then shared by every queue and worker created
/// from the built [`Steda`](crate::Steda) handle.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutionService;

impl Service<ExecutionRequest> for ExecutionService {
    type Response = ExecutionResponse;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ExecutionRequest) -> Self::Future {
        Box::pin(async move {
            let (context, input, executor) = request.into_parts();
            let output = executor(input, context).await?;
            Ok(ExecutionResponse::new(output))
        })
    }
}

/// Type-erased process-shared execution stack owned by [`Steda`](crate::Steda).
#[derive(Clone)]
pub(crate) struct SharedExecutionService {
    /// Erased service call implementation.
    call: Arc<
        dyn Fn(ExecutionRequest) -> BoxFuture<'static, Result<ExecutionResponse>> + Send + Sync,
    >,
}

impl fmt::Debug for SharedExecutionService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedExecutionService").finish_non_exhaustive()
    }
}

impl SharedExecutionService {
    /// Erase one fully composed Tower service for process-wide sharing.
    pub(crate) fn new<S>(service: S) -> Self
    where
        S: Service<ExecutionRequest, Response = ExecutionResponse, Error = Error> + Send + 'static,
        S::Future: Send + 'static,
    {
        let service = Arc::new(Mutex::new(service));
        Self {
            call: Arc::new(move |request| {
                let service = Arc::clone(&service);
                Box::pin(async move {
                    let mut service = service.lock().await;
                    poll_fn(|cx| service.poll_ready(cx)).await?;
                    let response = service.call(request);
                    drop(service);
                    response.await
                })
            }),
        }
    }
}

impl Service<ExecutionRequest> for SharedExecutionService {
    type Response = ExecutionResponse;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ExecutionRequest) -> Self::Future {
        (self.call)(request)
    }
}
