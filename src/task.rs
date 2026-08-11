//! Typed task contracts and task handles.

use std::{fmt, marker::PhantomData, time::Duration};

use futures_util::future::BoxFuture;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    db::await_task_result_snapshot,
    error::{Error, Result},
    queue::Queue,
    types::{
        CancellationPolicy, JsonObject, RetryStrategy, SpawnConfig, TaskId, TaskResultSnapshot,
    },
};

/// Maximum persisted task name length in UTF-8 bytes.
pub(crate) const MAX_TASK_NAME_BYTES: usize = 1024;

/// Validate a task contract name before it reaches worker or storage APIs.
pub(crate) fn validate_task_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(Error::InvalidOptions("task name must be provided".to_owned()));
    }
    if name.len() > MAX_TASK_NAME_BYTES {
        return Err(Error::InvalidOptions(format!(
            "task name must be at most {MAX_TASK_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Compile-time contract for a durable task.
///
/// Implement this trait on a marker type shared by producers and workers. The
/// marker type couples the persisted task name to its input and output types
/// without requiring a runtime task value.
///
/// A producer cannot spawn the task with an unrelated input type:
///
/// ```compile_fail
/// use steda::{Queue, Task};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct AddInput { left: i64, right: i64 }
/// #[derive(Deserialize, Serialize)]
/// struct AddOutput { sum: i64 }
/// #[derive(Deserialize, Serialize)]
/// struct DifferentInput;
///
/// struct Add;
///
/// impl Task for Add {
///     const NAME: &'static str = "add";
///     type Input = AddInput;
///     type Output = AddOutput;
/// }
///
/// fn wrong(queue: &Queue) {
///     let _ = queue.spawn::<Add>(DifferentInput);
/// }
/// ```
pub trait Task: 'static {
    /// Persisted task name.
    const NAME: &'static str;

    /// Input accepted when spawning the task.
    type Input: Serialize + DeserializeOwned + Send + 'static;

    /// Output produced when the task completes successfully.
    type Output: Serialize + DeserializeOwned + Send + 'static;
}

/// Awaitable builder for one typed logical-task submission.
///
/// `Spawn` keeps the common path as `queue.spawn::<T>(input).await?` while allowing retry,
/// headers, cancellation, and idempotency options to be configured before the database write.
/// Awaiting the builder performs exactly one submission operation.
///
/// Unless overridden, `PostgreSQL` supplies the default attempt budget (five) and retry strategy
/// (30-second exponential backoff, factor 2, capped at one hour).
#[must_use = "spawn calls do nothing until awaited"]
pub struct Spawn<'a, T: Task> {
    /// Queue receiving the task.
    queue: &'a Queue,
    /// Typed task input.
    input: T::Input,
    /// Optional spawn configuration.
    options: SpawnConfig,
}

impl<T: Task> fmt::Debug for Spawn<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spawn")
            .field("task", &T::NAME)
            .field("queue", &self.queue.name())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<'a, T: Task> Spawn<'a, T> {
    /// Create a typed spawn call for a queue.
    pub(crate) fn new(queue: &'a Queue, input: T::Input) -> Self {
        Self { queue, input, options: SpawnConfig::default() }
    }

    /// Set the total attempt budget, including the first execution.
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.options.max_attempts = Some(max_attempts);
        self
    }

    /// Set the retry strategy used when a failed attempt still has budget remaining.
    pub const fn retry_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.options.retry_strategy = Some(strategy);
        self
    }

    /// Attach application-defined JSON headers visible through
    /// [`TaskContext::headers`](crate::TaskContext::headers).
    pub fn headers(mut self, headers: JsonObject) -> Self {
        self.options.headers = Some(headers);
        self
    }

    /// Set enqueue/start duration limits enforced durably by `PostgreSQL`.
    pub const fn cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.options.cancellation = Some(cancellation);
        self
    }

    /// Set the queue-scoped idempotency key for logical-task creation.
    ///
    /// Replaying the same key with the same original spawn request returns the existing task.
    /// Reusing it for a different request returns [`Error::IdempotencyConflict`].
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.options.idempotency_key = Some(key.into());
        self
    }
}

impl<'a, T: Task> IntoFuture for Spawn<'a, T> {
    type Output = Result<TaskHandle<T>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.queue.spawn_typed::<T>(self.input, self.options).await })
    }
}

/// Typed handle to one spawned logical task.
///
/// The task marker is retained so result decoding stays tied to `T::Output`. The handle may
/// refer either to a newly created task or to an existing task returned by idempotent spawn;
/// [`Self::created`] distinguishes those cases.
pub struct TaskHandle<T: Task> {
    /// Queue used to observe this task.
    queue: Queue,

    /// Logical task identifier.
    id: TaskId,

    /// Whether spawning created a new task rather than deduplicating.
    created: bool,

    /// Task contract identity.
    marker: PhantomData<fn() -> T>,
}

impl<T: Task> fmt::Debug for TaskHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle")
            .field("task", &T::NAME)
            .field("id", &self.id)
            .field("created", &self.created)
            .finish_non_exhaustive()
    }
}

impl<T: Task> Clone for TaskHandle<T> {
    fn clone(&self) -> Self {
        Self { queue: self.queue.clone(), id: self.id, created: self.created, marker: PhantomData }
    }
}

impl<T: Task> TaskHandle<T> {
    /// Create a typed task handle from a persisted spawn result.
    pub(crate) fn new(queue: Queue, id: TaskId, created: bool) -> Self {
        Self { queue, id, created, marker: PhantomData }
    }

    /// Return the logical task identifier.
    pub const fn id(&self) -> TaskId {
        self.id
    }

    /// Return whether this spawn created a new logical task.
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Wait for the task to reach a terminal state and return its typed output.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be observed, fails, is cancelled, or
    /// its persisted output cannot be decoded as [`Task::Output`].
    pub async fn result(&self) -> Result<T::Output> {
        self.result_inner(None).await
    }

    /// Wait up to `timeout` for the task to reach a terminal state and return
    /// its typed output.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout elapses, the task cannot be observed,
    /// fails, is cancelled, or its persisted output cannot be decoded as
    /// [`Task::Output`].
    pub async fn result_with_timeout(&self, timeout: Duration) -> Result<T::Output> {
        self.result_inner(Some(timeout)).await
    }

    /// Shared terminal-result implementation.
    async fn result_inner(&self, timeout: Option<Duration>) -> Result<T::Output> {
        let snapshot =
            await_task_result_snapshot(self.queue.pool(), self.queue.name(), self.id, timeout)
                .await?;
        decode_result(snapshot)
    }
}

/// Decode a terminal task snapshot into the output declared by the task contract.
fn decode_result<R: DeserializeOwned>(snapshot: TaskResultSnapshot) -> Result<R> {
    match snapshot {
        TaskResultSnapshot::Completed { result } => Ok(serde_json::from_value(result)?),
        TaskResultSnapshot::Failed { failure } => Err(Error::TaskFailed { failure }),
        TaskResultSnapshot::Cancelled => Err(Error::Cancelled),
        TaskResultSnapshot::Pending
        | TaskResultSnapshot::Running
        | TaskResultSnapshot::Sleeping => {
            Err(Error::Other("task result wait returned a non-terminal snapshot".to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_task_name;

    #[test]
    fn task_name_validation_uses_utf8_byte_length() {
        assert!(validate_task_name("task").is_ok());
        assert!(validate_task_name("   ").is_err());
        assert!(validate_task_name(&format!("{}é", "x".repeat(1022))).is_ok());
        assert!(validate_task_name(&format!("{}é", "x".repeat(1023))).is_err());
    }
}
