//! Typed task definitions, references, and task handles.

use std::{fmt, marker::PhantomData, time::Duration};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

use crate::{
    db::await_task_result_snapshot,
    error::{Error, Result},
    queue::{Queue, validate_queue_name},
    types::{
        CancellationPolicy, Json, JsonObject, RetryStrategy, SpawnConfig, TaskId,
        TaskResultSnapshot, TaskState,
    },
};

/// Maximum persisted task name length in UTF-8 bytes.
pub(crate) const MAX_TASK_NAME_BYTES: usize = 1024;

/// Validate a task name before it reaches worker or storage APIs.
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

/// Stable typed definition of a durable task.
///
/// Define tasks as constants shared by producers and workers:
///
/// ```
/// use steda::Task;
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct AddInput;
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct AddOutput;
///
/// const ADD: Task<AddInput, AddOutput> = Task::new("add");
/// ```
///
/// The task value owns the persisted name while its generic parameters preserve the input/output
/// relationship at compile time:
///
/// ```compile_fail
/// use steda::{Queue, Task};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct AddInput;
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct AddOutput;
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct DifferentInput;
///
/// const ADD: Task<AddInput, AddOutput> = Task::new("add");
///
/// fn wrong(queue: &Queue) {
///     let _ = queue.spawn(ADD, DifferentInput);
/// }
/// ```
pub struct Task<Input, Output> {
    /// Persisted task name.
    name: &'static str,
    /// Typed input/output relationship.
    marker: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output> Copy for Task<Input, Output> {}

impl<Input, Output> Clone for Task<Input, Output> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Input, Output> Task<Input, Output> {
    /// Return the persisted task name.
    pub const fn name(self) -> &'static str {
        self.name
    }
}

impl<Input, Output> Task<Input, Output>
where
    Input: Serialize + DeserializeOwned + Send + 'static,
    Output: Serialize + DeserializeOwned + Send + 'static,
{
    /// Define a task with a stable persisted name.
    pub const fn new(name: &'static str) -> Self {
        Self { name, marker: PhantomData }
    }
}

impl<Input, Output> fmt::Debug for Task<Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Task").field(&self.name).finish()
    }
}

/// Awaitable builder for one typed logical-task submission.
///
/// `Spawn` keeps the common path as `queue.spawn(TASK, input).await?` while allowing retry,
/// headers, cancellation, and idempotency options to be configured before the database write.
/// Awaiting the builder performs exactly one submission operation.
///
/// Unless overridden, `PostgreSQL` supplies the default attempt budget (five) and retry strategy
/// (30-second exponential backoff, factor 2, capped at one hour).
#[must_use = "spawn calls do nothing until awaited"]
pub struct Spawn<'a, Input, Output> {
    /// Queue receiving the task.
    queue: &'a Queue,
    /// Typed task definition.
    task: Task<Input, Output>,
    /// Typed task input.
    input: Input,
    /// Optional spawn configuration.
    options: SpawnConfig,
}

impl<Input, Output> fmt::Debug for Spawn<'_, Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spawn")
            .field("task_name", &self.task.name())
            .field("queue_name", &self.queue.name())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<'a, Input, Output> Spawn<'a, Input, Output> {
    /// Create a typed spawn call for a queue.
    pub(crate) fn new(queue: &'a Queue, task: Task<Input, Output>, input: Input) -> Self {
        Self { queue, task, input, options: SpawnConfig::default() }
    }

    /// Set the total attempt budget, including the first execution.
    pub const fn max_attempts(mut self, max_attempts: u32) -> Self {
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

impl<'a, Input, Output> IntoFuture for Spawn<'a, Input, Output>
where
    Input: Serialize + Send + 'static,
    Output: DeserializeOwned + Send + 'static,
{
    type Output = Result<SpawnedTask<Input, Output>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.queue.spawn_typed(self.task, self.input, self.options).await })
    }
}

/// Durable typed reference to one logical task.
///
/// The serialized representation includes the queue, persisted task name, and logical [`TaskId`].
/// The generic parameters retain the task's input/output relationship while the persisted task
/// name keeps the concrete task definition attached across process restarts.
pub struct TaskRef<Input, Output> {
    /// Queue containing the logical task.
    queue_name: String,
    /// Persisted task name.
    task_name: String,
    /// Logical task identifier.
    task_id: TaskId,
    /// Typed input/output relationship.
    marker: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output> TaskRef<Input, Output> {
    /// Create a trusted typed reference from an already-validated task definition and queue name.
    fn from_parts(task: Task<Input, Output>, queue_name: String, task_id: TaskId) -> Self {
        Self { queue_name, task_name: task.name().to_owned(), task_id, marker: PhantomData }
    }

    /// Return the queue containing this task.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Return the persisted task name.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Return the logical task identifier.
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl<Input, Output> Clone for TaskRef<Input, Output> {
    fn clone(&self) -> Self {
        Self {
            queue_name: self.queue_name.clone(),
            task_name: self.task_name.clone(),
            task_id: self.task_id,
            marker: PhantomData,
        }
    }
}

impl<Input, Output> fmt::Debug for TaskRef<Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskRef")
            .field("queue_name", &self.queue_name)
            .field("task_name", &self.task_name)
            .field("task_id", &self.task_id)
            .finish()
    }
}

/// Stable serialized representation of a typed task reference.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskRefWire {
    /// Queue containing the task.
    queue_name: String,
    /// Persisted task name.
    task_name: String,
    /// Logical task identifier.
    task_id: TaskId,
}

impl<Input, Output> Serialize for TaskRef<Input, Output>
where
    Input: Serialize + DeserializeOwned + Send + 'static,
    Output: Serialize + DeserializeOwned + Send + 'static,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        TaskRefWire {
            queue_name: self.queue_name.clone(),
            task_name: self.task_name.clone(),
            task_id: self.task_id,
        }
        .serialize(serializer)
    }
}

impl<'de, Input, Output> Deserialize<'de> for TaskRef<Input, Output>
where
    Input: Serialize + DeserializeOwned + Send + 'static,
    Output: Serialize + DeserializeOwned + Send + 'static,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TaskRefWire::deserialize(deserializer)?;
        validate_task_name(&wire.task_name).map_err(serde::de::Error::custom)?;
        let queue_name = validate_queue_name(&wire.queue_name).map_err(serde::de::Error::custom)?;
        Ok(Self {
            queue_name,
            task_name: wire.task_name,
            task_id: wire.task_id,
            marker: PhantomData,
        })
    }
}

/// Typed snapshot of one logical task's current durable state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum TaskSnapshot<T> {
    /// Task is waiting to run.
    Pending,

    /// Task is currently running.
    Running,

    /// Task is sleeping until a later time.
    Sleeping,

    /// Task completed successfully.
    Completed {
        /// Typed completion payload.
        result: T,
    },

    /// Task failed terminally.
    Failed {
        /// Persisted structured failure payload.
        failure: Json,
    },

    /// Task was cancelled.
    Cancelled,
}

impl<T> TaskSnapshot<T> {
    /// Return the state without its completion or failure payload.
    pub const fn state(&self) -> TaskState {
        match self {
            Self::Pending => TaskState::Pending,
            Self::Running => TaskState::Running,
            Self::Sleeping => TaskState::Sleeping,
            Self::Completed { .. } => TaskState::Completed,
            Self::Failed { .. } => TaskState::Failed,
            Self::Cancelled => TaskState::Cancelled,
        }
    }

    /// Return whether this snapshot is terminal.
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled)
    }
}

/// Typed runtime handle to one logical task.
///
/// A handle attaches a durable [`TaskRef`] to this process's Steda connection so the task can be
/// observed or controlled. Use [`Steda::task`](crate::Steda::task) to reattach a deserialized task
/// reference after a process restart.
pub struct TaskHandle<Input, Output> {
    /// Queue used to observe and control this task.
    queue: Queue,
    /// Durable typed task reference.
    task_ref: TaskRef<Input, Output>,
}

impl<Input, Output> fmt::Debug for TaskHandle<Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle").field("task_ref", &self.task_ref).finish_non_exhaustive()
    }
}

impl<Input, Output> Clone for TaskHandle<Input, Output> {
    fn clone(&self) -> Self {
        Self { queue: self.queue.clone(), task_ref: self.task_ref.clone() }
    }
}

impl<Input, Output> TaskHandle<Input, Output> {
    /// Create a typed task handle from a persisted spawn result.
    pub(crate) fn new(queue: Queue, task: Task<Input, Output>, task_id: TaskId) -> Self {
        let task_ref = TaskRef::from_parts(task, queue.name().to_owned(), task_id);
        Self { queue, task_ref }
    }

    /// Attach a durable typed task reference to a queue handle.
    pub(crate) fn from_ref(queue: Queue, task_ref: TaskRef<Input, Output>) -> Self {
        debug_assert_eq!(queue.name(), task_ref.queue_name());
        Self { queue, task_ref }
    }

    /// Return the logical task identifier.
    pub const fn task_id(&self) -> TaskId {
        self.task_ref.task_id()
    }

    /// Return a durable typed reference suitable for serialization or workflow checkpointing.
    pub fn task_ref(&self) -> TaskRef<Input, Output> {
        self.task_ref.clone()
    }

    /// Fetch the current durable task result snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the result snapshot cannot be fetched.
    pub async fn snapshot(&self) -> Result<Option<TaskSnapshot<Output>>>
    where
        Output: DeserializeOwned,
    {
        self.queue
            .fetch_task_result(self.task_ref.task_name(), self.task_id())
            .await?
            .map(decode_snapshot::<Output>)
            .transpose()
    }

    /// Cancel this logical task.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cancellation fails.
    pub async fn cancel(&self) -> Result<()> {
        self.queue.ensure_task_ref(self.task_ref.task_name(), self.task_id()).await?;
        self.queue.cancel_task(self.task_id()).await
    }

    /// Retry a terminally failed logical task with one additional attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is missing, is not failed, or the database retry fails.
    pub async fn retry(&self) -> Result<crate::RunId> {
        self.queue.ensure_task_ref(self.task_ref.task_name(), self.task_id()).await?;
        self.queue.retry_task(self.task_id()).await
    }

    /// Wait for the task to reach a terminal state and return its typed output.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be observed, fails, is cancelled, or
    /// its persisted output cannot be decoded as the task output type.
    pub async fn result(&self) -> Result<Output>
    where
        Output: DeserializeOwned,
    {
        self.result_inner(None).await
    }

    /// Wait up to `timeout` for the task to reach a terminal state and return
    /// its typed output.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout elapses, the task cannot be observed,
    /// fails, is cancelled, or its persisted output cannot be decoded as
    /// the task output type.
    pub async fn result_with_timeout(&self, timeout: Duration) -> Result<Output>
    where
        Output: DeserializeOwned,
    {
        self.result_inner(Some(timeout)).await
    }

    /// Shared terminal-result implementation.
    async fn result_inner(&self, timeout: Option<Duration>) -> Result<Output>
    where
        Output: DeserializeOwned,
    {
        let snapshot = await_task_result_snapshot(
            self.queue.pool(),
            self.queue.name(),
            self.task_ref.task_name(),
            self.task_id(),
            timeout,
        )
        .await?;
        decode_result(snapshot)
    }
}

/// Result of one typed task submission.
///
/// This retains whether the submission created a new logical task while exposing the same typed
/// observation and control operations as the attached [`TaskHandle`].
pub struct SpawnedTask<Input, Output> {
    /// Attached typed task handle.
    handle: TaskHandle<Input, Output>,
    /// Whether this submission created the logical task.
    created: bool,
}

impl<Input, Output> fmt::Debug for SpawnedTask<Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnedTask")
            .field("handle", &self.handle)
            .field("created", &self.created)
            .finish()
    }
}

impl<Input, Output> Clone for SpawnedTask<Input, Output> {
    fn clone(&self) -> Self {
        Self { handle: self.handle.clone(), created: self.created }
    }
}

impl<Input, Output> SpawnedTask<Input, Output> {
    /// Create a submission result from its attached handle and creation outcome.
    pub(crate) const fn new(handle: TaskHandle<Input, Output>, created: bool) -> Self {
        Self { handle, created }
    }

    /// Return whether this submission created a new logical task.
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Return the logical task identifier.
    pub const fn task_id(&self) -> TaskId {
        self.handle.task_id()
    }

    /// Return a durable typed task reference.
    pub fn task_ref(&self) -> TaskRef<Input, Output> {
        self.handle.task_ref()
    }

    /// Convert this submission result into its attached runtime handle.
    pub fn into_handle(self) -> TaskHandle<Input, Output> {
        self.handle
    }

    /// Fetch the current durable task result snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the result snapshot cannot be fetched.
    pub async fn snapshot(&self) -> Result<Option<TaskSnapshot<Output>>>
    where
        Output: DeserializeOwned,
    {
        self.handle.snapshot().await
    }

    /// Cancel this logical task.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cancellation fails.
    pub async fn cancel(&self) -> Result<()> {
        self.handle.cancel().await
    }

    /// Retry a terminally failed logical task with one additional attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if retrying the task fails.
    pub async fn retry(&self) -> Result<crate::RunId> {
        self.handle.retry().await
    }

    /// Wait for terminal completion and return the typed task output.
    ///
    /// # Errors
    ///
    /// Returns an error if observation fails, the task fails, or it is cancelled.
    pub async fn result(&self) -> Result<Output>
    where
        Output: DeserializeOwned,
    {
        self.handle.result().await
    }

    /// Wait up to `timeout` for terminal completion and return the typed task output.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout elapses, observation fails, the task fails, or it is
    /// cancelled.
    pub async fn result_with_timeout(&self, timeout: Duration) -> Result<Output>
    where
        Output: DeserializeOwned,
    {
        self.handle.result_with_timeout(timeout).await
    }
}

/// Decode a raw database snapshot into the typed public snapshot.
fn decode_snapshot<R: DeserializeOwned>(snapshot: TaskResultSnapshot) -> Result<TaskSnapshot<R>> {
    Ok(match snapshot {
        TaskResultSnapshot::Pending => TaskSnapshot::Pending,
        TaskResultSnapshot::Running => TaskSnapshot::Running,
        TaskResultSnapshot::Sleeping => TaskSnapshot::Sleeping,
        TaskResultSnapshot::Completed { result } => {
            TaskSnapshot::Completed { result: serde_json::from_value(result)? }
        }
        TaskResultSnapshot::Failed { failure } => TaskSnapshot::Failed { failure },
        TaskResultSnapshot::Cancelled => TaskSnapshot::Cancelled,
    })
}

/// Decode a terminal task snapshot into the task output type.
pub(crate) fn decode_result<R: DeserializeOwned>(snapshot: TaskResultSnapshot) -> Result<R> {
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
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{Task, TaskId, TaskRef, TaskSnapshot, validate_task_name};

    const REFERENCE_TASK: Task<Value, Value> = Task::new("reference-task");
    const OTHER_TASK: Task<Value, Value> = Task::new("other-task");

    #[test]
    fn task_name_validation_uses_utf8_byte_length() {
        assert!(validate_task_name("task").is_ok());
        assert!(validate_task_name("   ").is_err());
        assert!(validate_task_name(&format!("{}é", "x".repeat(1022))).is_ok());
        assert!(validate_task_name(&format!("{}é", "x".repeat(1023))).is_err());
    }

    #[test]
    fn task_reference_serialization_preserves_task_identity() {
        let task_id = TaskId::from_uuid(Uuid::nil());
        let task_ref = TaskRef::from_parts(REFERENCE_TASK, "queue".to_owned(), task_id);
        let encoded = serde_json::to_value(&task_ref).unwrap();

        assert_eq!(
            encoded,
            json!({
                "queueName": "queue",
                "taskName": "reference-task",
                "taskId": task_id,
            })
        );

        let decoded: TaskRef<Value, Value> = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.queue_name(), "queue");
        assert_eq!(decoded.task_name(), "reference-task");
        assert_eq!(decoded.task_id(), task_id);
    }

    #[test]
    fn task_reference_deserialization_rejects_invalid_identity() {
        let task_id = TaskId::from_uuid(Uuid::nil());

        for task_name in ["   ".to_owned(), format!("{}é", "x".repeat(1023))] {
            let encoded = json!({
                "queueName": "queue",
                "taskName": task_name,
                "taskId": task_id,
            });
            let error = serde_json::from_value::<TaskRef<Value, Value>>(encoded)
                .expect_err("invalid task name must be rejected");
            assert!(error.to_string().contains("task name"));
        }

        let encoded = json!({
            "queueName": "   ",
            "taskName": REFERENCE_TASK.name(),
            "taskId": task_id,
        });
        let error = serde_json::from_value::<TaskRef<Value, Value>>(encoded)
            .expect_err("invalid queue name must be rejected");
        assert!(error.to_string().contains("queue name"));
    }

    #[test]
    fn task_references_keep_same_typed_tasks_distinct_by_name() {
        let task_id = TaskId::from_uuid(Uuid::nil());
        let reference = TaskRef::from_parts(REFERENCE_TASK, "queue".to_owned(), task_id);
        let other = TaskRef::from_parts(OTHER_TASK, "queue".to_owned(), task_id);

        assert_eq!(reference.task_name(), "reference-task");
        assert_eq!(other.task_name(), "other-task");
    }

    #[test]
    fn typed_task_snapshot_serializes_with_state_tag() {
        let snapshot = TaskSnapshot::Completed { result: json!({"ok": true}) };
        assert_eq!(
            serde_json::to_value(snapshot).unwrap(),
            json!({"state": "completed", "result": {"ok": true}})
        );
    }
}
