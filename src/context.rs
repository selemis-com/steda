//! Runtime context passed to task executors.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, SignedDuration};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    db::await_task_result_snapshot,
    error::{Error, Result, map_sqlx_error},
    task::{TaskRef, decode_result},
    types::{ClaimedTask, Json, JsonObject, RunId, TaskId},
    workflow::{KeyedStep, Sleep, Step},
};

/// Maximum persisted durable workflow identity length in UTF-8 bytes.
const MAX_WORKFLOW_STORAGE_NAME_BYTES: usize = 1024;
/// Internal namespace for typed result-bearing steps.
const STEP_PREFIX: &str = "$step:";
/// Internal namespace for runtime-keyed typed result-bearing steps.
const KEYED_STEP_PREFIX: &str = "$step-keyed:";
/// Internal namespace for durable sleeps.
const SLEEP_PREFIX: &str = "$sleep:";
/// Internal namespace for cross-task result waits.
const TASK_WAIT_PREFIX: &str = "$await-task:";

/// Persisted payload for one completed cross-task wait.
///
/// The task name is stored alongside the decoded output so replay can keep enforcing the
/// concrete task identity even after the child task itself has been removed by retention.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskWaitCheckpoint<Output> {
    /// Persisted concrete task name.
    task_name: String,
    /// Decoded child output.
    output: Output,
}

/// Durable execution context for the currently claimed task attempt.
///
/// The context exposes the logical task/run identity, task headers, checkpointed steps,
/// durable sleeps, and cross-queue result waits. Cloning a context does not create another
/// durable execution: every clone refers to the same claimed run and remains subject to the
/// same `PostgreSQL` lease and cancellation fencing.
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Shared task execution context state.
    inner: Arc<TaskContextInner>,
}

/// Shared state backing cloned [`TaskContext`] handles.
struct TaskContextInner {
    /// Database pool.
    ///
    /// We intentionally keep a pool here rather than holding a single checked-out
    /// connection for the whole task execution. Task executors may do external work,
    /// sleep or run for a long time; holding a connection across
    /// that whole lifetime would unnecessarily starve the pool.
    pool: PgPool,

    /// Queue name.
    queue_name: String,

    /// Current claimed task/run details.
    task: ClaimedTask,

    /// Task headers.
    headers: JsonObject,

    /// Committed checkpoint cache loaded at task start and updated after writes.
    checkpoint_cache: Mutex<HashMap<String, Json>>,

    /// Per-name local serialization for durable checkpoint access.
    checkpoint_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl fmt::Debug for TaskContextInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContextInner")
            .field("task_id", &self.task.task_id)
            .field("run_id", &self.task.run_id)
            .field("task_name", &self.task.task_name)
            .field("attempt", &self.task.attempt)
            .field("pool", &self.pool)
            .field("queue_name", &self.queue_name)
            .field("task", &self.task)
            .field("headers", &self.headers)
            .field("checkpoint_cache", &self.checkpoint_cache)
            .field("checkpoint_locks", &self.checkpoint_locks)
            .finish()
    }
}

impl TaskContext {
    /// Create a new task context.
    pub(crate) async fn new(pool: PgPool, queue_name: String, task: ClaimedTask) -> Result<Self> {
        let rows = sqlx::query(
            r#"
            SELECT checkpoint_name, checkpoint_state
            FROM steda.get_task_checkpoint_states($1, $2, $3)
            "#,
        )
        .bind(&queue_name)
        .bind(task.task_id)
        .bind(task.run_id)
        .fetch_all(&pool)
        .await
        .map_err(map_sqlx_error)?;

        let checkpoint_cache: HashMap<String, Json> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("checkpoint_name");
                let state: Json = row.get("checkpoint_state");
                (name, state)
            })
            .collect();

        let headers = task.headers.clone().unwrap_or_default();

        Ok(Self {
            inner: Arc::new(TaskContextInner {
                pool,
                queue_name,
                task,
                headers,
                checkpoint_cache: Mutex::new(checkpoint_cache),
                checkpoint_locks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Return the task id.
    pub fn task_id(&self) -> TaskId {
        self.inner.task.task_id
    }

    /// Return the current run id.
    pub fn run_id(&self) -> RunId {
        self.inner.task.run_id
    }

    /// Return the task name.
    pub fn task_name(&self) -> &str {
        &self.inner.task.task_name
    }

    /// Return the queue name.
    pub fn queue_name(&self) -> &str {
        &self.inner.queue_name
    }

    /// Return the attempt number.
    pub fn attempt(&self) -> u32 {
        self.inner.task.attempt
    }

    /// Get headers attached to this task.
    pub fn headers(&self) -> &JsonObject {
        &self.inner.headers
    }

    /// Execute a durable step with checkpointing.
    ///
    /// [`Step`] is the stable identity of the step for the lifetime of the logical
    /// task. Reusing the same step returns the committed value instead of
    /// executing `f` again. Cloned contexts serialize execution of the same step
    /// locally; different steps may execute concurrently.
    ///
    /// A checkpoint prevents repeated Steda step execution after a retry, but it
    /// cannot make external side effects exactly-once. External systems should
    /// still use their own idempotency or fencing keys.
    ///
    /// # Errors
    ///
    /// Returns an error if the step identity is invalid, the step function fails, or the
    /// checkpoint cannot be persisted or deserialized.
    pub async fn step<Output, F, Fut>(&self, step: Step<Output>, f: F) -> Result<Output>
    where
        Output: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Output>> + Send,
    {
        let name = workflow_storage_name(STEP_PREFIX, step.name())?;
        self.checkpoint(&name, f).await
    }

    /// Execute one runtime-keyed instance of a durable typed step.
    ///
    /// [`KeyedStep`] is useful when a workflow repeats the same statically defined operation for
    /// runtime-identified items. The pair `(step, key)` is the durable identity: the same pair
    /// replays its committed value, while distinct keys may execute and checkpoint concurrently.
    ///
    /// As with [`Self::step`], checkpointing prevents repeated Steda execution after retry but
    /// cannot make external side effects exactly-once. Runtime keys should therefore be stable,
    /// deterministic identifiers rather than attempt-local counters.
    ///
    /// # Errors
    ///
    /// Returns an error if the static step name or runtime key is invalid, the step function
    /// fails, or the checkpoint cannot be persisted or deserialized.
    pub async fn step_keyed<Output, F, Fut>(&self, step: KeyedStep<Output>, f: F) -> Result<Output>
    where
        Output: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Output>> + Send,
    {
        let name = keyed_workflow_storage_name(KEYED_STEP_PREFIX, step.step().name(), step.key())?;
        self.checkpoint(&name, f).await
    }

    /// Durably suspend this logical task for `duration`.
    ///
    /// The wake time is checkpointed under the [`Sleep`] identity. When the wake time is still in
    /// the future, Steda persists the run as sleeping and releases the worker claim. A later
    /// worker invokes the handler from the beginning; reaching the same durable sleep reuses
    /// the persisted wake time instead of starting a new delay.
    ///
    /// No Rust future or process-local state is retained while the task sleeps.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Suspended`] when the current attempt has been durably suspended,
    /// [`Error::Cancelled`] if a durable cancellation deadline wins, or another error if the
    /// duration cannot be represented or checkpoint/scheduling state cannot be persisted.
    pub async fn sleep_for(&self, sleep: Sleep, duration: Duration) -> Result<()> {
        let duration = SignedDuration::try_from(duration)
            .map_err(|err| Error::InvalidOptions(err.to_string()))?;
        let now = self.database_time().await?;
        let wake_at = now
            .checked_add(duration)
            .ok_or_else(|| Error::InvalidOptions("sleep wake time is out of range".to_owned()))?;
        self.sleep_until_inner(sleep, wake_at).await
    }

    /// Durably suspend this logical task until `wake_at`.
    ///
    /// The first call for this [`Sleep`] commits the wake time as durable workflow state. Replays
    /// use that committed time even if subsequent handler invocations pass a different `wake_at`.
    /// This keeps a retry from accidentally extending the sleep window.
    ///
    /// If the wake time has already arrived, the method returns `Ok(())` and execution
    /// continues. Otherwise the run is persisted as sleeping and the worker releases its claim.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Suspended`] when the current attempt has been durably suspended,
    /// [`Error::Cancelled`] if a durable cancellation deadline wins, or another error if the
    /// checkpoint cannot be read, written, or deserialized.
    pub async fn sleep_until(&self, sleep: Sleep, wake_at: OffsetDateTime) -> Result<()> {
        self.sleep_until_inner(sleep, wake_at).await
    }

    /// Shared durable-sleep implementation after the sleep value has established its identity.
    async fn sleep_until_inner(&self, sleep: Sleep, wake_at: OffsetDateTime) -> Result<()> {
        let name = workflow_storage_name(SLEEP_PREFIX, sleep.name())?;
        let checkpoint_lock = self.checkpoint_lock(&name);
        let _guard = checkpoint_lock.lock().await;

        let actual_wake_at = if let Some(cached) = self.cached_checkpoint(&name) {
            serde_json::from_value(cached)?
        } else {
            let serialized = serde_json::to_value(wake_at)?;
            let (checkpoint_state, _) = self.persist_checkpoint(&name, serialized).await?;
            serde_json::from_value(checkpoint_state)?
        };

        match self.schedule_run(actual_wake_at).await? {
            ScheduleOutcome::Ready => Ok(()),
            ScheduleOutcome::Suspended => Err(Error::Suspended),
            ScheduleOutcome::Cancelled => Err(Error::Cancelled),
        }
    }

    /// Begin waiting for another queue's typed task.
    ///
    /// The target [`TaskRef`] supplies the queue, task identity, and output type. The returned
    /// [`TaskWait`] is awaitable directly and can be configured with a per-attempt timeout.
    ///
    /// Same-queue waits are rejected when awaited because a finite worker pool can otherwise
    /// deadlock with every slot occupied by parents waiting for children that need those slots.
    pub fn await_task<'a, Input, Output>(
        &'a self,
        task: &TaskRef<Input, Output>,
    ) -> TaskWait<'a, Input, Output>
    where
        Input: Serialize + DeserializeOwned + Send + 'static,
        Output: Serialize + DeserializeOwned + Send + 'static,
    {
        TaskWait::new(self, task.clone())
    }

    /// Wait for one typed task reference and checkpoint its decoded output.
    async fn await_task_ref<Input, Output>(
        &self,
        task: TaskRef<Input, Output>,
        timeout: Option<Duration>,
    ) -> Result<Output>
    where
        Input: Serialize + DeserializeOwned + Send + 'static,
        Output: Serialize + DeserializeOwned + Send + 'static,
    {
        if task.queue_name() == self.inner.queue_name {
            return Err(Error::InvalidOptions(
                "TaskContext::await_task cannot wait on tasks in the same queue because this can deadlock workers. Spawn the child in a different queue.".to_owned(),
            ));
        }

        let checkpoint_name = format!("{TASK_WAIT_PREFIX}{}:{}", task.queue_name(), task.task_id());
        let pool = self.inner.pool.clone();
        let queue_name = task.queue_name().to_owned();
        let expected_task_name = task.task_name().to_owned();
        let task_name = expected_task_name.clone();
        let task_id = task.task_id();
        let checkpoint: TaskWaitCheckpoint<Output> = self
            .checkpoint(&checkpoint_name, async move || {
                let snapshot =
                    await_task_result_snapshot(&pool, &queue_name, &task_name, task_id, timeout)
                        .await?;
                let output = decode_result(snapshot)?;
                Ok(TaskWaitCheckpoint { task_name, output })
            })
            .await?;

        if checkpoint.task_name != expected_task_name {
            return Err(Error::TaskNameMismatch {
                task_id,
                expected: expected_task_name,
                actual: checkpoint.task_name,
            });
        }

        Ok(checkpoint.output)
    }

    /// Execute or replay one checkpoint under an already-namespaced persisted identity.
    async fn checkpoint<T, F, Fut>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        let checkpoint_lock = self.checkpoint_lock(name);
        let _guard = checkpoint_lock.lock().await;

        if let Some(state) = self.cached_checkpoint(name) {
            return Ok(serde_json::from_value(state)?);
        }

        let value = f().await?;
        let serialized = serde_json::to_value(&value)?;
        let (checkpoint_state, written) = self.persist_checkpoint(name, serialized).await?;
        if written { Ok(value) } else { Ok(serde_json::from_value(checkpoint_state)?) }
    }

    /// Return the local serializer for one durable checkpoint identity.
    fn checkpoint_lock(&self, name: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.inner.checkpoint_locks.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(locks.entry(name.to_owned()).or_insert_with(|| Arc::new(AsyncMutex::new(()))))
    }

    /// Return cached checkpoint state loaded for this claimed attempt.
    fn cached_checkpoint(&self, name: &str) -> Option<Json> {
        self.inner
            .checkpoint_cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    /// Persist checkpoint state through the authoritative `PostgreSQL` transition.
    async fn persist_checkpoint(&self, name: &str, value: Json) -> Result<(Json, bool)> {
        let row = sqlx::query(
            r#"
            SELECT checkpoint_state, written
            FROM steda.set_task_checkpoint_state($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&self.inner.queue_name)
        .bind(self.inner.task.task_id)
        .bind(name)
        .bind(value)
        .bind(self.inner.task.run_id)
        .fetch_one(&self.inner.pool)
        .await
        .map_err(map_sqlx_error)?;

        let checkpoint_state: Json = row.get("checkpoint_state");
        let written: bool = row.get("written");
        self.cache_checkpoint(name, checkpoint_state.clone());

        Ok((checkpoint_state, written))
    }

    /// Read the queue's authoritative clock from `PostgreSQL`.
    async fn database_time(&self) -> Result<OffsetDateTime> {
        let now =
            sqlx::query_scalar("SELECT steda.current_time()").fetch_one(&self.inner.pool).await?;
        Ok(now)
    }

    /// Ask `PostgreSQL` whether this run should suspend until `wake_at`.
    async fn schedule_run(&self, wake_at: OffsetDateTime) -> Result<ScheduleOutcome> {
        let outcome: String = sqlx::query_scalar("SELECT steda.schedule_run($1, $2, $3)")
            .bind(&self.inner.queue_name)
            .bind(self.inner.task.run_id)
            .bind(wake_at)
            .fetch_one(&self.inner.pool)
            .await
            .map_err(map_sqlx_error)?;

        match outcome.as_str() {
            "ready" => Ok(ScheduleOutcome::Ready),
            "suspended" => Ok(ScheduleOutcome::Suspended),
            "cancelled" => Ok(ScheduleOutcome::Cancelled),
            other => {
                Err(Error::Other(format!("PostgreSQL returned unknown schedule outcome {other:?}")))
            }
        }
    }

    /// Stores checkpoint state in the in-memory task cache.
    fn cache_checkpoint(&self, name: &str, value: Json) {
        self.inner
            .checkpoint_cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), value);
    }
}

/// Authoritative outcome of a `PostgreSQL` durable-sleep scheduling request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScheduleOutcome {
    /// The wake time has already arrived; execution remains running.
    Ready,
    /// The run was persisted as sleeping until the requested wake time.
    Suspended,
    /// The task was cancelled by its durable deadline.
    Cancelled,
}

/// Build the namespaced persisted key for one user-defined workflow identity.
fn workflow_storage_name(prefix: &str, name: &str) -> Result<String> {
    if name.trim().is_empty() {
        return Err(Error::InvalidOptions("workflow identity must not be empty".to_owned()));
    }

    let maximum_name_bytes = MAX_WORKFLOW_STORAGE_NAME_BYTES
        .checked_sub(prefix.len())
        .expect("workflow namespace prefix must fit the PostgreSQL checkpoint name limit");
    if name.len() > maximum_name_bytes {
        return Err(Error::InvalidOptions(format!(
            "workflow identity must be at most {maximum_name_bytes} bytes"
        )));
    }

    Ok(format!("{prefix}{name}"))
}

/// Build an unambiguous namespaced persisted key for one runtime-keyed workflow identity.
fn keyed_workflow_storage_name(prefix: &str, name: &str, key: &str) -> Result<String> {
    if name.trim().is_empty() {
        return Err(Error::InvalidOptions("workflow identity must not be empty".to_owned()));
    }
    if key.trim().is_empty() {
        return Err(Error::InvalidOptions("workflow instance key must not be empty".to_owned()));
    }

    // Length-prefix the static name so arbitrary UTF-8 keys cannot alias another `(name, key)`
    // pair through separator placement. `name.len()` is a UTF-8 byte count, matching the
    // PostgreSQL checkpoint-name byte ceiling enforced below.
    let identity = format!("{}:{name}{key}", name.len());
    let maximum_identity_bytes = MAX_WORKFLOW_STORAGE_NAME_BYTES
        .checked_sub(prefix.len())
        .expect("workflow namespace prefix must fit the PostgreSQL checkpoint name limit");
    if identity.len() > maximum_identity_bytes {
        return Err(Error::InvalidOptions(format!(
            "keyed workflow identity must be at most {maximum_identity_bytes} bytes"
        )));
    }

    Ok(format!("{prefix}{identity}"))
}

/// Awaitable typed cross-task result wait.
///
/// Created by [`TaskContext::await_task`]. Awaiting polls the target task until it reaches a
/// terminal state, decodes its typed output, and checkpoints that value in the parent workflow.
#[must_use = "task waits do nothing until awaited"]
pub struct TaskWait<'a, Input, Output> {
    /// Parent task context whose workflow owns the durable wait checkpoint.
    context: &'a TaskContext,
    /// Durable typed target reference.
    task: TaskRef<Input, Output>,
    /// Optional timeout for this execution attempt's polling wait.
    timeout: Option<Duration>,
}

impl<Input, Output> fmt::Debug for TaskWait<'_, Input, Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskWait")
            .field("task", &self.task)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl<'a, Input, Output> TaskWait<'a, Input, Output> {
    /// Create a typed task wait.
    const fn new(context: &'a TaskContext, task: TaskRef<Input, Output>) -> Self {
        Self { context, task, timeout: None }
    }

    /// Limit how long this execution attempt polls for the target result.
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<'a, Input, Output> IntoFuture for TaskWait<'a, Input, Output>
where
    Input: Serialize + DeserializeOwned + Send + 'static,
    Output: Serialize + DeserializeOwned + Send + 'static,
{
    type Output = Result<Output>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.context.await_task_ref(self.task, self.timeout).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KEYED_STEP_PREFIX, SLEEP_PREFIX, STEP_PREFIX, keyed_workflow_storage_name,
        workflow_storage_name,
    };

    #[test]
    fn workflow_identity_limit_includes_internal_namespace() {
        let maximum_step_name = "x".repeat(1024 - STEP_PREFIX.len());
        assert_eq!(
            workflow_storage_name(STEP_PREFIX, &maximum_step_name)
                .expect("maximum step name should fit")
                .len(),
            1024
        );
        assert!(workflow_storage_name(STEP_PREFIX, &(maximum_step_name + "x")).is_err());

        let maximum_sleep_name = "x".repeat(1024 - SLEEP_PREFIX.len());
        assert_eq!(
            workflow_storage_name(SLEEP_PREFIX, &maximum_sleep_name)
                .expect("maximum sleep name should fit")
                .len(),
            1024
        );
        assert!(workflow_storage_name(SLEEP_PREFIX, &(maximum_sleep_name + "x")).is_err());
    }

    #[test]
    fn keyed_workflow_identity_is_unambiguous_and_bounded() {
        let first = keyed_workflow_storage_name(KEYED_STEP_PREFIX, "ab", "c")
            .expect("valid keyed step should encode");
        let second = keyed_workflow_storage_name(KEYED_STEP_PREFIX, "a", "bc")
            .expect("valid keyed step should encode");
        assert_ne!(first, second);
        assert_eq!(first, "$step-keyed:2:abc");
        assert!(keyed_workflow_storage_name(KEYED_STEP_PREFIX, "step", "").is_err());

        let fixed = KEYED_STEP_PREFIX.len() + "1:a".len();
        let maximum_key = "x".repeat(1024 - fixed);
        assert_eq!(
            keyed_workflow_storage_name(KEYED_STEP_PREFIX, "a", &maximum_key)
                .expect("maximum keyed identity should fit")
                .len(),
            1024
        );
        assert!(keyed_workflow_storage_name(KEYED_STEP_PREFIX, "a", &(maximum_key + "x")).is_err());
    }
}
