//! Runtime context passed to task executors.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration as StdDuration,
};

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, SignedDuration};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    db::await_task_result_snapshot,
    error::{Error, Result, map_sqlx_error},
    queue::validate_queue_name,
    types::{ClaimedTask, Json, JsonObject, RunId, TaskId, TaskResultSnapshot},
    workflow::{Sleep, Step},
};

/// Maximum user-defined durable workflow identity length in UTF-8 bytes.
const MAX_WORKFLOW_NAME_BYTES: usize = 1024;
/// Internal namespace for typed result-bearing steps.
const STEP_PREFIX: &str = "$step:";
/// Internal namespace for durable sleeps.
const SLEEP_PREFIX: &str = "$sleep:";
/// Internal namespace for cross-task result waits.
const TASK_WAIT_PREFIX: &str = "$await-task:";

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

    /// Per-name local serialization for durable step execution.
    step_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl fmt::Debug for TaskContextInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContextInner")
            .field("id", &self.task.id)
            .field("run_id", &self.task.run_id)
            .field("name", &self.task.name)
            .field("attempt", &self.task.attempt)
            .field("pool", &self.pool)
            .field("queue_name", &self.queue_name)
            .field("task", &self.task)
            .field("headers", &self.headers)
            .field("checkpoint_cache", &self.checkpoint_cache)
            .field("step_locks", &self.step_locks)
            .finish()
    }
}

impl TaskContext {
    /// Create a new task context.
    pub(crate) async fn new(pool: PgPool, queue_name: String, task: ClaimedTask) -> Result<Self> {
        let rows = sqlx::query(
            r#"
            SELECT name, state
            FROM steda.get_task_checkpoint_states($1, $2, $3)
            "#,
        )
        .bind(&queue_name)
        .bind(task.id)
        .bind(task.run_id)
        .fetch_all(&pool)
        .await
        .map_err(map_sqlx_error)?;

        let checkpoint_cache: HashMap<String, Json> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let state: Json = row.get("state");
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
                step_locks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Return the task id.
    pub fn task_id(&self) -> TaskId {
        self.inner.task.id
    }

    /// Return the current run id.
    pub fn run_id(&self) -> RunId {
        self.inner.task.run_id
    }

    /// Return the task name.
    pub fn task_name(&self) -> &str {
        &self.inner.task.name
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
    pub async fn step<S, F, Fut>(&self, _step: S, f: F) -> Result<S::Output>
    where
        S: Step,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<S::Output>> + Send,
    {
        let name = workflow_storage_name(STEP_PREFIX, S::NAME)?;
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
    pub async fn sleep_for<S: Sleep>(&self, _sleep: S, duration: StdDuration) -> Result<()> {
        let duration = SignedDuration::try_from(duration)
            .map_err(|err| Error::InvalidOptions(err.to_string()))?;
        let now = self.database_time().await?;
        let wake_at = now
            .checked_add(duration)
            .ok_or_else(|| Error::InvalidOptions("sleep wake time is out of range".to_owned()))?;
        self.sleep_until_inner::<S>(wake_at).await
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
    pub async fn sleep_until<S: Sleep>(&self, _sleep: S, wake_at: OffsetDateTime) -> Result<()> {
        self.sleep_until_inner::<S>(wake_at).await
    }

    /// Shared durable-sleep implementation after the marker value has established its type.
    async fn sleep_until_inner<S: Sleep>(&self, wake_at: OffsetDateTime) -> Result<()> {
        let name = workflow_storage_name(SLEEP_PREFIX, S::NAME)?;
        let step_lock = self.step_lock(&name);
        let _guard = step_lock.lock().await;

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

    /// Wait for another queue's task to reach a terminal result state.
    ///
    /// The terminal [`TaskResultSnapshot`] is checkpointed under an internal identity derived from
    /// the target queue and task ID, so a later retry replays the completed wait. Unlike a durable
    /// sleep, this
    /// operation keeps the current worker execution slot occupied while polling.
    ///
    /// Same-queue waits are rejected because a finite worker pool can otherwise deadlock with
    /// every slot occupied by parents waiting for children that require those same slots.
    ///
    /// # Errors
    ///
    /// Returns an error if the target queue is the current queue or otherwise invalid,
    /// checkpointing fails, the target task cannot be fetched, or the configured timeout elapses.
    pub async fn await_task_result(
        &self,
        task_id: TaskId,
        options: AwaitTaskResultOptions,
    ) -> Result<TaskResultSnapshot> {
        let AwaitTaskResultOptions { queue, timeout } = options;
        let queue = validate_queue_name(&queue)?;
        if queue == self.inner.queue_name {
            return Err(Error::InvalidOptions(
                "TaskContext::await_task_result cannot wait on tasks in the same queue because this can deadlock workers. Spawn the child in a different queue.".to_owned(),
            ));
        }
        let checkpoint_name = format!("{TASK_WAIT_PREFIX}{queue}:{task_id}");
        let pool = self.inner.pool.clone();
        self.checkpoint(&checkpoint_name, async move || {
            await_task_result_snapshot(&pool, &queue, task_id, timeout).await
        })
        .await
    }

    /// Execute or replay one checkpoint under an already-namespaced persisted identity.
    async fn checkpoint<T, F, Fut>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        let step_lock = self.step_lock(name);
        let _guard = step_lock.lock().await;

        if let Some(state) = self.cached_checkpoint(name) {
            return Ok(serde_json::from_value(state)?);
        }

        let value = f().await?;
        let serialized = serde_json::to_value(&value)?;
        let (checkpoint_state, written) = self.persist_checkpoint(name, serialized).await?;
        if written { Ok(value) } else { Ok(serde_json::from_value(checkpoint_state)?) }
    }

    /// Return the local serializer for one durable checkpoint identity.
    fn step_lock(&self, name: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.inner.step_locks.lock().unwrap_or_else(PoisonError::into_inner);
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
        .bind(self.inner.task.id)
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
    if name.len() > MAX_WORKFLOW_NAME_BYTES {
        return Err(Error::InvalidOptions(format!(
            "workflow identity must be at most {MAX_WORKFLOW_NAME_BYTES} bytes"
        )));
    }
    Ok(format!("{prefix}{name}"))
}

/// Options for waiting on another task result from a task context.
///
/// A target queue is required because waiting on the current queue is rejected: a
/// worker can otherwise consume all local capacity while waiting for work that only
/// that same queue can execute.
#[derive(Debug, Clone)]
pub struct AwaitTaskResultOptions {
    /// Queue containing the awaited task.
    queue: String,

    /// Optional timeout.
    timeout: Option<StdDuration>,
}

impl AwaitTaskResultOptions {
    /// Create wait options for a task in `queue`.
    #[must_use]
    pub fn new(queue: impl Into<String>) -> Self {
        Self { queue: queue.into(), timeout: None }
    }

    /// Limit how long each execution attempt waits for the target result.
    #[must_use]
    pub const fn timeout(mut self, timeout: StdDuration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
