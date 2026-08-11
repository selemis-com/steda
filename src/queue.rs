//! Queue-scoped producer and administration API.

use std::time::Duration;

use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{
    db::{await_task_result_snapshot, duration_seconds, fetch_task_result_snapshot},
    error::{Error, Result, map_sqlx_error},
    execution::SharedExecutionService,
    metrics::QueueMetrics,
    task::{Spawn, Task, TaskHandle, validate_task_name},
    types::{
        CancellationPolicy, Json, QueuePolicy, QueuePolicyOptions, RetryStrategy, RunId,
        SpawnConfig, SpawnResult, TaskId, TaskResultSnapshot,
    },
    worker::WorkerBuilder,
};

/// Maximum queue name length accepted by the database schema.
const MAX_QUEUE_NAME_LENGTH: usize = 33;
/// Largest whole-second retry delay passed through Steda's integer database APIs.
const MAX_DATABASE_DELAY_SECONDS: f64 = i32::MAX as f64;
/// Maximum queue-scoped idempotency key length in UTF-8 bytes.
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 1024;

/// Lightweight handle for one named Steda queue.
///
/// Constructing a handle with [`Steda::queue`](crate::Steda::queue) does not create database
/// state; call [`Queue::create`] before using a new queue. Durable task state lives in
/// `PostgreSQL`, while worker capabilities are process-local and owned separately by
/// [`WorkerBuilder`] and [`Worker`](crate::Worker).
#[derive(Debug, Clone)]
pub struct Queue {
    /// Shared Postgres pool.
    pool: PgPool,

    /// Queue name.
    name: String,

    /// Shared process-local task execution service.
    execution: SharedExecutionService,

    /// Exporter-agnostic counters shared by all clones of this queue.
    metrics: QueueMetrics,
}

impl Queue {
    /// Create a lightweight queue handle from a shared Steda root.
    pub(crate) fn from_parts(
        pool: PgPool,
        name: impl Into<String>,
        execution: SharedExecutionService,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            name: validate_queue_name(&name.into())?,
            execution,
            metrics: QueueMetrics::new(),
        })
    }

    /// Return the underlying pool.
    pub(crate) const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Return this queue's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return exporter-agnostic counters for this queue.
    ///
    /// The returned clone shares counters with this queue and all of its clones.
    /// An exporter can retain the handle and poll its read-only accessors without
    /// keeping a borrow of the queue.
    pub fn metrics(&self) -> QueueMetrics {
        self.metrics.clone()
    }

    /// Begin constructing a worker for this queue.
    ///
    /// The builder receives its own local task registry, so registering worker
    /// capabilities does not mutate this producer-facing queue handle.
    pub fn worker(&self) -> WorkerBuilder {
        WorkerBuilder::new(self.clone())
    }

    /// Return the process-shared task execution service.
    pub(crate) const fn execution(&self) -> &SharedExecutionService {
        &self.execution
    }

    /// Begin spawning a typed task.
    ///
    /// The returned call is awaitable directly and can be configured fluently
    /// before awaiting it.
    pub fn spawn<T>(&self, input: T::Input) -> Spawn<'_, T>
    where
        T: Task,
    {
        Spawn::new(self, input)
    }

    /// Serialize and persist a typed task spawn.
    pub(crate) async fn spawn_typed<T>(
        &self,
        input: T::Input,
        options: SpawnConfig,
    ) -> Result<TaskHandle<T>>
    where
        T: Task,
    {
        let spawned = self.spawn_serialized(T::NAME, serde_json::to_value(input)?, options).await?;

        Ok(TaskHandle::new(self.clone(), spawned.id, spawned.created))
    }

    /// Persist a serialized task spawn. JSON erasure stays private to the queue boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if spawn options are invalid or the database write fails.
    async fn spawn_serialized(
        &self,
        task_name: &str,
        params: Json,
        options: SpawnConfig,
    ) -> Result<SpawnResult> {
        validate_task_name(task_name)?;
        let options_json = Value::Object(normalize_spawn_options(options)?);

        let row = sqlx::query(
            r#"
            SELECT id, created
            FROM steda.spawn_task($1, $2, $3, $4)
            "#,
        )
        .bind(&self.name)
        .bind(task_name)
        .bind(params)
        .bind(options_json)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(SpawnResult { id: row.get("id"), created: row.get("created") })
    }

    /// Cancel a task by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cancellation fails.
    pub async fn cancel_task(&self, task_id: TaskId) -> Result<()> {
        sqlx::query("SELECT steda.cancel_task($1, $2)")
            .bind(&self.name)
            .bind(task_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Create this queue using the default persisted maintenance policy.
    ///
    /// Repeated creation is idempotent for a healthy existing queue and verifies its expected
    /// storage rather than silently recreating missing queue objects.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue name is invalid, queue creation fails, or policy setup fails.
    pub async fn create(&self) -> Result<()> {
        self.create_with_policy(QueuePolicyOptions::default()).await
    }

    /// Create this queue with explicit persisted maintenance-policy overrides.
    ///
    /// Fields left as `None` use the database defaults. Repeated creation has the same storage
    /// verification semantics as [`Self::create`].
    ///
    /// # Errors
    ///
    /// Returns an error if queue creation or policy setup fails.
    pub async fn create_with_policy(&self, policy: QueuePolicyOptions) -> Result<()> {
        let queue = &self.name;
        let cleanup_ttl_seconds = policy.cleanup_ttl.map(duration_seconds).transpose()?;
        let mut transaction = self.pool.begin().await?;

        sqlx::query("SELECT steda.create_queue($1)").bind(queue).execute(&mut *transaction).await?;

        if cleanup_ttl_seconds.is_some() || policy.cleanup_limit.is_some() {
            sqlx::query("SELECT steda.set_queue_policy($1, $2, $3)")
                .bind(queue)
                .bind(cleanup_ttl_seconds)
                .bind(policy.cleanup_limit)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Update queue maintenance policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the policy update fails.
    pub async fn set_policy(&self, options: QueuePolicyOptions) -> Result<()> {
        let queue = &self.name;
        let cleanup_ttl_seconds = options.cleanup_ttl.map(duration_seconds).transpose()?;

        sqlx::query("SELECT steda.set_queue_policy($1, $2, $3)")
            .bind(queue)
            .bind(cleanup_ttl_seconds)
            .bind(options.cleanup_limit)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Fetch queue maintenance policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or a policy row cannot be decoded.
    pub async fn policy(&self) -> Result<Option<QueuePolicy>> {
        let queue = &self.name;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                ceil(extract(epoch FROM cleanup_ttl))::bigint AS cleanup_ttl_seconds,
                cleanup_limit
            FROM steda.get_queue_policy($1)
            "#,
        )
        .bind(queue)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(queue_policy_from_row).transpose()
    }

    /// Delete this queue and its durable task storage.
    ///
    /// This is an administrative lifecycle operation, not retention cleanup. Existing task
    /// handles for the queue can no longer resolve their state after deletion.
    ///
    /// # Errors
    ///
    /// Returns an error if the database drop operation fails.
    pub async fn delete(&self) -> Result<()> {
        sqlx::query("SELECT steda.drop_queue($1)").bind(&self.name).execute(&self.pool).await?;

        Ok(())
    }

    /// Fetch the current result snapshot for a task.
    ///
    /// # Errors
    ///
    /// Returns an error if the result snapshot cannot be fetched.
    pub async fn fetch_task_result(&self, task_id: TaskId) -> Result<Option<TaskResultSnapshot>> {
        fetch_task_result_snapshot(&self.pool, &self.name, task_id).await
    }

    /// Wait until a task reaches a terminal result state.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be fetched, does not exist, or the timeout elapses
    /// before a terminal result is available.
    pub async fn await_task_result(
        &self,
        task_id: TaskId,
        timeout: Option<Duration>,
    ) -> Result<TaskResultSnapshot> {
        await_task_result_snapshot(&self.pool, &self.name, task_id, timeout).await
    }

    /// Retry a terminally failed logical task with one additional attempt.
    ///
    /// The logical task ID and previously committed checkpoints are preserved. The effective
    /// attempt budget grows to admit the new run, while the original spawn-time attempt budget
    /// remains unchanged for later idempotency comparison.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is missing, is not failed, or the database retry fails.
    pub async fn retry_task(&self, task_id: TaskId) -> Result<RunId> {
        let run_id = sqlx::query_scalar("SELECT steda.retry_task($1, $2)")
            .bind(&self.name)
            .bind(task_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(run_id)
    }

    /// Delete old terminal tasks according to this queue's persisted retention policy.
    ///
    /// Only logical tasks in terminal states are retention candidates. Deleting a task cascades
    /// to its runs and checkpoints through `PostgreSQL` foreign keys; pending, running, and
    /// sleeping tasks are not removed by retention cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    pub async fn cleanup(&self) -> Result<i32> {
        let tasks_deleted = sqlx::query_scalar("SELECT steda.cleanup_tasks($1)")
            .bind(&self.name)
            .fetch_one(&self.pool)
            .await?;

        Ok(tasks_deleted)
    }
}

/// Validate and clone a queue name for database calls.
///
/// # Errors
///
/// Returns an error for empty or oversized queue names.
pub(crate) fn validate_queue_name(queue_name: &str) -> Result<String> {
    if queue_name.trim().is_empty() {
        return Err(Error::MissingQueueName);
    }
    if queue_name.len() > MAX_QUEUE_NAME_LENGTH {
        return Err(Error::QueueNameTooLong {
            name: queue_name.to_owned(),
            max: MAX_QUEUE_NAME_LENGTH,
        });
    }
    Ok(queue_name.to_owned())
}

/// Converts spawn options into the JSON payload expected by the database function.
///
/// # Errors
///
/// Returns an error when retry, cancellation, attempt, or idempotency options are invalid, or an
/// option cannot be serialized to JSON.
pub(crate) fn normalize_spawn_options(options: SpawnConfig) -> Result<Map<String, Value>> {
    validate_spawn_options(&options)?;
    let mut payload = Map::new();
    if let Some(headers) = options.headers.filter(|headers| !headers.is_empty()) {
        payload.insert("headers".to_owned(), Value::Object(headers));
    }
    if let Some(max_attempts) = options.max_attempts {
        payload.insert("max_attempts".to_owned(), json!(max_attempts));
    }
    if let Some(retry_strategy) = options.retry_strategy {
        payload.insert("retry_strategy".to_owned(), serde_json::to_value(retry_strategy)?);
    }
    if let Some(cancellation) = options.cancellation {
        let value = serde_json::to_value(cancellation)?;
        if value.as_object().is_some_and(|obj| !obj.is_empty()) {
            payload.insert("cancellation".to_owned(), value);
        }
    }
    if let Some(idempotency_key) = options.idempotency_key {
        payload.insert("idempotency_key".to_owned(), Value::String(idempotency_key));
    }
    Ok(payload)
}

/// Validate spawn options before they cross the database boundary.
fn validate_spawn_options(options: &SpawnConfig) -> Result<()> {
    if matches!(options.max_attempts, Some(value) if value < 1) {
        return Err(Error::InvalidOptions("max_attempts must be at least 1".to_owned()));
    }
    if let Some(strategy) = options.retry_strategy {
        validate_retry_strategy(strategy)?;
    }
    if let Some(cancellation) = options.cancellation {
        validate_cancellation_policy(cancellation)?;
    }
    if let Some(key) = options.idempotency_key.as_deref() {
        if key.trim().is_empty() {
            return Err(Error::InvalidOptions("idempotency_key must not be empty".to_owned()));
        }
        if key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(Error::InvalidOptions(format!(
                "idempotency_key must be at most {MAX_IDEMPOTENCY_KEY_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

/// Validate retry strategy numeric values before crossing the database boundary.
///
/// Variant shape is guaranteed by [`RetryStrategy`] itself; `PostgreSQL` remains
/// authoritative for the persisted retry policy and delay calculation.
fn validate_retry_strategy(strategy: RetryStrategy) -> Result<()> {
    let invalid_seconds =
        |value: f64| !value.is_finite() || !(0.0..=MAX_DATABASE_DELAY_SECONDS).contains(&value);

    match strategy {
        RetryStrategy::Fixed { base_seconds } => {
            if invalid_seconds(base_seconds) {
                return Err(Error::InvalidOptions(format!(
                    "retry base_seconds must be finite and between 0 and {MAX_DATABASE_DELAY_SECONDS} seconds"
                )));
            }
        }
        RetryStrategy::Exponential { base_seconds, factor, max_seconds } => {
            if invalid_seconds(base_seconds) {
                return Err(Error::InvalidOptions(format!(
                    "retry base_seconds must be finite and between 0 and {MAX_DATABASE_DELAY_SECONDS} seconds"
                )));
            }
            if !factor.is_finite() || factor <= 0.0 {
                return Err(Error::InvalidOptions(
                    "retry factor must be finite and greater than zero".to_owned(),
                ));
            }
            if max_seconds.is_some_and(invalid_seconds) {
                return Err(Error::InvalidOptions(format!(
                    "retry max_seconds must be finite and between 0 and {MAX_DATABASE_DELAY_SECONDS} seconds"
                )));
            }
        }
        RetryStrategy::None => {}
    }

    Ok(())
}

/// Validate cancellation deadline values.
fn validate_cancellation_policy(policy: CancellationPolicy) -> Result<()> {
    if policy.max_duration.is_some_and(|value| value < 0) {
        return Err(Error::InvalidOptions(
            "cancellation max_duration must be non-negative".to_owned(),
        ));
    }
    if policy.max_delay.is_some_and(|value| value < 0) {
        return Err(Error::InvalidOptions(
            "cancellation max_delay must be non-negative".to_owned(),
        ));
    }
    Ok(())
}

/// Decodes a queue policy row returned by Postgres.
fn queue_policy_from_row(row: &PgRow) -> Result<QueuePolicy> {
    let cleanup_ttl_seconds: i64 = row.get("cleanup_ttl_seconds");
    let cleanup_ttl_seconds = u64::try_from(cleanup_ttl_seconds)
        .map_err(|_| Error::Other("PostgreSQL returned a negative queue cleanup TTL".to_owned()))?;
    let cleanup_limit: i32 = row.get("cleanup_limit");
    if cleanup_limit < 1 {
        return Err(Error::Other("PostgreSQL returned an invalid queue cleanup limit".to_owned()));
    }

    Ok(QueuePolicy {
        name: row.get("name"),
        cleanup_ttl: Duration::from_secs(cleanup_ttl_seconds),
        cleanup_limit,
    })
}
