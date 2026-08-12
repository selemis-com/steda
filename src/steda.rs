//! Root Steda database handle.

use sqlx::{PgPool, Row};
use tower_layer::{Identity, Layer, Stack};
use tower_service::Service;

use crate::{
    error::{Error, Result},
    execution::{ExecutionRequest, ExecutionResponse, ExecutionService, SharedExecutionService},
    queue::Queue,
    task::{TaskHandle, TaskRef, validate_task_name},
    types::QueueCleanup,
};

/// Root handle for a Steda installation.
///
/// `Steda` is a lightweight owner of a shared `PostgreSQL` pool and one
/// process-local execution middleware stack. Named [`Queue`] values created
/// from it are lightweight views over the same durable state and execution
/// boundary.
#[derive(Clone, Debug)]
pub struct Steda {
    /// Shared `PostgreSQL` pool.
    pool: PgPool,
    /// Shared process-local task execution service.
    execution: SharedExecutionService,
}

impl Steda {
    /// Connect to `PostgreSQL` with the default execution service.
    ///
    /// TLS support is opt-in through Steda's `tls-rustls` and `tls-native-tls` crate features.
    /// Applications that already configure their own [`PgPool`] can use [`Steda::from_pool`]
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection cannot be established.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::from_pool(pool))
    }

    /// Create a Steda handle from an existing `SQLx` pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self::builder(pool).build()
    }

    /// Begin configuring a Steda handle around an existing `SQLx` pool.
    ///
    /// Use the builder when the process needs Tower execution layers. The
    /// complete layer stack is composed before [`StedaBuilder::build`] and is
    /// then shared by all queues and workers created from the resulting handle.
    pub const fn builder(pool: PgPool) -> StedaBuilder {
        StedaBuilder::new(pool)
    }

    /// Return a lightweight handle for a named queue.
    ///
    /// # Errors
    ///
    /// Returns an error when `name` is not a valid Steda queue name.
    pub fn queue(&self, name: impl Into<String>) -> Result<Queue> {
        Queue::from_parts(self.pool.clone(), name, self.execution.clone())
    }

    /// Attach a durable typed task reference to this Steda connection.
    ///
    /// The returned handle is lightweight and does not query the database until it is observed or
    /// controlled.
    ///
    /// # Errors
    ///
    /// Returns an error if the persisted task name or referenced queue name is invalid.
    pub fn task<Input, Output>(
        &self,
        task: &TaskRef<Input, Output>,
    ) -> Result<TaskHandle<Input, Output>> {
        validate_task_name(task.task_name())?;
        let queue = self.queue(task.queue_name().to_owned())?;
        Ok(TaskHandle::from_ref(queue, task.clone()))
    }

    /// List all queues in this Steda installation.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub async fn queues(&self) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT name FROM steda.list_queues()").fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|row| row.get("name")).collect())
    }

    /// Run one policy-driven retention cleanup pass for every configured queue.
    ///
    /// Each queue's own persisted TTL and row limit are authoritative; callers do not supply
    /// retention policy at cleanup time. The returned values report logical tasks deleted per
    /// queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup query fails.
    pub async fn cleanup(&self) -> Result<Vec<QueueCleanup>> {
        let rows = sqlx::query(
            r#"
            SELECT queue_name, tasks_deleted
            FROM steda.cleanup_all_queues(NULL::text)
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let tasks_deleted: i32 = row.get("tasks_deleted");
                let tasks_deleted = u32::try_from(tasks_deleted).map_err(|_| {
                    Error::Other("PostgreSQL returned a negative cleanup count".to_owned())
                })?;
                Ok(QueueCleanup { queue_name: row.get("queue_name"), tasks_deleted })
            })
            .collect()
    }

    /// Return the underlying `PostgreSQL` pool.
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Staged builder for Steda's process-shared execution middleware stack.
///
/// Ordinary applications that do not need custom Tower middleware can use
/// [`Steda::from_pool`] directly. The builder exists so multiple layers retain
/// their concrete Tower composition until `build()`, where the finished stack
/// is erased once for cheap sharing across workers.
#[derive(Debug)]
pub struct StedaBuilder<L = Identity> {
    /// Shared `PostgreSQL` pool.
    pool: PgPool,
    /// Tower layers waiting to be applied to the base execution service.
    layers: L,
}

impl StedaBuilder<Identity> {
    /// Create a builder with no execution middleware.
    const fn new(pool: PgPool) -> Self {
        Self { pool, layers: Identity::new() }
    }
}

impl<L> StedaBuilder<L> {
    /// Add a Tower layer around registered task execution.
    ///
    /// Layers run inside Steda's durable execution envelope. Errors and panics
    /// therefore flow through the normal `PostgreSQL` failure transitions rather
    /// than becoming a second source of durable state.
    #[must_use]
    pub fn layer<T>(self, layer: T) -> StedaBuilder<Stack<L, T>> {
        StedaBuilder { pool: self.pool, layers: Stack::new(self.layers, layer) }
    }

    /// Build the root Steda handle and freeze the execution layer stack.
    #[must_use]
    pub fn build(self) -> Steda
    where
        L: Layer<ExecutionService>,
        L::Service:
            Service<ExecutionRequest, Response = ExecutionResponse, Error = Error> + Send + 'static,
        <L::Service as Service<ExecutionRequest>>::Future: Send + 'static,
    {
        let execution = SharedExecutionService::new(self.layers.layer(ExecutionService));
        Steda { pool: self.pool, execution }
    }
}
