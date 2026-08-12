//! Crate error and result types.

use thiserror::Error;

use crate::types::TaskId;

/// Result type used by the queue crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the queue handle, worker, and task context.
#[derive(Error, Debug)]
pub enum Error {
    /// Migration failed.
    #[error("Steda PostgreSQL migration error: {source}")]
    Migrate {
        /// Underlying migration failure.
        #[source]
        source: sqlx::migrate::MigrateError,
    },

    /// Error returned by `SQLx` while talking to Postgres.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Invalid queue or worker options.
    #[error("{0}")]
    InvalidOptions(String),

    /// Queue name was omitted.
    #[error("queue name must be provided")]
    MissingQueueName,

    /// Queue name exceeds `PostgreSQL` identifier-derived byte limit.
    #[error("queue name {name:?} is too long (max {max} bytes)")]
    QueueNameTooLong {
        /// Provided queue name.
        name: String,

        /// Maximum allowed UTF-8 byte length.
        max: usize,
    },

    /// Task was not found in storage.
    #[error("task {0} not found")]
    TaskNotFound(TaskId),

    /// A durable task reference names a different task definition than the persisted task.
    #[error("task {task_id} is persisted as {actual:?}, but the task reference names {expected:?}")]
    TaskNameMismatch {
        /// Logical task identifier.
        task_id: TaskId,
        /// Task name carried by the durable reference.
        expected: String,
        /// Task name stored with the logical task.
        actual: String,
    },

    /// Task reached a terminal failed state.
    #[error("task failed: {failure}")]
    TaskFailed {
        /// Persisted task failure payload.
        failure: serde_json::Value,
    },

    /// Task intentionally suspended itself while sleeping.
    #[error("Task suspended")]
    Suspended,

    /// Task or run was cancelled.
    #[error("Task cancelled")]
    Cancelled,

    /// Run had already failed when attempting a state transition.
    #[error("task already failed")]
    FailedRun,

    /// The worker no longer owns the run because its finite lease expired.
    #[error("task lease lost")]
    LeaseLost,

    /// An idempotency key was reused for a different spawn request.
    #[error("idempotency key conflicts with an existing task request")]
    IdempotencyConflict,

    /// Generic timeout.
    #[error("{0}")]
    Timeout(String),

    /// Headers in storage were not a JSON object.
    #[error("invalid task headers: {0}")]
    InvalidTaskHeaders(String),

    /// JSON serialization or deserialization failed.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Catch-all error for cases that do not deserve a dedicated variant.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Returns true when the error represents a deliberate task suspension.
    pub const fn is_suspended(&self) -> bool {
        matches!(self, Self::Suspended)
    }

    /// Returns true when the error represents task cancellation.
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Returns true when the error represents an already failed run.
    pub const fn is_failed_run(&self) -> bool {
        matches!(self, Self::FailedRun)
    }

    /// Returns true when the current worker has lost its finite run lease.
    pub const fn is_lease_lost(&self) -> bool {
        matches!(self, Self::LeaseLost)
    }
}

/// Maps Steda queue SQLSTATE errors into typed Rust errors.
pub(crate) fn map_sqlx_error(e: sqlx::Error) -> Error {
    if let sqlx::Error::Database(db_err) = &e {
        match db_err.code().as_deref() {
            // Task cancelled.
            Some("ST001") => return Error::Cancelled,

            // Run already failed.
            Some("ST002") => return Error::FailedRun,

            // Finite run lease expired.
            Some("ST003") => return Error::LeaseLost,

            // Idempotency key reused for a different request.
            Some("ST004") => return Error::IdempotencyConflict,

            _ => {}
        }
    }

    Error::Database(e)
}
