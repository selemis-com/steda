//! Shared setup for the runnable examples.
//!
//! Production applications normally own schema installation and process supervision separately;
//! these helpers intentionally keep each example self-contained and finite.

use std::{
    env, process,
    time::{SystemTime, UNIX_EPOCH},
};

use steda::{Error, Result, Steda, Worker};
use tokio::{sync::oneshot, task::JoinHandle};

/// Connect to `PostgreSQL` and apply the bundled Steda schema for an example run.
///
/// # Errors
///
/// Returns an error when `DATABASE_URL` is absent, `PostgreSQL` cannot be reached,
/// or the bundled Steda schema cannot be applied.
pub(super) async fn connect() -> Result<Steda> {
    let database_url = env::var("DATABASE_URL").map_err(|_| {
        Error::Other(
            "DATABASE_URL is not set; point it at a PostgreSQL database before running this example"
                .to_owned(),
        )
    })?;
    let steda = Steda::connect(&database_url).await?;
    sqlx::raw_sql(include_str!("../../sql/steda.sql")).execute(steda.pool()).await?;
    Ok(steda)
}

/// Create a process-and-time-scoped identifier for repeatable example runs.
///
/// # Errors
///
/// Returns an error if the system clock is before the Unix epoch.
#[allow(
    dead_code,
    clippy::allow_attributes,
    reason = "shared example helper is not used by every example crate"
)]
pub(super) fn unique_key(prefix: &str) -> Result<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Other(format!("system clock is before UNIX_EPOCH: {error}")))?
        .as_nanos();
    Ok(format!("{prefix}:{}:{nanos}", process::id()))
}

/// Background worker with an explicit shutdown handle for finite examples.
pub(super) struct RunningWorker {
    /// Signal used to stop claiming new work.
    shutdown: Option<oneshot::Sender<()>>,
    /// Tokio task running the worker loop.
    task: JoinHandle<Result<()>>,
}

impl RunningWorker {
    /// Start a worker in the background.
    pub(super) fn start(worker: Worker) -> Self {
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            worker
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self { shutdown: Some(shutdown), task }
    }

    /// Request shutdown and wait for the worker to drain.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker fails or its Tokio task cannot be joined.
    pub(super) async fn stop(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .map_err(|error| Error::Other(format!("worker task failed to join: {error}")))?
    }
}
