//! Explicit and deadline-driven cancellation of running tasks.
//!
//! The first task is cancelled explicitly after its handler starts. The second uses a durable
//! `max_duration` policy and is cancelled automatically after its execution budget expires. Both
//! end in the same terminal cancelled state, and neither handler is allowed to complete later.

/// Shared setup and finite-worker helpers.
mod common;

use std::{future, sync::Arc, time::Duration};

use common::RunningWorker;
use steda::{CancellationPolicy, Error, Result, Task, TaskContext};
use tokio::sync::Notify;

/// Task cancelled explicitly by its caller.
const MANUAL_CANCELLATION: Task<(), ()> = Task::new("manual-cancellation");

/// Task cancelled automatically by its durable execution deadline.
const DEADLINE_CANCELLATION: Task<(), ()> = Task::new("deadline-cancellation");

/// Run the cancellation example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-cancellation")?;
    queue.create().await?;

    let manual_started = Arc::new(Notify::new());
    let worker = queue
        .worker()
        .task(MANUAL_CANCELLATION, {
            let manual_started = Arc::clone(&manual_started);
            move |(), _ctx: TaskContext| {
                let manual_started = Arc::clone(&manual_started);
                async move {
                    println!("manual task started");
                    manual_started.notify_one();
                    future::pending::<Result<()>>().await
                }
            }
        })
        .task(DEADLINE_CANCELLATION, async |(), _ctx: TaskContext| {
            println!("deadline-limited task started");
            future::pending::<Result<()>>().await
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let manual = queue.spawn(MANUAL_CANCELLATION, ()).await?;
    tokio::time::timeout(Duration::from_secs(5), manual_started.notified())
        .await
        .map_err(|_| Error::Timeout("timed out waiting for manual task to start".to_owned()))?;

    manual.cancel().await?;
    match manual.result_with_timeout(Duration::from_secs(5)).await {
        Err(Error::Cancelled) => println!("manual task cancelled explicitly"),
        Err(error) => return Err(error),
        Ok(()) => return Err(Error::Other("cancelled task unexpectedly completed".to_owned())),
    }

    let deadline = queue
        .spawn(DEADLINE_CANCELLATION, ())
        .cancellation(CancellationPolicy::new().max_duration(Duration::from_secs(1)))
        .await?;

    match deadline.result_with_timeout(Duration::from_secs(5)).await {
        Err(Error::Cancelled) => println!("deadline-limited task cancelled automatically"),
        Err(error) => return Err(error),
        Ok(()) => {
            return Err(Error::Other("deadline-limited task unexpectedly completed".to_owned()));
        }
    }

    worker.stop().await?;
    Ok(())
}
