//! Worker-driving support for integration tests that execute tasks.

use std::time::Duration;

use steda::{Error, Result, Worker, metrics::QueueMetrics};
use tokio::{task::yield_now, time::timeout};

/// Runs a worker until the supplied observable test condition becomes true.
async fn run_worker_until(
    worker: &Worker,
    condition: impl Future<Output = ()> + Send,
) -> Result<()> {
    timeout(Duration::from_secs(5), worker.run_until(condition))
        .await
        .map_err(|_| Error::Timeout("worker did not reach test condition".to_owned()))?
}

/// Runs a worker until it has claimed `count` additional runs, then lets normal shutdown drain
/// them.
pub(crate) async fn run_worker_for_claims(
    worker: &Worker,
    metrics: QueueMetrics,
    count: u64,
) -> Result<()> {
    let start = metrics.claimed_runs();
    let target = start.saturating_add(count);
    run_worker_until(worker, async move {
        while metrics.claimed_runs() < target {
            yield_now().await;
        }
    })
    .await
}
