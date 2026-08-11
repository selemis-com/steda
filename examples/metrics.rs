//! Observe Steda's exporter-agnostic queue metrics after successful and failed work.
//!
//! Metrics are process-local counters attached to a `Queue` handle lineage; they are not a
//! replacement for `PostgreSQL`'s durable task state. A cloned `QueueMetrics` handle observes the
//! same counters as the queue and workers created from it, which makes it suitable for polling
//! from a Prometheus, OpenTelemetry, or application-specific exporter.
//!
//! This example executes one successful attempt and one terminally failed attempt, then prints the
//! resulting counters after the worker has drained.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Error, Result, RetryStrategy, Task, TaskContext};

/// Input used to choose whether the demonstration task succeeds.
#[derive(Debug, Deserialize, Serialize)]
struct MetricProbeInput {
    /// Human-readable label printed by the successful task.
    label: String,
    /// Whether this attempt should return an application error.
    should_fail: bool,
}

/// Output produced by a successful metrics probe.
#[derive(Debug, Deserialize, Serialize)]
struct MetricProbeOutput {
    /// Label copied from the input after successful execution.
    label: String,
}

/// Task definition used to generate bounded execution outcomes.
const METRIC_PROBE: Task<MetricProbeInput, MetricProbeOutput> = Task::new("metric-probe");

/// Run the queue-metrics example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-metrics")?;
    queue.create().await?;

    // Retain the clone before starting the worker, as an exporter normally would. The worker and
    // this handle share the queue lineage's atomics; creating a separate `steda.queue(...)` handle
    // for the same database queue would intentionally create a separate process-local metric set.
    let metrics = queue.metrics();

    let worker = queue
        .worker()
        .task(METRIC_PROBE, async |input: MetricProbeInput, _ctx: TaskContext| {
            if input.should_fail {
                return Err(Error::Other("simulated application failure".to_owned()));
            }
            Ok(MetricProbeOutput { label: input.label })
        })
        .build()?;
    // `Worker::metrics` returns another clone of the same lineage, which can be handed to a
    // component that only owns the worker side of the application.
    let worker_metrics = worker.metrics();
    let worker = RunningWorker::start(worker);

    let successful = queue
        .spawn(
            METRIC_PROBE,
            MetricProbeInput { label: "successful attempt".to_owned(), should_fail: false },
        )
        .await?;
    let failing = queue
        .spawn(
            METRIC_PROBE,
            MetricProbeInput { label: "failed attempt".to_owned(), should_fail: true },
        )
        .max_attempts(1)
        .retry_strategy(RetryStrategy::none())
        .await?;

    let output = successful.result_with_timeout(Duration::from_secs(10)).await?;
    println!("completed: {}", output.label);

    match failing.result_with_timeout(Duration::from_secs(10)).await {
        Err(Error::TaskFailed { failure }) => println!("failed as expected: {failure}"),
        Err(error) => return Err(error),
        Ok(output) => {
            return Err(Error::Other(format!("expected failure, got success: {}", output.label)));
        }
    }

    // Stop before reading the final snapshot so no additional task execution can change it.
    worker.stop().await?;

    println!("claimed runs: {}", metrics.claimed_runs());
    println!("claim errors: {}", metrics.claim_errors());
    println!("executions: {}", metrics.executions());
    println!("worker view of executions: {}", worker_metrics.executions());
    println!("completed executions: {}", metrics.completed_executions());
    println!("failed executions: {}", metrics.failed_executions());
    println!("lease-lost executions: {}", metrics.lease_lost_executions());
    println!("cancelled executions: {}", metrics.cancelled_executions());
    println!("suspended executions: {}", metrics.suspended_executions());
    println!("unhandled executions: {}", metrics.unhandled_executions());
    println!("cumulative execution time: {} ns", metrics.execution_duration_nanoseconds());

    Ok(())
}
