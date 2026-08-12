//! Retry a transient failure with an explicit bounded retry policy.
//!
//! The simulated remote delivery fails twice and succeeds on the third attempt. The
//! handler reads `TaskContext::attempt` only to make the demonstration deterministic;
//! production handlers should normally classify real errors and let the configured
//! Steda retry policy decide whether another persisted attempt is available.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Error, Result, RetryStrategy, Task, TaskContext};

/// Input for a remote document delivery.
#[derive(Debug, Deserialize, Serialize)]
struct DeliverDocumentInput {
    /// Document to deliver.
    document_id: String,
    /// Remote delivery destination.
    destination: String,
}

/// Receipt returned after a successful delivery.
#[derive(Debug, Deserialize, Serialize)]
struct DeliveryReceipt {
    /// Document to deliver.
    document_id: String,
    /// Remote delivery destination.
    destination: String,
    /// Attempt number that finally succeeded.
    delivered_on_attempt: u32,
}

/// Task definition for remote document delivery.
const DELIVER_DOCUMENT: Task<DeliverDocumentInput, DeliveryReceipt> = Task::new("deliver-document");

/// Run the retry-policy example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-retries")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task(DELIVER_DOCUMENT, async |input: DeliverDocumentInput, ctx: TaskContext| {
            if ctx.attempt() < 3 {
                println!("delivery attempt {}: remote service unavailable", ctx.attempt());
                return Err(Error::Other("remote service temporarily unavailable".to_owned()));
            }

            println!("delivery attempt {}: succeeded", ctx.attempt());

            Ok(DeliveryReceipt {
                document_id: input.document_id,
                destination: input.destination,
                delivered_on_attempt: ctx.attempt(),
            })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    // The attempt budget includes the first execution, so this permits exactly three tries.
    let task = queue
        .spawn(
            DELIVER_DOCUMENT,
            DeliverDocumentInput {
                document_id: "STATEMENT-1001".to_owned(),
                destination: "archive@example.invalid".to_owned(),
            },
        )
        .max_attempts(3)
        .retry_strategy(RetryStrategy::fixed(Duration::from_millis(250)))
        .await?;

    let receipt = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!("{} delivered to {}", receipt.document_id, receipt.destination);
    println!("completed on attempt {}", receipt.delivered_on_attempt);

    worker.stop().await?;
    Ok(())
}
