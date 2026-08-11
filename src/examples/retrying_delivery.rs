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
    delivered_on_attempt: i32,
}

/// Durable task contract for remote document delivery.
struct DeliverDocument;

impl Task for DeliverDocument {
    const NAME: &'static str = "deliver-document";
    type Input = DeliverDocumentInput;
    type Output = DeliveryReceipt;
}

/// Run the retry-policy example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-retries")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task::<DeliverDocument>(async |input: DeliverDocumentInput, ctx: TaskContext| {
            println!("attempt {} for {}", ctx.attempt(), input.document_id);

            if ctx.attempt() < 3 {
                return Err(Error::Other("remote service temporarily unavailable".to_owned()));
            }

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
        .spawn::<DeliverDocument>(DeliverDocumentInput {
            document_id: common::unique_key("statement")?,
            destination: "archive@example.invalid".to_owned(),
        })
        .max_attempts(3)
        .retry_strategy(RetryStrategy::fixed(0.25))
        .await?;

    let receipt = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!(
        "{} delivered to {} on attempt {}",
        receipt.document_id, receipt.destination, receipt.delivered_on_attempt
    );

    worker.stop().await?;
    Ok(())
}
