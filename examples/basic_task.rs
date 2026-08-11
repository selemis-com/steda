//! Smallest complete typed producer/worker flow.
//!
//! The example defines one `Task`, registers an in-process async handler, spawns a
//! logical task, and awaits its typed result. It intentionally avoids retries and
//! checkpoints so the basic relationship between `Task`, `Queue`, `Worker`, and
//! `SpawnedTask` and its typed result stay visible.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Result, Task, TaskContext};

/// Input accepted by the invoice renderer.
#[derive(Debug, Deserialize, Serialize)]
struct RenderInvoiceInput {
    /// Human-facing invoice identifier.
    invoice_number: String,
    /// Pre-tax subtotal in cents.
    subtotal_cents: u64,
    /// Tax amount in cents.
    tax_cents: u64,
}

/// Typed result returned by the task.
#[derive(Debug, Deserialize, Serialize)]
struct RenderInvoiceOutput {
    /// Human-facing invoice identifier.
    invoice_number: String,
    /// Final total in cents.
    total_cents: u64,
}

/// Task definition for rendering an invoice.
const RENDER_INVOICE: Task<RenderInvoiceInput, RenderInvoiceOutput> = Task::new("render-invoice");

/// Run the basic producer/worker example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-basic")?;
    queue.create().await?;

    // Register the handler this worker can execute.
    let worker = queue
        .worker()
        .task(RENDER_INVOICE, async |input: RenderInvoiceInput, _ctx: TaskContext| {
            Ok(RenderInvoiceOutput {
                invoice_number: input.invoice_number,
                total_cents: input.subtotal_cents + input.tax_cents,
            })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    // The task definition fixes both the accepted input and decoded result type.
    let task = queue
        .spawn(
            RENDER_INVOICE,
            RenderInvoiceInput {
                invoice_number: common::unique_key("INV")?,
                subtotal_cents: 12_500,
                tax_cents: 2_625,
            },
        )
        .await?;

    let invoice = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!(
        "{} rendered with total €{}.{:02}",
        invoice.invoice_number,
        invoice.total_cents / 100,
        invoice.total_cents % 100
    );

    worker.stop().await?;
    Ok(())
}
