//! Smallest complete typed producer/worker flow.
//!
//! The example defines one `Task`, registers an in-process async handler, spawns a
//! logical task, and awaits its typed result. It intentionally avoids retries and
//! checkpoints so the basic relationship between `Task`, `Queue`, `Worker`, and
//! `TaskHandle` stays visible.

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

/// Durable task contract for rendering an invoice.
struct RenderInvoice;

impl Task for RenderInvoice {
    const NAME: &'static str = "render-invoice";
    type Input = RenderInvoiceInput;
    type Output = RenderInvoiceOutput;
}

/// Run the basic producer/worker example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-basic")?;
    queue.create().await?;

    // Registration is a worker capability declaration; producers do not need this registry.
    let worker = queue
        .worker()
        .task::<RenderInvoice>(async |input: RenderInvoiceInput, _ctx: TaskContext| {
            Ok(RenderInvoiceOutput {
                invoice_number: input.invoice_number,
                total_cents: input.subtotal_cents + input.tax_cents,
            })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    // `spawn::<RenderInvoice>` preserves the task type in the returned handle, including result
    // decoding.
    let task = queue
        .spawn::<RenderInvoice>(RenderInvoiceInput {
            invoice_number: common::unique_key("INV")?,
            subtotal_cents: 12_500,
            tax_cents: 2_625,
        })
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
