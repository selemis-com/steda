//! Coordinate a parent task with child work in another queue.
//!
//! The parent checkpoints child creation, waits for the child's terminal result,
//! and checkpoints that wait. Separate queues are intentional: Steda rejects
//! same-queue waits because a finite worker pool could otherwise deadlock with every
//! slot occupied by parents waiting for children that need those same slots.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{AwaitTaskResultOptions, Error, Queue, Result, Task, TaskContext, TaskResultSnapshot};

/// Input for the child email task.
#[derive(Debug, Deserialize, Serialize)]
struct EmailReceiptInput {
    /// Order whose receipt is being sent.
    order_id: String,
    /// Destination email address.
    address: String,
}

/// Result returned by the child email task.
#[derive(Debug, Deserialize, Serialize)]
struct EmailReceiptOutput {
    /// Identifier assigned to the sent message.
    message_id: String,
}

/// Durable child task contract.
struct EmailReceipt;

impl Task for EmailReceipt {
    const NAME: &'static str = "email-receipt";
    type Input = EmailReceiptInput;
    type Output = EmailReceiptOutput;
}

/// Input for the parent order task.
#[derive(Debug, Deserialize, Serialize)]
struct CompleteOrderInput {
    /// Order whose receipt is being sent.
    order_id: String,
    /// Address that should receive the receipt.
    email: String,
}

/// Parent result after the child task completes.
#[derive(Debug, Deserialize, Serialize)]
struct CompleteOrderOutput {
    /// Order whose receipt is being sent.
    order_id: String,
    /// Message identifier returned by the child task.
    receipt_message_id: String,
}

/// Durable parent task contract.
struct CompleteOrder;

impl Task for CompleteOrder {
    const NAME: &'static str = "complete-order";
    type Input = CompleteOrderInput;
    type Output = CompleteOrderOutput;
}

/// Completes one order workflow attempt and waits for the receipt task.
async fn complete_order(
    input: CompleteOrderInput,
    ctx: TaskContext,
    child_queue: Queue,
) -> Result<CompleteOrderOutput> {
    let order_id = input.order_id.clone();
    let email_address = input.email.clone();
    let receipt_queue = child_queue.clone();
    let child_order_id = order_id.clone();
    // Checkpoint child creation so a parent retry cannot create a second logical child.
    let child_id = ctx
        .step("spawn-receipt", async move || {
            let child = receipt_queue
                .spawn::<EmailReceipt>(EmailReceiptInput {
                    order_id: child_order_id.clone(),
                    address: email_address,
                })
                .idempotency_key(format!("receipt:{child_order_id}"))
                .await?;
            Ok(child.id())
        })
        .await?;

    // The completed child result is itself checkpointed under `wait-for-receipt`.
    let snapshot = ctx
        .await_task_result(
            child_id,
            AwaitTaskResultOptions::new(child_queue.name())
                .step_name("wait-for-receipt")
                .timeout(Duration::from_secs(10)),
        )
        .await?;

    let receipt: EmailReceiptOutput = match &snapshot {
        TaskResultSnapshot::Completed { .. } => snapshot
            .result()?
            .ok_or_else(|| Error::Other("completed child had no result".to_owned()))?,
        TaskResultSnapshot::Failed { failure } => {
            return Err(Error::TaskFailed { failure: failure.clone() });
        }
        TaskResultSnapshot::Cancelled => return Err(Error::Cancelled),
        _ => {
            return Err(Error::Other("cross-queue wait returned a non-terminal result".to_owned()));
        }
    };

    Ok(CompleteOrderOutput { order_id: input.order_id, receipt_message_id: receipt.message_id })
}

/// Run the cross-queue dependency example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let orders = steda.queue("example-orders")?;
    let email = steda.queue("example-email")?;
    orders.create().await?;
    email.create().await?;

    let email_worker = email
        .worker()
        .task::<EmailReceipt>(async |input: EmailReceiptInput, _ctx: TaskContext| {
            println!("sending receipt for {} to {}", input.order_id, input.address);
            Ok(EmailReceiptOutput { message_id: format!("msg:{}", input.order_id) })
        })
        .build()?;
    let email_worker = RunningWorker::start(email_worker);

    let child_queue = email.clone();
    let orders_worker = orders
        .worker()
        .task::<CompleteOrder>(move |input, ctx| complete_order(input, ctx, child_queue.clone()))
        .build()?;
    let orders_worker = RunningWorker::start(orders_worker);

    let order_id = common::unique_key("order")?;
    let task = orders
        .spawn::<CompleteOrder>(CompleteOrderInput {
            order_id,
            email: "buyer@example.invalid".to_owned(),
        })
        .await?;

    let completed = task.result_with_timeout(Duration::from_secs(15)).await?;
    println!("{} completed after receipt {}", completed.order_id, completed.receipt_message_id);

    orders_worker.stop().await?;
    email_worker.stop().await?;
    Ok(())
}
