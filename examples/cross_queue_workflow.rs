//! Coordinate a parent task with child work in another queue.
//!
//! The parent checkpoints a typed `TaskRef` to the child, then deliberately fails.
//! On retry, the checkpoint replays that same child reference, so the parent can await
//! the exact task it already created instead of spawning another one. The child spawn
//! also uses an idempotency key to cover the smaller crash window between creating the
//! child and committing the parent checkpoint.
//!
//! Separate queues are intentional: Steda rejects same-queue waits because a finite
//! worker pool could otherwise deadlock with every slot occupied by parents waiting
//! for children that need those same slots.

/// Shared setup and finite-worker helpers.
mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Error, Queue, Result, RetryStrategy, Step, Task, TaskContext, TaskRef};

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

/// Child task definition.
const EMAIL_RECEIPT: Task<EmailReceiptInput, EmailReceiptOutput> = Task::new("email-receipt");

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

/// Parent task definition.
const COMPLETE_ORDER: Task<CompleteOrderInput, CompleteOrderOutput> = Task::new("complete-order");

/// Checkpoint used for child task creation.
const SPAWN_RECEIPT: Step<TaskRef<EmailReceiptInput, EmailReceiptOutput>> =
    Step::new("spawn-receipt");

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
    let child_idempotency_key = format!("receipt:{}", ctx.task_id());
    // Checkpoint child creation so a parent retry cannot create a second logical child.
    let child = ctx
        .step(SPAWN_RECEIPT, async move || {
            let child = receipt_queue
                .spawn(
                    EMAIL_RECEIPT,
                    EmailReceiptInput { order_id: child_order_id.clone(), address: email_address },
                )
                .idempotency_key(child_idempotency_key)
                .await?;
            Ok(child.task_ref())
        })
        .await?;

    // Simulate a transient parent failure after the child reference is durable.
    if ctx.attempt() == 1 {
        println!("parent attempt 1 checkpointed the child task, then simulated a restart");
        return Err(Error::Other("order worker restarted after spawning receipt".to_owned()));
    }

    // The retry recovered the same typed child reference from the checkpoint above.
    println!("parent attempt 2 reused the checkpointed child task");
    let receipt = ctx.await_task(&child).timeout(Duration::from_secs(10)).await?;

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

    let receipt_runs = Arc::new(AtomicUsize::new(0));
    let worker_receipt_runs = Arc::clone(&receipt_runs);
    let email_worker = email
        .worker()
        .task(EMAIL_RECEIPT, move |input: EmailReceiptInput, _ctx: TaskContext| {
            let worker_receipt_runs = Arc::clone(&worker_receipt_runs);
            async move {
                worker_receipt_runs.fetch_add(1, Ordering::SeqCst);
                println!("sending one receipt for {} to {}", input.order_id, input.address);
                Ok(EmailReceiptOutput { message_id: "MSG-1001".to_owned() })
            }
        })
        .build()?;
    let email_worker = RunningWorker::start(email_worker);

    let child_queue = email.clone();
    let orders_worker = orders
        .worker()
        .task(COMPLETE_ORDER, move |input, ctx| complete_order(input, ctx, child_queue.clone()))
        .build()?;
    let orders_worker = RunningWorker::start(orders_worker);

    let task = orders
        .spawn(
            COMPLETE_ORDER,
            CompleteOrderInput {
                order_id: "ORD-1001".to_owned(),
                email: "buyer@example.invalid".to_owned(),
            },
        )
        .max_attempts(2)
        .retry_strategy(RetryStrategy::fixed(Duration::from_millis(250)))
        .await?;

    let completed = task.result_with_timeout(Duration::from_secs(15)).await?;
    assert_eq!(receipt_runs.load(Ordering::SeqCst), 1);
    println!("order {} completed", completed.order_id);
    println!("receipt: {}", completed.receipt_message_id);
    println!("receipt task executions: {}", receipt_runs.load(Ordering::SeqCst));

    orders_worker.stop().await?;
    email_worker.stop().await?;
    Ok(())
}
