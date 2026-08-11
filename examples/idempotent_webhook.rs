//! Deduplicate repeated external delivery at task submission time.
//!
//! Two copies of the same simulated payment webhook use one queue-scoped idempotency
//! key and resolve to the same logical Steda task. This protects task creation only:
//! side effects performed by the handler may still require idempotency at the remote
//! system.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Result, Task, TaskContext};

/// Payload received from a payment provider webhook.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct PaymentCaptured {
    /// Provider payment identifier.
    payment_id: String,
    /// Order associated with the payment.
    order_id: String,
    /// Captured amount in cents.
    amount_cents: u64,
}

/// Result of accepting the payment for fulfillment.
#[derive(Debug, Deserialize, Serialize)]
struct Fulfillment {
    /// Order associated with the payment.
    order_id: String,
    /// Payment identifier accepted by fulfillment.
    accepted_payment: String,
}

/// Task definition for order fulfillment.
const FULFILL_ORDER: Task<PaymentCaptured, Fulfillment> = Task::new("fulfill-order");

/// Run the idempotent webhook example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-webhooks")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task(FULFILL_ORDER, async |payment: PaymentCaptured, _ctx: TaskContext| {
            println!(
                "fulfilling {} after payment {} for {} cents",
                payment.order_id, payment.payment_id, payment.amount_cents
            );
            Ok(Fulfillment { order_id: payment.order_id, accepted_payment: payment.payment_id })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let payment_id = common::unique_key("pay")?;
    let webhook = PaymentCaptured {
        payment_id: payment_id.clone(),
        order_id: common::unique_key("order")?,
        amount_cents: 8_990,
    };
    let idempotency_key = format!("payment-captured:{payment_id}");

    // The same key and same original request resolve to one logical task ID.
    let first =
        queue.spawn(FULFILL_ORDER, webhook.clone()).idempotency_key(&idempotency_key).await?;
    let duplicate = queue.spawn(FULFILL_ORDER, webhook).idempotency_key(&idempotency_key).await?;

    assert!(first.created());
    assert!(!duplicate.created());
    assert_eq!(first.task_id(), duplicate.task_id());

    let fulfillment = first.result_with_timeout(Duration::from_secs(10)).await?;
    println!(
        "{} and payment {} resolved to one logical fulfillment task",
        fulfillment.order_id, fulfillment.accepted_payment
    );

    worker.stop().await?;
    Ok(())
}
