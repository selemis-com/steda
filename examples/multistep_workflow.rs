//! Straightforward task composed from several durable typed steps.
//!
//! Each step has a stable name and a typed output. If the task is retried later,
//! Steda can reuse a completed step instead of executing that step body again.
//! This example stays on the happy path so the normal multi-step workflow shape
//! remains easy to see.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Result, Step, Task, TaskContext};

/// Input for one order fulfillment workflow.
#[derive(Debug, Deserialize, Serialize)]
struct FulfillOrderInput {
    /// Order being fulfilled.
    order_id: String,
    /// Amount charged for the order.
    amount_cents: u64,
}

/// Inventory reserved for the order.
#[derive(Debug, Deserialize, Serialize)]
struct Reservation {
    /// Reservation created by the inventory system.
    reservation_id: String,
}

/// Payment captured for the order.
#[derive(Debug, Deserialize, Serialize)]
struct Payment {
    /// Payment created by the payment system.
    payment_id: String,
}

/// Shipment created for the order.
#[derive(Debug, Deserialize, Serialize)]
struct Shipment {
    /// Tracking number assigned to the shipment.
    tracking_number: String,
}

/// Final result of the fulfillment workflow.
#[derive(Debug, Deserialize, Serialize)]
struct FulfillOrderOutput {
    /// Order that was fulfilled.
    order_id: String,
    /// Inventory reservation used by the order.
    reservation_id: String,
    /// Payment used by the order.
    payment_id: String,
    /// Tracking number for the shipment.
    tracking_number: String,
}

/// Task definition for order fulfillment.
const FULFILL_ORDER: Task<FulfillOrderInput, FulfillOrderOutput> = Task::new("fulfill-order");

/// Reserve inventory for the order.
const RESERVE_INVENTORY: Step<Reservation> = Step::new("reserve-inventory");

/// Capture payment after inventory has been reserved.
const CAPTURE_PAYMENT: Step<Payment> = Step::new("capture-payment");

/// Create the shipment after inventory and payment are ready.
const CREATE_SHIPMENT: Step<Shipment> = Step::new("create-shipment");

/// Fulfill one order as three durable steps.
async fn fulfill_order(input: FulfillOrderInput, ctx: TaskContext) -> Result<FulfillOrderOutput> {
    let reservation = ctx
        .step(RESERVE_INVENTORY, async || {
            Ok(Reservation { reservation_id: format!("reservation:{}", input.order_id) })
        })
        .await?;

    let payment = ctx
        .step(CAPTURE_PAYMENT, async || {
            Ok(Payment {
                payment_id: format!(
                    "payment:{}:{}:{}",
                    input.order_id, reservation.reservation_id, input.amount_cents
                ),
            })
        })
        .await?;

    let shipment = ctx
        .step(CREATE_SHIPMENT, async || {
            Ok(Shipment {
                tracking_number: format!(
                    "tracking:{}:{}",
                    reservation.reservation_id, payment.payment_id
                ),
            })
        })
        .await?;

    Ok(FulfillOrderOutput {
        order_id: input.order_id,
        reservation_id: reservation.reservation_id,
        payment_id: payment.payment_id,
        tracking_number: shipment.tracking_number,
    })
}

/// Run the multi-step workflow example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-multistep")?;
    queue.create().await?;

    let worker = queue.worker().task(FULFILL_ORDER, fulfill_order).build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn(
            FULFILL_ORDER,
            FulfillOrderInput { order_id: common::unique_key("order")?, amount_cents: 14_950 },
        )
        .await?;

    let result = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!(
        "{} fulfilled with {}, {}, and {}",
        result.order_id, result.reservation_id, result.payment_id, result.tracking_number
    );

    worker.stop().await?;
    Ok(())
}
