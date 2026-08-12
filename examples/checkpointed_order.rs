//! Preserve successful workflow steps across task retries.
//!
//! The first shipping attempt commits inventory and payment checkpoints and then
//! fails. The retry starts the handler again but replays those committed values, so
//! the step bodies execute only once. The counters make that behavior observable.
//! Real external inventory/payment systems still need their own idempotency or
//! fencing because a crash can occur between an external side effect and checkpoint commit.

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
use steda::{Error, Result, RetryStrategy, Step, Task, TaskContext};

/// Input for a small order-shipping workflow.
#[derive(Debug, Deserialize, Serialize)]
struct ShipOrderInput {
    /// Order being shipped.
    order_id: String,
    /// Amount charged for the order.
    amount_cents: u64,
}

/// Result assembled from checkpointed workflow steps.
#[derive(Debug, Deserialize, Serialize)]
struct Shipment {
    /// Order being shipped.
    order_id: String,
    /// Durable inventory reservation identifier.
    reservation_id: String,
    /// Durable payment charge identifier.
    payment_id: String,
}

/// Task definition for the shipping workflow.
const SHIP_ORDER: Task<ShipOrderInput, Shipment> = Task::new("ship-order");

/// Inventory reservation checkpoint.
const RESERVE_INVENTORY: Step<String> = Step::new("reserve-inventory");

/// Payment charge checkpoint.
const CHARGE_PAYMENT: Step<String> = Step::new("charge-payment");

/// Executes one shipment attempt using cloned durable state handles.
async fn ship_order(
    input: ShipOrderInput,
    ctx: TaskContext,
    reservation_calls: Arc<AtomicUsize>,
    charge_calls: Arc<AtomicUsize>,
) -> Result<Shipment> {
    // The named step stores this value once and replays it on later attempts.
    let reservation_id = ctx
        .step(RESERVE_INVENTORY, async || {
            reservation_calls.fetch_add(1, Ordering::SeqCst);
            println!("reserving inventory for {}", input.order_id);
            Ok("RES-1001".to_owned())
        })
        .await?;

    let payment_id = ctx
        .step(CHARGE_PAYMENT, async || {
            charge_calls.fetch_add(1, Ordering::SeqCst);
            println!(
                "capturing payment of €{}.{:02}",
                input.amount_cents / 100,
                input.amount_cents % 100
            );
            Ok("PAY-1001".to_owned())
        })
        .await?;

    // Fail only after both checkpoints commit so the retry must replay them.
    if ctx.attempt() == 1 {
        println!("shipping attempt 1 failed after both checkpoints committed");
        return Err(Error::Other("label printer temporarily unavailable".to_owned()));
    }

    println!("shipping attempt 2 replayed reservation and payment checkpoints");

    Ok(Shipment { order_id: input.order_id, reservation_id, payment_id })
}

/// Run the checkpoint replay example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-checkpoints")?;
    queue.create().await?;

    let reservations = Arc::new(AtomicUsize::new(0));
    let charges = Arc::new(AtomicUsize::new(0));
    let reservation_calls = Arc::clone(&reservations);
    let charge_calls = Arc::clone(&charges);

    let worker = queue
        .worker()
        .task(SHIP_ORDER, move |input, ctx| {
            ship_order(input, ctx, Arc::clone(&reservation_calls), Arc::clone(&charge_calls))
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn(SHIP_ORDER, ShipOrderInput { order_id: "ORD-1001".to_owned(), amount_cents: 14_950 })
        .max_attempts(2)
        .retry_strategy(RetryStrategy::fixed(Duration::from_millis(250)))
        .await?;

    let shipment = task.result_with_timeout(Duration::from_secs(10)).await?;
    // Both handlers ran twice, but each checkpoint body ran exactly once in this logical task.
    assert_eq!(reservations.load(Ordering::SeqCst), 1);
    assert_eq!(charges.load(Ordering::SeqCst), 1);

    println!("order {} shipped", shipment.order_id);
    println!("  reservation: {}", shipment.reservation_id);
    println!("  payment: {}", shipment.payment_id);

    worker.stop().await?;
    Ok(())
}
