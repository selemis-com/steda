//! Suspend a logical task without keeping a worker claim alive.
//!
//! `TaskContext::sleep_for` persists a wake time and suspends the current run. When
//! the task becomes runnable again, a worker invokes the handler from the beginning;
//! the same `Sleep` identity replays and execution continues past it. No Rust future or stack
//! frame survives the delay.

/// Shared setup and finite-worker helpers.
mod common;

use std::time::Duration;

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Result, Sleep, Step, Task, TaskContext};

/// Input for a delayed trial reminder.
#[derive(Debug, Deserialize, Serialize)]
struct SendTrialReminderInput {
    /// Account receiving the reminder.
    account_id: String,
}

/// Result returned after the durable delay expires.
#[derive(Debug, Deserialize, Serialize)]
struct ReminderSent {
    /// Account receiving the reminder.
    account_id: String,
    /// Attempt number observed after resumption.
    attempt: u32,
}

/// Task definition for the delayed reminder.
const SEND_TRIAL_REMINDER: Task<SendTrialReminderInput, ReminderSent> =
    Task::new("send-trial-reminder");

/// Trial-start checkpoint.
const RECORD_TRIAL: Step<()> = Step::new("record-trial");

/// Reminder delay.
const REMINDER_DELAY: Sleep = Sleep::new("reminder-delay");

/// Run the durable-sleep example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-delays")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task(SEND_TRIAL_REMINDER, async |input: SendTrialReminderInput, ctx: TaskContext| {
            ctx.step(RECORD_TRIAL, async || {
                println!("trial started for {}", input.account_id);
                println!("reminder scheduled with a durable 2-second delay");
                Ok(())
            })
            .await?;

            ctx.sleep_for(REMINDER_DELAY, Duration::from_secs(2)).await?;

            println!("task resumed after the durable delay");
            println!("sending trial reminder to {}", input.account_id);
            Ok(ReminderSent { account_id: input.account_id, attempt: ctx.attempt() })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn(SEND_TRIAL_REMINDER, SendTrialReminderInput { account_id: "ACCT-1001".to_owned() })
        .await?;

    let sent = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!("reminder completed for {} on attempt {}", sent.account_id, sent.attempt);

    worker.stop().await?;
    Ok(())
}
