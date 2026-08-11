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

/// Durable task contract for the delayed reminder.
struct SendTrialReminder;

impl Task for SendTrialReminder {
    const NAME: &'static str = "send-trial-reminder";
    type Input = SendTrialReminderInput;
    type Output = ReminderSent;
}

/// Durable identity for recording the trial start.
struct RecordTrial;

impl Step for RecordTrial {
    const NAME: &'static str = "record-trial";
    type Output = ();
}

/// Durable identity for the reminder delay.
struct ReminderDelay;

impl Sleep for ReminderDelay {
    const NAME: &'static str = "reminder-delay";
}

/// Run the durable-sleep example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-delays")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task::<SendTrialReminder>(async |input: SendTrialReminderInput, ctx: TaskContext| {
            ctx.step(RecordTrial, async || {
                println!("recorded trial start for {}", input.account_id);
                Ok(())
            })
            .await?;

            println!("waiting durably before reminding {}", input.account_id);
            ctx.sleep_for(ReminderDelay, Duration::from_secs(2)).await?;

            println!("sending reminder for {}", input.account_id);
            Ok(ReminderSent { account_id: input.account_id, attempt: ctx.attempt() })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn::<SendTrialReminder>(SendTrialReminderInput {
            account_id: common::unique_key("account")?,
        })
        .await?;

    let sent = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!("reminder for {} completed on attempt {}", sent.account_id, sent.attempt);

    worker.stop().await?;
    Ok(())
}
