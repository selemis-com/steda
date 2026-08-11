//! Delegate each Steda attempt to a fresh provisioned execution environment.
//!
//! The "sandbox" is simulated in-process so the example runs without Docker or a
//! cluster. A production `TaskExecutor` can launch a process, container, VM,
//! Kubernetes Job, or remote execution. The long-lived Steda worker still owns the
//! claim, lease, retry, checkpoint, and result lifecycle; only the compute location
//! changes.
//!
//! The first simulated environment fails after committing a checkpoint. The retry
//! receives a different run/environment while replaying that logical-task checkpoint,
//! demonstrating that provisioned compute is not a parallel durability path.

/// Shared setup and finite-worker helpers.
mod common;

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{Error, Result, RetryStrategy, Task, TaskContext, TaskExecutor};

/// Input supplied to one isolated agent execution.
#[derive(Debug, Deserialize, Serialize)]
struct AgentTurnInput {
    /// Durable session identifier owned by the application.
    session_id: String,
    /// User request passed to the provisioned runtime.
    prompt: String,
}

/// Prepared request persisted before entering the ephemeral runtime.
#[derive(Debug, Deserialize, Serialize)]
struct PreparedTurn {
    /// Prompt normalized for the execution environment.
    prompt: String,
}

/// Typed result returned by the provisioned runtime.
#[derive(Debug, Deserialize, Serialize)]
struct AgentTurnOutput {
    /// Durable session identifier from the request.
    session_id: String,
    /// Execution environment that produced the successful result.
    environment_id: u64,
    /// Example response from the runtime.
    response: String,
}

/// Durable task contract for one agent turn.
struct AgentTurn;

impl Task for AgentTurn {
    const NAME: &'static str = "agent-turn";
    type Input = AgentTurnInput;
    type Output = AgentTurnOutput;
}

/// Reusable long-lived provisioner used by the worker for many attempts.
#[derive(Debug)]
struct SandboxExecutor {
    /// Monotonic identifier used to make fresh environments visible in the example.
    next_environment_id: AtomicU64,
}

impl SandboxExecutor {
    /// Create an empty provisioner.
    const fn new() -> Self {
        Self { next_environment_id: AtomicU64::new(1) }
    }
}

impl TaskExecutor<AgentTurn> for SandboxExecutor {
    fn execute(
        &self,
        input: AgentTurnInput,
        context: TaskContext,
    ) -> impl Future<Output = Result<AgentTurnOutput>> + Send {
        let environment_id = self.next_environment_id.fetch_add(1, Ordering::Relaxed);
        let attempt = context.attempt();
        let run_id = context.run_id();

        async move {
            println!(
                "provisioning environment {environment_id} for run {run_id} (attempt {attempt})"
            );

            // This durable preparation belongs to the logical task, not the
            // ephemeral environment. On the second attempt its body is skipped.
            // A real out-of-process executor can expose these TaskContext
            // capabilities to the child over its own IPC/RPC protocol.
            let prepared = context
                .step("prepare-turn", async || {
                    println!("preparing durable input for {}", input.session_id);
                    Ok(PreparedTurn { prompt: input.prompt.clone() })
                })
                .await?;

            // Simulate the first sandbox dying after durable preparation. Steda
            // fails this normal run and schedules another attempt; the executor
            // itself does not implement retry logic.
            if attempt == 1 {
                println!("environment {environment_id} exited unexpectedly");
                return Err(Error::Other("provisioned runtime exited unexpectedly".to_owned()));
            }

            // Stand in for launching and waiting on a real isolated runtime.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let output = AgentTurnOutput {
                session_id: input.session_id,
                environment_id,
                response: format!("processed: {}", prepared.prompt),
            };

            println!("destroying environment {environment_id}");
            Ok(output)
        }
    }
}

/// Run the provisioned-executor example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let steda = common::connect().await?;
    let queue = steda.queue("example-provisioned")?;
    queue.create().await?;

    // The worker is long-lived and reusable. `SandboxExecutor::execute` chooses
    // where each individual claimed attempt computes.
    let worker = queue.worker().task_executor::<AgentTurn>(SandboxExecutor::new()).build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn::<AgentTurn>(AgentTurnInput {
            session_id: common::unique_key("session")?,
            prompt: "Summarize the attached repository changes".to_owned(),
        })
        .max_attempts(2)
        .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
        .await?;

    let output = task.result_with_timeout(Duration::from_secs(10)).await?;
    println!(
        "{} completed in environment {}: {}",
        output.session_id, output.environment_id, output.response
    );

    worker.stop().await?;
    Ok(())
}
