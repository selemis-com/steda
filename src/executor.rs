//! Shared task execution helpers.

use std::{any, collections::HashMap, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use sqlx::PgPool;
use tokio::{select, time::sleep};
use tower_service::Service;

use crate::{
    context::TaskContext,
    db::{RunSupervision, supervise_run},
    error::{Error, Result, map_sqlx_error},
    execution::{ExecutionRequest, SharedExecutionService},
    metrics::{ExecutionOutcome, QueueMetrics, TaskExecution},
    types::{ClaimedTask, Json, RunId},
    worker::RegisteredTask,
};

/// Maximum interval between authoritative `PostgreSQL` supervision calls.
const MAX_SUPERVISION_INTERVAL: Duration = Duration::from_secs(1);

/// Dependencies shared by task execution attempts for one queue handle.
#[derive(Clone)]
pub(crate) struct ExecutionContext {
    /// Shared database pool.
    pool: PgPool,
    /// Queue containing the claimed task.
    queue_name: String,
    /// Registered task executors.
    registry: Arc<HashMap<String, RegisteredTask>>,
    /// Exporter-agnostic queue counters.
    metrics: QueueMetrics,
    /// Shared process-local task execution service.
    execution_service: SharedExecutionService,
}

impl ExecutionContext {
    /// Build the dependencies shared by task execution attempts.
    pub(crate) const fn new(
        pool: PgPool,
        queue_name: String,
        registry: Arc<HashMap<String, RegisteredTask>>,
        metrics: QueueMetrics,
        execution_service: SharedExecutionService,
    ) -> Self {
        Self { pool, queue_name, registry, metrics, execution_service }
    }
}

/// Shared task execution logic used by `Worker`.
pub(crate) async fn execute_task(
    context: ExecutionContext,
    task: ClaimedTask,
    lease_seconds: i32,
) -> Result<()> {
    let ExecutionContext { pool, queue_name, registry, metrics, execution_service } = context;
    let execution = TaskExecution::start(metrics);

    let task_executor = registry
        .get(&task.task_name)
        .map(|registered| registered.executor.clone())
        .ok_or_else(|| {
            Error::Other(format!(
                "database returned unsupported task {:?} to capability-filtered worker",
                task.task_name
            ))
        })?;

    // PostgreSQL performs one authoritative execution-state observation before
    // the task executor is invoked. This prevents a due cancellation or
    // already-lost lease from racing the first executor instructions.
    let initial_supervision =
        supervise_run(&pool, &queue_name, task.run_id, Some(lease_seconds)).await?;
    if initial_supervision != RunSupervision::Running {
        let outcome = execution_outcome_from_supervision(initial_supervision);
        execution.finish(outcome);
        return if matches!(outcome, ExecutionOutcome::LeaseLost) {
            Err(Error::LeaseLost)
        } else {
            Ok(())
        };
    }

    let ctx = match TaskContext::new(pool.clone(), queue_name.clone(), task.clone()).await {
        Ok(ctx) => ctx,
        Err(err @ Error::InvalidTaskHeaders(_)) => {
            let failure = serialize_error(&err);
            let fail_result = fail_run(&pool, &queue_name, task.run_id, failure).await;
            return finish_failure_transition(
                execution,
                &pool,
                &queue_name,
                task.run_id,
                fail_result,
            )
            .await;
        }
        Err(Error::LeaseLost) => return lease_lost(execution),
        Err(err) => return Err(err),
    };

    let request = ExecutionRequest::new(ctx.clone(), task.params.clone(), task_executor);
    let mut execution_service = execution_service.clone();
    let task_execution =
        AssertUnwindSafe(async move { execution_service.call(request).await }).catch_unwind();
    tokio::pin!(task_execution);

    let supervision =
        supervise_execution(pool.clone(), queue_name.clone(), task.run_id, lease_seconds);
    tokio::pin!(supervision);

    let run_result = select! {
        result = &mut task_execution => result,
        result = &mut supervision => {
            let outcome = result?;
            execution.finish(outcome);
            return if matches!(outcome, ExecutionOutcome::LeaseLost) {
                Err(Error::LeaseLost)
            } else {
                Ok(())
            };
        }
    };

    match run_result {
        Ok(Ok(response)) => {
            match complete_run(&pool, &queue_name, task.run_id, response.into_output()).await {
                Ok(()) => execution.finish(ExecutionOutcome::Completed),
                Err(Error::Cancelled) => execution.finish(ExecutionOutcome::Cancelled),
                Err(Error::LeaseLost) => return lease_lost(execution),
                Err(err) => return Err(err),
            }
        }
        Ok(Err(
            err @ (Error::Suspended | Error::Cancelled | Error::FailedRun | Error::LeaseLost),
        )) => {
            if let Some(outcome) =
                persisted_execution_outcome(&pool, &queue_name, task.run_id).await?
            {
                if matches!(outcome, ExecutionOutcome::LeaseLost) {
                    return lease_lost(execution);
                }
                execution.finish(outcome);
            } else {
                let failure = serialize_error(&err);
                return finish_failure_transition(
                    execution,
                    &pool,
                    &queue_name,
                    task.run_id,
                    fail_run(&pool, &queue_name, task.run_id, failure).await,
                )
                .await;
            }
        }
        Ok(Err(err)) => {
            let failure = serialize_error(&err);
            return finish_failure_transition(
                execution,
                &pool,
                &queue_name,
                task.run_id,
                fail_run(&pool, &queue_name, task.run_id, failure).await,
            )
            .await;
        }
        Err(payload) => {
            let message = panic_message(payload.as_ref());
            let failure = serde_json::json!({
                "name": "Panic",
                "message": format!("panic: {message}"),
            });
            return finish_failure_transition(
                execution,
                &pool,
                &queue_name,
                task.run_id,
                fail_run(&pool, &queue_name, task.run_id, failure).await,
            )
            .await;
        }
    }

    Ok(())
}

/// Finishes an execution after the canonical failure transition.
async fn finish_failure_transition(
    execution: TaskExecution,
    pool: &PgPool,
    queue_name: &str,
    run_id: RunId,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => {
            if matches!(
                persisted_execution_outcome(pool, queue_name, run_id).await?,
                Some(ExecutionOutcome::Cancelled)
            ) {
                execution.finish(ExecutionOutcome::Cancelled);
            } else {
                execution.finish(ExecutionOutcome::Failed);
            }
            Ok(())
        }
        Err(Error::Cancelled) => {
            execution.finish(ExecutionOutcome::Cancelled);
            Ok(())
        }
        Err(Error::FailedRun) => {
            execution.finish(ExecutionOutcome::Failed);
            Ok(())
        }
        Err(Error::LeaseLost) => lease_lost(execution),
        Err(err) => Err(err),
    }
}

/// Returns the execution outcome established by authoritative `PostgreSQL` supervision.
///
/// Public [`Error`] variants are also usable by task executors, so the durable executor
/// never treats a control-shaped error as proof that the corresponding Steda
/// transition occurred. `PostgreSQL` remains authoritative.
async fn persisted_execution_outcome(
    pool: &PgPool,
    queue_name: &str,
    run_id: RunId,
) -> Result<Option<ExecutionOutcome>> {
    let supervision = supervise_run(pool, queue_name, run_id, None).await?;
    Ok((supervision != RunSupervision::Running)
        .then(|| execution_outcome_from_supervision(supervision)))
}

/// Convert one authoritative `PostgreSQL` supervision state into execution metrics.
fn execution_outcome_from_supervision(supervision: RunSupervision) -> ExecutionOutcome {
    match supervision {
        RunSupervision::Running => unreachable!("running supervision has no bounded outcome"),
        RunSupervision::Completed => ExecutionOutcome::Completed,
        RunSupervision::Failed => ExecutionOutcome::Failed,
        RunSupervision::Cancelled => ExecutionOutcome::Cancelled,
        RunSupervision::Suspended => ExecutionOutcome::Suspended,
        RunSupervision::LeaseLost => ExecutionOutcome::LeaseLost,
    }
}

/// Finishes execution metrics for a worker that lost its finite run lease.
fn lease_lost(execution: TaskExecution) -> Result<()> {
    execution.finish(ExecutionOutcome::LeaseLost);
    Err(Error::LeaseLost)
}

/// Renews and supervises a running attempt through `PostgreSQL` until it leaves
/// the executable state. Rust controls only when to poll; `PostgreSQL` decides
/// ownership, lease validity, cancellation deadlines, and durable outcomes.
async fn supervise_execution(
    pool: PgPool,
    queue_name: String,
    run_id: RunId,
    lease_seconds: i32,
) -> Result<ExecutionOutcome> {
    let interval = supervision_interval(lease_seconds);

    loop {
        sleep(interval).await;
        let supervision = supervise_run(&pool, &queue_name, run_id, Some(lease_seconds)).await?;
        if supervision != RunSupervision::Running {
            return Ok(execution_outcome_from_supervision(supervision));
        }
    }
}

/// Choose a local wake-up cadence that always asks `PostgreSQL` before half of the
/// configured lease elapses, while retaining one-second cancellation responsiveness.
fn supervision_interval(lease_seconds: i32) -> Duration {
    let half_lease = Duration::from_secs(u64::try_from(lease_seconds).unwrap_or(1)) / 2;
    half_lease.min(MAX_SUPERVISION_INTERVAL)
}

/// Marks a claimed run as completed with a JSON result.
async fn complete_run(pool: &PgPool, queue_name: &str, run_id: RunId, result: Json) -> Result<()> {
    let completed: bool = sqlx::query_scalar("SELECT steda.complete_run($1, $2, $3)")
        .bind(queue_name)
        .bind(run_id)
        .bind(result)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

    if completed { Ok(()) } else { Err(Error::Cancelled) }
}

/// Marks a claimed run as failed with a JSON failure payload.
async fn fail_run(pool: &PgPool, queue_name: &str, run_id: RunId, failure: Json) -> Result<()> {
    sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
        .bind(queue_name)
        .bind(run_id)
        .bind(failure)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

    Ok(())
}

/// Converts an execution error into the persisted failure JSON shape.
fn serialize_error(error: &Error) -> Json {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        let rendered = cause.to_string();
        if !message.contains(&rendered) {
            message.push_str(": ");
            message.push_str(&rendered);
        }
        source = cause.source();
    }

    serde_json::json!({
        "name": error_name(error),
        "message": message,
    })
}

/// Maps internal queue errors to persisted failure names.
const fn error_name(error: &Error) -> &'static str {
    match error {
        Error::Timeout(_) => "TimeoutError",
        Error::Cancelled => "CancelledTask",
        Error::FailedRun => "FailedTask",
        Error::LeaseLost => "LeaseLost",
        Error::Suspended => "SuspendTask",
        _ => "Error",
    }
}

/// Renders a panic payload as a failure message.
fn panic_message(payload: &(dyn any::Any + Send)) -> String {
    payload.downcast_ref::<&str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .map_or_else(|| "unknown panic payload".to_owned(), Clone::clone)
        },
        |message| (*message).to_owned(),
    )
}
