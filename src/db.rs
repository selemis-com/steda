//! Shared queue database helpers.

use std::time::Duration;

use serde_json::Value;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};
use tokio::time::{Instant, sleep};

use crate::{
    error::{Error, Result, map_sqlx_error},
    types::{ClaimedTask, RunId, TaskId, TaskResultSnapshot},
};

/// Initial polling backoff while awaiting task results.
const INITIAL_RESULT_BACKOFF: Duration = Duration::from_millis(50);
/// Maximum polling backoff while awaiting task results.
const MAX_RESULT_BACKOFF: Duration = Duration::from_secs(1);

/// Converts a duration to rounded-up whole seconds for database functions.
pub(crate) fn duration_seconds(duration: Duration) -> Result<i32> {
    let seconds = duration
        .as_secs()
        .checked_add(u64::from(duration.subsec_nanos() > 0))
        .ok_or_else(|| Error::InvalidOptions("duration is too large to represent".to_owned()))?;
    i32::try_from(seconds).map_err(|_| {
        Error::InvalidOptions(format!("duration must round to at most {} seconds", i32::MAX))
    })
}

/// Claims runnable tasks whose names are supported by the local worker.
pub(crate) async fn claim_tasks(
    pool: &PgPool,
    queue_name: &str,
    worker_id: &str,
    lease_seconds: i32,
    batch_size: i32,
    supported_tasks: &[String],
) -> Result<Vec<ClaimedTask>> {
    let rows = sqlx::query(
        r#"
        SELECT
            run_id,
            task_id,
            attempt,
            task_name,
            params,
            headers
        FROM steda.claim_tasks($1, $2, $3, $4, $5)
        "#,
    )
    .bind(queue_name)
    .bind(worker_id)
    .bind(lease_seconds)
    .bind(batch_size)
    .bind(supported_tasks)
    .fetch_all(pool)
    .await?;

    rows.iter().map(claimed_task_from_row).collect()
}

/// Authoritative outcome returned by `PostgreSQL` run supervision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunSupervision {
    /// The run remains owned and executable.
    Running,
    /// The run completed durably.
    Completed,
    /// The run failed durably.
    Failed,
    /// The run was cancelled durably.
    Cancelled,
    /// The run suspended itself durably.
    Suspended,
    /// The finite lease expired and ownership was reaped.
    LeaseLost,
}

/// Ask `PostgreSQL` to supervise one execution attempt.
///
/// When `renew_for_seconds` is provided, `PostgreSQL` renews a still-valid running
/// claim from its own clock. A `None` value observes/enforces the authoritative
/// state without extending the lease.
pub(crate) async fn supervise_run(
    pool: &PgPool,
    queue_name: &str,
    run_id: RunId,
    renew_for_seconds: Option<i32>,
) -> Result<RunSupervision> {
    let outcome: String = sqlx::query_scalar("SELECT steda.supervise_run($1, $2, $3)")
        .bind(queue_name)
        .bind(run_id)
        .bind(renew_for_seconds)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
    match outcome.as_str() {
        "running" => Ok(RunSupervision::Running),
        "completed" => Ok(RunSupervision::Completed),
        "failed" => Ok(RunSupervision::Failed),
        "cancelled" => Ok(RunSupervision::Cancelled),
        "suspended" => Ok(RunSupervision::Suspended),
        "lease_lost" => Ok(RunSupervision::LeaseLost),
        other => Err(Error::Other(format!(
            "PostgreSQL returned unknown run supervision outcome {other:?}"
        ))),
    }
}

/// Fetches the current task result snapshot from the database.
pub(crate) async fn fetch_task_result_snapshot(
    pool: &PgPool,
    queue_name: &str,
    task_name: &str,
    task_id: TaskId,
) -> Result<Option<TaskResultSnapshot>> {
    fetch_task_result_snapshot_with(pool, queue_name, task_name, task_id).await
}

/// Fetches the current task result snapshot through one SQLx executor.
pub(crate) async fn fetch_task_result_snapshot_with<'e, E>(
    executor: E,
    queue_name: &str,
    task_name: &str,
    task_id: TaskId,
) -> Result<Option<TaskResultSnapshot>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT task_name, state, result, failure_reason
        FROM steda.get_task_result($1, $2)
        "#,
    )
    .bind(queue_name)
    .bind(task_id)
    .fetch_optional(executor)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let persisted_task_name: String = row.get("task_name");
    if persisted_task_name != task_name {
        return Err(Error::TaskNameMismatch {
            task_id,
            expected: task_name.to_owned(),
            actual: persisted_task_name,
        });
    }

    let state: String = row.get("state");
    let result: Option<Value> = row.get("result");
    let failure: Option<Value> = row.get("failure_reason");

    Ok(Some(match state.as_str() {
        "pending" => TaskResultSnapshot::Pending,
        "running" => TaskResultSnapshot::Running,
        "sleeping" => TaskResultSnapshot::Sleeping,
        "completed" => TaskResultSnapshot::Completed {
            result: result.ok_or_else(|| {
                Error::Other(format!("completed task {task_id} has no persisted result"))
            })?,
        },
        "failed" => TaskResultSnapshot::Failed {
            failure: failure.ok_or_else(|| {
                Error::Other(format!("failed task {task_id} has no persisted failure"))
            })?,
        },
        "cancelled" => TaskResultSnapshot::Cancelled,
        other => {
            return Err(Error::Other(format!(
                "PostgreSQL returned unknown task state {other:?} for task {task_id}"
            )));
        }
    }))
}

/// Wait for a task to reach a terminal state using the shared polling policy.
pub(crate) async fn await_task_result_snapshot(
    pool: &PgPool,
    queue_name: &str,
    task_name: &str,
    task_id: TaskId,
    timeout: Option<Duration>,
) -> Result<TaskResultSnapshot> {
    let started = Instant::now();
    let mut delay = INITIAL_RESULT_BACKOFF;

    loop {
        let snapshot = fetch_task_result_snapshot(pool, queue_name, task_name, task_id)
            .await?
            .ok_or_else(|| Error::TaskNotFound(task_id))?;
        if snapshot.is_terminal() {
            return Ok(snapshot);
        }

        if let Some(timeout) = timeout {
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return Err(Error::Timeout(format!("timed out waiting for task {task_id}")));
            }
            delay = delay.min(timeout - elapsed);
        }

        sleep(delay).await;
        delay = (delay * 2).min(MAX_RESULT_BACKOFF);
    }
}

/// Decodes a claimed task row returned by Postgres.
fn claimed_task_from_row(row: &PgRow) -> Result<ClaimedTask> {
    let headers: Option<Value> = row.get("headers");
    let headers = match headers {
        Some(Value::Object(map)) => Some(map),
        Some(Value::Null) | None => None,
        Some(other) => {
            return Err(Error::InvalidTaskHeaders(format!(
                "headers payload must be a JSON object, got {other}"
            )));
        }
    };

    let attempt: i32 = row.get("attempt");
    let attempt = u32::try_from(attempt)
        .map_err(|_| Error::Other("PostgreSQL returned a negative task attempt".to_owned()))?;

    Ok(ClaimedTask {
        run_id: row.get("run_id"),
        task_id: row.get("task_id"),
        attempt,
        task_name: row.get("task_name"),
        params: row.get("params"),
        headers,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::duration_seconds;

    #[test]
    fn duration_seconds_rounds_up_and_enforces_database_range() {
        assert_eq!(duration_seconds(Duration::ZERO).unwrap(), 0);
        assert_eq!(duration_seconds(Duration::from_nanos(1)).unwrap(), 1);
        assert_eq!(duration_seconds(Duration::from_secs(i32::MAX as u64)).unwrap(), i32::MAX);
        assert!(duration_seconds(Duration::from_secs(i32::MAX as u64 + 1)).is_err());
    }
}
