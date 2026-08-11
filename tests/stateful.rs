//! Property-based task histories executed directly against `PostgreSQL`.
//!
//! This suite intentionally has no reference state machine. Proptest generates
//! task/run/checkpoint histories, including idempotent spawn replay, Steda executes
//! them against real queue tables, and the harness audits storage invariants after
//! every transition. Each operation also checks its observable `PostgreSQL`
//! contract; stale mutations are sent to the database and must be rejected there
//! rather than filtered out by the harness.
//!
//! Queue lifecycle, retention cleanup, Rust worker scheduling, and cross-queue
//! result waits are tested deterministically elsewhere. They compose around this
//! state machine rather than introducing additional task/run transitions here.

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, env, error::Error as StdError, fmt, io};

    use proptest::{
        collection,
        prelude::*,
        test_runner::{Config, TestRunner},
    };
    use serde::Serialize;
    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};
    use time::{OffsetDateTime, SignedDuration};
    use uuid::Uuid;

    use super::common::unique_queue;

    type BoxError = Box<dyn StdError + Send + Sync>;
    type StatefulResult<T> = Result<T, BoxError>;

    const INITIAL_FAKE_NOW: &str = "2030-01-01T00:00:00Z";

    #[derive(Clone, Copy, Serialize)]
    struct Operation {
        kind: OperationKind,
        args: [u8; 6],
    }

    #[derive(Debug, Clone, Copy, Serialize)]
    #[serde(rename_all = "snake_case")]
    enum OperationKind {
        Spawn,
        Replay,
        Claim,
        Supervise,
        Fail,
        Complete,
        Cancel,
        Retry,
        Checkpoint,
        Sleep,
        AdvanceTime,
        ReapExpired,
        CancelExpired,
    }

    impl OperationKind {
        const fn index(self) -> usize {
            match self {
                Self::Spawn => 0,
                Self::Replay => 1,
                Self::Claim => 2,
                Self::Supervise => 3,
                Self::Fail => 4,
                Self::Complete => 5,
                Self::Cancel => 6,
                Self::Retry => 7,
                Self::Checkpoint => 8,
                Self::Sleep => 9,
                Self::AdvanceTime => 10,
                Self::ReapExpired => 11,
                Self::CancelExpired => 12,
            }
        }
    }

    impl Operation {
        const fn new(kind: OperationKind, args: [u8; 6]) -> Self {
            Self { kind, args }
        }
    }

    impl fmt::Debug for Operation {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.kind {
                OperationKind::Spawn => {
                    let [alpha, payload, max_attempts, retry, cancellation, seconds] = self.args;
                    formatter
                        .debug_struct("Spawn")
                        .field("task", &task_name(alpha))
                        .field("payload", &payload)
                        .field("max_attempts", &max_attempts)
                        .field("retry", &retry_description(retry))
                        .field("cancellation", &cancellation_description(cancellation, seconds))
                        .finish()
                }
                OperationKind::Replay => {
                    let [task, _, _, _, _, _] = self.args;
                    formatter.debug_struct("Replay").field("task_selector", &task).finish()
                }
                OperationKind::Claim => {
                    let [worker, capability, lease, _, _, _] = self.args;
                    formatter
                        .debug_struct("Claim")
                        .field("worker", &worker_name(worker))
                        .field("tasks", &capability_description(capability))
                        .field("lease", &lease_description(lease))
                        .finish()
                }
                OperationKind::Supervise => {
                    let [run, extension, _, _, _, _] = self.args;
                    formatter
                        .debug_struct("Supervise")
                        .field("run_selector", &run)
                        .field("extension_seconds", &extension)
                        .finish()
                }
                OperationKind::Fail => {
                    let [run, reason, _, _, _, _] = self.args;
                    formatter
                        .debug_struct("Fail")
                        .field("run_selector", &run)
                        .field("reason", &reason)
                        .finish()
                }
                OperationKind::Complete => {
                    let [run, value, _, _, _, _] = self.args;
                    formatter
                        .debug_struct("Complete")
                        .field("run_selector", &run)
                        .field("result", &value)
                        .finish()
                }
                OperationKind::Cancel => {
                    let [task, _, _, _, _, _] = self.args;
                    formatter.debug_struct("Cancel").field("task_selector", &task).finish()
                }
                OperationKind::Retry => {
                    let [task, _, _, _, _, _] = self.args;
                    formatter.debug_struct("Retry").field("task_selector", &task).finish()
                }
                OperationKind::Checkpoint => {
                    let [run, step, value, _, _, _] = self.args;
                    formatter
                        .debug_struct("Checkpoint")
                        .field("run_selector", &run)
                        .field("step", &checkpoint_name(step))
                        .field("value", &value)
                        .finish()
                }
                OperationKind::Sleep => {
                    let [run, seconds, _, _, _, _] = self.args;
                    formatter
                        .debug_struct("Sleep")
                        .field("run_selector", &run)
                        .field("seconds", &seconds)
                        .finish()
                }
                OperationKind::AdvanceTime => {
                    let [seconds, _, _, _, _, _] = self.args;
                    formatter.debug_struct("AdvanceTime").field("seconds", &seconds).finish()
                }
                OperationKind::ReapExpired => {
                    let [limit, _, _, _, _, _] = self.args;
                    formatter.debug_struct("ReapExpired").field("limit", &limit).finish()
                }
                OperationKind::CancelExpired => {
                    let [limit, _, _, _, _, _] = self.args;
                    formatter.debug_struct("CancelExpired").field("limit", &limit).finish()
                }
            }
        }
    }

    const fn task_name(alpha: u8) -> &'static str {
        if alpha == 0 { "beta" } else { "alpha" }
    }

    const fn worker_name(worker: u8) -> &'static str {
        if worker == 0 { "worker-b" } else { "worker-a" }
    }

    const fn retry_description(retry: u8) -> &'static str {
        match retry % 3 {
            0 => "default",
            1 => "none",
            _ => "fixed 0s",
        }
    }

    fn cancellation_description(cancellation: u8, seconds: u8) -> String {
        match cancellation % 3 {
            0 => "none".to_owned(),
            1 => format!("max delay {seconds}s"),
            _ => format!("max duration {seconds}s"),
        }
    }

    const fn capability_description(capability: u8) -> &'static str {
        match capability % 3 {
            0 => "alpha,beta",
            1 => "alpha",
            _ => "beta",
        }
    }

    const fn checkpoint_name(step: u8) -> &'static str {
        match step % 4 {
            0 => "step-a",
            1 => "step-b",
            2 => "step-c",
            _ => "step-d",
        }
    }

    fn lease_description(lease: u8) -> String {
        format!("{lease}s")
    }

    #[derive(Debug, Clone)]
    struct SpawnRequest {
        task_id: Uuid,
        name: String,
        payload: u8,
        options: Value,
    }

    #[derive(Debug, Default)]
    struct Bindings {
        tasks: Vec<Uuid>,
        runs: Vec<Uuid>,
        spawns: Vec<SpawnRequest>,
    }

    #[derive(Debug)]
    struct SpawnedTask {
        task_id: Uuid,
        run_id: Uuid,
        attempt: i32,
        created: bool,
    }

    #[derive(Debug)]
    struct RunStatus {
        task_id: Uuid,
        attempt: i32,
        state: String,
        claimed_by: Option<String>,
        claim_expires_at: Option<OffsetDateTime>,
        available_at: OffsetDateTime,
        result: Option<Value>,
        failure_reason: Option<Value>,
    }

    #[derive(Debug, PartialEq)]
    struct TaskStatus {
        state: String,
        attempts: i32,
        initial_max_attempts: i32,
        max_attempts: i32,
        retry_strategy: Value,
        cancellation: Option<Value>,
        first_started_at: Option<OffsetDateTime>,
        last_attempt_run: Uuid,
    }

    #[derive(Clone, Copy)]
    enum FailureOutcome {
        RetryReady,
        RetryScheduled,
        TerminalFailed,
        Cancelled,
    }

    impl FailureOutcome {
        const fn index(self) -> usize {
            match self {
                Self::RetryReady => 0,
                Self::RetryScheduled => 1,
                Self::TerminalFailed => 2,
                Self::Cancelled => 3,
            }
        }
    }

    struct StepOutcome {
        message: String,
        affected: usize,
        rejected: usize,
        completion_cancelled: bool,
        failure_outcome: Option<FailureOutcome>,
    }

    #[derive(Default)]
    struct Coverage {
        attempted: [usize; 13],
        affected: [usize; 13],
        rejected: [usize; 13],
        completion_cancellations: usize,
        failure_outcomes: [usize; 4],
    }

    impl Coverage {
        fn record(&mut self, kind: OperationKind, outcome: &StepOutcome) {
            let index = kind.index();
            self.attempted[index] += 1;
            self.affected[index] += outcome.affected;
            self.rejected[index] += outcome.rejected;
            self.completion_cancellations += usize::from(outcome.completion_cancelled);
            if let Some(failure_outcome) = outcome.failure_outcome {
                self.failure_outcomes[failure_outcome.index()] += 1;
            }
        }

        fn merge(&mut self, other: &Self) {
            for index in 0..self.attempted.len() {
                self.attempted[index] += other.attempted[index];
                self.affected[index] += other.affected[index];
                self.rejected[index] += other.rejected[index];
            }
            self.completion_cancellations += other.completion_cancellations;
            for (total, value) in self.failure_outcomes.iter_mut().zip(&other.failure_outcomes) {
                *total += *value;
            }
        }

        fn attempted(&self, kind: OperationKind) -> usize {
            self.attempted[kind.index()]
        }

        fn affected(&self, kind: OperationKind) -> usize {
            self.affected[kind.index()]
        }

        fn rejected(&self, kind: OperationKind) -> usize {
            self.rejected[kind.index()]
        }

        fn not_applied(&self, kind: OperationKind) -> usize {
            self.attempted(kind)
                .saturating_sub(self.affected(kind))
                .saturating_sub(self.rejected(kind))
        }

        fn failure_outcome(&self, outcome: FailureOutcome) -> usize {
            self.failure_outcomes[outcome.index()]
        }

        fn steps(&self) -> usize {
            self.attempted.iter().sum()
        }
    }

    fn operation_strategy() -> impl Strategy<Value = Operation> {
        prop_oneof![
            6 => (any::<bool>(), any::<u8>(), 1_u8..=4, 0_u8..=2, 0_u8..=2, 0_u8..=12)
                .prop_map(|(alpha, payload, max_attempts, retry, cancellation, seconds)| {
                    Operation::new(
                        OperationKind::Spawn,
                        [u8::from(alpha), payload, max_attempts, retry, cancellation, seconds],
                    )
                }),
            3 => any::<u8>()
                .prop_map(|task| Operation::new(OperationKind::Replay, [task, 0, 0, 0, 0, 0])),
            7 => (any::<bool>(), 0_u8..=2, 1_u8..=5).prop_map(|(worker, capability, lease)| {
                Operation::new(
                    OperationKind::Claim,
                    [u8::from(worker), capability, lease, 0, 0, 0],
                )
            }),
            2 => (any::<u8>(), 1_u8..=10).prop_map(|(run, extension)| {
                Operation::new(OperationKind::Supervise, [run, extension, 0, 0, 0, 0])
            }),
            4 => (any::<u8>(), any::<u8>()).prop_map(|(run, reason)| {
                Operation::new(OperationKind::Fail, [run, reason, 0, 0, 0, 0])
            }),
            4 => (any::<u8>(), any::<u8>()).prop_map(|(run, value)| {
                Operation::new(OperationKind::Complete, [run, value, 0, 0, 0, 0])
            }),
            3 => any::<u8>()
                .prop_map(|task| Operation::new(OperationKind::Cancel, [task, 0, 0, 0, 0, 0])),
            2 => any::<u8>()
                .prop_map(|task| Operation::new(OperationKind::Retry, [task, 0, 0, 0, 0, 0])),
            3 => (any::<u8>(), any::<u8>(), any::<u8>()).prop_map(|(run, step, value)| {
                Operation::new(OperationKind::Checkpoint, [run, step, value, 0, 0, 0])
            }),
            3 => (any::<u8>(), 0_u8..=12).prop_map(|(run, seconds)| {
                Operation::new(OperationKind::Sleep, [run, seconds, 0, 0, 0, 0])
            }),
            4 => (0_u8..=12).prop_map(|seconds| {
                Operation::new(OperationKind::AdvanceTime, [seconds, 0, 0, 0, 0, 0])
            }),
            2 => (1_u8..=4).prop_map(|limit| {
                Operation::new(OperationKind::ReapExpired, [limit, 0, 0, 0, 0, 0])
            }),
            2 => (1_u8..=4).prop_map(|limit| {
                Operation::new(OperationKind::CancelExpired, [limit, 0, 0, 0, 0, 0])
            }),
        ]
    }

    fn stateful_config() -> Config {
        Config {
            cases: env_u32("STEDA_STATEFUL_CASES", 64),
            failure_persistence: None,
            ..Default::default()
        }
    }

    fn history_strategy() -> impl Strategy<Value = Vec<Operation>> {
        let minimum = env_usize("STEDA_STATEFUL_MIN_STEPS", 32);
        let maximum = env_usize("STEDA_STATEFUL_STEPS", 96);
        assert!(
            minimum <= maximum,
            "STEDA_STATEFUL_MIN_STEPS must not exceed STEDA_STATEFUL_STEPS"
        );
        collection::vec(operation_strategy(), minimum..=maximum)
    }

    fn env_u32(name: &str, default: u32) -> u32 {
        let value = env::var(name).map_or(default, |value| {
            value.parse().unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        });
        assert!(value > 0, "{name} must be a positive integer");
        value
    }

    fn env_usize(name: &str, default: usize) -> usize {
        let value = env::var(name).map_or(default, |value| {
            value.parse().unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        });
        assert!(value > 0, "{name} must be a positive integer");
        value
    }

    fn env_flag(name: &str) -> bool {
        env::var(name).is_ok_and(|value| {
            matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        })
    }

    fn stateful_error(message: impl Into<String>) -> BoxError {
        Box::new(io::Error::other(message.into()))
    }

    fn ensure(condition: bool, message: impl Into<String>) -> StatefulResult<()> {
        if condition { Ok(()) } else { Err(stateful_error(message)) }
    }

    async fn set_initial_time(connection: &mut PgConnection) -> StatefulResult<()> {
        sqlx::query("SELECT set_config('steda.fake_now', $1, false)")
            .bind(INITIAL_FAKE_NOW)
            .execute(&mut *connection)
            .await?;
        Ok(())
    }

    async fn advance_time(connection: &mut PgConnection, seconds: u8) -> StatefulResult<()> {
        sqlx::query(
            "SELECT set_config(\
                'steda.fake_now', \
                (steda.current_time() + make_interval(secs => $1))::text, \
                false\
            )",
        )
        .bind(i32::from(seconds))
        .execute(&mut *connection)
        .await?;
        Ok(())
    }

    async fn current_time(connection: &mut PgConnection) -> StatefulResult<OffsetDateTime> {
        Ok(sqlx::query_scalar("SELECT steda.current_time()").fetch_one(&mut *connection).await?)
    }

    fn spawn_options(
        max_attempts: u8,
        retry_mode: u8,
        cancellation_mode: u8,
        cancellation_seconds: u8,
    ) -> Value {
        let retry_strategy = match retry_mode % 3 {
            0 => None,
            1 => Some(json!({ "kind": "none" })),
            _ => Some(json!({ "kind": "fixed", "base_seconds": 0.0 })),
        };
        let cancellation = match cancellation_mode % 3 {
            0 => None,
            1 => Some(json!({ "max_delay": cancellation_seconds })),
            _ => Some(json!({ "max_duration": cancellation_seconds })),
        };

        let mut options = serde_json::Map::new();
        options.insert("max_attempts".to_owned(), json!(max_attempts));
        if let Some(retry_strategy) = retry_strategy {
            options.insert("retry_strategy".to_owned(), retry_strategy);
        }
        if let Some(cancellation) = cancellation {
            options.insert("cancellation".to_owned(), cancellation);
        }
        Value::Object(options)
    }

    async fn spawn(
        connection: &mut PgConnection,
        queue: &str,
        name: &str,
        payload: u8,
        options: Value,
    ) -> StatefulResult<SpawnedTask> {
        let row = sqlx::query(
            "SELECT id, run_id, attempt, created FROM steda.spawn_task($1, $2, $3, $4)",
        )
        .bind(queue)
        .bind(name)
        .bind(json!({ "value": payload }))
        .bind(options)
        .fetch_one(&mut *connection)
        .await?;
        Ok(SpawnedTask {
            task_id: row.get("id"),
            run_id: row.get("run_id"),
            attempt: row.get("attempt"),
            created: row.get("created"),
        })
    }

    async fn fetch_run(
        connection: &mut PgConnection,
        queue: &str,
        run_id: Uuid,
    ) -> StatefulResult<Option<RunStatus>> {
        let query = format!(
            "SELECT task_id, attempt, state, claimed_by, claim_expires_at, available_at, result, failure_reason \
             FROM steda.runs_{queue} WHERE id = $1",
        );
        let Some(row) =
            sqlx::query(AssertSqlSafe(query)).bind(run_id).fetch_optional(&mut *connection).await?
        else {
            return Ok(None);
        };
        Ok(Some(RunStatus {
            task_id: row.get("task_id"),
            attempt: row.get("attempt"),
            state: row.get("state"),
            claimed_by: row.get("claimed_by"),
            claim_expires_at: row.get("claim_expires_at"),
            available_at: row.get("available_at"),
            result: row.get("result"),
            failure_reason: row.get("failure_reason"),
        }))
    }

    async fn fetch_task(
        connection: &mut PgConnection,
        queue: &str,
        task_id: Uuid,
    ) -> StatefulResult<Option<TaskStatus>> {
        let query = format!(
            "SELECT state, attempts, initial_max_attempts, max_attempts, retry_strategy, cancellation, \
             first_started_at, last_attempt_run \
             FROM steda.tasks_{queue} WHERE id = $1",
        );
        let Some(row) = sqlx::query(AssertSqlSafe(query))
            .bind(task_id)
            .fetch_optional(&mut *connection)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(TaskStatus {
            state: row.get("state"),
            attempts: row.get("attempts"),
            initial_max_attempts: row.get("initial_max_attempts"),
            max_attempts: row.get("max_attempts"),
            retry_strategy: row.get("retry_strategy"),
            cancellation: row.get("cancellation"),
            first_started_at: row.get("first_started_at"),
            last_attempt_run: row.get("last_attempt_run"),
        }))
    }

    async fn fetch_checkpoint_state(
        connection: &mut PgConnection,
        queue: &str,
        task_id: Uuid,
        name: &str,
    ) -> StatefulResult<Option<Value>> {
        let query = format!(
            "SELECT state FROM steda.checkpoints_{queue} WHERE task_id = $1 AND name = $2",
        );
        Ok(sqlx::query_scalar(AssertSqlSafe(query))
            .bind(task_id)
            .bind(name)
            .fetch_optional(&mut *connection)
            .await?)
    }

    fn max_duration_due(task: &TaskStatus, at: OffsetDateTime) -> bool {
        task.cancellation
            .as_ref()
            .and_then(|policy| policy.get("max_duration"))
            .and_then(Value::as_i64)
            .zip(task.first_started_at)
            .is_some_and(|(seconds, first_started_at)| {
                (at - first_started_at).whole_seconds() >= seconds
            })
    }

    fn task_label(bindings: &Bindings, task_id: Uuid) -> String {
        bindings
            .tasks
            .iter()
            .position(|id| *id == task_id)
            .map_or_else(|| format!("task<{task_id}>"), |index| format!("task#{index}"))
    }

    fn run_label(bindings: &Bindings, run_id: Uuid) -> String {
        bindings
            .runs
            .iter()
            .position(|id| *id == run_id)
            .map_or_else(|| format!("run<{run_id}>"), |index| format!("run#{index}"))
    }

    fn expected_run_rejection(
        status: &RunStatus,
        now: OffsetDateTime,
    ) -> Option<Option<&'static str>> {
        match status.state.as_str() {
            "cancelled" => Some(Some("AB001")),
            "failed" => Some(Some("AB002")),
            "running" if status.claim_expires_at.is_some_and(|expires_at| expires_at <= now) => {
                Some(Some("AB003"))
            }
            "running" => None,
            _ => Some(None),
        }
    }

    fn ensure_rejected<T>(
        result: Result<T, sqlx::Error>,
        expected_sqlstate: Option<&str>,
        context: &str,
    ) -> StatefulResult<()> {
        let error = match result {
            Ok(_) => {
                return Err(stateful_error(format!("{context}: operation unexpectedly succeeded")));
            }
            Err(error) => error,
        };
        if let Some(expected_sqlstate) = expected_sqlstate {
            let sqlx::Error::Database(database_error) = &error else {
                return Err(stateful_error(format!(
                    "{context}: expected SQLSTATE {expected_sqlstate}, got {error:?}",
                )));
            };
            ensure(
                database_error.code().as_deref() == Some(expected_sqlstate),
                format!(
                    "{context}: expected SQLSTATE {expected_sqlstate}, got {:?}",
                    database_error.code(),
                ),
            )?;
        }
        Ok(())
    }

    async fn count_claimable(
        connection: &mut PgConnection,
        queue: &str,
        task_names: &[String],
    ) -> StatefulResult<i64> {
        let query = format!(
            r#"
            SELECT count(*)
            FROM steda.runs_{queue} run
            JOIN steda.tasks_{queue} task ON task.id = run.task_id
            WHERE run.state IN ('pending', 'sleeping')
              AND task.state IN ('pending', 'sleeping')
              AND run.available_at <= steda.current_time()
              AND task.name = ANY($1)
              AND NOT (
                (
                    (task.cancellation->>'max_delay')::bigint IS NOT NULL
                    AND task.first_started_at IS NULL
                    AND extract(epoch FROM (steda.current_time() - task.enqueue_at))
                        >= (task.cancellation->>'max_delay')::bigint
                )
                OR
                (
                    (task.cancellation->>'max_duration')::bigint IS NOT NULL
                    AND task.first_started_at IS NOT NULL
                    AND extract(epoch FROM (steda.current_time() - task.first_started_at))
                        >= (task.cancellation->>'max_duration')::bigint
                )
              )
            "#,
        );
        Ok(sqlx::query_scalar(AssertSqlSafe(query))
            .bind(task_names)
            .fetch_one(&mut *connection)
            .await?)
    }

    async fn count_expired_runs(connection: &mut PgConnection, queue: &str) -> StatefulResult<i64> {
        let query = format!(
            "SELECT count(*) FROM steda.runs_{queue} \
             WHERE state = 'running' AND claim_expires_at IS NOT NULL \
             AND claim_expires_at <= steda.current_time()",
        );
        Ok(sqlx::query_scalar(AssertSqlSafe(query)).fetch_one(&mut *connection).await?)
    }

    async fn count_policy_expired_tasks(
        connection: &mut PgConnection,
        queue: &str,
    ) -> StatefulResult<i64> {
        let query = format!(
            r#"
            SELECT count(*)
            FROM steda.tasks_{queue}
            WHERE state IN ('pending', 'sleeping', 'running')
              AND (
                (
                    (cancellation->>'max_delay')::bigint IS NOT NULL
                    AND first_started_at IS NULL
                    AND extract(epoch FROM (steda.current_time() - enqueue_at))
                        >= (cancellation->>'max_delay')::bigint
                )
                OR
                (
                    (cancellation->>'max_duration')::bigint IS NOT NULL
                    AND first_started_at IS NOT NULL
                    AND extract(epoch FROM (steda.current_time() - first_started_at))
                        >= (cancellation->>'max_duration')::bigint
                )
              )
            "#,
        );
        Ok(sqlx::query_scalar(AssertSqlSafe(query)).fetch_one(&mut *connection).await?)
    }

    async fn count_active_runs_for_task(
        connection: &mut PgConnection,
        queue: &str,
        task_id: Uuid,
    ) -> StatefulResult<i64> {
        let query = format!(
            "SELECT count(*) FROM steda.runs_{queue} \
             WHERE task_id = $1 AND state IN ('pending', 'running', 'sleeping')",
        );
        Ok(sqlx::query_scalar(AssertSqlSafe(query))
            .bind(task_id)
            .fetch_one(&mut *connection)
            .await?)
    }

    async fn refresh_bindings(
        connection: &mut PgConnection,
        queue: &str,
        bindings: &mut Bindings,
    ) -> StatefulResult<()> {
        let task_query = format!("SELECT id FROM steda.tasks_{queue} ORDER BY id");
        for row in sqlx::query(AssertSqlSafe(task_query)).fetch_all(&mut *connection).await? {
            let id: Uuid = row.get("id");
            if !bindings.tasks.contains(&id) {
                bindings.tasks.push(id);
            }
        }

        let run_query = format!("SELECT id FROM steda.runs_{queue} ORDER BY id");
        for row in sqlx::query(AssertSqlSafe(run_query)).fetch_all(&mut *connection).await? {
            let id: Uuid = row.get("id");
            if !bindings.runs.contains(&id) {
                bindings.runs.push(id);
            }
        }
        Ok(())
    }

    fn select_task(bindings: &Bindings, selector: u8) -> Option<Uuid> {
        let length = bindings.tasks.len();
        if length == 0 {
            return None;
        }
        bindings.tasks.get(usize::from(selector) % length).copied()
    }

    fn select_spawn(bindings: &Bindings, selector: u8) -> Option<&SpawnRequest> {
        let length = bindings.spawns.len();
        if length == 0 {
            return None;
        }
        bindings.spawns.get(usize::from(selector) % length)
    }

    fn select_run(bindings: &Bindings, selector: u8) -> Option<Uuid> {
        let length = bindings.runs.len();
        if length == 0 {
            return None;
        }
        bindings.runs.get(usize::from(selector) % length).copied()
    }

    async fn audit_invariants(connection: &mut PgConnection, queue: &str) -> StatefulResult<()> {
        let tasks = format!("tasks_{queue}");
        let runs = format!("runs_{queue}");
        let query = format!(
            r#"
            WITH violations AS (
                SELECT concat('invalid attempt budget for task ', task.id) AS problem
                FROM steda.{tasks} task
                WHERE task.initial_max_attempts < 1
                   OR task.max_attempts < task.initial_max_attempts
                   OR task.attempts < 1
                   OR task.attempts > task.max_attempts

                UNION ALL

                SELECT concat('authoritative run mismatch for task ', task.id)
                FROM steda.{tasks} task
                LEFT JOIN steda.{runs} run ON run.id = task.last_attempt_run
                WHERE run.id IS NULL
                   OR run.task_id <> task.id
                   OR run.attempt <> task.attempts
                   OR (task.state <> run.state AND NOT (task.state = 'cancelled' AND run.state = 'failed'))

                UNION ALL

                SELECT concat('task attempt does not match maximum run attempt for task ', task.id)
                FROM steda.{tasks} task
                JOIN (
                    SELECT task_id, max(attempt) AS maximum_attempt
                    FROM steda.{runs}
                    GROUP BY task_id
                ) run_attempt ON run_attempt.task_id = task.id
                WHERE run_attempt.maximum_attempt <> task.attempts

                UNION ALL

                SELECT concat('orphan run ', run.id)
                FROM steda.{runs} run
                LEFT JOIN steda.{tasks} task ON task.id = run.task_id
                WHERE task.id IS NULL

                UNION ALL

                SELECT concat('duplicate attempt ', run.attempt, ' for task ', run.task_id)
                FROM steda.{runs} run
                GROUP BY run.task_id, run.attempt
                HAVING count(*) > 1

                UNION ALL

                SELECT concat('multiple active runs for task ', run.task_id)
                FROM steda.{runs} run
                WHERE run.state IN ('pending', 'running', 'sleeping')
                GROUP BY run.task_id
                HAVING count(*) > 1

                UNION ALL

                SELECT concat('active run is not authoritative for task ', run.task_id)
                FROM steda.{runs} run
                JOIN steda.{tasks} task ON task.id = run.task_id
                WHERE run.state IN ('pending', 'running', 'sleeping')
                  AND run.id <> task.last_attempt_run

                UNION ALL

                SELECT concat('running run has no worker for run ', run.id)
                FROM steda.{runs} run
                WHERE run.state = 'running'
                  AND run.claimed_by IS NULL

                UNION ALL

                SELECT concat('non-running run retains worker ownership for run ', run.id)
                FROM steda.{runs} run
                WHERE run.state <> 'running'
                  AND (run.claimed_by IS NOT NULL OR run.claim_expires_at IS NOT NULL)
            )
            SELECT problem
            FROM violations
            LIMIT 20
            "#,
        );
        let problems: Vec<String> =
            sqlx::query_scalar(AssertSqlSafe(query)).fetch_all(&mut *connection).await?;
        ensure(problems.is_empty(), format!("queue invariant violations: {}", problems.join("; ")))
    }

    fn outcome(message: String, affected: usize) -> StepOutcome {
        StepOutcome {
            message,
            affected,
            rejected: 0,
            completion_cancelled: false,
            failure_outcome: None,
        }
    }

    fn rejected_outcome(message: String) -> StepOutcome {
        StepOutcome {
            message,
            affected: 0,
            rejected: 1,
            completion_cancelled: false,
            failure_outcome: None,
        }
    }

    fn failed_outcome(message: String, failure_outcome: FailureOutcome) -> StepOutcome {
        StepOutcome {
            message,
            affected: 1,
            rejected: 0,
            completion_cancelled: false,
            failure_outcome: Some(failure_outcome),
        }
    }

    fn completion_cancelled_outcome(message: String) -> StepOutcome {
        StepOutcome {
            message,
            affected: 1,
            rejected: 0,
            completion_cancelled: true,
            failure_outcome: None,
        }
    }

    async fn apply_operation(
        connection: &mut PgConnection,
        queue: &str,
        bindings: &mut Bindings,
        operation: Operation,
    ) -> StatefulResult<StepOutcome> {
        match operation.kind {
            OperationKind::Spawn => {
                let [alpha, payload, max_attempts, retry, cancellation, seconds] = operation.args;
                let name = task_name(alpha);
                let mut options = spawn_options(max_attempts, retry, cancellation, seconds);
                let idempotency_key = format!("stateful-{}", bindings.spawns.len());
                options
                    .as_object_mut()
                    .expect("spawn options are always an object")
                    .insert("idempotency_key".to_owned(), json!(idempotency_key));
                let now = current_time(connection).await?;
                let spawned = spawn(connection, queue, name, payload, options.clone()).await?;
                ensure(spawned.created, "fresh generated spawn was unexpectedly replayed")?;
                ensure(
                    spawned.attempt == 1,
                    "fresh generated spawn returned a non-initial attempt",
                )?;
                bindings.spawns.push(SpawnRequest {
                    task_id: spawned.task_id,
                    name: name.to_owned(),
                    payload,
                    options,
                });
                if !bindings.tasks.contains(&spawned.task_id) {
                    bindings.tasks.push(spawned.task_id);
                }
                if !bindings.runs.contains(&spawned.run_id) {
                    bindings.runs.push(spawned.run_id);
                }
                let task = fetch_task(connection, queue, spawned.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("spawned task disappeared"))?;
                let run = fetch_run(connection, queue, spawned.run_id)
                    .await?
                    .ok_or_else(|| stateful_error("spawned run disappeared"))?;
                ensure(task.state == "pending", "spawned task is not pending")?;
                ensure(task.attempts == 1, "spawned task did not start at attempt one")?;
                ensure(
                    task.initial_max_attempts == i32::from(max_attempts),
                    "spawned task initial_max_attempts mismatch",
                )?;
                ensure(
                    task.max_attempts == i32::from(max_attempts),
                    "spawned task max_attempts mismatch",
                )?;
                ensure(
                    task.last_attempt_run == spawned.run_id,
                    "spawned task does not point at its first run",
                )?;
                ensure(run.task_id == spawned.task_id, "spawned run belongs to another task")?;
                ensure(run.attempt == 1, "spawned run did not start at attempt one")?;
                ensure(run.state == "pending", "spawned run is not pending")?;
                ensure(run.available_at == now, "spawned run is not immediately available")?;
                ensure(run.claimed_by.is_none(), "spawned run is unexpectedly owned")?;
                ensure(run.claim_expires_at.is_none(), "spawned run has an unexpected lease")?;
                let expected_retry = match retry % 3 {
                    0 => "exponential",
                    1 => "none",
                    _ => "fixed",
                };
                ensure(
                    task.retry_strategy.get("kind").and_then(Value::as_str) == Some(expected_retry),
                    "spawned task retry strategy mismatch",
                )?;
                let expected_cancellation_key = match cancellation % 3 {
                    0 => None,
                    1 => Some("max_delay"),
                    _ => Some("max_duration"),
                };
                match expected_cancellation_key {
                    None => ensure(
                        task.cancellation.is_none(),
                        "spawned task gained a cancellation policy",
                    )?,
                    Some(key) => ensure(
                        task.cancellation
                            .as_ref()
                            .and_then(|policy| policy.get(key))
                            .and_then(Value::as_u64)
                            == Some(u64::from(seconds)),
                        "spawned task cancellation policy mismatch",
                    )?,
                }
                Ok(outcome(
                    format!(
                        "SPAWN {name} payload={payload} max_attempts={max_attempts} \
                         retry={} cancellation={} -> {} {} pending",
                        retry_description(retry),
                        cancellation_description(cancellation, seconds),
                        task_label(bindings, spawned.task_id),
                        run_label(bindings, spawned.run_id),
                    ),
                    1,
                ))
            }
            OperationKind::Replay => {
                let [task, _, _, _, _, _] = operation.args;
                let Some(request) = select_spawn(bindings, task).cloned() else {
                    return Ok(outcome(
                        format!("REPLAY selector={task} -> skipped: no idempotent spawns exist"),
                        0,
                    ));
                };
                let before = fetch_task(connection, queue, request.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("idempotent replay target disappeared"))?;
                let replay = spawn(
                    connection,
                    queue,
                    &request.name,
                    request.payload,
                    request.options.clone(),
                )
                .await?;
                ensure(!replay.created, "idempotent replay created a second task")?;
                ensure(replay.task_id == request.task_id, "idempotent replay changed task id")?;
                ensure(
                    replay.run_id == before.last_attempt_run,
                    "idempotent replay did not return the authoritative run",
                )?;
                ensure(
                    replay.attempt == before.attempts,
                    "idempotent replay did not return the current attempt",
                )?;
                let after = fetch_task(connection, queue, request.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("idempotent replay target disappeared"))?;
                ensure(after == before, "idempotent replay mutated task state")?;
                Ok(outcome(
                    format!(
                        "REPLAY {} -> existing {} attempt={}",
                        task_label(bindings, request.task_id),
                        run_label(bindings, replay.run_id),
                        replay.attempt,
                    ),
                    1,
                ))
            }
            OperationKind::Claim => {
                let [worker, capability, lease_seconds, _, _, _] = operation.args;
                let worker_id = worker_name(worker);
                let task_names = match capability % 3 {
                    0 => vec!["alpha".to_owned(), "beta".to_owned()],
                    1 => vec!["alpha".to_owned()],
                    _ => vec!["beta".to_owned()],
                };
                let claimable_before = count_claimable(connection, queue, &task_names).await?;
                let now = current_time(connection).await?;
                let row = sqlx::query(
                    "SELECT run_id, id, attempt, name FROM steda.claim_tasks($1, $2, $3, 1, $4)",
                )
                .bind(queue)
                .bind(worker_id)
                .bind(i32::from(lease_seconds))
                .bind(task_names.as_slice())
                .fetch_optional(&mut *connection)
                .await?;
                let lease = lease_description(lease_seconds);
                let accepts = capability_description(capability);
                let Some(row) = row else {
                    ensure(
                        claimable_before == 0,
                        format!(
                            "claim returned no task despite {claimable_before} eligible run(s) before the call",
                        ),
                    )?;
                    return Ok(outcome(
                        format!(
                            "CLAIM {worker_id} accepts=[{accepts}] lease={lease} -> no eligible task"
                        ),
                        0,
                    ));
                };
                let name: String = row.get("name");
                ensure(task_names.contains(&name), "claim returned an unsupported task")?;
                let run_id: Uuid = row.get("run_id");
                let task_id: Uuid = row.get("id");
                let attempt: i32 = row.get("attempt");
                if !bindings.tasks.contains(&task_id) {
                    bindings.tasks.push(task_id);
                }
                if !bindings.runs.contains(&run_id) {
                    bindings.runs.push(run_id);
                }
                let run = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("claimed run disappeared"))?;
                let task = fetch_task(connection, queue, task_id)
                    .await?
                    .ok_or_else(|| stateful_error("claimed task disappeared"))?;
                ensure(run.task_id == task_id, "claim returned a run for another task")?;
                ensure(run.attempt == attempt, "claim returned a mismatched attempt")?;
                ensure(run.state == "running", "claimed run is not running")?;
                ensure(
                    run.claimed_by.as_deref() == Some(worker_id),
                    "claimed run has the wrong worker owner",
                )?;
                let expected_expiry = Some(now + SignedDuration::seconds(i64::from(lease_seconds)));
                ensure(
                    run.claim_expires_at == expected_expiry,
                    format!(
                        "claim lease mismatch: expected {expected_expiry:?}, got {:?}",
                        run.claim_expires_at,
                    ),
                )?;
                ensure(task.state == "running", "claimed task is not running")?;
                ensure(task.attempts == attempt, "claimed task attempt counter is wrong")?;
                ensure(
                    task.last_attempt_run == run_id,
                    "claimed run is not the task's authoritative run",
                )?;
                Ok(outcome(
                    format!(
                        "CLAIM {worker_id} accepts=[{accepts}] lease={lease} -> {} {} {name} attempt={attempt}",
                        task_label(bindings, task_id),
                        run_label(bindings, run_id),
                    ),
                    1,
                ))
            }
            OperationKind::Supervise => {
                let [run, extension, _, _, _, _] = operation.args;
                let Some(run_id) = select_run(bindings, run) else {
                    return Ok(outcome(
                        format!("SUPERVISE selector={run} +{extension}s -> skipped: no runs exist"),
                        0,
                    ));
                };
                let label = run_label(bindings, run_id);
                let Some(before) = fetch_run(connection, queue, run_id).await? else {
                    return Ok(outcome(
                        format!("SUPERVISE {label} +{extension}s -> skipped: run missing"),
                        0,
                    ));
                };
                let before_task = fetch_task(connection, queue, before.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("supervised run lost its task"))?;
                let before_expiry = before.claim_expires_at;
                let before_run_state = before.state.clone();
                let before_task_state = before_task.state;
                let now = current_time(connection).await?;

                if before.state == "pending" {
                    let result =
                        sqlx::query_scalar::<_, String>("SELECT steda.supervise_run($1, $2, $3)")
                            .bind(queue)
                            .bind(run_id)
                            .bind(i32::from(extension))
                            .fetch_one(&mut *connection)
                            .await;
                    ensure_rejected(result, None, "pending run supervision succeeded")?;
                    return Ok(rejected_outcome(format!(
                        "SUPERVISE {label} attempt={} +{extension}s -> rejected by PostgreSQL",
                        before.attempt,
                    )));
                }

                let supervision_outcome: String =
                    sqlx::query_scalar("SELECT steda.supervise_run($1, $2, $3)")
                        .bind(queue)
                        .bind(run_id)
                        .bind(i32::from(extension))
                        .fetch_one(&mut *connection)
                        .await?;

                let after = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("supervised run disappeared"))?;
                let after_task =
                    fetch_task(connection, queue, after.task_id).await?.ok_or_else(|| {
                        stateful_error("supervised run lost its task after supervision")
                    })?;

                match supervision_outcome.as_str() {
                    "running" => {
                        ensure(after.state == "running", "running supervision changed run state")?;
                        ensure(
                            after_task.state == "running",
                            "running supervision left task outside running state",
                        )?;
                        let after_expiry = after.claim_expires_at.ok_or_else(|| {
                            stateful_error("running supervision lost finite lease")
                        })?;
                        let requested_expiry = now + SignedDuration::seconds(i64::from(extension));
                        let renewal_threshold =
                            now + SignedDuration::milliseconds(i64::from(extension) * 500);
                        if let Some(before_expiry) = before_expiry {
                            ensure(
                                after_expiry >= before_expiry,
                                "run supervision shortened the existing finite lease",
                            )?;
                            if before_expiry <= renewal_threshold {
                                ensure(
                                    after_expiry >= requested_expiry,
                                    "run supervision did not renew inside PostgreSQL renewal window",
                                )?;
                            } else {
                                ensure(
                                    after_expiry == before_expiry,
                                    "run supervision renewed before the PostgreSQL renewal window",
                                )?;
                            }
                        }
                    }
                    "cancelled" => ensure(
                        after_task.state == "cancelled" || after.state == "cancelled",
                        "cancelled supervision outcome is not persisted",
                    )?,
                    "failed" => ensure(
                        after.state == "failed"
                            && after
                                .failure_reason
                                .as_ref()
                                .and_then(|failure| failure.get("name"))
                                .and_then(Value::as_str)
                                != Some("$LeaseExpired"),
                        "failed supervision outcome is not persisted",
                    )?,
                    "lease_lost" => ensure(
                        after.state == "failed"
                            && after
                                .failure_reason
                                .as_ref()
                                .and_then(|failure| failure.get("name"))
                                .and_then(Value::as_str)
                                == Some("$LeaseExpired"),
                        "lease-lost supervision outcome is not persisted",
                    )?,
                    "suspended" => ensure(
                        after.state == "sleeping",
                        "suspended supervision outcome is not persisted",
                    )?,
                    "completed" => ensure(
                        after.state == "completed",
                        "completed supervision outcome is not persisted",
                    )?,
                    other => {
                        return Err(stateful_error(format!(
                            "unknown supervision outcome {other:?}",
                        )));
                    }
                }

                let affected = usize::from(
                    before_run_state != after.state
                        || before_task_state != after_task.state
                        || before_expiry != after.claim_expires_at,
                );
                Ok(outcome(
                    format!(
                        "SUPERVISE {label} attempt={} +{extension}s -> {supervision_outcome}",
                        after.attempt,
                    ),
                    affected,
                ))
            }
            OperationKind::Fail => {
                let [run, reason, _, _, _, _] = operation.args;
                let Some(run_id) = select_run(bindings, run) else {
                    return Ok(outcome(
                        format!("FAIL selector={run} reason={reason} -> skipped: no runs exist"),
                        0,
                    ));
                };
                let label = run_label(bindings, run_id);
                let Some(status) = fetch_run(connection, queue, run_id).await? else {
                    return Ok(outcome(
                        format!("FAIL {label} reason={reason} -> skipped: run missing"),
                        0,
                    ));
                };
                let task_label = task_label(bindings, status.task_id);
                let now = current_time(connection).await?;
                if let Some(expected_sqlstate) = expected_run_rejection(&status, now) {
                    let result = sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
                        .bind(queue)
                        .bind(run_id)
                        .bind(json!({ "reason": reason }))
                        .execute(&mut *connection)
                        .await;
                    ensure_rejected(result, expected_sqlstate, "inapplicable failure succeeded")?;
                    return Ok(rejected_outcome(format!(
                        "FAIL {label} {task_label} attempt={} reason={reason} -> rejected by PostgreSQL",
                        status.attempt,
                    )));
                }

                let before_task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("failure run lost its task"))?;
                let retry_kind = before_task
                    .retry_strategy
                    .get("kind")
                    .and_then(Value::as_str)
                    .ok_or_else(|| stateful_error("task retry strategy has no kind"))?;
                let retry_available =
                    retry_kind != "none" && status.attempt < before_task.max_attempts;
                let duration_cancellation_due = before_task
                    .cancellation
                    .as_ref()
                    .and_then(|policy| policy.get("max_duration"))
                    .and_then(Value::as_i64)
                    .zip(before_task.first_started_at)
                    .is_some_and(|(max_duration, first_started_at)| {
                        (now - first_started_at).whole_seconds() >= max_duration
                    });

                sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
                    .bind(queue)
                    .bind(run_id)
                    .bind(json!({ "reason": reason }))
                    .execute(&mut *connection)
                    .await?;
                let transitioned_run = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("failure-transition run disappeared"))?;
                let task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("failure-transition task disappeared"))?;

                if duration_cancellation_due {
                    ensure(
                        transitioned_run.state == "cancelled",
                        "deadline-due failure did not cancel the owned run",
                    )?;
                    ensure(
                        transitioned_run.failure_reason.is_none(),
                        "deadline-due failure persisted an attempt failure",
                    )?;
                    ensure(
                        transitioned_run.claimed_by.is_none(),
                        "cancelled run retained its worker",
                    )?;
                    ensure(
                        transitioned_run.claim_expires_at.is_none(),
                        "cancelled run retained its lease",
                    )?;
                    ensure(
                        task.state == "cancelled",
                        "failure past max_duration did not cancel the task",
                    )?;
                    ensure(
                        task.attempts == status.attempt,
                        "duration cancellation changed attempt count",
                    )?;
                    ensure(
                        task.last_attempt_run == run_id,
                        "duration cancellation changed authoritative run",
                    )?;
                    return Ok(failed_outcome(
                        format!(
                            "FAIL {label} {task_label} attempt={} reason={reason} -> cancellation deadline won",
                            status.attempt,
                        ),
                        FailureOutcome::Cancelled,
                    ));
                }

                ensure(transitioned_run.state == "failed", "fail_run did not fail the owned run")?;
                ensure(transitioned_run.claimed_by.is_none(), "failed run retained its worker")?;
                ensure(
                    transitioned_run.claim_expires_at.is_none(),
                    "failed run retained its lease",
                )?;
                ensure(
                    transitioned_run.failure_reason == Some(json!({ "reason": reason })),
                    "failed run did not retain the submitted failure reason",
                )?;

                if !retry_available {
                    ensure(
                        task.state == "failed",
                        "failure without a retry budget was not terminal",
                    )?;
                    ensure(
                        task.attempts == status.attempt,
                        "terminal failure changed attempt count",
                    )?;
                    ensure(
                        task.last_attempt_run == run_id,
                        "terminal failure changed authoritative run",
                    )?;
                } else if matches!(task.state.as_str(), "pending" | "sleeping") {
                    ensure(
                        task.attempts == status.attempt + 1,
                        "retry did not advance the task attempt exactly once",
                    )?;
                    let next_run_id = task.last_attempt_run;
                    ensure(next_run_id != run_id, "retry reused the failed run")?;
                    let next_run = fetch_run(connection, queue, next_run_id)
                        .await?
                        .ok_or_else(|| stateful_error("retry run disappeared"))?;
                    ensure(
                        next_run.task_id == status.task_id,
                        "retry run belongs to another task",
                    )?;
                    ensure(
                        next_run.attempt == status.attempt + 1,
                        "retry run has the wrong attempt number",
                    )?;
                    ensure(next_run.state == task.state, "retry task/run states disagree")?;
                    ensure(next_run.claimed_by.is_none(), "new retry run is already owned")?;
                    ensure(
                        next_run.claim_expires_at.is_none(),
                        "new retry run already has a lease",
                    )?;
                    if retry_kind == "fixed"
                        && before_task.retry_strategy.get("base_seconds").and_then(Value::as_f64)
                            == Some(0.0)
                    {
                        ensure(
                            task.state == "pending",
                            "zero-delay retry was not immediately pending",
                        )?;
                        ensure(
                            next_run.available_at == now,
                            "zero-delay retry has a future availability",
                        )?;
                    } else if retry_kind == "exponential" {
                        ensure(task.state == "sleeping", "exponential retry was not scheduled")?;
                        ensure(
                            next_run.available_at > now,
                            "exponential retry was not scheduled in the future",
                        )?;
                    }
                } else if task.state == "cancelled" {
                    ensure(
                        before_task
                            .cancellation
                            .as_ref()
                            .and_then(|policy| policy.get("max_duration"))
                            .is_some(),
                        "retryable failure cancelled a task without max_duration",
                    )?;
                    ensure(
                        task.last_attempt_run == run_id,
                        "duration cancellation did not retain the failed authoritative run",
                    )?;
                } else {
                    return Err(stateful_error(format!(
                        "retryable fail_run left task in unexpected state {:?}",
                        task.state,
                    )));
                }

                let (consequence, failure_outcome) = match task.state.as_str() {
                    "pending" => ("retry ready", FailureOutcome::RetryReady),
                    "sleeping" => ("retry scheduled", FailureOutcome::RetryScheduled),
                    "failed" => {
                        ("attempts exhausted or retry disabled", FailureOutcome::TerminalFailed)
                    }
                    "cancelled" => ("cancellation policy ended task", FailureOutcome::Cancelled),
                    unexpected => {
                        return Err(stateful_error(format!(
                            "fail_run left task in unexpected state {unexpected:?}"
                        )));
                    }
                };
                Ok(failed_outcome(
                    format!(
                        "FAIL {label} {task_label} attempt={} reason={reason} -> run failed; task={} ({consequence})",
                        status.attempt, task.state,
                    ),
                    failure_outcome,
                ))
            }
            OperationKind::Complete => {
                let [run, value, _, _, _, _] = operation.args;
                let Some(run_id) = select_run(bindings, run) else {
                    return Ok(outcome(
                        format!("COMPLETE selector={run} result={value} -> skipped: no runs exist"),
                        0,
                    ));
                };
                let label = run_label(bindings, run_id);
                let Some(status) = fetch_run(connection, queue, run_id).await? else {
                    return Ok(outcome(
                        format!("COMPLETE {label} result={value} -> skipped: run missing"),
                        0,
                    ));
                };
                let task_label = task_label(bindings, status.task_id);
                let now = current_time(connection).await?;
                if let Some(expected_sqlstate) = expected_run_rejection(&status, now) {
                    let result =
                        sqlx::query_scalar::<_, bool>("SELECT steda.complete_run($1, $2, $3)")
                            .bind(queue)
                            .bind(run_id)
                            .bind(json!({ "value": value }))
                            .fetch_one(&mut *connection)
                            .await;
                    ensure_rejected(
                        result,
                        expected_sqlstate,
                        "inapplicable completion succeeded",
                    )?;
                    return Ok(rejected_outcome(format!(
                        "COMPLETE {label} {task_label} attempt={} result={value} -> rejected by PostgreSQL",
                        status.attempt,
                    )));
                }

                let completed: bool = sqlx::query_scalar("SELECT steda.complete_run($1, $2, $3)")
                    .bind(queue)
                    .bind(run_id)
                    .bind(json!({ "value": value }))
                    .fetch_one(&mut *connection)
                    .await?;
                let run = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("completed run disappeared"))?;
                let task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("completed task disappeared"))?;
                if completed {
                    ensure(
                        run.state == "completed",
                        "complete_run returned true without completing run",
                    )?;
                    ensure(
                        task.state == "completed",
                        "complete_run returned true without completing task",
                    )?;
                    ensure(
                        run.result == Some(json!({ "value": value })),
                        "completed run result mismatch",
                    )?;
                    ensure(run.claimed_by.is_none(), "completed run retained its worker")?;
                    ensure(run.claim_expires_at.is_none(), "completed run retained its lease")?;
                    Ok(outcome(
                        format!(
                            "COMPLETE {label} {task_label} attempt={} result={value} -> task completed",
                            status.attempt,
                        ),
                        1,
                    ))
                } else {
                    ensure(
                        run.state == "cancelled",
                        "complete_run returned false without cancelling run",
                    )?;
                    ensure(
                        task.state == "cancelled",
                        "complete_run returned false without cancelling task",
                    )?;
                    ensure(
                        task.cancellation
                            .as_ref()
                            .and_then(|policy| policy.get("max_duration"))
                            .is_some(),
                        "completion was cancelled without a max_duration policy",
                    )?;
                    Ok(completion_cancelled_outcome(format!(
                        "COMPLETE {label} {task_label} attempt={} result={value} -> max_duration elapsed; task cancelled",
                        status.attempt,
                    )))
                }
            }
            OperationKind::Cancel => {
                let [task, _, _, _, _, _] = operation.args;
                let Some(task_id) = select_task(bindings, task) else {
                    return Ok(outcome(
                        format!("CANCEL selector={task} -> skipped: no tasks exist"),
                        0,
                    ));
                };
                let label = task_label(bindings, task_id);
                let Some(before) = fetch_task(connection, queue, task_id).await? else {
                    return Ok(outcome(format!("CANCEL {label} -> skipped: task missing"), 0));
                };
                sqlx::query("SELECT steda.cancel_task($1, $2)")
                    .bind(queue)
                    .bind(task_id)
                    .execute(&mut *connection)
                    .await?;
                let after = fetch_task(connection, queue, task_id)
                    .await?
                    .ok_or_else(|| stateful_error("cancelled task disappeared"))?;
                if matches!(before.state.as_str(), "completed" | "failed" | "cancelled") {
                    ensure(after.state == before.state, "cancel_task changed a terminal task")?;
                    Ok(outcome(format!("CANCEL {label} -> unchanged: already {}", after.state), 0))
                } else {
                    ensure(
                        after.state == "cancelled",
                        "cancel_task did not cancel an active task",
                    )?;
                    ensure(
                        count_active_runs_for_task(connection, queue, task_id).await? == 0,
                        "cancel_task left an active run behind",
                    )?;
                    Ok(outcome(format!("CANCEL {label} {} -> cancelled", before.state), 1))
                }
            }
            OperationKind::Retry => {
                let [task, _, _, _, _, _] = operation.args;
                let Some(task_id) = select_task(bindings, task) else {
                    return Ok(outcome(
                        format!("RETRY selector={task} -> skipped: no tasks exist"),
                        0,
                    ));
                };
                let label = task_label(bindings, task_id);
                let Some(before) = fetch_task(connection, queue, task_id).await? else {
                    return Ok(outcome(format!("RETRY {label} -> skipped: task missing"), 0));
                };
                let now = current_time(connection).await?;
                if before.state != "failed" || max_duration_due(&before, now) {
                    let result = sqlx::query_scalar::<_, Uuid>("SELECT steda.retry_task($1, $2)")
                        .bind(queue)
                        .bind(task_id)
                        .fetch_one(&mut *connection)
                        .await;
                    ensure_rejected(result, None, "inapplicable manual retry succeeded")?;
                    let after = fetch_task(connection, queue, task_id)
                        .await?
                        .ok_or_else(|| stateful_error("rejected retry task disappeared"))?;
                    ensure(after.state == before.state, "rejected retry changed task state")?;
                    ensure(
                        after.attempts == before.attempts,
                        "rejected retry changed task attempts",
                    )?;
                    ensure(
                        after.max_attempts == before.max_attempts,
                        "rejected retry changed effective max_attempts",
                    )?;
                    ensure(
                        after.initial_max_attempts == before.initial_max_attempts,
                        "rejected retry changed initial_max_attempts",
                    )?;
                    return Ok(rejected_outcome(format!(
                        "RETRY {label} state={} -> rejected by PostgreSQL",
                        before.state,
                    )));
                }

                let run_id: Uuid = sqlx::query_scalar("SELECT steda.retry_task($1, $2)")
                    .bind(queue)
                    .bind(task_id)
                    .fetch_one(&mut *connection)
                    .await?;
                if !bindings.runs.contains(&run_id) {
                    bindings.runs.push(run_id);
                }
                let after = fetch_task(connection, queue, task_id)
                    .await?
                    .ok_or_else(|| stateful_error("manually retried task disappeared"))?;
                let run = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("manual retry run disappeared"))?;
                let next_attempt = before.attempts + 1;
                ensure(after.state == "pending", "manual retry did not make task pending")?;
                ensure(after.attempts == next_attempt, "manual retry attempt mismatch")?;
                ensure(
                    after.initial_max_attempts == before.initial_max_attempts,
                    "manual retry changed immutable initial_max_attempts",
                )?;
                ensure(
                    after.max_attempts == before.max_attempts.max(next_attempt),
                    "manual retry effective max_attempts mismatch",
                )?;
                ensure(after.last_attempt_run == run_id, "manual retry is not authoritative")?;
                ensure(run.task_id == task_id, "manual retry run belongs to another task")?;
                ensure(run.attempt == next_attempt, "manual retry run attempt mismatch")?;
                ensure(run.state == "pending", "manual retry run is not pending")?;
                ensure(run.available_at == now, "manual retry run is not immediately available")?;
                ensure(run.claimed_by.is_none(), "manual retry run is unexpectedly owned")?;
                ensure(run.claim_expires_at.is_none(), "manual retry run has a lease")?;
                Ok(outcome(
                    format!(
                        "RETRY {label} -> {} attempt={next_attempt} pending",
                        run_label(bindings, run_id),
                    ),
                    1,
                ))
            }
            OperationKind::Checkpoint => {
                let [run, step, value, _, _, _] = operation.args;
                let Some(run_id) = select_run(bindings, run) else {
                    return Ok(outcome(
                        format!(
                            "CHECKPOINT selector={run} {}={value} -> skipped: no runs exist",
                            checkpoint_name(step),
                        ),
                        0,
                    ));
                };
                let run_label = run_label(bindings, run_id);
                let Some(status) = fetch_run(connection, queue, run_id).await? else {
                    return Ok(outcome(
                        format!(
                            "CHECKPOINT {run_label} {}={value} -> skipped: run missing",
                            checkpoint_name(step),
                        ),
                        0,
                    ));
                };
                let task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("checkpoint run lost its task"))?;
                let now = current_time(connection).await?;
                let step_name = checkpoint_name(step);
                let expected_rejection = expected_run_rejection(&status, now)
                    .or_else(|| max_duration_due(&task, now).then_some(Some("AB001")));
                if let Some(expected_sqlstate) = expected_rejection {
                    let result = sqlx::query(
                        "SELECT checkpoint_state, written FROM steda.set_task_checkpoint_state($1, $2, $3, $4, $5)",
                    )
                    .bind(queue)
                    .bind(status.task_id)
                    .bind(step_name)
                    .bind(json!({ "value": value }))
                    .bind(run_id)
                    .fetch_one(&mut *connection)
                    .await;
                    ensure_rejected(
                        result,
                        expected_sqlstate,
                        "inapplicable checkpoint succeeded",
                    )?;
                    return Ok(rejected_outcome(format!(
                        "CHECKPOINT {run_label} {step_name}={value} -> rejected by PostgreSQL",
                    )));
                }

                let before =
                    fetch_checkpoint_state(connection, queue, status.task_id, step_name).await?;
                let existed = before.is_some();
                let row = sqlx::query(
                    "SELECT checkpoint_state, written FROM steda.set_task_checkpoint_state($1, $2, $3, $4, $5)",
                )
                .bind(queue)
                .bind(status.task_id)
                .bind(step_name)
                .bind(json!({ "value": value }))
                .bind(run_id)
                .fetch_one(&mut *connection)
                .await?;
                let checkpoint_state: Value = row.get("checkpoint_state");
                let written: bool = row.get("written");
                let expected_state = before.unwrap_or_else(|| json!({ "value": value }));
                ensure(checkpoint_state == expected_state, "checkpoint replay value changed")?;
                ensure(written == !existed, "checkpoint write flag mismatch")?;
                // First-write-wins is the semantic contract. A repeated name returns
                // the previously committed value without mutating it.
                let after = fetch_checkpoint_state(connection, queue, status.task_id, step_name)
                    .await?
                    .ok_or_else(|| stateful_error("checkpoint disappeared after write"))?;
                ensure(after == expected_state, "checkpoint storage changed on replay")?;
                let visible: Vec<(String, Value)> = sqlx::query_as(
                    "SELECT name, state FROM steda.get_task_checkpoint_states($1, $2, $3)",
                )
                .bind(queue)
                .bind(status.task_id)
                .bind(run_id)
                .fetch_all(&mut *connection)
                .await?;
                ensure(
                    visible
                        .iter()
                        .any(|(name, state)| name == step_name && state == &expected_state),
                    "committed checkpoint is not visible to its owning attempt",
                )?;
                let affected = usize::from(written);
                Ok(outcome(
                    format!(
                        "CHECKPOINT {run_label} {step_name}={value} -> {}",
                        if written { "written" } else { "replayed" },
                    ),
                    affected,
                ))
            }
            OperationKind::Sleep => {
                let [run, seconds, _, _, _, _] = operation.args;
                let Some(run_id) = select_run(bindings, run) else {
                    return Ok(outcome(
                        format!("SLEEP selector={run} +{seconds}s -> skipped: no runs exist"),
                        0,
                    ));
                };
                let label = run_label(bindings, run_id);
                let Some(status) = fetch_run(connection, queue, run_id).await? else {
                    return Ok(outcome(
                        format!("SLEEP {label} +{seconds}s -> skipped: run missing"),
                        0,
                    ));
                };
                let task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("sleep run lost its task"))?;
                let now = current_time(connection).await?;
                let wake_at = now + SignedDuration::seconds(i64::from(seconds));
                if let Some(expected_sqlstate) = expected_run_rejection(&status, now) {
                    let result =
                        sqlx::query_scalar::<_, String>("SELECT steda.schedule_run($1, $2, $3)")
                            .bind(queue)
                            .bind(run_id)
                            .bind(wake_at)
                            .fetch_one(&mut *connection)
                            .await;
                    ensure_rejected(result, expected_sqlstate, "inapplicable sleep succeeded")?;
                    return Ok(rejected_outcome(format!(
                        "SLEEP {label} +{seconds}s -> rejected by PostgreSQL",
                    )));
                }

                let outcome_name: String =
                    sqlx::query_scalar("SELECT steda.schedule_run($1, $2, $3)")
                        .bind(queue)
                        .bind(run_id)
                        .bind(wake_at)
                        .fetch_one(&mut *connection)
                        .await?;
                let after_run = fetch_run(connection, queue, run_id)
                    .await?
                    .ok_or_else(|| stateful_error("scheduled run disappeared"))?;
                let after_task = fetch_task(connection, queue, status.task_id)
                    .await?
                    .ok_or_else(|| stateful_error("scheduled task disappeared"))?;
                let deadline_due = max_duration_due(&task, now) || max_duration_due(&task, wake_at);
                match outcome_name.as_str() {
                    "cancelled" => {
                        ensure(deadline_due, "sleep cancelled without a max_duration deadline")?;
                        ensure(after_run.state == "cancelled", "cancelled sleep left run active")?;
                        ensure(
                            after_task.state == "cancelled",
                            "cancelled sleep left task active",
                        )?;
                        Ok(outcome(
                            format!("SLEEP {label} +{seconds}s -> cancellation deadline won"),
                            1,
                        ))
                    }
                    "ready" => {
                        ensure(!deadline_due, "sleep reported ready past cancellation deadline")?;
                        ensure(wake_at <= now, "future sleep unexpectedly reported ready")?;
                        ensure(after_run.state == "running", "ready sleep changed run state")?;
                        ensure(after_task.state == "running", "ready sleep changed task state")?;
                        ensure(
                            after_run.claimed_by == status.claimed_by,
                            "ready sleep changed run ownership",
                        )?;
                        ensure(
                            after_run.claim_expires_at == status.claim_expires_at,
                            "ready sleep changed run lease",
                        )?;
                        Ok(outcome(format!("SLEEP {label} +{seconds}s -> already ready"), 0))
                    }
                    "suspended" => {
                        ensure(!deadline_due, "sleep suspended past cancellation deadline")?;
                        ensure(wake_at > now, "non-future sleep was suspended")?;
                        ensure(after_run.state == "sleeping", "sleep did not suspend run")?;
                        ensure(after_task.state == "sleeping", "sleep did not suspend task")?;
                        ensure(after_run.available_at == wake_at, "sleep wake time mismatch")?;
                        ensure(after_run.claimed_by.is_none(), "sleeping run retained worker")?;
                        ensure(
                            after_run.claim_expires_at.is_none(),
                            "sleeping run retained lease",
                        )?;
                        Ok(outcome(format!("SLEEP {label} +{seconds}s -> suspended"), 1))
                    }
                    other => Err(stateful_error(format!(
                        "schedule_run returned unknown outcome {other:?}",
                    ))),
                }
            }
            OperationKind::AdvanceTime => {
                let [seconds, _, _, _, _, _] = operation.args;
                let before = current_time(connection).await?;
                advance_time(connection, seconds).await?;
                let now = current_time(connection).await?;
                let expected = before + SignedDuration::seconds(i64::from(seconds));
                ensure(
                    now == expected,
                    format!("logical clock mismatch: expected {expected:?}, got {now:?}"),
                )?;
                Ok(outcome(format!("TIME +{seconds}s -> {now:?}"), if seconds > 0 { 1 } else { 0 }))
            }
            OperationKind::ReapExpired => {
                let [limit, _, _, _, _, _] = operation.args;
                let expired_before = count_expired_runs(connection, queue).await?;
                let expected = expired_before.min(i64::from(limit));
                let reaped: i32 = sqlx::query_scalar("SELECT steda.reap_expired_runs($1, $2)")
                    .bind(queue)
                    .bind(i32::from(limit))
                    .fetch_one(&mut *connection)
                    .await?;
                ensure(
                    i64::from(reaped) == expected,
                    format!(
                        "reaper count mismatch: expected {expected} from {expired_before} expired run(s), got {reaped}",
                    ),
                )?;
                if reaped == 0 {
                    Ok(outcome(format!("REAP expired leases limit={limit} -> none expired"), 0))
                } else {
                    Ok(outcome(
                        format!(
                            "REAP expired leases limit={limit} -> reaped {reaped} expired run{}",
                            if reaped == 1 { "" } else { "s" },
                        ),
                        usize::try_from(reaped).unwrap_or_default(),
                    ))
                }
            }
            OperationKind::CancelExpired => {
                let [limit, _, _, _, _, _] = operation.args;
                let expired_before = count_policy_expired_tasks(connection, queue).await?;
                let expected = expired_before.min(i64::from(limit));
                let cancelled: i32 =
                    sqlx::query_scalar("SELECT steda.cancel_expired_tasks($1, $2)")
                        .bind(queue)
                        .bind(i32::from(limit))
                        .fetch_one(&mut *connection)
                        .await?;
                ensure(
                    i64::from(cancelled) == expected,
                    format!(
                        "policy cancellation count mismatch: expected {expected} from {expired_before} expired task(s), got {cancelled}",
                    ),
                )?;
                if cancelled == 0 {
                    Ok(outcome(
                        format!(
                            "CANCEL_EXPIRED limit={limit} -> no task exceeded its cancellation policy"
                        ),
                        0,
                    ))
                } else {
                    Ok(outcome(
                        format!(
                            "CANCEL_EXPIRED limit={limit} -> cancelled {cancelled} task{}",
                            if cancelled == 1 { "" } else { "s" },
                        ),
                        usize::try_from(cancelled).unwrap_or_default(),
                    ))
                }
            }
        }
    }

    async fn run_history(pool: &PgPool, history: &[Operation]) -> StatefulResult<Coverage> {
        let queue = unique_queue("stateful");
        let trace = env_flag("STEDA_STATEFUL_TRACE");
        let mut connection = pool.acquire().await?;
        set_initial_time(&mut connection).await?;
        sqlx::query("SELECT steda.create_queue($1)").bind(&queue).execute(&mut *connection).await?;

        let result = async {
            let mut bindings = Bindings::default();
            let mut coverage = Coverage::default();
            let initial_options = json!({
                "max_attempts": 3,
                "retry_strategy": { "kind": "fixed", "base_seconds": 0.0 },
                "idempotency_key": "stateful-initial"
            });
            let initial =
                spawn(&mut connection, &queue, "alpha", 0, initial_options.clone()).await?;
            ensure(initial.created, "initial stateful task was unexpectedly replayed")?;
            bindings.tasks.push(initial.task_id);
            bindings.runs.push(initial.run_id);
            bindings.spawns.push(SpawnRequest {
                task_id: initial.task_id,
                name: "alpha".to_owned(),
                payload: 0,
                options: initial_options,
            });
            refresh_bindings(&mut connection, &queue, &mut bindings).await?;
            audit_invariants(&mut connection, &queue).await?;

            if trace {
                eprintln!("[stateful setup] task#0 alpha -> run#0 pending; invariants hold");
            }

            for (index, operation) in history.iter().copied().enumerate() {
                let step = index + 1;
                let outcome = apply_operation(&mut connection, &queue, &mut bindings, operation)
                    .await
                    .map_err(|error| {
                        stateful_error(format!("step {step} {operation:?}: {error}"))
                    })?;
                refresh_bindings(&mut connection, &queue, &mut bindings).await.map_err(
                    |error| {
                        stateful_error(format!(
                            "step {step} after {}: refresh failed: {error}",
                            outcome.message,
                        ))
                    },
                )?;
                audit_invariants(&mut connection, &queue).await.map_err(|error| {
                    stateful_error(format!("step {step} after {}: {error}", outcome.message))
                })?;
                coverage.record(operation.kind, &outcome);
                if trace {
                    eprintln!("[stateful step {step}] {}; invariants hold", outcome.message);
                }
            }
            Ok(coverage)
        }
        .await;

        let cleanup =
            sqlx::query("SELECT steda.drop_queue($1)").bind(&queue).execute(&mut *connection).await;
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
            (Ok(coverage), Ok(_)) => Ok(coverage),
        }
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn generated_histories_preserve_postgres_contracts(pool: PgPool) {
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let strategy = history_strategy();
            let config = stateful_config();
            let cases = config.cases;
            let mut runner = TestRunner::new(config);
            let coverage = RefCell::new(Coverage::default());

            let result = runner.run(&strategy, |history| {
                let history_coverage = runtime
                    .block_on(run_history(&pool, &history))
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                coverage.borrow_mut().merge(&history_coverage);
                Ok(())
            });
            if let Err(error) = result {
                panic!("PostgreSQL stateful property failed: {error}");
            }
            let coverage = coverage.into_inner();
            eprintln!(
                "[stateful] PASS: {cases} histories, {} generated transitions; PostgreSQL invariants and operation contracts held after every transition",
                coverage.steps(),
            );
            eprintln!(
                "[stateful] workload: generated_spawns={} | idempotent_replays={} | runs_claimed={} ({} empty claims) | supervised_run_changes={}",
                coverage.affected(OperationKind::Spawn),
                coverage.affected(OperationKind::Replay),
                coverage.affected(OperationKind::Claim),
                coverage.not_applied(OperationKind::Claim),
                coverage.affected(OperationKind::Supervise),
            );
            eprintln!(
                "[stateful] durable control: manual_retries={} | checkpoint_writes={} ({} replay/skipped) | sleeps_suspended_or_cancelled={} ({} ready/skipped)",
                coverage.affected(OperationKind::Retry),
                coverage.affected(OperationKind::Checkpoint),
                coverage.not_applied(OperationKind::Checkpoint),
                coverage.affected(OperationKind::Sleep),
                coverage.not_applied(OperationKind::Sleep),
            );
            eprintln!(
                "[stateful] outcomes: tasks_completed={} | completion_policy_cancellations={} | direct_task_cancellations={} | failed_runs={} -> \
                 retry_ready={}, retry_scheduled={}, task_failed={}, task_cancelled={}",
                coverage
                    .affected(OperationKind::Complete)
                    .saturating_sub(coverage.completion_cancellations),
                coverage.completion_cancellations,
                coverage.affected(OperationKind::Cancel),
                coverage.affected(OperationKind::Fail),
                coverage.failure_outcome(FailureOutcome::RetryReady),
                coverage.failure_outcome(FailureOutcome::RetryScheduled),
                coverage.failure_outcome(FailureOutcome::TerminalFailed),
                coverage.failure_outcome(FailureOutcome::Cancelled),
            );
            eprintln!(
                "[stateful] maintenance: nonzero_time_advances={}/{} calls | expired_runs_reaped={} across {} sweeps | \
                 policy_cancellations={} across {} sweeps",
                coverage.affected(OperationKind::AdvanceTime),
                coverage.attempted(OperationKind::AdvanceTime),
                coverage.affected(OperationKind::ReapExpired),
                coverage.attempted(OperationKind::ReapExpired),
                coverage.affected(OperationKind::CancelExpired),
                coverage.attempted(OperationKind::CancelExpired),
            );
            eprintln!(
                "[stateful] rejected mutations exercised against PostgreSQL: supervisions={} | failures={} | completions={} | retries={} | checkpoints={} | sleeps={}",
                coverage.rejected(OperationKind::Supervise),
                coverage.rejected(OperationKind::Fail),
                coverage.rejected(OperationKind::Complete),
                coverage.rejected(OperationKind::Retry),
                coverage.rejected(OperationKind::Checkpoint),
                coverage.rejected(OperationKind::Sleep),
            );
            eprintln!(
                "[stateful] no-op/skipped: supervision={} | fail={} | complete={} | cancel_terminal_or_missing={} | retry={} | checkpoint={} | sleep={}",
                coverage.not_applied(OperationKind::Supervise),
                coverage.not_applied(OperationKind::Fail),
                coverage.not_applied(OperationKind::Complete),
                coverage.not_applied(OperationKind::Cancel),
                coverage.not_applied(OperationKind::Retry),
                coverage.not_applied(OperationKind::Checkpoint),
                coverage.not_applied(OperationKind::Sleep),
            );
        })
        .await
        .expect("run PostgreSQL stateful property test");
    }
}
