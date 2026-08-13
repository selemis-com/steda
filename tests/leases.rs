//! Run ownership, leases, and fencing tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool, Row};
    use steda::{
        Result, RetryStrategy, RunId, Steda, Step, Task, TaskContext, TaskId, TaskSnapshot,
    };
    use time::OffsetDateTime;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const STALE_CHECKPOINT: Step<Value> = Step::new("stale-checkpoint");

    const ALPHA_ONLY: Task<Value, Value> = Task::new("alpha-only");

    const CLAIMED_ONLY: Task<Value, Value> = Task::new("claimed-only");

    const HANG_PAST_LEASE: Task<Value, Value> = Task::new("hang-past-lease");

    const LOSE_LEASE: Task<Value, Value> = Task::new("lose-lease");

    const OTHER_TASK: Task<Value, Value> = Task::new("other-task");

    struct TaskRow {
        state: String,
        last_attempt_run: RunId,
    }

    async fn fetch_task(pool: &PgPool, queue: &str, task_id: TaskId) -> Result<TaskRow> {
        let query =
            format!("SELECT state, last_attempt_run FROM steda.tasks_{queue} WHERE id = $1");
        let row = sqlx::query(AssertSqlSafe(query)).bind(task_id).fetch_one(pool).await?;
        Ok(TaskRow { state: row.get("state"), last_attempt_run: row.get("last_attempt_run") })
    }

    async fn fetch_checkpoints(
        pool: &PgPool,
        queue: &str,
        task_id: TaskId,
    ) -> Result<Vec<(String, Value)>> {
        let query = format!(
            "SELECT name, state FROM steda.checkpoints_{queue} WHERE task_id = $1 ORDER BY name"
        );
        let rows = sqlx::query(AssertSqlSafe(query)).bind(task_id).fetch_all(pool).await?;
        Ok(rows.into_iter().map(|row| (row.get("name"), row.get("state"))).collect())
    }

    async fn force_expire_run(pool: &PgPool, queue: &str, run_id: RunId) -> Result<()> {
        let query = format!(
            "UPDATE steda.runs_{queue} SET claim_expires_at = steda.current_time() - interval '1 second' WHERE id = $1"
        );
        sqlx::query(AssertSqlSafe(query)).bind(run_id).execute(pool).await?;
        Ok(())
    }

    fn assert_sqlstate(error: &sqlx::Error, expected: &str) {
        let sqlx::Error::Database(database_error) = error else {
            panic!("expected database error {expected}, got {error:?}");
        };
        assert_eq!(database_error.code().as_deref(), Some(expected));
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn expired_claim_rejects_all_worker_owned_transitions(pool: PgPool) -> Result<()> {
        let queue = unique_queue("lease_fence_sql");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let first = app
            .spawn(CLAIMED_ONLY, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;
        let run_id: RunId =
            sqlx::query_scalar("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1")
                .bind(&queue)
                .bind("lease-sql-worker")
                .bind(30_i32)
                .bind(1_i32)
                .bind(vec![CLAIMED_ONLY.name().to_owned()])
                .fetch_one(&pool)
                .await?;

        let second = app.spawn(OTHER_TASK, json!({})).await?;
        let ownership_error =
            sqlx::query("SELECT steda.set_task_checkpoint_state($1, $2, $3, $4, $5)")
                .bind(&queue)
                .bind(second.task_id())
                .bind("cross-task")
                .bind(json!({"bad": true}))
                .bind(run_id)
                .execute(&pool)
                .await
                .expect_err("a run must not checkpoint another task");
        assert!(ownership_error.to_string().contains("does not belong to task"));

        force_expire_run(&pool, &queue, run_id).await?;

        let expired_transitions = [
            sqlx::query("SELECT steda.complete_run($1, $2, $3)")
                .bind(&queue)
                .bind(run_id)
                .bind(json!({"late": true}))
                .execute(&pool)
                .await
                .expect_err("expired completion must fail"),
            sqlx::query("SELECT steda.schedule_run($1, $2, steda.current_time())")
                .bind(&queue)
                .bind(run_id)
                .execute(&pool)
                .await
                .expect_err("expired suspension must fail"),
            sqlx::query("SELECT steda.set_task_checkpoint_state($1, $2, $3, $4, $5)")
                .bind(&queue)
                .bind(first.task_id())
                .bind("late-checkpoint")
                .bind(json!({"late": true}))
                .bind(run_id)
                .execute(&pool)
                .await
                .expect_err("expired checkpoint must fail"),
            sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
                .bind(&queue)
                .bind(run_id)
                .bind(json!({"name": "LateFailure"}))
                .execute(&pool)
                .await
                .expect_err("expired worker failure must fail"),
        ];
        for error in &expired_transitions {
            assert_sqlstate(error, "ST003");
        }
        assert!(fetch_checkpoints(&pool, &queue, first.task_id()).await?.is_empty());

        let reaped: bool = sqlx::query_scalar("SELECT steda.reap_expired_run($1, $2)")
            .bind(&queue)
            .bind(run_id)
            .fetch_one(&pool)
            .await?;
        assert!(reaped);
        assert_eq!(fetch_task(&pool, &queue, first.task_id()).await?.state, "failed");

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn expired_lease_is_typed_and_rejects_stale_worker_progress(pool: PgPool) -> Result<()> {
        let queue = unique_queue("lease_fence_rust");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let checkpoint_rejected = Arc::new(AtomicBool::new(false));
        let worker = app
            .worker()
            .task(LOSE_LEASE, {
                let pool = pool.clone();
                let queue = queue.clone();
                let checkpoint_rejected = checkpoint_rejected.clone();
                move |_params: Value, ctx: TaskContext| {
                    let pool = pool.clone();
                    let queue = queue.clone();
                    let checkpoint_rejected = checkpoint_rejected.clone();
                    async move {
                        force_expire_run(&pool, &queue, ctx.run_id()).await?;

                        let checkpoint_result: Result<Value> =
                            ctx.step(STALE_CHECKPOINT, async || Ok(json!({"bad": true}))).await;
                        let checkpoint_error =
                            checkpoint_result.expect_err("expired checkpoint must lose ownership");
                        checkpoint_rejected
                            .store(checkpoint_error.is_lease_lost(), Ordering::SeqCst);

                        Ok(json!({"late": true}))
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn(LOSE_LEASE, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert!(checkpoint_rejected.load(Ordering::SeqCst));
        assert!(fetch_checkpoints(&pool, &queue, spawned.task_id()).await?.is_empty());
        assert_eq!(fetch_task(&pool, &queue, spawned.task_id()).await?.state, "running");

        let metrics = app.metrics();
        assert_eq!(metrics.lease_lost_executions(), 1);
        assert_eq!(metrics.unhandled_executions(), 0);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_reaps_its_own_expired_lease(pool: PgPool) -> Result<()> {
        let queue = unique_queue("worker_lease");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(HANG_PAST_LEASE, {
                let pool = pool.clone();
                let queue = queue.clone();
                move |_params: Value, ctx: TaskContext| {
                    let pool = pool.clone();
                    let queue = queue.clone();
                    async move {
                        force_expire_run(&pool, &queue, ctx.run_id()).await?;
                        std::future::pending::<Result<Value>>().await
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn(HANG_PAST_LEASE, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(app.metrics().lease_lost_executions(), 1);
        let task = fetch_task(&pool, &queue, spawned.task_id()).await?;
        assert_eq!(task.state, "failed");
        let run_id = task.last_attempt_run;

        let run_table = format!("runs_{queue}");
        let query = format!("SELECT failure_reason FROM steda.{run_table} WHERE id = $1");
        let row = sqlx::query(AssertSqlSafe(query)).bind(run_id).fetch_one(&pool).await?;
        let failure_reason: Option<Value> = row.get("failure_reason");
        assert_eq!(
            failure_reason.as_ref().and_then(|failure| failure.get("name")).and_then(Value::as_str),
            Some("$LeaseExpired")
        );

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn claims_require_explicit_finite_ownership(pool: PgPool) -> Result<()> {
        let queue = unique_queue("claim_ownership");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let spawned = app.spawn(ALPHA_ONLY, json!({})).await?;
        let capabilities = vec![ALPHA_ONLY.name().to_owned()];

        let zero_lease = sqlx::query("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5)")
            .bind(&queue)
            .bind("claim-owner")
            .bind(0_i32)
            .bind(1_i32)
            .bind(&capabilities)
            .execute(&pool)
            .await;
        assert!(zero_lease.is_err());

        let no_capabilities =
            sqlx::query("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5)")
                .bind(&queue)
                .bind("claim-owner")
                .bind(30_i32)
                .bind(1_i32)
                .bind(Vec::<String>::new())
                .execute(&pool)
                .await;
        assert!(no_capabilities.is_err());
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Pending));

        let row = sqlx::query("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1")
            .bind(&queue)
            .bind("claim-owner")
            .bind(30_i32)
            .bind(1_i32)
            .bind(&capabilities)
            .fetch_one(&pool)
            .await?;
        let run_id: RunId = row.get("run_id");
        let run_table = format!("runs_{queue}");
        let run = sqlx::query(AssertSqlSafe(format!(
            "SELECT state, claimed_by, claim_expires_at FROM steda.{run_table} WHERE id = $1"
        )))
        .bind(run_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(run.get::<String, _>("state"), "running");
        assert_eq!(run.get::<Option<String>, _>("claimed_by").as_deref(), Some("claim-owner"));
        assert!(run.get::<Option<OffsetDateTime>, _>("claim_expires_at").is_some());

        app.delete().await?;
        Ok(())
    }
}
