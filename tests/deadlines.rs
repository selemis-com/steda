//! Cancellation deadlines and durable sleep tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool, Row};
    use steda::{
        Error, Result, RetryStrategy, RunId, Sleep, Steda, Step, Task, TaskContext, TaskId,
        TaskSnapshot, TaskState,
    };
    use time::OffsetDateTime;

    use super::{
        common::{install_fake_clock, unique_queue},
        worker_support::run_worker_for_claims,
    };

    const LATE_CHECKPOINT: Step<Value> = Step::new("late");

    const DATABASE_WAIT: Sleep = Sleep::new("wait");

    const ABSOLUTE_WAIT: Sleep = Sleep::new("absolute-wait");

    const CANCEL_BEFORE_START: Task<Value, Value> = Task::new("cancel-before-start");

    const CHECKPOINT_ONLY: Task<Value, Value> = Task::new("checkpoint-only");

    const DATABASE_SLEEP: Task<Value, Value> = Task::new("database-sleep");

    const SLEEP_UNTIL_REPLAY: Task<Value, Value> = Task::new("sleep-until-replay");

    const DEADLINE: Task<Value, Value> = Task::new("deadline");

    const DEADLINE_FAILURE: Task<Value, Value> = Task::new("deadline-failure");

    const EXPIRED_BEFORE_START: Task<Value, Value> = Task::new("expired-before-start");

    const HANG_PAST_DURATION: Task<Value, Value> = Task::new("hang-past-duration");

    struct TaskRow {
        state: String,
        attempts: i32,
        last_attempt_run: RunId,
    }

    async fn fetch_task(pool: &PgPool, queue: &str, task_id: TaskId) -> Result<TaskRow> {
        let query = format!(
            "SELECT state, attempts, last_attempt_run FROM steda.tasks_{queue} WHERE id = $1"
        );
        let row = sqlx::query(AssertSqlSafe(query)).bind(task_id).fetch_one(pool).await?;
        Ok(TaskRow {
            state: row.get("state"),
            attempts: row.get("attempts"),
            last_attempt_run: row.get("last_attempt_run"),
        })
    }

    async fn set_fake_now(pool: &PgPool, now: OffsetDateTime) -> Result<()> {
        install_fake_clock(pool).await?;
        let mut connections = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            let mut connection = pool.acquire().await?;
            sqlx::query("SELECT set_config('steda.fake_now', $1, false)")
                .bind(now.to_string())
                .execute(&mut *connection)
                .await?;
            connections.push(connection);
        }
        drop(connections);
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_enforces_max_duration_for_hanging_handler(pool: PgPool) -> Result<()> {
        let queue = unique_queue("worker_duration");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(HANG_PAST_DURATION, async |_params: Value, _ctx| {
                std::future::pending::<Result<Value>>().await
            })
            .build()?;

        let spawned = app
            .spawn(HANG_PAST_DURATION, json!({}))
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::ZERO))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn max_duration_cancels_completion_after_deadline(pool: PgPool) -> Result<()> {
        let queue = unique_queue("max_duration_complete");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(DEADLINE, async |_params: Value, _ctx| Ok(json!({"too_late": true})))
            .build()?;

        let spawned = app
            .spawn(DEADLINE, json!({}))
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::ZERO))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn max_duration_cancels_failure_without_retry_budget(pool: PgPool) -> Result<()> {
        let queue = unique_queue("max_duration_fail");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(DEADLINE_FAILURE, async |_params: Value, _ctx| {
                Err::<Value, _>(Error::Other("boom".to_owned()))
            })
            .build()?;

        let spawned = app
            .spawn(DEADLINE_FAILURE, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::ZERO))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        let task = fetch_task(&pool, &queue, spawned.task_id()).await?;
        let run_id = task.last_attempt_run;
        let run_table = format!("runs_{queue}");
        let query = format!("SELECT state, failure_reason FROM steda.{run_table} WHERE id = $1");
        let row = sqlx::query(AssertSqlSafe(query)).bind(run_id).fetch_one(&pool).await?;
        assert_eq!(row.get::<String, _>("state"), "cancelled");
        assert_eq!(row.get::<Option<Value>, _>("failure_reason"), None);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn checkpoint_cannot_commit_after_max_duration_deadline(pool: PgPool) -> Result<()> {
        let queue = unique_queue("checkpoint_deadline");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(CHECKPOINT_ONLY, async |_params: Value, ctx: TaskContext| {
                ctx.step(LATE_CHECKPOINT, async || Ok::<_, Error>(json!({"value": 1}))).await?;
                Ok(json!({"done": true}))
            })
            .build()?;

        let spawned = app
            .spawn(CHECKPOINT_ONLY, json!({}))
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::ZERO))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        let query = format!("SELECT count(*) FROM steda.checkpoints_{queue} WHERE task_id = $1");
        let checkpoint_count: i64 = sqlx::query_scalar(AssertSqlSafe(query))
            .bind(spawned.task_id())
            .fetch_one(&pool)
            .await?;
        assert_eq!(checkpoint_count, 0);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn manual_retry_cannot_cross_max_duration_deadline(pool: PgPool) -> Result<()> {
        let queue = unique_queue("manual_retry_deadline");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(DEADLINE_FAILURE, async |_params: Value, _ctx| {
                Err::<Value, _>(Error::Other("boom".to_owned()))
            })
            .build()?;

        let spawned = app
            .spawn(DEADLINE_FAILURE, json!({}))
            .max_attempts(1)
            .retry_strategy(RetryStrategy::none())
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::from_secs(3_600)))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        let before = fetch_task(&pool, &queue, spawned.task_id()).await?;
        assert_eq!(before.state, "failed");

        let now: OffsetDateTime =
            sqlx::query_scalar("SELECT steda.current_time()").fetch_one(&pool).await?;
        set_fake_now(&pool, now + time::Duration::hours(1)).await?;

        assert!(spawned.retry().await.is_err());
        let after = fetch_task(&pool, &queue, spawned.task_id()).await?;
        assert_eq!(after.state, "failed");
        assert_eq!(after.attempts, before.attempts);
        assert_eq!(after.last_attempt_run, before.last_attempt_run);

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn durable_sleep_that_crosses_max_duration_cancels_immediately(
        pool: PgPool,
    ) -> Result<()> {
        let queue = unique_queue("sleep_deadline");
        let app = Steda::from_pool(pool).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(DATABASE_SLEEP, async |_params: Value, ctx: TaskContext| {
                ctx.sleep_for(DATABASE_WAIT, Duration::from_secs(30)).await?;
                Ok(json!({"awake": true}))
            })
            .build()?;

        let spawned = app
            .spawn(DATABASE_SLEEP, json!({}))
            .cancellation(steda::CancellationPolicy::new().max_duration(Duration::from_secs(10)))
            .await?;

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn concurrent_cancellation_sweeps_count_one_transition(pool: PgPool) -> Result<()> {
        let queue = unique_queue("cancel_sweep_count");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let spawned = app
            .spawn(CANCEL_BEFORE_START, json!({}))
            .cancellation(steda::CancellationPolicy::new().max_delay(Duration::ZERO))
            .await?;

        let first = sqlx::query_scalar::<_, i32>("SELECT steda.cancel_expired_tasks($1, 1)")
            .bind(&queue)
            .fetch_one(&pool);
        let second = sqlx::query_scalar::<_, i32>("SELECT steda.cancel_expired_tasks($1, 1)")
            .bind(&queue)
            .fetch_one(&pool);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first? + second?, 1);
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Cancelled));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn claim_never_returns_tasks_past_max_delay(pool: PgPool) -> Result<()> {
        let queue = unique_queue("max_delay_claim");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        for _ in 0..3 {
            app.spawn(EXPIRED_BEFORE_START, json!({}))
                .cancellation(steda::CancellationPolicy::new().max_delay(Duration::ZERO))
                .await?;
        }

        // Cancel only one expired task so the claim query still has expired,
        // non-terminal rows to filter out on its own.
        let cancelled: i32 = sqlx::query_scalar("SELECT steda.cancel_expired_tasks($1, 1)")
            .bind(&queue)
            .fetch_one(&pool)
            .await?;
        assert_eq!(cancelled, 1);

        let claimed: Vec<RunId> =
            sqlx::query_scalar("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5)")
                .bind(&queue)
                .bind("max-delay-worker")
                .bind(30_i32)
                .bind(3_i32)
                .bind(vec![EXPIRED_BEFORE_START.name().to_owned()])
                .fetch_all(&pool)
                .await?;
        assert!(claimed.is_empty());

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn sleep_until_replays_the_original_wake_time(pool: PgPool) -> Result<()> {
        let queue = unique_queue("sleep_until_replay");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let base = OffsetDateTime::from_unix_timestamp(2_100_100_000)
            .map_err(|err| Error::InvalidOptions(err.to_string()))?;
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task(SLEEP_UNTIL_REPLAY, {
                let calls = Arc::clone(&calls);
                move |_params: Value, ctx: TaskContext| {
                    let calls = Arc::clone(&calls);
                    async move {
                        let invocation = calls.fetch_add(1, Ordering::SeqCst);
                        let wake_at = if invocation == 0 {
                            base + time::Duration::seconds(30)
                        } else {
                            base + time::Duration::minutes(5)
                        };
                        ctx.sleep_until(ABSOLUTE_WAIT, wake_at).await?;
                        Ok(json!({"awake": true}))
                    }
                }
            })
            .build()?;

        set_fake_now(&pool, base).await?;
        let spawned = app.spawn(SLEEP_UNTIL_REPLAY, json!({})).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let sleeping = spawned.snapshot().await?.expect("sleeping task must have a snapshot");
        assert_eq!(sleeping.state(), TaskState::Sleeping);
        assert!(!sleeping.is_terminal());

        let run_id = fetch_task(&pool, &queue, spawned.task_id()).await?.last_attempt_run;
        let query = format!("SELECT available_at FROM steda.runs_{queue} WHERE id = $1");
        let available_at: OffsetDateTime =
            sqlx::query_scalar(AssertSqlSafe(query)).bind(run_id).fetch_one(&pool).await?;
        assert_eq!(available_at, base + time::Duration::seconds(30));

        set_fake_now(&pool, base + time::Duration::seconds(31)).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let completed = spawned.snapshot().await?.expect("completed task must have a snapshot");
        assert_eq!(completed.state(), TaskState::Completed);
        assert!(completed.is_terminal());
        assert_eq!(completed, TaskSnapshot::Completed { result: json!({"awake": true}) });

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn steda_sleep_uses_database_clock(pool: PgPool) -> Result<()> {
        let queue = unique_queue("database_sleep_clock");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task(DATABASE_SLEEP, async |_params: Value, ctx: TaskContext| {
                ctx.sleep_for(DATABASE_WAIT, Duration::from_secs(30)).await?;
                Ok(json!({"awake": true}))
            })
            .build()?;

        let base = OffsetDateTime::from_unix_timestamp(2_100_000_000)
            .map_err(|err| Error::InvalidOptions(err.to_string()))?;
        set_fake_now(&pool, base).await?;

        let spawned = app.spawn(DATABASE_SLEEP, json!({})).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let run_id = fetch_task(&pool, &queue, spawned.task_id()).await?.last_attempt_run;
        let query = format!("SELECT available_at FROM steda.runs_{queue} WHERE id = $1");
        let available_at: OffsetDateTime =
            sqlx::query_scalar(AssertSqlSafe(query)).bind(run_id).fetch_one(&pool).await?;
        assert_eq!(available_at, base + time::Duration::seconds(30));

        set_fake_now(&pool, base + time::Duration::seconds(31)).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(
            spawned.snapshot().await?,
            Some(TaskSnapshot::Completed { result: json!({"awake": true}) })
        );

        app.delete().await?;
        Ok(())
    }
}
