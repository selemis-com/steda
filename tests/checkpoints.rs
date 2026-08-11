//! Durable checkpoint semantics.

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
    };
    use tokio::time::sleep;

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct ExpensiveCheckpoint;

    impl Step for ExpensiveCheckpoint {
        const NAME: &'static str = "expensive";
        type Output = i64;
    }

    #[derive(Clone, Copy, Debug)]
    struct SharedCheckpoint;

    impl Step for SharedCheckpoint {
        const NAME: &'static str = "shared";
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct SameCheckpoint;

    impl Step for SameCheckpoint {
        const NAME: &'static str = "same";
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct SameSleep;

    impl Sleep for SameSleep {
        const NAME: &'static str = "same";
    }

    #[derive(Clone, Copy, Debug)]
    struct CachedStep;

    impl Task for CachedStep {
        const NAME: &'static str = "cached-step";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct CheckpointOnly;

    impl Task for CheckpointOnly {
        const NAME: &'static str = "checkpoint-only";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct MixedWorkflowIdentity;

    impl Task for MixedWorkflowIdentity {
        const NAME: &'static str = "mixed-workflow-identity";
        type Input = Value;
        type Output = Value;
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
    #[sqlx::test(migrations = "./sql/migrations")]
    async fn step_checkpoint_is_reused_after_retry(pool: PgPool) -> Result<()> {
        let queue = unique_queue("checkpoints");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let step_executions = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task::<CachedStep>({
                let step_executions = step_executions.clone();
                let attempts = attempts.clone();
                move |_params: Value, ctx: TaskContext| {
                    let step_executions = step_executions.clone();
                    let attempts = attempts.clone();
                    async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        let executions_for_step = step_executions.clone();
                        let value: i64 = ctx
                            .step(ExpensiveCheckpoint, async move || {
                                executions_for_step.fetch_add(1, Ordering::SeqCst);
                                Ok(42)
                            })
                            .await?;
                        if attempt == 1 {
                            return Err(Error::InvalidOptions("retry after checkpoint".to_owned()));
                        }
                        Ok(json!({"value": value, "executions": step_executions.load(Ordering::SeqCst)}))
                    }
                }
            })
            .build()?;

        let spawned = app
            .spawn::<CachedStep>(json!({}))
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(step_executions.load(Ordering::SeqCst), 1);
        assert_eq!(
            fetch_checkpoints(&pool, &queue, spawned.id()).await?,
            vec![("$step:expensive".to_owned(), json!(42))]
        );

        run_worker_for_claims(&worker, app.metrics(), 1).await?;
        assert_eq!(step_executions.load(Ordering::SeqCst), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        assert_eq!(spawned.result().await?, json!({"value": 42, "executions": 1}));

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cloned_contexts_share_one_semantic_step(pool: PgPool) -> Result<()> {
        let queue = unique_queue("checkpoint_concurrent");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let executions = Arc::new(AtomicUsize::new(0));
        let worker = app
            .worker()
            .task::<CheckpointOnly>({
                let executions = Arc::clone(&executions);
                move |_params: Value, ctx: TaskContext| {
                    let first_ctx = ctx.clone();
                    let second_ctx = ctx;
                    let first_executions = Arc::clone(&executions);
                    let second_executions = Arc::clone(&executions);
                    async move {
                        let (first, second) = tokio::join!(
                            first_ctx.step(SharedCheckpoint, async move || {
                                first_executions.fetch_add(1, Ordering::SeqCst);
                                sleep(Duration::from_millis(50)).await;
                                Ok::<_, Error>(json!({"source": "first"}))
                            }),
                            second_ctx.step(SharedCheckpoint, async move || {
                                second_executions.fetch_add(1, Ordering::SeqCst);
                                Ok::<_, Error>(json!({"source": "second"}))
                            }),
                        );
                        let first = first?;
                        let second = second?;
                        assert_eq!(first, second);
                        Ok(json!({"checkpoint": first}))
                    }
                }
            })
            .build()?;

        let spawned = app.spawn::<CheckpointOnly>(json!({})).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        assert_eq!(executions.load(Ordering::SeqCst), 1);
        let result = spawned.result().await?;
        assert_eq!(
            fetch_checkpoints(&pool, &queue, spawned.id()).await?,
            vec![("$step:shared".to_owned(), result["checkpoint"].clone())]
        );

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn step_and_sleep_with_same_logical_name_use_distinct_namespaces(
        pool: PgPool,
    ) -> Result<()> {
        let queue = unique_queue("checkpoint_namespaces");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let worker = app
            .worker()
            .task::<MixedWorkflowIdentity>(async |_params: Value, ctx: TaskContext| {
                let value =
                    ctx.step(SameCheckpoint, async || Ok::<_, Error>(json!({"value": 1}))).await?;
                ctx.sleep_for(SameSleep, Duration::ZERO).await?;
                Ok(value)
            })
            .build()?;

        let spawned = app.spawn::<MixedWorkflowIdentity>(json!({})).await?;
        run_worker_for_claims(&worker, app.metrics(), 1).await?;

        let checkpoints = fetch_checkpoints(&pool, &queue, spawned.id()).await?;
        let names: Vec<_> = checkpoints.into_iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["$sleep:same".to_owned(), "$step:same".to_owned()]);
        assert_eq!(spawned.result().await?, json!({"value": 1}));

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn committed_checkpoint_is_immutable_across_attempts(pool: PgPool) -> Result<()> {
        let queue = unique_queue("checkpoint_immutable");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let spawned = app
            .spawn::<CheckpointOnly>(json!({}))
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;
        let run_id: RunId =
            sqlx::query_scalar("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1")
                .bind(&queue)
                .bind("checkpoint-worker")
                .bind(30_i32)
                .bind(1_i32)
                .bind(vec![CheckpointOnly::NAME.to_owned()])
                .fetch_one(&pool)
                .await?;

        let first = sqlx::query(
            "SELECT checkpoint_state, written FROM steda.set_task_checkpoint_state($1, $2, $3, $4, $5)",
        )
        .bind(&queue)
        .bind(spawned.id())
        .bind("immutable")
        .bind(json!({"value": 1}))
        .bind(run_id)
        .fetch_one(&pool)
        .await?;
        assert!(first.get::<bool, _>("written"));
        assert_eq!(first.get::<Value, _>("checkpoint_state"), json!({"value": 1}));

        sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
            .bind(&queue)
            .bind(run_id)
            .bind(json!({"name": "RetryForCheckpointTest"}))
            .execute(&pool)
            .await?;

        let second_run_id: RunId =
            sqlx::query_scalar("SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1")
                .bind(&queue)
                .bind("checkpoint-worker-2")
                .bind(30_i32)
                .bind(1_i32)
                .bind(vec![CheckpointOnly::NAME.to_owned()])
                .fetch_one(&pool)
                .await?;
        assert_ne!(run_id, second_run_id);

        let second = sqlx::query(
            "SELECT checkpoint_state, written FROM steda.set_task_checkpoint_state($1, $2, $3, $4, $5)",
        )
        .bind(&queue)
        .bind(spawned.id())
        .bind("immutable")
        .bind(json!({"value": 2}))
        .bind(second_run_id)
        .fetch_one(&pool)
        .await?;
        assert!(!second.get::<bool, _>("written"));
        assert_eq!(second.get::<Value, _>("checkpoint_state"), json!({"value": 1}));

        assert_eq!(
            fetch_checkpoints(&pool, &queue, spawned.id()).await?,
            vec![("immutable".to_owned(), json!({"value": 1}))]
        );

        app.delete().await?;
        Ok(())
    }
}
