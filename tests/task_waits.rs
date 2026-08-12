//! Cross-queue task result waiting tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{Error, Result, RetryStrategy, Steda, Task, TaskContext, TaskRef, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const CHILD: Task<Value, Value> = Task::new("wait-child");

    const PARENT: Task<TaskRef<Value, Value>, Value> = Task::new("wait-parent");

    const SAME_QUEUE_WAIT: Task<TaskRef<Value, Value>, Value> = Task::new("same-queue-wait");

    const REPLAY_NAME_PARENT: Task<TaskRef<Value, Value>, Value> = Task::new("replay-name-parent");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cross_queue_wait_is_reused_from_checkpoint_after_retry(pool: PgPool) -> Result<()> {
        let child_name = unique_queue("wait_child");
        let parent_name = unique_queue("wait_parent");
        let steda = Steda::from_pool(pool);
        let child_queue = steda.queue(child_name.clone())?;
        let parent_queue = steda.queue(parent_name)?;
        child_queue.create().await?;
        parent_queue.create().await?;

        let child_worker = child_queue
            .worker()
            .task(CHILD, async |_params: Value, _ctx| Ok(json!({"value": 42})))
            .build()?;
        let child = child_queue.spawn(CHILD, json!({})).await?;
        run_worker_for_claims(&child_worker, child_queue.metrics(), 1).await?;
        assert_eq!(child.result().await?, json!({"value": 42}));

        let parent_worker = parent_queue
            .worker()
            .task(PARENT, async |child: TaskRef<Value, Value>, ctx: TaskContext| {
                let result = ctx.await_task(&child).await?;
                if ctx.attempt() == 1 {
                    return Err(Error::InvalidOptions(
                        "retry after durable child result".to_owned(),
                    ));
                }
                Ok(result)
            })
            .build()?;

        let parent = parent_queue
            .spawn(PARENT, child.task_ref())
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;

        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;
        child_queue.delete().await?;
        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;

        assert_eq!(parent.result().await?, json!({"value": 42}));

        parent_queue.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn replayed_task_wait_keeps_concrete_task_name(pool: PgPool) -> Result<()> {
        let child_name = unique_queue("wait_name_child");
        let parent_name = unique_queue("wait_name_parent");
        let steda = Steda::from_pool(pool);
        let child_queue = steda.queue(child_name)?;
        let parent_queue = steda.queue(parent_name)?;
        child_queue.create().await?;
        parent_queue.create().await?;

        let child_worker = child_queue
            .worker()
            .task(CHILD, async |_params: Value, _ctx| Ok(json!({"value": 42})))
            .build()?;
        let child = child_queue.spawn(CHILD, json!({})).await?;
        run_worker_for_claims(&child_worker, child_queue.metrics(), 1).await?;

        let parent_worker = parent_queue
            .worker()
            .task(REPLAY_NAME_PARENT, async |child: TaskRef<Value, Value>, ctx: TaskContext| {
                if ctx.attempt() == 1 {
                    let _ = ctx.await_task(&child).await?;
                    return Err(Error::InvalidOptions(
                        "retry after durable child result".to_owned(),
                    ));
                }

                let mut encoded = serde_json::to_value(&child)?;
                encoded["taskName"] = json!("different-child-task");
                let mismatched: TaskRef<Value, Value> = serde_json::from_value(encoded)?;
                let error = ctx
                    .await_task(&mismatched)
                    .await
                    .expect_err("replayed wait must retain the original task name");
                Ok(json!({
                    "task_name_mismatch": matches!(error, Error::TaskNameMismatch { .. })
                }))
            })
            .build()?;

        let parent = parent_queue
            .spawn(REPLAY_NAME_PARENT, child.task_ref())
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;

        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;
        child_queue.delete().await?;
        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;

        assert_eq!(parent.result().await?, json!({"task_name_mismatch": true}));

        parent_queue.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cross_queue_wait_timeout_does_not_poison_retry(pool: PgPool) -> Result<()> {
        let child_name = unique_queue("wait_timeout_child");
        let parent_name = unique_queue("wait_timeout_parent");
        let steda = Steda::from_pool(pool.clone());
        let child_queue = steda.queue(child_name)?;
        let parent_queue = steda.queue(parent_name.clone())?;
        child_queue.create().await?;
        parent_queue.create().await?;

        let child_worker = child_queue
            .worker()
            .task(CHILD, async |_params: Value, _ctx| Ok(json!({"value": 42})))
            .build()?;
        let parent_worker = parent_queue
            .worker()
            .task(PARENT, async |child: TaskRef<Value, Value>, ctx: TaskContext| {
                ctx.await_task(&child).timeout(Duration::ZERO).await
            })
            .build()?;

        let child = child_queue.spawn(CHILD, json!({})).await?;
        let parent = parent_queue
            .spawn(PARENT, child.task_ref())
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(Duration::ZERO))
            .await?;

        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;
        assert_eq!(parent.snapshot().await?, Some(TaskSnapshot::Pending));

        let checkpoint_table = format!("checkpoints_{parent_name}");
        let query = format!("SELECT count(*) FROM steda.{checkpoint_table} WHERE task_id = $1");
        let checkpoint_count: i64 = sqlx::query_scalar(AssertSqlSafe(query))
            .bind(parent.task_id())
            .fetch_one(&pool)
            .await?;
        assert_eq!(checkpoint_count, 0);

        run_worker_for_claims(&child_worker, child_queue.metrics(), 1).await?;
        assert_eq!(child.result().await?, json!({"value": 42}));
        run_worker_for_claims(&parent_worker, parent_queue.metrics(), 1).await?;
        assert_eq!(parent.result().await?, json!({"value": 42}));

        child_queue.delete().await?;
        parent_queue.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn task_context_rejects_same_queue_result_wait(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("same_queue_wait");
        let queue = Steda::from_pool(pool).queue(queue_name)?;
        queue.create().await?;

        let worker = queue
            .worker()
            .task(SAME_QUEUE_WAIT, async |task: TaskRef<Value, Value>, ctx: TaskContext| {
                let error =
                    ctx.await_task(&task).await.expect_err("same-queue task wait must be rejected");
                Ok(json!({"invalid_options": matches!(error, Error::InvalidOptions(_))}))
            })
            .build()?;

        let target = queue.spawn(CHILD, json!({})).await?;
        let waiter = queue.spawn(SAME_QUEUE_WAIT, target.task_ref()).await?;
        run_worker_for_claims(&worker, queue.metrics(), 1).await?;

        assert_eq!(waiter.result().await?, json!({"invalid_options": true}));
        assert_eq!(target.snapshot().await?, Some(TaskSnapshot::Pending));

        queue.delete().await?;
        Ok(())
    }
}
