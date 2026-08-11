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
    use sqlx::PgPool;
    use steda::{Error, Result, RetryStrategy, Steda, Task, TaskContext, TaskRef, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const CHILD: Task<Value, Value> = Task::new("wait-child");

    const PARENT: Task<TaskRef<Value, Value>, Value> = Task::new("wait-parent");

    const SAME_QUEUE_WAIT: Task<TaskRef<Value, Value>, Value> = Task::new("same-queue-wait");

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
