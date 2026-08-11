//! Cross-queue task result waiting tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{
        AwaitTaskResultOptions, Error, Result, RetryStrategy, Steda, Task, TaskContext, TaskId,
        TaskResultSnapshot,
    };

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct Child;

    impl Task for Child {
        const NAME: &'static str = "wait-child";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct Parent;

    impl Task for Parent {
        const NAME: &'static str = "wait-parent";
        type Input = TaskId;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct SameQueueWait;

    impl Task for SameQueueWait {
        const NAME: &'static str = "same-queue-wait";
        type Input = TaskId;
        type Output = Value;
    }

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
            .task::<Child>(async |_params: Value, _ctx| Ok(json!({"value": 42})))
            .build()?;
        let child = child_queue.spawn::<Child>(json!({})).await?;
        run_worker_for_claims(&child_worker, child_queue.metrics(), 1).await?;
        assert_eq!(child.result().await?, json!({"value": 42}));

        let parent_worker = parent_queue
            .worker()
            .task::<Parent>({
                let child_name = child_name.clone();
                move |child_id: TaskId, ctx: TaskContext| {
                    let child_name = child_name.clone();
                    async move {
                        let snapshot = ctx
                            .await_task_result(child_id, AwaitTaskResultOptions::new(child_name))
                            .await?;
                        if ctx.attempt() == 1 {
                            return Err(Error::InvalidOptions(
                                "retry after durable child result".to_owned(),
                            ));
                        }
                        let TaskResultSnapshot::Completed { result } = snapshot else {
                            return Err(Error::Other(
                                "durable child wait returned non-completed result".to_owned(),
                            ));
                        };
                        Ok(result)
                    }
                }
            })
            .build()?;

        let parent = parent_queue
            .spawn::<Parent>(child.id())
            .max_attempts(2)
            .retry_strategy(RetryStrategy::fixed(0.0))
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
            .task::<SameQueueWait>(async |task_id: TaskId, ctx: TaskContext| {
                let own_queue = ctx.queue_name().to_owned();
                let error = ctx
                    .await_task_result(task_id, AwaitTaskResultOptions::new(own_queue))
                    .await
                    .expect_err("same-queue task wait must be rejected");
                Ok(json!({"invalid_options": matches!(error, Error::InvalidOptions(_))}))
            })
            .build()?;

        let target = queue.spawn::<Child>(json!({})).await?;
        let waiter = queue.spawn::<SameQueueWait>(target.id()).await?;
        run_worker_for_claims(&worker, queue.metrics(), 1).await?;

        assert_eq!(waiter.result().await?, json!({"invalid_options": true}));
        assert_eq!(queue.fetch_task_result(target.id()).await?, Some(TaskResultSnapshot::Pending));

        queue.delete().await?;
        Ok(())
    }
}
