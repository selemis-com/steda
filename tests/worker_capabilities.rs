//! Worker task capability and registration tests.

#[cfg(test)]
mod common;

#[cfg(test)]
#[path = "common/worker.rs"]
mod worker_support;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Error, Result, Steda, Task, TaskSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    const ALPHA_ONLY: Task<Value, Value> = Task::new("alpha-only");

    const BETA_ONLY: Task<Value, Value> = Task::new("beta-only");

    const DUPLICATE: Task<Value, Value> = Task::new("duplicate");

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_rejects_duplicate_task_registration(pool: PgPool) -> Result<()> {
        let queue = unique_queue("duplicate_registration");
        let app = Steda::from_pool(pool).queue(queue)?;
        let duplicate = app
            .worker()
            .task(DUPLICATE, async |_params: Value, _ctx| Ok(json!({"first": true})))
            .task(DUPLICATE, async |_params: Value, _ctx| Ok(json!({"second": true})))
            .build();
        assert!(matches!(duplicate, Err(Error::InvalidOptions(_))));
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn workers_only_claim_registered_task_names(pool: PgPool) -> Result<()> {
        let queue = unique_queue("capability_claim");
        let alpha = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        let beta = Steda::from_pool(pool).queue(queue.clone())?;
        alpha.create().await?;

        let alpha_worker = alpha
            .worker()
            .task(ALPHA_ONLY, async |_params: Value, _ctx| Ok(json!({"worker": "alpha"})))
            .build()?;
        let beta_worker = beta
            .worker()
            .task(BETA_ONLY, async |_params: Value, _ctx| Ok(json!({"worker": "beta"})))
            .build()?;

        let alpha_task = alpha.spawn(ALPHA_ONLY, json!({})).await?;
        let beta_task = beta.spawn(BETA_ONLY, json!({})).await?;

        run_worker_for_claims(&alpha_worker, alpha.metrics(), 1).await?;
        assert!(matches!(alpha_task.snapshot().await?, Some(TaskSnapshot::Completed { .. })));
        assert_eq!(beta_task.snapshot().await?, Some(TaskSnapshot::Pending));

        run_worker_for_claims(&beta_worker, beta.metrics(), 1).await?;
        assert!(matches!(beta_task.snapshot().await?, Some(TaskSnapshot::Completed { .. })));

        alpha.delete().await?;
        Ok(())
    }
}
