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
    use steda::{Error, Result, Steda, Task, TaskResultSnapshot};

    use super::{common::unique_queue, worker_support::run_worker_for_claims};

    #[derive(Clone, Copy, Debug)]
    struct AlphaOnly;

    impl Task for AlphaOnly {
        const NAME: &'static str = "alpha-only";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct BetaOnly;

    impl Task for BetaOnly {
        const NAME: &'static str = "beta-only";
        type Input = Value;
        type Output = Value;
    }

    #[derive(Clone, Copy, Debug)]
    struct Duplicate;

    impl Task for Duplicate {
        const NAME: &'static str = "duplicate";
        type Input = Value;
        type Output = Value;
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn worker_rejects_duplicate_task_registration(pool: PgPool) -> Result<()> {
        let queue = unique_queue("duplicate_registration");
        let app = Steda::from_pool(pool).queue(queue)?;
        let duplicate = app
            .worker()
            .task::<Duplicate>(async |_params: Value, _ctx| Ok(json!({"first": true})))
            .task::<Duplicate>(async |_params: Value, _ctx| Ok(json!({"second": true})))
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
            .task::<AlphaOnly>(async |_params: Value, _ctx| Ok(json!({"worker": "alpha"})))
            .build()?;
        let beta_worker = beta
            .worker()
            .task::<BetaOnly>(async |_params: Value, _ctx| Ok(json!({"worker": "beta"})))
            .build()?;

        let alpha_task = alpha.spawn::<AlphaOnly>(json!({})).await?;
        let beta_task = beta.spawn::<BetaOnly>(json!({})).await?;

        run_worker_for_claims(&alpha_worker, alpha.metrics(), 1).await?;
        assert!(matches!(
            alpha.fetch_task_result(alpha_task.id()).await?,
            Some(TaskResultSnapshot::Completed { .. })
        ));
        assert_eq!(
            alpha.fetch_task_result(beta_task.id()).await?,
            Some(TaskResultSnapshot::Pending)
        );

        run_worker_for_claims(&beta_worker, beta.metrics(), 1).await?;
        assert!(matches!(
            beta.fetch_task_result(beta_task.id()).await?,
            Some(TaskResultSnapshot::Completed { .. })
        ));

        alpha.delete().await?;
        Ok(())
    }
}
