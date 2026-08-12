//! Distribution-schema installation and upgrade contract tests.

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Result, Steda, Task, TaskSnapshot};

    use super::common::unique_queue;

    const SCHEMA_PROBE: Task<Value, Value> = Task::new("schema-probe");

    #[sqlx::test]
    async fn merged_schema_can_be_reapplied_without_losing_state(pool: PgPool) -> Result<()> {
        sqlx::raw_sql(include_str!("../sql/steda.sql")).execute(&pool).await?;

        let queue_name = unique_queue("schema_reapply");
        let queue = Steda::from_pool(pool.clone()).queue(queue_name.clone())?;
        queue.create().await?;
        let spawned = queue.spawn(SCHEMA_PROBE, json!({"preserved": true})).await?;
        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Pending));

        sqlx::raw_sql(include_str!("../sql/steda.sql")).execute(&pool).await?;

        assert_eq!(spawned.snapshot().await?, Some(TaskSnapshot::Pending));
        assert!(Steda::from_pool(pool).queues().await?.contains(&queue_name));

        Ok(())
    }
}
