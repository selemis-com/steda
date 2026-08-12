//! Distribution-schema installation and upgrade contract tests.

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{Result, RunId, Steda, Task, TaskSnapshot};

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

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_storage_rejects_missing_durable_payloads(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("schema_invariants");
        let queue = Steda::from_pool(pool.clone()).queue(queue_name.clone())?;
        queue.create().await?;
        let spawned = queue.spawn(SCHEMA_PROBE, json!({})).await?;

        let task_table = format!("tasks_{queue_name}");
        let run_table = format!("runs_{queue_name}");
        let checkpoint_table = format!("checkpoints_{queue_name}");
        let run_id: RunId = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT last_attempt_run FROM steda.{task_table} WHERE id = $1"
        )))
        .bind(spawned.task_id())
        .fetch_one(&pool)
        .await?;

        for (state, timestamp) in [("completed", "completed_at"), ("failed", "failed_at")] {
            let query = format!(
                "UPDATE steda.{run_table} SET state = '{state}', {timestamp} = steda.current_time() WHERE id = $1"
            );
            sqlx::query(AssertSqlSafe(query))
                .bind(run_id)
                .execute(&pool)
                .await
                .expect_err("terminal run state without its JSON payload must be rejected");
        }

        sqlx::query(AssertSqlSafe(format!(
            "UPDATE steda.{task_table} SET state = 'cancelled', cancelled_at = NULL WHERE id = $1"
        )))
        .bind(spawned.task_id())
        .execute(&pool)
        .await
        .expect_err("cancelled task without cancelled_at must be rejected");

        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO steda.{checkpoint_table} (task_id, name, state) VALUES ($1, '$step:null', NULL)"
        )))
        .bind(spawned.task_id())
        .execute(&pool)
        .await
        .expect_err("checkpoint state must not be SQL NULL");

        queue.delete().await?;
        Ok(())
    }
}
