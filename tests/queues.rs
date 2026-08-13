//! Queue lifecycle, policy, and storage integrity tests.

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};
    use sqlx::{AssertSqlSafe, PgPool};
    use steda::{Error, QueuePolicyOptions, Result, Steda, Task};
    use uuid::Uuid;

    use super::common::unique_queue;

    const CONSTRAINT_TASK: Task<Value, Value> = Task::new("constraint-task");

    fn relation_name(prefix: &str, queue: &str) -> String {
        format!("{prefix}_{queue}")
    }

    async fn queue_storage_relation_count(pool: &PgPool, queue: &str) -> Result<i64> {
        let relation_names =
            ["checkpoints", "runs", "tasks"].map(|prefix| relation_name(prefix, queue)).to_vec();
        Ok(sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'steda' AND c.relname = ANY($1::text[])
            "#,
        )
        .bind(&relation_names)
        .fetch_one(pool)
        .await?)
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_operations(pool: PgPool) -> Result<()> {
        let queue = unique_queue("queue_ops");
        let steda = Steda::from_pool(pool);
        let app = steda.queue(queue.clone())?;

        app.create().await?;
        app.create().await?;
        let queues = steda.queues().await?;
        assert!(queues.contains(&queue));

        app.delete().await?;
        let queues = steda.queues().await?;
        assert!(!queues.contains(&queue));

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_drop_waits_for_in_progress_create(pool: PgPool) -> Result<()> {
        let queue = unique_queue("queue_lifecycle_lock");
        let mut creator = pool.begin().await?;
        sqlx::query("SELECT steda.create_queue($1)").bind(&queue).execute(&mut *creator).await?;

        let mut contender = pool.acquire().await?;
        sqlx::query("SET lock_timeout = '50ms'").execute(&mut *contender).await?;
        let error = sqlx::query("SELECT steda.drop_queue($1)")
            .bind(&queue)
            .execute(&mut *contender)
            .await
            .expect_err("drop_queue must wait for the matching create_queue transaction");
        let sqlx::Error::Database(database_error) = &error else {
            return Err(error.into());
        };
        assert_eq!(database_error.code().as_deref(), Some("55P03"));
        sqlx::query("SET lock_timeout = '0'").execute(&mut *contender).await?;
        drop(contender);

        creator.commit().await?;

        let steda = Steda::from_pool(pool);
        let app = steda.queue(queue.clone())?;
        assert!(steda.queues().await?.contains(&queue));
        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn repeated_create_reports_corrupt_queue_storage(pool: PgPool) -> Result<()> {
        let queue = unique_queue("create_integrity");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let missing_index = relation_name("runs", &queue) + "_state_available_at_index";
        let drop_index = format!("DROP INDEX steda.{missing_index}");
        sqlx::query(AssertSqlSafe(drop_index)).execute(&pool).await?;

        let error = app.create().await.expect_err("repeated create must detect damaged storage");
        assert!(error.to_string().contains("missing required indexes"));
        let index_exists: bool =
            sqlx::query_scalar("SELECT to_regclass(format('steda.%I', $1)) IS NOT NULL")
                .bind(&missing_index)
                .fetch_one(&pool)
                .await?;
        assert!(!index_exists, "repeated create must not repair missing storage");

        app.delete().await?;
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn default_policy_update_requires_existing_queue(pool: PgPool) -> Result<()> {
        let queue = unique_queue("missing_policy_queue");
        let app = Steda::from_pool(pool).queue(queue)?;

        let error = app
            .set_policy(QueuePolicyOptions::default())
            .await
            .expect_err("default policy update must still verify queue existence");
        assert!(error.to_string().contains("does not exist"));
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_name_validation_matches_storage_rules(pool: PgPool) -> Result<()> {
        let steda = Steda::from_pool(pool.clone());
        assert!(steda.queue("default").is_ok());
        assert!(steda.queue(format!("{}é", "a".repeat(31))).is_ok());
        assert!(matches!(steda.queue("   	"), Err(Error::MissingQueueName)));
        assert!(matches!(
            steda.queue(format!("{}é", "a".repeat(32))),
            Err(Error::QueueNameTooLong { .. })
        ));

        let error = sqlx::query("SELECT steda.create_queue($1)")
            .bind("   	")
            .execute(&pool)
            .await
            .expect_err("PostgreSQL must reject whitespace-only queue names");
        assert!(error.to_string().contains("Queue name must be provided"));

        let error = sqlx::query("SELECT steda.create_queue($1)")
            .bind(format!("{}é", "a".repeat(32)))
            .execute(&pool)
            .await
            .expect_err("PostgreSQL must enforce the queue-name byte limit");
        assert!(error.to_string().contains("max 33 bytes"));
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_policy_round_trip(pool: PgPool) -> Result<()> {
        let queue = unique_queue("policy");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;

        app.create_with_policy(
            QueuePolicyOptions::new().cleanup_ttl(Duration::from_secs(12_345)).cleanup_limit(77),
        )
        .await?;

        let policy = app
            .policy()
            .await?
            .ok_or_else(|| Error::InvalidOptions("expected queue policy".to_owned()))?;
        assert_eq!(policy.queue_name, queue);
        assert_eq!(policy.cleanup_ttl, Duration::from_secs(12_345));
        assert_eq!(policy.cleanup_limit, 77);

        app.set_policy(
            QueuePolicyOptions::new().cleanup_ttl(Duration::from_secs(4_321)).cleanup_limit(12),
        )
        .await?;
        let updated = app
            .policy()
            .await?
            .ok_or_else(|| Error::InvalidOptions("expected updated queue policy".to_owned()))?;
        assert_eq!(updated.cleanup_ttl, Duration::from_secs(4_321));
        assert_eq!(updated.cleanup_limit, 12);

        app.delete().await?;

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_creation_rolls_back_when_initial_policy_is_invalid(pool: PgPool) -> Result<()> {
        let queue = unique_queue("atomic_queue");
        let steda = Steda::from_pool(pool.clone());
        let app = steda.queue(queue.clone())?;

        let result = app.create_with_policy(QueuePolicyOptions::new().cleanup_limit(0)).await;
        assert!(result.is_err());
        assert!(!steda.queues().await?.contains(&queue));
        assert_eq!(queue_storage_relation_count(&pool, &queue).await?, 0);

        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn queue_storage_enforces_task_run_checkpoint_relationships(pool: PgPool) -> Result<()> {
        let queue = unique_queue("storage_constraints");
        let app = Steda::from_pool(pool.clone()).queue(queue.clone())?;
        app.create().await?;

        let spawned = app.spawn(CONSTRAINT_TASK, json!({})).await?;
        let runs = relation_name("runs", &queue);
        let checkpoints = relation_name("checkpoints", &queue);

        let duplicate_attempt = format!(
            "INSERT INTO steda.{runs} (id, task_id, attempt, state, available_at) VALUES ($1, $2, 1, 'pending', steda.current_time())"
        );
        assert!(
            sqlx::query(AssertSqlSafe(duplicate_attempt))
                .bind(Uuid::now_v7())
                .bind(spawned.task_id())
                .execute(&pool)
                .await
                .is_err()
        );

        let second_active_attempt = format!(
            "INSERT INTO steda.{runs} (id, task_id, attempt, state, available_at) VALUES ($1, $2, 2, 'pending', steda.current_time())"
        );
        assert!(
            sqlx::query(AssertSqlSafe(second_active_attempt))
                .bind(Uuid::now_v7())
                .bind(spawned.task_id())
                .execute(&pool)
                .await
                .is_err()
        );

        let orphan_run = format!(
            "INSERT INTO steda.{runs} (id, task_id, attempt, state, available_at) VALUES ($1, $2, 2, 'pending', steda.current_time())"
        );
        assert!(
            sqlx::query(AssertSqlSafe(orphan_run))
                .bind(Uuid::now_v7())
                .bind(Uuid::now_v7())
                .execute(&pool)
                .await
                .is_err()
        );

        let orphan_checkpoint = format!(
            "INSERT INTO steda.{checkpoints} (task_id, name, state) VALUES ($1, 'orphan', '{{}}'::jsonb)"
        );
        assert!(
            sqlx::query(AssertSqlSafe(orphan_checkpoint))
                .bind(Uuid::now_v7())
                .execute(&pool)
                .await
                .is_err()
        );

        let other = app.spawn(CONSTRAINT_TASK, json!({"other": true})).await?;
        let other_run_query =
            format!("SELECT last_attempt_run FROM steda.tasks_{queue} WHERE id = $1");
        let other_run: Uuid = sqlx::query_scalar(AssertSqlSafe(other_run_query))
            .bind(other.task_id())
            .fetch_one(&pool)
            .await?;
        let tasks = relation_name("tasks", &queue);
        let mismatched_authoritative_run =
            format!("UPDATE steda.{tasks} SET last_attempt_run = $2 WHERE id = $1");
        assert!(
            sqlx::query(AssertSqlSafe(mismatched_authoritative_run))
                .bind(spawned.task_id())
                .bind(other_run)
                .execute(&pool)
                .await
                .is_err()
        );

        app.delete().await?;
        Ok(())
    }
}
