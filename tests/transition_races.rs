//! Concurrent terminal-transition contract tests.

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use steda::{Result, RetryStrategy, RunId, Steda, Task, TaskSnapshot};

    use super::common::unique_queue;

    const RACE_TASK: Task<Value, Value> = Task::new("race-task");

    #[derive(Clone, Copy)]
    enum TerminalTransition {
        Complete,
        Fail,
    }

    async fn apply_terminal_transition(
        pool: &PgPool,
        queue: &str,
        run_id: RunId,
        transition: TerminalTransition,
    ) -> std::result::Result<(), sqlx::Error> {
        match transition {
            TerminalTransition::Complete => {
                let completed: bool =
                    sqlx::query_scalar("SELECT steda.complete_run($1, $2, $3)")
                        .bind(queue)
                        .bind(run_id)
                        .bind(json!({"ok": true}))
                        .fetch_one(pool)
                        .await?;
                assert!(completed);
            }
            TerminalTransition::Fail => {
                sqlx::query("SELECT steda.fail_run($1, $2, $3, FALSE)")
                    .bind(queue)
                    .bind(run_id)
                    .bind(json!({"name": "RaceFailure"}))
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    #[sqlx::test(migrations = "./sql/migrations")]
    async fn cancellation_and_terminal_transition_have_one_winner(pool: PgPool) -> Result<()> {
        let queue_name = unique_queue("terminal_cancel_race");
        let queue = Steda::from_pool(pool.clone()).queue(queue_name.clone())?;
        queue.create().await?;

        for transition in [TerminalTransition::Complete, TerminalTransition::Fail] {
            let task = queue
                .spawn(RACE_TASK, json!({}))
                .max_attempts(1)
                .retry_strategy(RetryStrategy::none())
                .await?;
            let run_id: RunId = sqlx::query_scalar(
                "SELECT run_id FROM steda.claim_tasks($1, $2, $3, $4, $5) LIMIT 1",
            )
            .bind(&queue_name)
            .bind("transition-race-worker")
            .bind(30_i32)
            .bind(1_i32)
            .bind(vec![RACE_TASK.name().to_owned()])
            .fetch_one(&pool)
            .await?;

            let terminal = apply_terminal_transition(&pool, &queue_name, run_id, transition);
            let cancellation = sqlx::query_scalar::<_, bool>("SELECT steda.cancel_task($1, $2)")
                .bind(&queue_name)
                .bind(task.task_id())
                .fetch_one(&pool);
            let (terminal, cancelled) = tokio::join!(terminal, cancellation);
            let cancelled = cancelled?;

            match terminal {
                Ok(()) => {
                    assert!(!cancelled);
                    let snapshot = task.snapshot().await?;
                    match transition {
                        TerminalTransition::Complete => {
                            assert!(matches!(snapshot, Some(TaskSnapshot::Completed { .. })));
                        }
                        TerminalTransition::Fail => {
                            assert!(matches!(snapshot, Some(TaskSnapshot::Failed { .. })));
                        }
                    }
                }
                Err(sqlx::Error::Database(error)) => {
                    assert_eq!(error.code().as_deref(), Some("ST001"));
                    assert!(cancelled);
                    assert_eq!(task.snapshot().await?, Some(TaskSnapshot::Cancelled));
                }
                Err(error) => return Err(error.into()),
            }
        }

        queue.delete().await?;
        Ok(())
    }
}
