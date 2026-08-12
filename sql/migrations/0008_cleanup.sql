-- ======================================================================
-- Retention cleanup
-- ======================================================================
--
-- Cleanup removes only terminal logical tasks according to the policy persisted
-- in `steda.queues`. Active pending/running/sleeping work is never eligible.
-- Deleting the task row cascades its run and checkpoint history. Cleanup is
-- bounded and uses `SKIP LOCKED` so multiple maintenance processes can coexist.

-- Delete terminal tasks according to the queue's persisted retention policy.
--
-- A task becomes eligible when its terminal timestamp is at or before
-- current_time() - cleanup_ttl. The task row is the only row deleted directly;
-- run and checkpoint storage follows through the queue schema's FK cascades.
CREATE OR REPLACE FUNCTION steda.cleanup_tasks(
    queue_name text
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    cleanup_ttl interval;
    cleanup_limit integer;
    cutoff timestamptz;
    deleted_count integer;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    -- Cleanup participates in the same lifecycle lock as create/drop. This
    -- prevents dynamic queue relations from disappearing while cleanup uses
    -- them and makes persisted metadata + physical storage one lifecycle unit.
    PERFORM pg_advisory_xact_lock(hashtext('steda.queue'), hashtext(queue_name));

    SELECT
        queue.cleanup_ttl,
        queue.cleanup_limit
    INTO
        cleanup_ttl,
        cleanup_limit
    FROM steda.queues queue
    WHERE queue.name = queue_name;

    IF cleanup_ttl IS NULL OR cleanup_limit IS NULL THEN
        RAISE EXCEPTION 'Queue "%" does not exist', queue_name;
    END IF;

    cutoff := now_at - cleanup_ttl;

    EXECUTE format(
        $query$
        WITH tasks_to_delete AS (
            SELECT
                task.id,
                CASE
                    WHEN task.state = 'completed' THEN run.completed_at
                    WHEN task.state = 'failed' THEN run.failed_at
                    WHEN task.state = 'cancelled' THEN task.cancelled_at
                    ELSE NULL
                END AS terminal_at
            FROM steda.%1$I task
            LEFT JOIN steda.%2$I run ON run.id = task.last_attempt_run
            WHERE task.state IN ('completed', 'failed', 'cancelled')
              AND CASE
                    WHEN task.state = 'completed' THEN run.completed_at
                    WHEN task.state = 'failed' THEN run.failed_at
                    WHEN task.state = 'cancelled' THEN task.cancelled_at
                    ELSE NULL
                  END IS NOT NULL
              AND CASE
                    WHEN task.state = 'completed' THEN run.completed_at
                    WHEN task.state = 'failed' THEN run.failed_at
                    WHEN task.state = 'cancelled' THEN task.cancelled_at
                    ELSE NULL
                  END <= $1
            ORDER BY terminal_at, task.id
            LIMIT $2
            FOR UPDATE OF task SKIP LOCKED
        ),
        deleted_tasks AS (
            DELETE FROM steda.%1$I task
            WHERE task.id IN (SELECT id FROM tasks_to_delete)
            RETURNING 1
        )
        SELECT
            count(*)
        FROM deleted_tasks
        $query$,
        'tasks_' || queue_name,
        'runs_' || queue_name
    )
    INTO deleted_count
    USING cutoff, cleanup_limit;

    RETURN deleted_count;
END;
$$;

-- Run retention cleanup for one queue or every registered queue.
--
-- Passing NULL processes all queues in deterministic name order. Supplying a
-- queue name restricts cleanup to that queue and raises if it does not exist.
-- The function returns one row per processed queue with the number of logical
-- tasks deleted from that batch.
CREATE OR REPLACE FUNCTION steda.cleanup_all_queues(
    requested_queue_name text DEFAULT NULL
)
RETURNS TABLE (
    queue_name text,
    tasks_deleted integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    cleanup_queue_name text;
BEGIN
    IF requested_queue_name IS NOT NULL THEN
        requested_queue_name := steda.validate_queue_name(requested_queue_name);

        IF NOT EXISTS (
            SELECT 1
            FROM steda.queues queue
            WHERE queue.name = requested_queue_name
        ) THEN
            RAISE EXCEPTION 'Queue "%" does not exist', requested_queue_name;
        END IF;
    END IF;

    FOR cleanup_queue_name IN
        SELECT queue.name
        FROM steda.queues queue
        WHERE requested_queue_name IS NULL
           OR queue.name = requested_queue_name
        ORDER BY queue.name
    LOOP
        queue_name := cleanup_queue_name;
        tasks_deleted := steda.cleanup_tasks(cleanup_queue_name);
        RETURN NEXT;
    END LOOP;
END;
$$;
