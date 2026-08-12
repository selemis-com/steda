-- ======================================================================
-- Workflow checkpoint persistence
-- ======================================================================
--
-- Checkpoints provide durable step replay within one logical task. A checkpoint
-- name has one immutable committed value for the lifetime of that task, even
-- across retries and process restarts. Checkpoint writes are still fenced by
-- the current run lease, so a stale worker cannot publish new durable state.
--
-- Checkpoints do not make external side effects exactly-once: a process can
-- fail after an external system accepts an operation but before the checkpoint
-- is committed. External effects should therefore use their own idempotency or
-- fencing mechanism.

-- Commit or replay one named checkpoint.
--
-- The first successful insert for `(task_id, checkpoint_name)` wins. Later attempts
-- receive the existing value with `written = false`; they do not overwrite it.
CREATE OR REPLACE FUNCTION steda.set_task_checkpoint_state(
    queue_name text,
    task_id uuid,
    checkpoint_name text,
    result jsonb,
    run_id uuid
)
RETURNS TABLE (
    checkpoint_state jsonb,
    written boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
    inserted_rows integer;
BEGIN
    IF checkpoint_name IS NULL OR checkpoint_name ~ '^[[:space:]]*$' THEN
        RAISE EXCEPTION 'checkpoint_name must be provided';
    END IF;

    IF octet_length(checkpoint_name) > 1024 THEN
        RAISE EXCEPTION 'checkpoint_name must be at most 1024 bytes';
    END IF;

    SELECT *
    INTO context
    FROM steda.lock_running_attempt(queue_name, run_id, FALSE);

    IF context.task_id <> task_id THEN
        RAISE EXCEPTION 'Run "%" does not belong to task "%" in queue "%"',
            run_id,
            task_id,
            queue_name;
    END IF;

    IF steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        context.observed_at
    ) THEN
        RAISE EXCEPTION sqlstate 'ST001'
            USING message = 'Task cancellation deadline has elapsed';
    END IF;

    -- A durable step name is immutable for the lifetime of the logical task.
    -- The first committed value wins, including across retries.
    EXECUTE format(
        $query$
        INSERT INTO steda.%I (
            task_id,
            name,
            state
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (task_id, name) DO NOTHING
        RETURNING state
        $query$,
        'checkpoints_' || queue_name
    )
    INTO checkpoint_state
    USING task_id, checkpoint_name, result;

    GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    written := inserted_rows = 1;

    IF NOT written THEN
        EXECUTE format(
            'SELECT state FROM steda.%I WHERE task_id = $1 AND name = $2',
            'checkpoints_' || queue_name
        )
        INTO checkpoint_state
        USING task_id, checkpoint_name;
    END IF;

    RETURN NEXT;
END;
$$;

-- Load checkpoint state for the current authoritative attempt.
--
-- Checkpoints belong to the logical task and remain visible across all later
-- attempts. The caller must still own a valid running lease; reading checkpoints
-- is part of execution, not unrestricted historical inspection.
CREATE OR REPLACE FUNCTION steda.get_task_checkpoint_states(
    queue_name text,
    task_id uuid,
    run_id uuid
)
RETURNS TABLE (
    name text,
    state jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_running_attempt(queue_name, run_id, FALSE);

    IF context.task_id <> task_id THEN
        RAISE EXCEPTION 'Run "%" does not belong to task "%" in queue "%"',
            run_id,
            task_id,
            queue_name;
    END IF;

    RETURN QUERY EXECUTE format(
        $query$
        SELECT
            checkpoint.name,
            checkpoint.state
        FROM steda.%I checkpoint
        WHERE checkpoint.task_id = $1
        $query$,
        'checkpoints_' || queue_name
    )
    USING task_id;
END;
$$;
