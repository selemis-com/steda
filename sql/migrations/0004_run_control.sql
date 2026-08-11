-- ======================================================================
-- Normal run lifecycle
-- ======================================================================
--
-- Attempt-scoped mutations are fenced in PostgreSQL. A worker may complete,
-- suspend, checkpoint, or fail only the authoritative running run while its
-- finite lease remains valid. This prevents stale workers from mutating durable
-- state after ownership has moved to another attempt.

-- Lock and load one run together with its logical task.
--
-- This is the single PostgreSQL authority for the run -> task lock order used by
-- attempt-scoped transitions and supervision. It observes time only after both
-- durable rows have been acquired in that order.
CREATE OR REPLACE FUNCTION steda.lock_attempt_context(
    queue_name text,
    run_id uuid
)
RETURNS TABLE (
    task_id uuid,
    attempt integer,
    run_state text,
    failure_reason jsonb,
    claimed_by text,
    claim_expires_at timestamptz,
    task_state text,
    authoritative_run_id uuid,
    observed_at timestamptz,
    enqueue_at timestamptz,
    first_started_at timestamptz,
    cancellation jsonb,
    retry_strategy jsonb,
    max_attempts integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_task_id uuid;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    EXECUTE format(
        $query$
        SELECT
            run.task_id,
            run.attempt,
            run.state,
            run.failure_reason,
            run.claimed_by,
            run.claim_expires_at
        FROM steda.%I run
        WHERE run.id = $1
        FOR UPDATE
        $query$,
        'runs_' || queue_name
    )
    INTO
        resolved_task_id,
        attempt,
        run_state,
        failure_reason,
        claimed_by,
        claim_expires_at
    USING run_id;

    IF resolved_task_id IS NULL THEN
        RAISE EXCEPTION 'Run "%" not found in queue "%"', run_id, queue_name;
    END IF;

    EXECUTE format(
        $query$
        SELECT
            task.state,
            task.last_attempt_run,
            task.enqueue_at,
            task.first_started_at,
            task.cancellation,
            task.retry_strategy,
            task.max_attempts
        FROM steda.%I task
        WHERE task.id = $1
        FOR UPDATE
        $query$,
        'tasks_' || queue_name
    )
    INTO
        task_state,
        authoritative_run_id,
        enqueue_at,
        first_started_at,
        cancellation,
        retry_strategy,
        max_attempts
    USING resolved_task_id;

    IF task_state IS NULL THEN
        RAISE EXCEPTION 'Task "%" for run "%" not found in queue "%"',
            resolved_task_id,
            run_id,
            queue_name;
    END IF;

    task_id := resolved_task_id;
    observed_at := steda.current_time();
    RETURN NEXT;
END;
$$;

-- Lock and validate the authoritative running attempt for a worker-owned mutation.
--
-- `expect_expired_lease = false` requires an unexpired lease and is used by
-- ordinary worker operations. `true` is reserved for the lease-reaper path,
-- which must prove that the addressed lease has actually expired.
CREATE OR REPLACE FUNCTION steda.lock_running_attempt(
    queue_name text,
    run_id uuid,
    expect_expired_lease boolean
)
RETURNS TABLE (
    task_id uuid,
    attempt integer,
    observed_at timestamptz,
    enqueue_at timestamptz,
    first_started_at timestamptz,
    cancellation jsonb,
    retry_strategy jsonb,
    max_attempts integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_attempt_context(queue_name, run_id);

    -- Attempt-scoped mutations classify the addressed run before consulting the
    -- logical task. A historical failed attempt remains failed even if a later
    -- authoritative retry is subsequently cancelled.
    IF context.run_state = 'cancelled' THEN
        RAISE EXCEPTION sqlstate 'AB001'
            USING message = 'Task has been cancelled';
    END IF;

    IF context.run_state = 'failed' THEN
        RAISE EXCEPTION sqlstate 'AB002'
            USING message = format(
                'Run "%s" has already failed in queue "%s"',
                run_id,
                queue_name
            );
    END IF;

    IF context.run_state <> 'running' THEN
        RAISE EXCEPTION 'Run "%" is not currently running in queue "%"', run_id, queue_name;
    END IF;

    IF context.task_state = 'cancelled' THEN
        RAISE EXCEPTION sqlstate 'AB001'
            USING message = 'Task has been cancelled';
    END IF;

    IF context.task_state <> 'running'
        OR context.authoritative_run_id IS DISTINCT FROM run_id
    THEN
        RAISE EXCEPTION 'Run "%" is not the authoritative running attempt for task "%" in queue "%"',
            run_id,
            context.task_id,
            queue_name;
    END IF;

    IF context.claim_expires_at IS NULL THEN
        RAISE EXCEPTION 'Running run "%" does not have a finite lease in queue "%"',
            run_id,
            queue_name;
    END IF;

    IF expect_expired_lease THEN
        IF context.claim_expires_at > context.observed_at THEN
            RAISE EXCEPTION 'Lease for run "%" has not expired in queue "%"', run_id, queue_name;
        END IF;
    ELSIF context.claim_expires_at <= context.observed_at THEN
        RAISE EXCEPTION sqlstate 'AB003'
            USING message = format(
                'Lease for run "%s" has expired in queue "%s"',
                run_id,
                queue_name
            );
    END IF;

    task_id := context.task_id;
    attempt := context.attempt;
    observed_at := context.observed_at;
    enqueue_at := context.enqueue_at;
    first_started_at := context.first_started_at;
    cancellation := context.cancellation;
    retry_strategy := context.retry_strategy;
    max_attempts := context.max_attempts;
    RETURN NEXT;
END;
$$;

-- Complete the authoritative running attempt with a durable result.
--
-- Cancellation deadlines are checked at the same authoritative database time
-- before completion. If cancellation is already due, cancellation wins and the
-- function returns FALSE instead of publishing a result.
CREATE OR REPLACE FUNCTION steda.complete_run(
    queue_name text,
    run_id uuid,
    result jsonb
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_running_attempt(queue_name, run_id, FALSE);

    IF steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        context.observed_at
    ) THEN
        PERFORM steda.cancel_task(queue_name, context.task_id);
        RETURN FALSE;
    END IF;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'completed',
            completed_at = $2,
            result = $3,
            claimed_by = NULL,
            claim_expires_at = NULL
        WHERE id = $1
        $query$,
        'runs_' || queue_name
    )
    USING run_id, context.observed_at, result;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'completed',
            last_attempt_run = $2
        WHERE id = $1
        $query$,
        'tasks_' || queue_name
    )
    USING context.task_id, run_id;

    RETURN TRUE;
END;
$$;

-- Durably suspend a running attempt until `wake_at`.
--
-- Sleeping releases worker ownership and keeps no process-local future alive.
-- The same run becomes claimable again when its `available_at` time arrives. If
-- the requested wake time has already passed, the function returns `ready` and
-- leaves the run running; if a cancellation deadline wins, it returns
-- `cancelled`; otherwise it returns `suspended`.
CREATE OR REPLACE FUNCTION steda.schedule_run(
    queue_name text,
    run_id uuid,
    wake_at timestamptz
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_running_attempt(queue_name, run_id, FALSE);

    IF steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        context.observed_at
    ) OR steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        wake_at
    ) THEN
        PERFORM steda.cancel_task(queue_name, context.task_id);
        RETURN 'cancelled';
    END IF;

    -- PostgreSQL decides whether suspension is still necessary at the same
    -- authoritative timestamp used for ownership validation.
    IF wake_at <= context.observed_at THEN
        RETURN 'ready';
    END IF;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'sleeping',
            claimed_by = NULL,
            claim_expires_at = NULL,
            available_at = $2
        WHERE id = $1
        $query$,
        'runs_' || queue_name
    )
    USING run_id, wake_at;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET state = 'sleeping'
        WHERE id = $1
        $query$,
        'tasks_' || queue_name
    )
    USING context.task_id;

    RETURN 'suspended';
END;
$$;
