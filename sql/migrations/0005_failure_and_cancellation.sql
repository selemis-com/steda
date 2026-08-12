-- ======================================================================
-- Failure, cancellation, and explicit retry
-- ======================================================================
--
-- Failures are recorded per run while retry policy belongs to the logical task.
-- Automatic retry creates a fresh run ID/attempt; cancellation terminates the
-- logical task and all of its non-terminal runs; manual retry may reopen only a
-- terminally failed task.

-- Fail one authoritative running attempt and apply the task's retry policy.
--
-- When another attempt remains, a new pending or sleeping run is created using
-- the persisted retry strategy. If no attempt remains the logical task becomes
-- failed. A cancellation deadline takes precedence over retry creation.
CREATE OR REPLACE FUNCTION steda.fail_run(
    queue_name text,
    run_id uuid,
    reason jsonb,
    expect_expired_lease boolean
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
    next_attempt integer;
    next_available_at timestamptz;
    new_run_id uuid;
    next_task_state text;
    recorded_attempt integer;
    last_attempt_run_id uuid := run_id;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_running_attempt(queue_name, run_id, expect_expired_lease);

    IF steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        context.observed_at
    ) THEN
        PERFORM steda.cancel_task(queue_name, context.task_id);
        RETURN;
    END IF;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'failed',
            failed_at = $2,
            failure_reason = $3,
            claimed_by = NULL,
            claim_expires_at = NULL
        WHERE id = $1
        $query$,
        'runs_' || queue_name
    )
    USING run_id, context.observed_at, reason;

    next_attempt := context.attempt + 1;
    next_task_state := 'failed';
    recorded_attempt := context.attempt;

    IF (context.retry_strategy ->> 'kind') <> 'none'
        AND next_attempt <= context.max_attempts
    THEN
        next_available_at := context.observed_at + (
            steda.retry_delay_seconds(context.retry_strategy, context.attempt)
            * interval '1 second'
        );

        IF next_available_at < context.observed_at THEN
            next_available_at := context.observed_at;
        END IF;

        -- Do not create a retry that can never become eligible before the
        -- task's durable cancellation deadline. The current attempt remains a
        -- failed attempt; cancellation terminates the logical task.
        IF steda.cancellation_due(
            context.cancellation,
            context.enqueue_at,
            context.first_started_at,
            next_available_at
        ) THEN
            PERFORM steda.cancel_task(queue_name, context.task_id);
            RETURN;
        END IF;

        next_task_state := CASE
            WHEN next_available_at > context.observed_at THEN 'sleeping'
            ELSE 'pending'
        END;
        new_run_id := uuidv7();
        recorded_attempt := next_attempt;
        last_attempt_run_id := new_run_id;

        EXECUTE format(
            $query$
            INSERT INTO steda.%I (
                id,
                task_id,
                attempt,
                state,
                available_at,
                result,
                failure_reason
            )
            VALUES ($1, $2, $3, $4, $5, NULL, NULL)
            $query$,
            'runs_' || queue_name
        )
        USING
            new_run_id,
            context.task_id,
            next_attempt,
            next_task_state,
            next_available_at;
    END IF;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = $2,
            attempts = greatest(attempts, $3),
            last_attempt_run = $4
        WHERE id = $1
        $query$,
        'tasks_' || queue_name
    )
    USING
        context.task_id,
        next_task_state,
        recorded_attempt,
        last_attempt_run_id;
END;
$$;

-- Cancel one non-terminal logical task.
--
-- Cancellation is idempotent for terminal tasks: completed, failed, or already
-- cancelled tasks return FALSE. Successful cancellation marks the task and all
-- non-terminal runs cancelled, clears worker ownership, and returns TRUE.
CREATE OR REPLACE FUNCTION steda.cancel_task(
    queue_name text,
    task_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    current_task_state text;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    -- Lock active runs before the task row so cancel_task() uses the same
    -- lock acquisition order as complete_run()/fail_run().
    EXECUTE format(
        $query$
        SELECT
            id
        FROM steda.%I
        WHERE task_id = $1
          AND state NOT IN ('completed', 'failed', 'cancelled')
        ORDER BY id
        FOR UPDATE
        $query$,
        'runs_' || queue_name
    )
    USING task_id;

    EXECUTE format(
        $query$
        SELECT
            state
        FROM steda.%I
        WHERE id = $1
        FOR UPDATE
        $query$,
        'tasks_' || queue_name
    )
    INTO current_task_state
    USING task_id;

    IF current_task_state IS NULL THEN
        RAISE EXCEPTION 'Task "%" not found in queue "%"', task_id, queue_name;
    END IF;

    IF current_task_state IN ('completed', 'failed', 'cancelled') THEN
        RETURN FALSE;
    END IF;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'cancelled',
            cancelled_at = coalesce(cancelled_at, $2)
        WHERE id = $1
        $query$,
        'tasks_' || queue_name
    )
    USING task_id, now_at;

    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'cancelled',
            claimed_by = NULL,
            claim_expires_at = NULL
        WHERE task_id = $1
          AND state NOT IN ('completed', 'failed', 'cancelled')
        $query$,
        'runs_' || queue_name
    )
    USING task_id;
    RETURN TRUE;
END;
$$;

-- Explicitly retry a terminally failed task.
--
-- Manual retry creates a fresh pending run immediately and extends the effective
-- attempt budget when necessary. It does not rewrite `initial_max_attempts`, so
-- the original spawn request used for idempotency remains immutable. Existing
-- checkpoints continue to belong to the same logical task and are reusable.
CREATE OR REPLACE FUNCTION steda.retry_task(
    queue_name text,
    task_id uuid
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    current_task_attempts integer;
    current_task_state text;
    enqueue_at timestamptz;
    first_started_at timestamptz;
    cancellation_policy jsonb;
    new_run_id uuid;
    next_attempt integer;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    EXECUTE format(
        $query$
        SELECT attempts, state, enqueue_at, first_started_at, cancellation
        FROM steda.%I
        WHERE id = $1
        FOR UPDATE
        $query$,
        'tasks_' || queue_name
    )
    INTO
        current_task_attempts,
        current_task_state,
        enqueue_at,
        first_started_at,
        cancellation_policy
    USING task_id;

    IF current_task_state IS NULL THEN
        RAISE EXCEPTION 'Task "%" not found in queue "%"', task_id, queue_name;
    END IF;

    IF current_task_state <> 'failed' THEN
        RAISE EXCEPTION 'Task "%" is not currently failed in queue "%"', task_id, queue_name;
    END IF;

    IF steda.cancellation_due(
        cancellation_policy,
        enqueue_at,
        first_started_at,
        now_at
    ) THEN
        RAISE EXCEPTION 'Task "%" cannot be retried in queue "%": cancellation deadline has elapsed',
            task_id,
            queue_name;
    END IF;

    next_attempt := current_task_attempts + 1;
    new_run_id := uuidv7();

    EXECUTE format(
        $query$
        INSERT INTO steda.%I (
            id,
            task_id,
            attempt,
            state,
            available_at,
            result,
            failure_reason
        )
        VALUES ($1, $2, $3, 'pending', $4, NULL, NULL)
        $query$,
        'runs_' || queue_name
    )
    USING new_run_id, task_id, next_attempt, now_at;

    -- Manual retry extends only the effective attempt budget. The immutable
    -- initial_max_attempts remains the spawn-time value used for idempotency.
    EXECUTE format(
        $query$
        UPDATE steda.%I
        SET
            state = 'pending',
            attempts = $2,
            max_attempts = greatest(max_attempts, $2),
            last_attempt_run = $3,
            cancelled_at = NULL
        WHERE id = $1
        $query$,
        'tasks_' || queue_name
    )
    USING task_id, next_attempt, new_run_id;

    RETURN new_run_id;
END;
$$;
