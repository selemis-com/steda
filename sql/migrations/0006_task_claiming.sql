-- ======================================================================
-- Worker task claiming and expired-lease handling
-- ======================================================================
--
-- Workers are disposable claimers of durable runs. Claims use finite leases and
-- `FOR UPDATE SKIP LOCKED`, allowing many workers to compete without handing the
-- same run to two claimers. Capability filtering happens in PostgreSQL before a
-- run is claimed. Expired ownership is converted through the same canonical
-- failure/retry transition used by ordinary failures.

-- Reap one expired authoritative running attempt.
--
-- The expired run is failed with the reserved `$LeaseExpired` failure payload,
-- after which normal retry/cancellation policy decides the logical task state.
-- Returns FALSE when the observed run is no longer an expired running attempt.
CREATE OR REPLACE FUNCTION steda.reap_expired_run(
    queue_name text,
    run_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
BEGIN
    SELECT *
    INTO context
    FROM steda.lock_attempt_context(queue_name, run_id);

    -- A caller may have observed expiry before waiting on the authoritative
    -- run -> task locks, so validate the persisted state again here.
    IF context.run_state <> 'running'
        OR context.claim_expires_at IS NULL
        OR context.claim_expires_at > context.observed_at
    THEN
        RETURN FALSE;
    END IF;

    IF context.task_state <> 'running'
        OR context.authoritative_run_id IS DISTINCT FROM run_id
    THEN
        RAISE EXCEPTION 'Expired run "%" is not authoritative for task "%" in queue "%"',
            run_id,
            context.task_id,
            queue_name;
    END IF;

    PERFORM steda.fail_run(
        queue_name,
        run_id,
        jsonb_strip_nulls(
            jsonb_build_object(
                'name', '$LeaseExpired',
                'message', 'worker did not renew task lease before expiry',
                'worker_id', context.claimed_by,
                'claim_expired_at', context.claim_expires_at,
                'attempt', context.attempt
            )
        ),
        TRUE
    );

    RETURN TRUE;
END;
$$;

-- Observe and optionally renew one claimed run.
--
-- Supervision reports `running`, `completed`, `failed`, `cancelled`, `suspended`,
-- or `lease_lost` from durable state. When `renew_for_seconds` is supplied, the
-- lease is extended only after it enters its renewal window; frequent
-- supervision therefore does not imply a write on every poll.
CREATE OR REPLACE FUNCTION steda.supervise_run(
    queue_name text,
    run_id uuid,
    renew_for_seconds integer
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    context record;
    current_task_state text;
BEGIN
    IF renew_for_seconds IS NOT NULL AND renew_for_seconds <= 0 THEN
        RAISE EXCEPTION 'renew_for_seconds must be > 0 when provided';
    END IF;

    SELECT *
    INTO context
    FROM steda.lock_attempt_context(queue_name, run_id);

    -- Cancellation of the logical task always wins over an attempt-local state.
    IF context.task_state = 'cancelled' OR context.run_state = 'cancelled' THEN
        RETURN 'cancelled';
    END IF;

    -- Terminal or suspended attempts are reported from persisted state. In
    -- particular, a reaped lease expiry remains distinguishable from an
    -- ordinary task failure even if a retry has already become authoritative.
    IF context.run_state = 'failed' THEN
        RETURN CASE
            WHEN context.failure_reason ->> 'name' = '$LeaseExpired' THEN 'lease_lost'
            ELSE 'failed'
        END;
    END IF;

    IF context.run_state = 'completed' THEN
        RETURN 'completed';
    END IF;

    IF context.run_state = 'sleeping' THEN
        RETURN 'suspended';
    END IF;

    IF context.run_state <> 'running' THEN
        RAISE EXCEPTION 'Run "%" has unsupported supervision state "%" in queue "%"',
            run_id,
            context.run_state,
            queue_name;
    END IF;

    IF context.task_state <> 'running'
        OR context.authoritative_run_id IS DISTINCT FROM run_id
    THEN
        RAISE EXCEPTION 'Running run "%" is not authoritative for task "%" in queue "%"',
            run_id,
            context.task_id,
            queue_name;
    END IF;

    IF steda.cancellation_due(
        context.cancellation,
        context.enqueue_at,
        context.first_started_at,
        context.observed_at
    ) THEN
        PERFORM steda.cancel_task(queue_name, context.task_id);
        RETURN 'cancelled';
    END IF;

    IF context.claim_expires_at IS NULL THEN
        RAISE EXCEPTION 'Running run "%" does not have a finite claim in queue "%"',
            run_id,
            queue_name;
    END IF;

    -- Cancellation wins when its deadline and the finite lease become due at
    -- the same database timestamp. Otherwise expiry is reaped through the
    -- canonical failure/retry transition before ownership is reported lost.
    IF context.claim_expires_at <= context.observed_at THEN
        PERFORM steda.reap_expired_run(queue_name, run_id);

        EXECUTE format(
            'SELECT state FROM steda.%I WHERE id = $1',
            'tasks_' || queue_name
        )
        INTO current_task_state
        USING context.task_id;

        RETURN CASE
            WHEN current_task_state = 'cancelled' THEN 'cancelled'
            ELSE 'lease_lost'
        END;
    END IF;

    -- Supervision may run more frequently than renewal is necessary (for
    -- cancellation responsiveness). PostgreSQL decides when the authoritative
    -- lease has reached its renewal window so polling does not imply a write.
    IF renew_for_seconds IS NOT NULL
        AND context.claim_expires_at
            <= context.observed_at
                + make_interval(secs => renew_for_seconds::double precision / 2.0)
    THEN
        EXECUTE format(
            $query$
            UPDATE steda.%I
            SET claim_expires_at = greatest(
                claim_expires_at,
                $2 + make_interval(secs => $3)
            )
            WHERE id = $1
            $query$,
            'runs_' || queue_name
        )
        USING run_id, context.observed_at, renew_for_seconds;
    END IF;

    RETURN 'running';
END;
$$;

-- Reap a bounded batch of expired running attempts.
--
-- `SKIP LOCKED` allows maintenance to run concurrently across workers. The
-- return value is the number of runs that were still expired when revalidated
-- under the canonical run/task locks.
CREATE OR REPLACE FUNCTION steda.reap_expired_runs(
    queue_name text,
    reap_limit integer
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    reaped integer := 0;
    expired_run record;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);
    IF reap_limit IS NULL OR reap_limit <= 0 THEN
        RAISE EXCEPTION 'reap_limit must be > 0';
    END IF;

    FOR expired_run IN
        EXECUTE format(
            $query$
            SELECT id
            FROM steda.%I
            WHERE state = 'running'
              AND claim_expires_at <= $1
            ORDER BY claim_expires_at, id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            $query$,
            'runs_' || queue_name
        )
        USING now_at, reap_limit
    LOOP
        IF steda.reap_expired_run(queue_name, expired_run.id) THEN
            reaped := reaped + 1;
        END IF;
    END LOOP;

    RETURN reaped;
END;
$$;

-- Cancel a bounded batch of tasks whose persisted cancellation deadline is due.
--
-- Candidates reserve their authoritative active run with `SKIP LOCKED`, keeping
-- the same run -> task lock order used by ordinary state transitions.
CREATE OR REPLACE FUNCTION steda.cancel_expired_tasks(
    queue_name text,
    cancel_limit integer
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    cancelled integer := 0;
    candidate record;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);
    IF cancel_limit IS NULL OR cancel_limit <= 0 THEN
        RAISE EXCEPTION 'cancel_limit must be > 0';
    END IF;

    -- Reserve the authoritative active run rather than the task row. This keeps
    -- the same run -> task lock order used by cancel_task()/fail_run()/complete_run()
    -- while allowing concurrent maintenance workers to skip one another.
    FOR candidate IN
        EXECUTE format(
            $query$
            SELECT task.id
            FROM steda.%1$I task
            JOIN steda.%2$I run ON run.id = task.last_attempt_run
            WHERE task.state IN ('pending', 'sleeping', 'running')
              AND run.state IN ('pending', 'sleeping', 'running')
              AND steda.cancellation_due(
                    task.cancellation,
                    task.enqueue_at,
                    task.first_started_at,
                    $1
              )
            ORDER BY task.id
            LIMIT $2
            FOR UPDATE OF run SKIP LOCKED
            $query$,
            'tasks_' || queue_name,
            'runs_' || queue_name
        )
        USING now_at, cancel_limit
    LOOP
        IF steda.cancel_task(queue_name, candidate.id) THEN
            cancelled := cancelled + 1;
        END IF;
    END LOOP;

    RETURN cancelled;
END;
$$;

-- Claim up to `quantity` runnable attempts for one worker.
--
-- Before claiming, Steda performs bounded cancellation and expired-lease
-- maintenance. Eligible runs must be authoritative, due by `available_at`, and
-- have a task name present in the worker's `task_names` capabilities. Claimed
-- runs become `running` with a finite lease and the logical task is updated in
-- the same statement.
--
-- Ordering is by `available_at` and run ID. `FOR UPDATE SKIP LOCKED` permits
-- concurrent workers to claim independently without duplicate ownership.
CREATE OR REPLACE FUNCTION steda.claim_tasks(
    queue_name text,
    worker_id text,
    lease_seconds integer,
    quantity integer,
    task_names text[]
)
RETURNS TABLE (
    run_id uuid,
    id uuid,
    attempt integer,
    name text,
    params jsonb,
    headers jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    now_at timestamptz := steda.current_time();
    claim_expires_at timestamptz;
    claim_sql text;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    IF worker_id IS NULL OR worker_id ~ '^[[:space:]]*$' THEN
        RAISE EXCEPTION 'worker_id must be provided';
    END IF;
    IF lease_seconds IS NULL OR lease_seconds <= 0 THEN
        RAISE EXCEPTION 'lease_seconds must be > 0';
    END IF;
    IF quantity IS NULL OR quantity <= 0 THEN
        RAISE EXCEPTION 'quantity must be > 0';
    END IF;
    IF task_names IS NULL OR cardinality(task_names) = 0 THEN
        RAISE EXCEPTION 'task_names must contain at least one capability';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(task_names) AS task_name
        WHERE task_name IS NULL OR task_name ~ '^[[:space:]]*$'
    ) THEN
        RAISE EXCEPTION 'task_names cannot contain empty capabilities';
    END IF;

    -- Keep maintenance bounded to the same order of work as this claim call.
    PERFORM steda.cancel_expired_tasks(queue_name, quantity);
    PERFORM steda.reap_expired_runs(queue_name, quantity);

    -- Maintenance can block or process multiple rows. Assign leases from a fresh
    -- clock reading so newly claimed work receives the full configured lease.
    now_at := steda.current_time();
    claim_expires_at := now_at + make_interval(secs => lease_seconds);

    claim_sql := format(
        $query$
        WITH candidate_runs AS (
            SELECT
                run.id
            FROM steda.%1$I run
            JOIN steda.%2$I task ON task.id = run.task_id
            WHERE run.state IN ('pending', 'sleeping')
              AND task.state IN ('pending', 'sleeping')
              AND task.last_attempt_run = run.id
              AND run.available_at <= $1
              AND task.name = ANY($5)
              AND NOT steda.cancellation_due(
                    task.cancellation,
                    task.enqueue_at,
                    task.first_started_at,
                    $1
              )
            ORDER BY run.available_at, run.id
            LIMIT $2
            FOR UPDATE OF run SKIP LOCKED
        ),
        updated_runs AS (
            UPDATE steda.%1$I run
            SET
                state = 'running',
                claimed_by = $3,
                claim_expires_at = $4,
                started_at = coalesce(run.started_at, $1),
                available_at = $1
            WHERE id IN (SELECT id FROM candidate_runs)
            RETURNING
                run.id,
                run.task_id,
                run.attempt
        ),
        updated_tasks AS (
            UPDATE steda.%2$I task
            SET
                state = 'running',
                attempts = greatest(task.attempts, updated_run.attempt),
                first_started_at = coalesce(task.first_started_at, $1),
                last_attempt_run = updated_run.id
            FROM updated_runs updated_run
            WHERE task.id = updated_run.task_id
            RETURNING task.id
        )
        SELECT
            updated_run.id,
            task.id,
            updated_run.attempt,
            task.name,
            task.params,
            task.headers
        FROM updated_runs updated_run
        JOIN steda.%1$I run ON run.id = updated_run.id
        JOIN steda.%2$I task ON task.id = updated_run.task_id
        ORDER BY run.available_at, updated_run.id
        $query$,
        'runs_' || queue_name,
        'tasks_' || queue_name
    );

    RETURN QUERY EXECUTE claim_sql
    USING now_at, quantity, worker_id, claim_expires_at, task_names;
END;
$$;
