-- ======================================================================
-- Per-queue physical storage provisioning
-- ======================================================================
--
-- Each logical queue owns three generated tables:
--
--   tasks_<queue>       one durable row per logical task
--   runs_<queue>        one row per execution attempt
--   checkpoints_<queue> immutable named workflow checkpoints
--
-- A task may have many historical runs, but at most one pending/running/sleeping
-- run is authoritative at a time. Steda owns these tables and their indexes.

-- Create the physical storage for a newly registered queue.
--
-- This function is called by `steda.create_queue`; callers normally should not
-- invoke it directly. Queue creation is intentionally not a repair operation:
-- existing queues are verified rather than silently recreated.
CREATE OR REPLACE FUNCTION steda.create_queue_tables(queue_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM steda.validate_queue_name(queue_name);

    -- Logical task state survives worker restarts and spans all attempts.
    -- `initial_max_attempts` preserves the spawn-time request for idempotency;
    -- `max_attempts` may later grow through explicit manual retry.
    EXECUTE format(
        $query$
        CREATE TABLE steda.%I (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL
                CHECK (name !~ '^[[:space:]]*$' AND octet_length(name) <= 1024),
            params JSONB NOT NULL,
            headers JSONB
                CHECK (headers IS NULL OR jsonb_typeof(headers) = 'object'),
            retry_strategy JSONB NOT NULL
                CHECK (steda.retry_delay_seconds(retry_strategy, 1) >= 0),
            initial_max_attempts INTEGER NOT NULL DEFAULT 5
                CHECK (initial_max_attempts >= 1),
            max_attempts INTEGER NOT NULL DEFAULT 5
                CHECK (max_attempts >= initial_max_attempts),
            cancellation JSONB,
            enqueue_at TIMESTAMPTZ NOT NULL DEFAULT steda.current_time(),
            first_started_at TIMESTAMPTZ,
            state TEXT NOT NULL
                CHECK (state IN (
                    'pending',
                    'running',
                    'sleeping',
                    'completed',
                    'failed',
                    'cancelled'
                )),
            attempts INTEGER NOT NULL DEFAULT 0
                CHECK (attempts >= 0),
            last_attempt_run UUID NOT NULL,
            cancelled_at TIMESTAMPTZ,
            idempotency_key TEXT UNIQUE
                CHECK (
                    idempotency_key IS NULL
                    OR (idempotency_key !~ '^[[:space:]]*$' AND octet_length(idempotency_key) <= 1024)
                )
        ) WITH (FILLFACTOR = 70)
        $query$,
        'tasks_' || queue_name
    );

    -- Runs represent individual attempts. A finite lease is present only while
    -- a run is `running`; terminal/suspended states cannot retain worker ownership.
    EXECUTE format(
        $query$
        CREATE TABLE steda.%I (
            id UUID PRIMARY KEY,
            task_id UUID NOT NULL REFERENCES steda.%I(id) ON DELETE CASCADE,
            attempt INTEGER NOT NULL
                CHECK (attempt >= 1),
            state TEXT NOT NULL
                CHECK (state IN (
                    'pending',
                    'running',
                    'sleeping',
                    'completed',
                    'failed',
                    'cancelled'
                )),
            claimed_by TEXT,
            claim_expires_at TIMESTAMPTZ,
            available_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            failed_at TIMESTAMPTZ,
            result JSONB,
            failure_reason JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT steda.current_time(),
            CHECK (
                (
                    state = 'running'
                    AND claimed_by IS NOT NULL
                    AND claimed_by !~ '^[[:space:]]*$'
                    AND claim_expires_at IS NOT NULL
                )
                OR (
                    state <> 'running'
                    AND claimed_by IS NULL
                    AND claim_expires_at IS NULL
                )
            ),
            UNIQUE (task_id, attempt),
            UNIQUE (task_id, id)
        ) WITH (FILLFACTOR = 70)
        $query$,
        'runs_' || queue_name,
        'tasks_' || queue_name
    );

    -- Tasks always point at their authoritative run. The reference is deferred
    -- because spawn creates the logical task row before its initial run inside
    -- the same transaction.
    EXECUTE format(
        'ALTER TABLE steda.%I ADD CONSTRAINT %I FOREIGN KEY (id, last_attempt_run) REFERENCES steda.%I(task_id, id) DEFERRABLE INITIALLY DEFERRED',
        'tasks_' || queue_name,
        ('tasks_' || queue_name) || '_last_attempt_run_fkey',
        'runs_' || queue_name
    );

    -- Checkpoints belong to the logical task, not to any individual run. The
    -- `(task_id, name)` primary key makes each durable step name unique and
    -- first-write-wins semantics make checkpoint rows immutable.
    EXECUTE format(
        $query$
        CREATE TABLE steda.%I (
            task_id UUID NOT NULL REFERENCES steda.%I(id) ON DELETE CASCADE,
            name TEXT NOT NULL
                CHECK (name !~ '^[[:space:]]*$' AND octet_length(name) <= 1024),
            state JSONB,
            PRIMARY KEY (task_id, name)
        )
        $query$,
        'checkpoints_' || queue_name,
        'tasks_' || queue_name
    );

    -- Claim selection scans runnable state by availability time.
    EXECUTE format(
        $query$
        CREATE INDEX %I
        ON steda.%I (state, available_at)
        $query$,
        ('runs_' || queue_name) || '_state_available_at_index',
        'runs_' || queue_name
    );

    -- Enforce the core invariant that a logical task has at most one active
    -- (pending, running, or sleeping) run at a time.
    EXECUTE format(
        $query$
        CREATE UNIQUE INDEX %I
        ON steda.%I (task_id)
        WHERE state IN ('pending', 'running', 'sleeping')
        $query$,
        ('runs_' || queue_name) || '_one_active_run_index',
        'runs_' || queue_name
    );

    -- Expired-lease maintenance scans only currently owned runs.
    EXECUTE format(
        $query$
        CREATE INDEX %I
        ON steda.%I (claim_expires_at)
        WHERE state = 'running'
          AND claim_expires_at IS NOT NULL
        $query$,
        ('runs_' || queue_name) || '_claim_expires_at_index',
        'runs_' || queue_name
    );
END;
$$;


-- Verify the storage created for an already-registered queue without mutating it.
--
-- Repeated queue creation uses this as a basic storage-shape check. Missing
-- tables, expected columns, required index names, primary keys, or foreign-key
-- relationships are treated as corruption/configuration errors rather than
-- repaired implicitly. This does not attempt exhaustive schema introspection.
CREATE OR REPLACE FUNCTION steda.verify_queue_storage(queue_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    tasks_relation regclass;
    runs_relation regclass;
    checkpoints_relation regclass;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);
    tasks_relation := to_regclass(format('steda.%I', 'tasks_' || queue_name));
    runs_relation := to_regclass(format('steda.%I', 'runs_' || queue_name));
    checkpoints_relation := to_regclass(format('steda.%I', 'checkpoints_' || queue_name));

    IF tasks_relation IS NULL OR runs_relation IS NULL OR checkpoints_relation IS NULL THEN
        RAISE EXCEPTION 'Queue "%" storage is incomplete', queue_name;
    END IF;

    -- These zero-row reads verify that the expected persisted columns are present
    -- without attempting to prove every type, nullability, or constraint detail.
    EXECUTE format(
        'SELECT id, name, params, headers, retry_strategy, initial_max_attempts, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, cancelled_at, idempotency_key FROM steda.%I LIMIT 0',
        'tasks_' || queue_name
    );
    EXECUTE format(
        'SELECT id, task_id, attempt, state, claimed_by, claim_expires_at, available_at, started_at, completed_at, failed_at, result, failure_reason, created_at FROM steda.%I LIMIT 0',
        'runs_' || queue_name
    );
    EXECUTE format(
        'SELECT task_id, name, state FROM steda.%I LIMIT 0',
        'checkpoints_' || queue_name
    );

    IF to_regclass(format('steda.%I', ('runs_' || queue_name) || '_state_available_at_index')) IS NULL
        OR to_regclass(format('steda.%I', ('runs_' || queue_name) || '_one_active_run_index')) IS NULL
        OR to_regclass(format('steda.%I', ('runs_' || queue_name) || '_claim_expires_at_index')) IS NULL
    THEN
        RAISE EXCEPTION 'Queue "%" storage is missing required indexes', queue_name;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = tasks_relation AND contype = 'p'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = runs_relation AND contype = 'p'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = checkpoints_relation AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'Queue "%" storage is missing a primary key', queue_name;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = tasks_relation AND confrelid = runs_relation AND contype = 'f'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = runs_relation AND confrelid = tasks_relation AND contype = 'f'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = checkpoints_relation AND confrelid = tasks_relation AND contype = 'f'
    ) THEN
        RAISE EXCEPTION 'Queue "%" storage is missing required foreign keys', queue_name;
    END IF;
END;
$$;
