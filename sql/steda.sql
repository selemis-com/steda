-- Steda PostgreSQL schema
--
-- The ordered migrations are the source for the complete `sql/steda.sql`
-- installation artifact. The merged file can be applied to a fresh database and
-- reapplied when upgrading an existing Steda installation, for example:
--
--   psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f sql/steda.sql
--
-- Edit the ordered migrations rather than the generated merged file.
--
-- Steda keeps durable task, run, retry, checkpoint, and queue state in
-- PostgreSQL. PostgreSQL is authoritative for execution state; workers are
-- disposable and may be restarted without losing durable work.
--
-- Steda-managed objects live in the `steda` schema. Applications should
-- normally use Steda's Rust API; the functions below are its database-side
-- implementation contract. Generated per-queue tables should not be modified
-- directly.
--
-- Steda reserves several application SQLSTATE values for conditions surfaced by
-- the Rust API:
--
--   ST001  task cancellation won the transition
--   ST002  addressed run has already failed
--   ST003  worker lease has expired
--   ST004  idempotency key conflicts with the original submission

-- ======================================================================
-- Core queue metadata and validation
-- ======================================================================
--
-- Establish the shared schema, logical queue registry, canonical database
-- clock, and queue-name rules used by all later sections.

DO $$
BEGIN
    IF current_setting('server_version_num')::integer < 180000 THEN
        RAISE EXCEPTION 'Steda requires PostgreSQL 18 or newer';
    END IF;
END;
$$;

CREATE SCHEMA IF NOT EXISTS steda;

-- Return the canonical database timestamp used for durable state transitions.
CREATE OR REPLACE FUNCTION steda.current_time()
RETURNS timestamptz
LANGUAGE sql
VOLATILE
AS $$
    SELECT clock_timestamp();
$$;

-- Registry of logical Steda queues.
--
-- Cleanup policy is persisted with the queue so retention behavior remains
-- authoritative in PostgreSQL rather than depending on whichever process
-- invokes cleanup. `cleanup_ttl` is the terminal-history retention period and
-- `cleanup_limit` bounds one cleanup batch.
CREATE TABLE IF NOT EXISTS steda.queues (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT steda.current_time(),
    cleanup_ttl interval NOT NULL DEFAULT interval '30 days'
        CHECK (cleanup_ttl >= interval '0 seconds'),
    cleanup_limit integer NOT NULL DEFAULT 1000
        CHECK (cleanup_limit >= 1)
);

-- Validate a logical queue name before it is used to derive PostgreSQL object
-- names.
--
-- PostgreSQL identifiers are limited to 63 bytes. Steda reserves the remaining
-- space for generated table/index/constraint prefixes, leaving 33 bytes for
-- the queue name. The limit is therefore byte-based rather than character-based.
CREATE OR REPLACE FUNCTION steda.validate_queue_name(queue_name text)
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    IF queue_name IS NULL OR queue_name ~ '^[[:space:]]*$' THEN
        RAISE EXCEPTION 'Queue name must be provided';
    END IF;

    IF octet_length(queue_name) > 33 THEN
        RAISE EXCEPTION 'Queue name "%" is too long (max 33 bytes).', queue_name;
    END IF;

    RETURN queue_name;
END;
$$;

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
            CHECK (state <> 'cancelled' OR cancelled_at IS NOT NULL),
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
            CHECK (state <> 'completed' OR (completed_at IS NOT NULL AND result IS NOT NULL)),
            CHECK (state <> 'failed' OR (failed_at IS NOT NULL AND failure_reason IS NOT NULL)),
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
            state JSONB NOT NULL,
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

-- ======================================================================
-- Queue lifecycle and maintenance policy
-- ======================================================================
--
-- Queue metadata and generated tables are one lifecycle unit. Advisory
-- transaction locks serialize create, drop, policy, and cleanup operations for
-- the same queue name so metadata cannot diverge from physical storage.

-- Create a logical queue and its physical storage.
--
-- Creation is idempotent only for a healthy existing queue: a repeated call
-- verifies the current storage shape and fails if it is incomplete.
CREATE OR REPLACE FUNCTION steda.create_queue(
    queue_name text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    row_count integer;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    -- Queue metadata and physical tables form one lifecycle unit. Serialize
    -- create/drop/cleanup/policy operations for the same validated name.
    PERFORM pg_advisory_xact_lock(hashtext('steda.queue'), hashtext(queue_name));

    INSERT INTO steda.queues (name)
    VALUES (queue_name)
    ON CONFLICT (name) DO NOTHING;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    IF row_count = 1 THEN
        PERFORM steda.create_queue_tables(queue_name);
    ELSE
        -- Repeated create is an integrity check, not a repair path.
        PERFORM steda.verify_queue_storage(queue_name);
    END IF;
END;
$$;

-- Drop a queue and all Steda-managed task history it contains.
--
-- Dropping a missing queue is a no-op. This operation is destructive: task,
-- run, and checkpoint rows for the queue are removed with the generated tables.
CREATE OR REPLACE FUNCTION steda.drop_queue(queue_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    existing_queue_name text;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);
    PERFORM pg_advisory_xact_lock(hashtext('steda.queue'), hashtext(queue_name));

    SELECT name
    INTO existing_queue_name
    FROM steda.queues
    WHERE name = queue_name;

    IF existing_queue_name IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format(
        $query$
        DROP TABLE IF EXISTS steda.%I CASCADE
        $query$,
        'checkpoints_' || queue_name
    );
    EXECUTE format(
        $query$
        DROP TABLE IF EXISTS steda.%I CASCADE
        $query$,
        'runs_' || queue_name
    );
    EXECUTE format(
        $query$
        DROP TABLE IF EXISTS steda.%I CASCADE
        $query$,
        'tasks_' || queue_name
    );

    DELETE FROM steda.queues
    WHERE name = queue_name;
END;
$$;

-- List registered logical queues in deterministic name order.
CREATE OR REPLACE FUNCTION steda.list_queues()
RETURNS TABLE (name text)
LANGUAGE sql
AS $$
    SELECT
        queue.name
    FROM steda.queues queue
    ORDER BY queue.name;
$$;

-- Return the persisted cleanup policy for one queue.
--
-- A missing queue produces no row; higher-level APIs may translate that into a
-- domain error.
CREATE OR REPLACE FUNCTION steda.get_queue_policy(requested_queue_name text)
RETURNS TABLE (
    queue_name text,
    cleanup_ttl interval,
    cleanup_limit integer
)
LANGUAGE sql
AS $$
    SELECT
        queue.name AS queue_name,
        queue.cleanup_ttl,
        queue.cleanup_limit
    FROM steda.queues queue
    WHERE queue.name = steda.validate_queue_name(requested_queue_name);
$$;

-- Update the persisted cleanup policy for one queue.
--
-- NULL arguments retain their current values. TTL is expressed in whole
-- seconds at this SQL boundary; zero TTL is valid and makes terminal tasks
-- immediately eligible for cleanup.
CREATE OR REPLACE FUNCTION steda.set_queue_policy(
    queue_name text,
    requested_cleanup_ttl_seconds integer DEFAULT NULL,
    requested_cleanup_limit integer DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    effective_cleanup_ttl interval;
    effective_cleanup_limit integer;
BEGIN
    queue_name := steda.validate_queue_name(queue_name);
    PERFORM pg_advisory_xact_lock(hashtext('steda.queue'), hashtext(queue_name));

    SELECT
        cleanup_ttl,
        cleanup_limit
    INTO
        effective_cleanup_ttl,
        effective_cleanup_limit
    FROM steda.queues
    WHERE name = queue_name
    FOR UPDATE;

    IF effective_cleanup_ttl IS NULL OR effective_cleanup_limit IS NULL THEN
        RAISE EXCEPTION 'Queue "%" does not exist', queue_name;
    END IF;

    IF requested_cleanup_ttl_seconds IS NOT NULL THEN
        effective_cleanup_ttl := requested_cleanup_ttl_seconds * interval '1 second';
    END IF;

    IF requested_cleanup_limit IS NOT NULL THEN
        effective_cleanup_limit := requested_cleanup_limit;
    END IF;

    IF effective_cleanup_ttl < interval '0 seconds' THEN
        RAISE EXCEPTION 'cleanup_ttl must be non-negative';
    END IF;

    IF effective_cleanup_limit < 1 THEN
        RAISE EXCEPTION 'cleanup_limit must be at least 1';
    END IF;

    UPDATE steda.queues
    SET
        cleanup_ttl = effective_cleanup_ttl,
        cleanup_limit = effective_cleanup_limit
    WHERE name = queue_name;
END;
$$;

-- ======================================================================
-- Task submission and result lookup
-- ======================================================================
--
-- A logical task stores its immutable submission request, effective retry and
-- cancellation policy, current durable state, and pointer to the authoritative
-- run. Each automatic or manual retry creates a new run row.

-- Return the canonical default retry strategy stored on newly submitted tasks.
--
-- The default is exponential backoff starting at 30 seconds, doubling per
-- failed attempt, and capped at one hour.
CREATE OR REPLACE FUNCTION steda.default_retry_strategy()
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT '{"kind":"exponential","baseSeconds":30.0,"factor":2.0,"maxSeconds":3600.0}'::jsonb;
$$;

-- Validate a canonical retry strategy and calculate the delay for a failed attempt.
--
-- This is the single database authority for retry timing. Strategies must already
-- have their canonical shape; defaults are resolved by spawn_task before storage.
-- Supported shapes are `none`, fixed delay (`baseSeconds`), and exponential
-- delay (`baseSeconds`, `factor`, and optional `maxSeconds`). Unknown keys and
-- non-finite/negative delays are rejected rather than ignored.
CREATE OR REPLACE FUNCTION steda.retry_delay_seconds(
    strategy jsonb,
    attempt integer
)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    maximum_delay_seconds constant double precision := 2147483647;
    retry_kind text;
    base_seconds double precision;
    retry_factor double precision;
    max_seconds double precision;
    exponent integer;
    unknown_key text;
BEGIN
    IF strategy IS NULL OR jsonb_typeof(strategy) <> 'object' THEN
        RAISE EXCEPTION 'retry strategy must be a JSON object';
    END IF;

    IF attempt IS NULL OR attempt < 1 THEN
        RAISE EXCEPTION 'retry attempt must be at least 1';
    END IF;

    IF NOT strategy ? 'kind' OR jsonb_typeof(strategy -> 'kind') <> 'string' THEN
        RAISE EXCEPTION 'retry strategy kind must be a string';
    END IF;

    retry_kind := strategy ->> 'kind';
    IF length(trim(retry_kind)) = 0 THEN
        RAISE EXCEPTION 'retry strategy kind must be provided';
    END IF;

    IF retry_kind = 'none' THEN
        SELECT keys.key
        INTO unknown_key
        FROM jsonb_object_keys(strategy) AS keys(key)
        WHERE keys.key <> 'kind'
        LIMIT 1;

        IF unknown_key IS NOT NULL THEN
            RAISE EXCEPTION 'retry strategy kind none does not support key "%"', unknown_key;
        END IF;

        RETURN 0;
    END IF;

    IF retry_kind = 'fixed' THEN
        SELECT keys.key
        INTO unknown_key
        FROM jsonb_object_keys(strategy) AS keys(key)
        WHERE keys.key NOT IN ('kind', 'baseSeconds')
        LIMIT 1;

        IF unknown_key IS NOT NULL THEN
            RAISE EXCEPTION 'fixed retry strategy does not support key "%"', unknown_key;
        END IF;

        IF NOT strategy ? 'baseSeconds' THEN
            RAISE EXCEPTION 'fixed retry strategy requires baseSeconds';
        END IF;
        IF jsonb_typeof(strategy -> 'baseSeconds') <> 'number' THEN
            RAISE EXCEPTION 'retry baseSeconds must be a JSON number';
        END IF;

        base_seconds := (strategy ->> 'baseSeconds')::double precision;
        IF base_seconds IS NULL
            OR base_seconds::text IN ('NaN', 'Infinity', '-Infinity')
            OR base_seconds < 0
            OR base_seconds > maximum_delay_seconds
        THEN
            RAISE EXCEPTION 'retry baseSeconds must be finite and between 0 and 2147483647 seconds';
        END IF;

        RETURN base_seconds;
    END IF;

    IF retry_kind = 'exponential' THEN
        SELECT keys.key
        INTO unknown_key
        FROM jsonb_object_keys(strategy) AS keys(key)
        WHERE keys.key NOT IN ('kind', 'baseSeconds', 'factor', 'maxSeconds')
        LIMIT 1;

        IF unknown_key IS NOT NULL THEN
            RAISE EXCEPTION 'exponential retry strategy does not support key "%"', unknown_key;
        END IF;

        IF NOT strategy ? 'baseSeconds' THEN
            RAISE EXCEPTION 'exponential retry strategy requires baseSeconds';
        END IF;
        IF NOT strategy ? 'factor' THEN
            RAISE EXCEPTION 'exponential retry strategy requires factor';
        END IF;
        IF jsonb_typeof(strategy -> 'baseSeconds') <> 'number' THEN
            RAISE EXCEPTION 'retry baseSeconds must be a JSON number';
        END IF;
        IF jsonb_typeof(strategy -> 'factor') <> 'number' THEN
            RAISE EXCEPTION 'retry factor must be a JSON number';
        END IF;
        IF strategy ? 'maxSeconds' AND jsonb_typeof(strategy -> 'maxSeconds') <> 'number' THEN
            RAISE EXCEPTION 'retry maxSeconds must be a JSON number';
        END IF;

        base_seconds := (strategy ->> 'baseSeconds')::double precision;
        retry_factor := (strategy ->> 'factor')::double precision;
        max_seconds := CASE
            WHEN strategy ? 'maxSeconds'
                THEN (strategy ->> 'maxSeconds')::double precision
            ELSE maximum_delay_seconds
        END;

        IF base_seconds IS NULL
            OR base_seconds::text IN ('NaN', 'Infinity', '-Infinity')
            OR base_seconds < 0
            OR base_seconds > maximum_delay_seconds
        THEN
            RAISE EXCEPTION 'retry baseSeconds must be finite and between 0 and 2147483647 seconds';
        END IF;

        IF retry_factor IS NULL
            OR retry_factor::text IN ('NaN', 'Infinity', '-Infinity')
            OR retry_factor <= 0
        THEN
            RAISE EXCEPTION 'retry factor must be finite and greater than zero';
        END IF;

        IF max_seconds IS NULL
            OR max_seconds::text IN ('NaN', 'Infinity', '-Infinity')
            OR max_seconds < 0
            OR max_seconds > maximum_delay_seconds
        THEN
            RAISE EXCEPTION 'retry maxSeconds must be finite and between 0 and 2147483647 seconds';
        END IF;

        IF base_seconds = 0 OR max_seconds = 0 THEN
            RETURN 0;
        END IF;

        exponent := attempt - 1;

        BEGIN
            RETURN least(base_seconds * power(retry_factor, exponent), max_seconds);
        EXCEPTION
            WHEN numeric_value_out_of_range THEN
                RETURN max_seconds;
        END;
    END IF;

    RAISE EXCEPTION 'unsupported retry strategy kind "%"', retry_kind;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range THEN
        RAISE EXCEPTION 'retry strategy contains an invalid number';
END;
$$;

-- Validate and canonicalize a persisted cancellation policy.
--
-- Cancellation values are whole non-negative seconds because the Rust API rounds
-- durations up before crossing the database boundary. Unknown keys and non-integer
-- values are rejected rather than becoming latent failures in later state transitions.
CREATE OR REPLACE FUNCTION steda.validate_cancellation_policy(
    cancellation jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    value numeric;
    unknown_key text;
BEGIN
    IF cancellation IS NULL OR cancellation = 'null'::jsonb THEN
        RETURN NULL;
    END IF;

    IF jsonb_typeof(cancellation) <> 'object' THEN
        RAISE EXCEPTION 'cancellation policy must be a JSON object';
    END IF;

    IF cancellation = '{}'::jsonb THEN
        RAISE EXCEPTION 'cancellation policy must define maxDelay or maxDuration';
    END IF;

    SELECT keys.key
    INTO unknown_key
    FROM jsonb_object_keys(cancellation) AS keys(key)
    WHERE keys.key NOT IN ('maxDelay', 'maxDuration')
    LIMIT 1;

    IF unknown_key IS NOT NULL THEN
        RAISE EXCEPTION 'cancellation policy does not support key "%"', unknown_key;
    END IF;

    FOREACH unknown_key IN ARRAY ARRAY['maxDelay', 'maxDuration']
    LOOP
        IF cancellation ? unknown_key THEN
            IF jsonb_typeof(cancellation -> unknown_key) <> 'number' THEN
                RAISE EXCEPTION 'cancellation % must be a whole number of seconds', unknown_key;
            END IF;

            value := (cancellation ->> unknown_key)::numeric;
            IF value <> trunc(value) OR value < 0 OR value > 9223372036854775807::numeric THEN
                RAISE EXCEPTION 'cancellation % must be a whole number between 0 and 9223372036854775807 seconds', unknown_key;
            END IF;
        END IF;
    END LOOP;

    RETURN cancellation;
END;
$$;

-- Evaluate a task cancellation deadline at one authoritative database timestamp.
--
-- Before a task starts, only maxDelay applies. After its first claim, maxDuration
-- applies for the remainder of the logical task, including retries and durable sleep.
CREATE OR REPLACE FUNCTION steda.cancellation_due(
    cancellation jsonb,
    enqueue_at timestamptz,
    first_started_at timestamptz,
    observed_at timestamptz
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN cancellation IS NULL THEN FALSE
        WHEN first_started_at IS NULL THEN coalesce(
            (cancellation ->> 'maxDelay')::bigint IS NOT NULL
            AND extract(epoch FROM (observed_at - enqueue_at))
                >= (cancellation ->> 'maxDelay')::bigint,
            FALSE
        )
        ELSE coalesce(
            (cancellation ->> 'maxDuration')::bigint IS NOT NULL
            AND extract(epoch FROM (observed_at - first_started_at))
                >= (cancellation ->> 'maxDuration')::bigint,
            FALSE
        )
    END;
$$;

-- Read the authoritative durable result state for one logical task.
--
-- Completed tasks expose the result from their authoritative run; failed tasks
-- expose its failure payload. Pending, running, sleeping, and cancelled tasks
-- return neither field. A missing task produces no row.
CREATE OR REPLACE FUNCTION steda.get_task_result(
    queue_name text,
    task_id uuid
)
RETURNS TABLE (
    task_name text,
    state text,
    result jsonb,
    failure_reason jsonb
)
LANGUAGE plpgsql
AS $$
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    RETURN QUERY EXECUTE format(
        $query$
        SELECT
            task.name AS task_name,
            task.state,
            CASE
                WHEN task.state = 'completed' THEN run.result
                ELSE NULL
            END AS result,
            CASE
                WHEN task.state = 'failed' THEN run.failure_reason
                ELSE NULL
            END AS failure_reason
        FROM steda.%I task
        LEFT JOIN steda.%I run ON run.id = task.last_attempt_run
        WHERE task.id = $1
        $query$,
        'tasks_' || queue_name,
        'runs_' || queue_name
    )
    USING task_id;
END;
$$;

-- Submit one logical task and create its initial pending run.
--
-- Supported options include headers, retry strategy, max attempts, cancellation
-- policy, and an optional idempotency key. Defaults are five attempts and the
-- canonical exponential retry strategy above. Empty headers are normalized to
-- NULL before persistence.
--
-- An idempotency key identifies the complete original submission request.
-- Replaying the same key with the same task name, params, headers, retry policy,
-- original attempt budget, and cancellation policy returns the existing task.
-- Reusing the key for a different request raises SQLSTATE `ST004`. Manual retry
-- does not alter that original submission identity.
CREATE OR REPLACE FUNCTION steda.spawn_task(
    queue_name text,
    task_name text,
    params jsonb,
    options jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE (
    task_id uuid,
    created boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_task_id uuid := uuidv7();
    initial_run_id uuid := uuidv7();
    current_attempt integer := 1;
    task_headers jsonb;
    task_retry_strategy jsonb;
    maximum_attempts integer;
    cancellation_policy jsonb;
    idempotency_key text;
    existing_task_id uuid;
    existing_name text;
    existing_params jsonb;
    existing_headers jsonb;
    existing_retry_strategy jsonb;
    existing_initial_max_attempts integer;
    existing_cancellation jsonb;
    maximum_attempts_numeric numeric;
    unknown_key text;
    row_count integer;
    now_at timestamptz := steda.current_time();
    normalized_params jsonb := coalesce(params, 'null'::jsonb);
BEGIN
    queue_name := steda.validate_queue_name(queue_name);

    IF options IS NULL THEN
        options := '{}'::jsonb;
    ELSIF jsonb_typeof(options) <> 'object' THEN
        RAISE EXCEPTION 'spawn options must be a JSON object';
    END IF;

    SELECT keys.key
    INTO unknown_key
    FROM jsonb_object_keys(options) AS keys(key)
    WHERE keys.key NOT IN (
        'headers',
        'maxAttempts',
        'retryStrategy',
        'cancellation',
        'idempotencyKey'
    )
    LIMIT 1;

    IF unknown_key IS NOT NULL THEN
        RAISE EXCEPTION 'spawn options do not support key "%"', unknown_key;
    END IF;

    IF task_name IS NULL OR task_name ~ '^[[:space:]]*$' THEN
        RAISE EXCEPTION 'task_name must be provided';
    END IF;

    IF octet_length(task_name) > 1024 THEN
        RAISE EXCEPTION 'task_name must be at most 1024 bytes';
    END IF;

    task_headers := options -> 'headers';
    task_retry_strategy := options -> 'retryStrategy';

    IF task_headers = 'null'::jsonb OR task_headers = '{}'::jsonb THEN
        task_headers := NULL;
    ELSIF task_headers IS NOT NULL AND jsonb_typeof(task_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be a JSON object';
    END IF;

    IF options ? 'maxAttempts' THEN
        IF jsonb_typeof(options -> 'maxAttempts') <> 'number' THEN
            RAISE EXCEPTION 'maxAttempts must be a JSON integer';
        END IF;

        maximum_attempts_numeric := (options ->> 'maxAttempts')::numeric;
        IF maximum_attempts_numeric <> trunc(maximum_attempts_numeric)
            OR maximum_attempts_numeric < 1
            OR maximum_attempts_numeric > 2147483647
        THEN
            RAISE EXCEPTION 'maxAttempts must be an integer between 1 and 2147483647';
        END IF;
        maximum_attempts := maximum_attempts_numeric::integer;
    END IF;

    cancellation_policy := steda.validate_cancellation_policy(options -> 'cancellation');

    IF options ? 'idempotencyKey' THEN
        IF jsonb_typeof(options -> 'idempotencyKey') <> 'string' THEN
            RAISE EXCEPTION 'idempotencyKey must be a JSON string';
        END IF;
        idempotency_key := options ->> 'idempotencyKey';
    END IF;

    maximum_attempts := coalesce(maximum_attempts, 5);
    task_retry_strategy := coalesce(task_retry_strategy, steda.default_retry_strategy());

    IF idempotency_key IS NOT NULL THEN
        IF idempotency_key ~ '^[[:space:]]*$' THEN
            RAISE EXCEPTION 'idempotency_key must not be empty';
        END IF;

        IF octet_length(idempotency_key) > 1024 THEN
            RAISE EXCEPTION 'idempotency_key must be at most 1024 bytes';
        END IF;
    END IF;

    -- Validate the complete strategy through the same function used later by
    -- fail_run. Delay growth itself is saturating, so large attempt budgets do
    -- not make an otherwise capped strategy invalid.
    PERFORM steda.retry_delay_seconds(task_retry_strategy, 1);

    -- One insert path serves both ordinary and idempotent spawns. PostgreSQL
    -- unique indexes permit multiple NULL idempotency keys, so only a concrete
    -- reused key can cause this statement to insert zero rows.
    EXECUTE format(
        $query$
        INSERT INTO steda.%I (
            id,
            name,
            params,
            headers,
            retry_strategy,
            initial_max_attempts,
            max_attempts,
            cancellation,
            enqueue_at,
            first_started_at,
            state,
            attempts,
            last_attempt_run,
            cancelled_at,
            idempotency_key
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $6,
            $7,
            $8,
            NULL,
            'pending',
            $9,
            $10,
            NULL,
            $11
        )
        ON CONFLICT (idempotency_key) DO NOTHING
        $query$,
        'tasks_' || queue_name
    )
    USING
        resolved_task_id,
        task_name,
        normalized_params,
        task_headers,
        task_retry_strategy,
        maximum_attempts,
        cancellation_policy,
        now_at,
        current_attempt,
        initial_run_id,
        idempotency_key;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    IF row_count = 0 THEN
        IF idempotency_key IS NULL THEN
            RAISE EXCEPTION 'Task insert unexpectedly conflicted without an idempotency key';
        END IF;

        EXECUTE format(
            $query$
            SELECT
                id,
                name,
                params,
                headers,
                retry_strategy,
                initial_max_attempts,
                cancellation
            FROM steda.%I
            WHERE idempotency_key = $1
            $query$,
            'tasks_' || queue_name
        )
        INTO
            existing_task_id,
            existing_name,
            existing_params,
            existing_headers,
            existing_retry_strategy,
            existing_initial_max_attempts,
            existing_cancellation
        USING idempotency_key;

        IF existing_name IS DISTINCT FROM task_name
            OR existing_params IS DISTINCT FROM normalized_params
            OR existing_headers IS DISTINCT FROM task_headers
            OR existing_retry_strategy IS DISTINCT FROM task_retry_strategy
            OR existing_initial_max_attempts IS DISTINCT FROM maximum_attempts
            OR existing_cancellation IS DISTINCT FROM cancellation_policy
        THEN
            RAISE EXCEPTION sqlstate 'ST004'
                USING message = format(
                    'Idempotency key "%s" was already used for a different task request',
                    idempotency_key
                );
        END IF;

        RETURN QUERY
        SELECT
            existing_task_id,
            FALSE;

        RETURN;
    END IF;

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
    USING initial_run_id, resolved_task_id, current_attempt, now_at;

    RETURN QUERY
    SELECT
        resolved_task_id,
        TRUE;
END;
$$;

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
        RAISE EXCEPTION sqlstate 'ST001'
            USING message = 'Task has been cancelled';
    END IF;

    IF context.run_state = 'failed' THEN
        RAISE EXCEPTION sqlstate 'ST002'
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
        RAISE EXCEPTION sqlstate 'ST001'
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
        RAISE EXCEPTION sqlstate 'ST003'
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
                'workerId', context.claimed_by,
                'claimExpiredAt', context.claim_expires_at,
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
    task_id uuid,
    attempt integer,
    task_name text,
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
        FROM unnest(task_names) AS requested(task_name)
        WHERE requested.task_name IS NULL
           OR requested.task_name ~ '^[[:space:]]*$'
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
            updated_run.id AS run_id,
            task.id AS task_id,
            updated_run.attempt,
            task.name AS task_name,
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

-- ======================================================================
-- Workflow checkpoint persistence
-- ======================================================================
--
-- Checkpoints provide durable workflow replay within one logical task. A checkpoint
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
    new_checkpoint_state jsonb,
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

    -- A durable checkpoint name is immutable for the lifetime of the logical task.
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
    USING task_id, checkpoint_name, new_checkpoint_state;

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
    checkpoint_name text,
    checkpoint_state jsonb
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
            checkpoint.name AS checkpoint_name,
            checkpoint.state AS checkpoint_state
        FROM steda.%I checkpoint
        WHERE checkpoint.task_id = $1
        $query$,
        'checkpoints_' || queue_name
    )
    USING task_id;
END;
$$;

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
