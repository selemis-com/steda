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
    SELECT '{"kind":"exponential","base_seconds":30.0,"factor":2.0,"max_seconds":3600.0}'::jsonb;
$$;

-- Validate a canonical retry strategy and calculate the delay for a failed attempt.
--
-- This is the single database authority for retry timing. Strategies must already
-- have their canonical shape; defaults are resolved by spawn_task before storage.
-- Supported shapes are `none`, fixed delay (`base_seconds`), and exponential
-- delay (`base_seconds`, `factor`, and optional `max_seconds`). Unknown keys and
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

    retry_kind := strategy ->> 'kind';
    IF retry_kind IS NULL OR length(trim(retry_kind)) = 0 THEN
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
        WHERE keys.key NOT IN ('kind', 'base_seconds')
        LIMIT 1;

        IF unknown_key IS NOT NULL THEN
            RAISE EXCEPTION 'fixed retry strategy does not support key "%"', unknown_key;
        END IF;

        IF NOT strategy ? 'base_seconds' THEN
            RAISE EXCEPTION 'fixed retry strategy requires base_seconds';
        END IF;

        base_seconds := (strategy ->> 'base_seconds')::double precision;
        IF base_seconds IS NULL
            OR base_seconds::text IN ('NaN', 'Infinity', '-Infinity')
            OR base_seconds < 0
            OR base_seconds > maximum_delay_seconds
        THEN
            RAISE EXCEPTION 'retry base_seconds must be finite and between 0 and 2147483647 seconds';
        END IF;

        RETURN base_seconds;
    END IF;

    IF retry_kind = 'exponential' THEN
        SELECT keys.key
        INTO unknown_key
        FROM jsonb_object_keys(strategy) AS keys(key)
        WHERE keys.key NOT IN ('kind', 'base_seconds', 'factor', 'max_seconds')
        LIMIT 1;

        IF unknown_key IS NOT NULL THEN
            RAISE EXCEPTION 'exponential retry strategy does not support key "%"', unknown_key;
        END IF;

        IF NOT strategy ? 'base_seconds' THEN
            RAISE EXCEPTION 'exponential retry strategy requires base_seconds';
        END IF;
        IF NOT strategy ? 'factor' THEN
            RAISE EXCEPTION 'exponential retry strategy requires factor';
        END IF;

        base_seconds := (strategy ->> 'base_seconds')::double precision;
        retry_factor := (strategy ->> 'factor')::double precision;
        max_seconds := CASE
            WHEN strategy ? 'max_seconds'
                THEN (strategy ->> 'max_seconds')::double precision
            ELSE maximum_delay_seconds
        END;

        IF base_seconds IS NULL
            OR base_seconds::text IN ('NaN', 'Infinity', '-Infinity')
            OR base_seconds < 0
            OR base_seconds > maximum_delay_seconds
        THEN
            RAISE EXCEPTION 'retry base_seconds must be finite and between 0 and 2147483647 seconds';
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
            RAISE EXCEPTION 'retry max_seconds must be finite and between 0 and 2147483647 seconds';
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
        RAISE EXCEPTION 'cancellation policy must define max_delay or max_duration';
    END IF;

    SELECT keys.key
    INTO unknown_key
    FROM jsonb_object_keys(cancellation) AS keys(key)
    WHERE keys.key NOT IN ('max_delay', 'max_duration')
    LIMIT 1;

    IF unknown_key IS NOT NULL THEN
        RAISE EXCEPTION 'cancellation policy does not support key "%"', unknown_key;
    END IF;

    FOREACH unknown_key IN ARRAY ARRAY['max_delay', 'max_duration']
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
-- Before a task starts, only max_delay applies. After its first claim, max_duration
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
            (cancellation ->> 'max_delay')::bigint IS NOT NULL
            AND extract(epoch FROM (observed_at - enqueue_at))
                >= (cancellation ->> 'max_delay')::bigint,
            FALSE
        )
        ELSE coalesce(
            (cancellation ->> 'max_duration')::bigint IS NOT NULL
            AND extract(epoch FROM (observed_at - first_started_at))
                >= (cancellation ->> 'max_duration')::bigint,
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
    id uuid,
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
            task.id,
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
    id uuid,
    run_id uuid,
    attempt integer,
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
    existing_run_id uuid;
    existing_attempt integer;
    existing_name text;
    existing_params jsonb;
    existing_headers jsonb;
    existing_retry_strategy jsonb;
    existing_initial_max_attempts integer;
    existing_cancellation jsonb;
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

    IF task_name IS NULL OR task_name ~ '^[[:space:]]*$' THEN
        RAISE EXCEPTION 'task_name must be provided';
    END IF;

    IF octet_length(task_name) > 1024 THEN
        RAISE EXCEPTION 'task_name must be at most 1024 bytes';
    END IF;

    task_headers := options -> 'headers';
    task_retry_strategy := options -> 'retry_strategy';

    IF task_headers = 'null'::jsonb OR task_headers = '{}'::jsonb THEN
        task_headers := NULL;
    ELSIF task_headers IS NOT NULL AND jsonb_typeof(task_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be a JSON object';
    END IF;

    IF options ? 'max_attempts' THEN
        maximum_attempts := (options ->> 'max_attempts')::int;

        IF maximum_attempts IS NOT NULL AND maximum_attempts < 1 THEN
            RAISE EXCEPTION 'max_attempts must be >= 1';
        END IF;
    END IF;

    cancellation_policy := steda.validate_cancellation_policy(options -> 'cancellation');
    idempotency_key := options ->> 'idempotency_key';

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
                last_attempt_run,
                attempts,
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
            existing_run_id,
            existing_attempt,
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
            existing_run_id,
            existing_attempt,
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
        initial_run_id,
        current_attempt,
        TRUE;
END;
$$;
