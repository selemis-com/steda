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
CREATE OR REPLACE FUNCTION steda.get_queue_policy(queue_name text)
RETURNS TABLE (
    name text,
    cleanup_ttl interval,
    cleanup_limit integer
)
LANGUAGE sql
AS $$
    SELECT
        queue.name,
        queue.cleanup_ttl,
        queue.cleanup_limit
    FROM steda.queues queue
    WHERE queue.name = queue_name;
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
