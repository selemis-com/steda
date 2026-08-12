-- Steda PostgreSQL schema
--
-- The ordered migrations are the source for the complete `sql/steda.sql`
-- installation artifact. The merged file can be applied to a fresh database and
-- reapplied when upgrading an existing Steda installation, for example:
--
--     psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/steda.sql
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
--   AB001  task cancellation won the transition
--   AB002  addressed run has already failed
--   AB003  worker lease has expired
--   AB004  idempotency key conflicts with the original submission

-- ======================================================================
-- Core queue metadata and validation
-- ======================================================================
--
-- Establish the shared schema, logical queue registry, canonical database
-- clock, and queue-name rules used by all later sections.

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
