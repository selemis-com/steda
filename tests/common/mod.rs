//! Shared support for integration tests.

use std::time::{SystemTime, UNIX_EPOCH};

const MAX_QUEUE_NAME_BYTES: usize = 33;

/// Produces a unique queue name with the supplied prefix.
pub(crate) fn unique_queue(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();
    let suffix = suffix.to_string();
    let maximum_prefix_bytes = MAX_QUEUE_NAME_BYTES.saturating_sub(suffix.len() + 1);
    let mut prefix_bytes = 0;
    let prefix: String = prefix
        .chars()
        .take_while(|character| {
            let next_length = prefix_bytes + character.len_utf8();
            if next_length > maximum_prefix_bytes {
                return false;
            }
            prefix_bytes = next_length;
            true
        })
        .collect();

    format!("{prefix}_{suffix}")
}

/// Replace the production clock with the session-overridable clock used by deterministic tests.
pub(crate) const INSTALL_FAKE_CLOCK_SQL: &str = r#"
CREATE OR REPLACE FUNCTION steda.current_time()
RETURNS timestamptz
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    configured_time text;
BEGIN
    configured_time := current_setting('steda.fake_now', true);

    IF configured_time IS NOT NULL AND length(trim(configured_time)) > 0 THEN
        RETURN configured_time::timestamptz;
    END IF;

    RETURN clock_timestamp();
END;
$$
"#;

/// Install the deterministic clock override into this test database.
#[allow(dead_code, clippy::allow_attributes, reason = "shared helper is not used by every test")]
pub(crate) async fn install_fake_clock(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(INSTALL_FAKE_CLOCK_SQL).execute(pool).await?;
    Ok(())
}
