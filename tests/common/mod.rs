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
