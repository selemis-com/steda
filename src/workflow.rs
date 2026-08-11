//! Typed contracts for durable workflow identities.

use serde::{Serialize, de::DeserializeOwned};

/// Compile-time contract for one durable checkpointed step.
///
/// Implement this trait on a marker type local to the workflow. The marker couples a stable
/// persisted step name to the value type Steda checkpoints for that step, so call sites pass a
/// Rust identity rather than an arbitrary string.
///
/// The value type is part of the contract:
///
/// ```compile_fail
/// use steda::{Result, Step, TaskContext};
///
/// struct Count;
///
/// impl Step for Count {
///     const NAME: &'static str = "count";
///     type Output = u64;
/// }
///
/// async fn wrong(ctx: &TaskContext) -> Result<String> {
///     ctx.step(Count, async || Ok(String::from("not a u64"))).await
/// }
/// ```
pub trait Step: Send + 'static {
    /// Stable persisted name for this workflow step.
    const NAME: &'static str;

    /// Value checkpointed after this step succeeds.
    type Output: Serialize + DeserializeOwned + Send + 'static;
}

/// Compile-time contract for one durable sleep point.
///
/// Sleep marker types occupy a distinct persisted namespace from [`Step`] checkpoints, so a sleep
/// cannot alias result-bearing workflow state even when both contracts use the same logical name.
pub trait Sleep: Send + 'static {
    /// Stable persisted name for this durable sleep point.
    const NAME: &'static str;
}
