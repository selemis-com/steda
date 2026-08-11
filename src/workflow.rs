//! Typed values for durable workflow identities.

use std::{fmt, marker::PhantomData};

use serde::{Serialize, de::DeserializeOwned};

/// Stable identity and checkpointed value type for one durable workflow step.
///
/// Define steps as constants shared by the workflow code that uses them:
///
/// ```
/// use steda::Step;
///
/// const COUNT: Step<u64> = Step::new("count");
/// ```
///
/// The output type is part of the value, so a step cannot be used with a body that returns an
/// unrelated type.
pub struct Step<Output> {
    /// Stable persisted step name.
    name: &'static str,
    /// Checkpointed Rust value type.
    marker: PhantomData<fn() -> Output>,
}

impl<Output> Copy for Step<Output> {}

impl<Output> Clone for Step<Output> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Output> Step<Output> {
    /// Return the stable persisted step name.
    pub const fn name(self) -> &'static str {
        self.name
    }
}

impl<Output> Step<Output>
where
    Output: Serialize + DeserializeOwned + Send + 'static,
{
    /// Define a durable workflow step with a stable persisted name.
    pub const fn new(name: &'static str) -> Self {
        Self { name, marker: PhantomData }
    }
}

impl<Output> fmt::Debug for Step<Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Step").field(&self.name).finish()
    }
}

/// Stable identity for one durable sleep point.
///
/// Sleep values occupy a distinct persisted namespace from [`Step`] checkpoints, so a sleep
/// cannot alias result-bearing workflow state even when both use the same logical name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sleep {
    /// Stable persisted sleep name.
    name: &'static str,
}

impl Sleep {
    /// Define a durable sleep point with a stable persisted name.
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }

    /// Return the stable persisted sleep name.
    pub const fn name(self) -> &'static str {
        self.name
    }
}
