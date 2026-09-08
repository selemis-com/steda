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

    /// Bind this static workflow step to one runtime identity.
    ///
    /// The key is persisted as part of the checkpoint identity and therefore must be stable across
    /// retries of the same logical workflow item. Validation occurs when the keyed step is used by
    /// [`crate::TaskContext::step_keyed`].
    pub fn keyed(self, key: impl Into<String>) -> KeyedStep<Output> {
        KeyedStep { step: self, key: key.into() }
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

/// One runtime-keyed instance of a statically defined durable [`Step`].
///
/// Keyed steps preserve the static typed workflow definition while allowing a workflow to repeat
/// the same operation for runtime-identified items such as pages, loop iterations, agent turns,
/// or tool calls. The key is durable workflow identity: reusing the same step and key returns the
/// previously committed value, while a different key creates an independent checkpoint.
pub struct KeyedStep<Output> {
    /// Static typed step definition shared by every instance.
    step: Step<Output>,
    /// Runtime identity of this particular step instance.
    key: String,
}

impl<Output> Clone for KeyedStep<Output> {
    fn clone(&self) -> Self {
        Self { step: self.step, key: self.key.clone() }
    }
}

impl<Output> KeyedStep<Output> {
    /// Return the static step definition.
    pub const fn step(&self) -> Step<Output> {
        self.step
    }

    /// Return the runtime key identifying this step instance.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl<Output> fmt::Debug for KeyedStep<Output> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedStep")
            .field("step", &self.step.name())
            .field("key", &self.key)
            .finish()
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
