//! PostgreSQL-backed durable task execution for Rust.
//!
//! Steda keeps the execution model deliberately small: application code defines typed
//! [`Task`] values, producers persist logical tasks into named [`Queue`]s, and workers
//! advertise the tasks they can execute. `PostgreSQL` is authoritative for task state,
//! attempts, leases, retries, checkpoints, durable sleeps, cancellation, and results; worker
//! processes are replaceable compute.
//!
//! Steda requires `PostgreSQL` 18 or later.
//!
//! # Installation
//!
//! Add the `steda` crate and apply the `sql/steda.sql` file from the same release to the target
//! database before producers or workers start. Apply it atomically; with `psql`, use
//! `--single-transaction -v ON_ERROR_STOP=1`. Reapply the new release's `steda.sql` the same way
//! when upgrading.
//!
//! Steda has no default crate features. Enable `tls-rustls` or `tls-native-tls` when
//! [`Steda::connect`] needs TLS support.
//!
//! # Quick start
//!
//! A task is a small constant value that couples a stable persisted name to serializable input
//! and output types. Producer and worker code share that definition without a runtime registry.
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use steda::{Result, Steda, Task, TaskContext};
//!
//! #[derive(Deserialize, Serialize)]
//! struct AddInput {
//!     left: i64,
//!     right: i64,
//! }
//!
//! #[derive(Deserialize, Serialize)]
//! struct AddOutput {
//!     sum: i64,
//! }
//!
//! const ADD: Task<AddInput, AddOutput> = Task::new("add");
//!
//! # async fn example() -> Result<()> {
//! let steda = Steda::connect("postgres://localhost/app").await?;
//!
//! let queue = steda.queue("default")?;
//! queue.create().await?;
//!
//! let worker = queue
//!     .worker()
//!     .task(ADD, async |input: AddInput, _ctx: TaskContext| {
//!         Ok(AddOutput { sum: input.left + input.right })
//!     })
//!     .build()?;
//!
//! let task = queue.spawn(ADD, AddInput { left: 20, right: 22 }).await?;
//!
//! // Long-lived applications normally run the worker in their process supervisor.
//! let worker_task = tokio::spawn(async move { worker.run().await });
//! assert_eq!(task.result().await?.sum, 42);
//! worker_task.abort();
//! # Ok(())
//! # }
//! ```
//!
//! # Execution model
//!
//! Steda provides **at-least-once execution**. A logical task may run more than once after retries
//! or lease recovery. `PostgreSQL` fencing prevents stale attempts from committing Steda state,
//! but arbitrary external side effects can still repeat and need their own idempotency or fencing
//! when that matters.
//!
//! ## Logical tasks and attempts
//!
//! One spawn creates one **logical task** identified by [`TaskId`]. Each execution try is a
//! separate **run** identified by [`RunId`] and a one-based attempt number. Automatic retries,
//! manual retries, durable sleeps, and worker loss do not change the logical task identity.
//!
//! [`TaskContext::task_id`] returns the logical task ID, while [`TaskContext::run_id`] and
//! [`TaskContext::attempt`] describe the currently executing attempt.
//!
//! The default attempt budget is five. The default [`RetryStrategy`] is exponential backoff
//! starting at 30 seconds, doubling on each retry, with a one-hour cap. Use
//! [`Spawn::max_attempts`] and [`Spawn::retry_strategy`] when a task needs a different policy.
//! Calling [`TaskHandle::retry`] adds another attempt to a terminally failed task without
//! changing the original spawn configuration used for idempotency comparison.
//!
//! ## Claims, leases, and stale-worker fencing
//!
//! A worker claim is a finite `PostgreSQL` lease, not permanent ownership. Healthy workers
//! supervise running attempts and renew their leases. If a worker disappears, an expired lease
//! allows another worker to recover the task. A stale worker cannot later complete, fail,
//! checkpoint, or otherwise mutate a run it no longer owns; ownership is enforced by the
//! database rather than by process-local convention.
//!
//! [`WorkerBuilder::lease_duration`] controls the requested lease duration. The default is
//! 120 seconds. A lease duration is **not** a task timeout: healthy long-running handlers may
//! execute for many lease periods while supervision renews ownership.
//!
//! ## In-process and provisioned execution
//!
//! [`WorkerBuilder::task`] registers the usual reusable in-process async handler. For workloads
//! that need per-attempt isolation, [`WorkerBuilder::task_executor`] registers a reusable
//! [`TaskExecutor`] that may provision a process, container, sandbox, VM, Kubernetes Job, or
//! remote execution environment for each attempt.
//!
//! This is one execution path, not two durability models. The long-lived Steda worker still owns
//! the claim, lease supervision, cancellation observation, retry policy, and terminal database
//! transition. A custom executor only decides **where one already-claimed attempt computes**.
//! Returning an error from a custom executor is therefore an ordinary failed attempt. If the
//! worker process itself disappears, recovery still follows lease expiry.
//! Steda may drop the executor future when supervision observes cancellation, suspension, or
//! lease loss; executors that provision external compute must make that drop terminate or fence
//! the external work.
//!
//! A provisioned runtime that needs checkpoint, sleep, or task-wait capabilities should bridge
//! the supplied [`TaskContext`] through its own IPC/RPC mechanism instead of creating parallel
//! durable storage.
//!
//! # Durable workflow primitives
//!
//! ## Checkpoints
//!
//! [`TaskContext::step`] assigns a stable name to a successful piece of work. On first execution,
//! Steda runs the step body, serializes its successful result, and commits that value in
//! `PostgreSQL`. When the logical task is replayed, Steda returns the committed value and skips the
//! body.
//!
//! [`Step`] values are therefore part of a workflow's durable shape. Define them as constants with
//! stable semantic names such as `reserve-inventory` rather than names derived from attempt
//! numbers or process-local state. `Step<Output>` keeps the checkpointed Rust value type attached
//! to that name, so workflow code does not pass raw strings.
//!
//! A checkpoint does **not** make an external side effect exactly once. A process can fail after
//! an external API accepts a request but before Steda commits the checkpoint. On retry, the step
//! body may run again. Use the external system's idempotency key, fencing token, unique constraint,
//! or equivalent mechanism when that boundary matters.
//!
//! ## Durable sleeps
//!
//! [`TaskContext::sleep_for`] and [`TaskContext::sleep_until`] persist a wake time and release the
//! current worker claim. No Rust future, stack frame, local variable, or network connection is
//! kept alive while the task sleeps.
//!
//! After the wake time, a worker invokes the handler from the beginning. Earlier checkpoints
//! replay, and when execution reaches the same [`Sleep`] identity, Steda observes that its durable
//! wake time has arrived and continues. Durable sleep preserves **workflow state**, not process
//! state.
//!
//! ## Waiting for another task
//!
//! [`TaskContext::await_task`] waits for a typed [`TaskRef`] in another queue and checkpoints
//! its decoded output under an internal identity derived from the target queue and task ID. A
//! completed wait can therefore replay if the parent later retries.
//!
//! Same-queue task execution is supported; same-queue waits are rejected because a finite worker
//! pool could otherwise be filled entirely by parents waiting for children that need those same
//! slots. Cross-queue waits retain the
//! current worker execution slot while polling, so use them for bounded dependencies rather than
//! as a generic long-term suspension mechanism.
//!
//! ## Durable compatibility
//!
//! Persisted task names, step names, sleep names, and their serialized input, output, and
//! checkpoint values are part of a workflow's durable compatibility boundary. Keep their
//! serialization compatible while old work may still exist, or introduce a new stable task, step,
//! or sleep name for an incompatible version. Serialized [`TaskRef`] values contain only the queue
//! name, task name, and [`TaskId`]; their Rust generic parameters are not persisted type metadata.
//!
//! # Submission guarantees
//!
//! ## Idempotent spawn
//!
//! [`Spawn::idempotency_key`] deduplicates logical task creation within one queue. Reusing a key
//! for the same original request returns the existing task; reusing it for a different request
//! returns [`Error::IdempotencyConflict`]. The comparison includes the task name, input, headers,
//! retry configuration, attempt budget, and cancellation policy.
//!
//! The idempotency key remains reserved only while the original logical task is retained. Retention
//! cleanup deletes that task row and releases the key, so the same key may create a new logical
//! task afterwards. Idempotent spawn protects **submission**; it does not deduplicate arbitrary
//! side effects inside a handler.
//!
//! ## Transactional task mutations
//!
//! Awaiting [`Queue::spawn`] directly submits through the queue's shared pool. When application
//! state and durable work must commit atomically, configure the same [`Spawn`] builder and call
//! [`Spawn::submit`] with a caller-owned `SQLx` transaction. The task then becomes visible only
//! when that transaction commits and disappears with the rest of the transaction on rollback.
//! Explicit cancellation and manual retry can join the same application transaction with
//! [`TaskHandle::cancel_in`] and [`TaskHandle::retry_in`].
//!
//! ## Cancellation deadlines
//!
//! [`CancellationPolicy`] `max_delay` limits how long a task may remain unstarted after enqueue.
//! Its `max_duration` limits elapsed time from the task's first start across the
//! remainder of the logical task, including retries and durable sleeps. `PostgreSQL` evaluates both
//! deadlines against its authoritative clock. Once a task has started, `max_delay` no longer
//! applies.
//!
//! Explicit [`TaskHandle::cancel`] and deadline cancellation are durable. A stale attempt cannot
//! later complete a cancelled task.
//!
//! See the runnable `cancellation` example for both paths.
//!
//! # Operating Steda
//!
//! ## Schema and queue lifecycle
//!
//! Apply the `sql/steda.sql` file from the Steda release before code that depends on that schema
//! begins spawning or claiming work. Apply the new release's file again when upgrading.
//!
//! [`Steda::queue`] creates a lightweight handle only. [`Queue::create`] creates its durable
//! queue-specific `PostgreSQL` storage and is idempotent for a healthy existing queue. Repeated
//! creation verifies the expected storage rather than silently repairing missing pieces.
//! [`Queue::delete`] removes a queue and all of its durable tasks and should therefore be treated
//! as an administrative lifecycle operation.
//!
//! ## Worker concurrency and shutdown
//!
//! [`WorkerBuilder::concurrency`] sets per-worker execution capacity; the default is one. A worker
//! claims only currently free capacity instead of accumulating claimed work in a second local
//! queue. Multiple worker processes can consume the same queue concurrently; `PostgreSQL` remains
//! the ownership authority.
//!
//! [`Worker::run_until`] stops new claims when its shutdown future resolves and waits for attempts
//! already executing to finish; there is no built-in drain timeout. An abrupt process stop is also
//! state-machine safe because finite leases allow later recovery, but graceful shutdown avoids
//! waiting for lease expiry when current work can finish normally.
//!
//! ## Cleanup and retention
//!
//! Each queue persists its own terminal-task retention policy. The defaults are a 30-day terminal
//! task TTL and at most 1,000 logical-task deletions per cleanup pass. Configure these values with
//! [`QueuePolicyOptions`] through [`Queue::create_with_policy`] or [`Queue::set_policy`].
//!
//! [`Queue::cleanup`] applies the persisted policy to one queue. [`Steda::cleanup`] performs one
//! policy-driven pass across all queues. Cleanup removes terminal logical tasks; `PostgreSQL`
//! foreign keys cascade to their runs and checkpoints. Pending, running, and sleeping tasks are not
//! retention candidates. Cleanup also releases a task's idempotency key, and existing [`TaskRef`]
//! values for a deleted task will subsequently resolve as not found.
//!
//! ## Metrics
//!
//! [`Queue::metrics`] and [`Worker::metrics`] expose cloneable, read-only, exporter-agnostic
//! process counters through [`metrics::QueueMetrics`]. They are useful for Prometheus,
//! OpenTelemetry, or application-specific exporters, but they are not a durable ledger;
//! `PostgreSQL` remains the source of truth for task state. The repository's runnable `metrics`
//! example demonstrates one successful and one failed attempt and prints every public counter.
//!
//! ## Tower middleware
//!
//! [`StedaBuilder::layer`] installs Tower middleware around the shared task-execution boundary.
//! Middleware can observe the task ID, run ID, task name, queue, attempt, headers, and
//! [`TaskContext`] through [`middleware::Request`]. It wraps executor invocation only: claims,
//! leases, retries, cancellation, and terminal transitions remain Steda-owned database behavior.
//! The runnable `tower_layer` example implements a timing/observation layer against this exact
//! public boundary.
//!
//! ## Database connections
//!
//! [`TaskContext`] retains a [`sqlx::PgPool`] internally rather than one checked-out connection for
//! the entire handler lifetime. Long external calls and durable workflow code therefore do not pin
//! one database connection merely because a task is running. Size the application's pool for
//! aggregate database activity rather than reserving one connection per worker slot.
//!
//! ## Database trust boundary
//!
//! Steda's runtime database role is trusted: direct writes to Steda-managed tables bypass the
//! transition functions and their invariants. Queues are namespaces rather than tenant-security
//! boundaries, and [`TaskRef`] values are durable locators rather than authorization tokens.
//! Handler error messages and string panic messages may be persisted as failure data, so they
//! should not contain secrets.
//!
//! # Examples
//!
//! The crate repository contains runnable examples using only the public API:
//!
//! - `basic_task`: typed producer/worker flow and typed results;
//! - `idempotent_webhook`: duplicate external delivery at submission time;
//! - `retrying_delivery`: bounded transient retries;
//! - `cancellation`: explicit and deadline-driven cancellation;
//! - `multistep_workflow`: a task composed from several typed durable steps;
//! - `checkpointed_order`: checkpoint replay across a failed attempt;
//! - `durable_delay`: suspension without occupying a worker claim;
//! - `cross_queue_workflow`: checkpointed parent/child work across queues;
//! - `provisioned_executor`: fresh execution environments under the normal worker/retry path;
//! - `metrics`: exporter-agnostic process counters after successful and failed execution;
//! - `tower_layer`: timing and request observation with a custom Tower execution layer.
//!
//! The examples intentionally use small simulated integrations so they can be run directly with
//! Cargo and `PostgreSQL`. Their comments call out where production systems still need external
//! idempotency, real provisioning, or application-specific error classification.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/selemis-com/steda/master/.github/assets/logo.jpg",
    html_favicon_url = "https://raw.githubusercontent.com/selemis-com/steda/master/.github/assets/favicon.ico"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod context;
mod db;
mod error;
mod execution;
mod executor;
pub mod metrics;
mod queue;
mod steda;
mod task;
mod types;
mod worker;
mod workflow;

/// Tower integration for Steda's shared task-execution boundary.
///
/// Layers installed with [`StedaBuilder::layer`] receive [`middleware::Request`] values for
/// already-claimed attempts and may wrap executor invocation for tracing, metrics, or error
/// reporting. Middleware does not own durable state transitions: `PostgreSQL` remains authoritative
/// for claims, leases, retries, cancellation, checkpoints, and completion.
pub mod middleware {
    pub use tower_layer::Layer;
    pub use tower_service::Service;

    pub use crate::execution::{
        ExecutionRequest as Request, ExecutionResponse as Response, ExecutionService as Execution,
    };
}

pub use context::{TaskContext, TaskWait};
pub use error::{Error, Result};
pub use queue::Queue;
pub use steda::{Steda, StedaBuilder};
pub use task::{Spawn, SpawnedTask, Task, TaskHandle, TaskRef, TaskSnapshot};
pub use types::{
    CancellationPolicy, Json, JsonObject, QueueCleanup, QueuePolicy, QueuePolicyOptions,
    RetryStrategy, RunId, TaskId, TaskState,
};
pub use worker::{TaskExecutor, TaskHandler, Worker, WorkerBuilder};
pub use workflow::{Sleep, Step};
