<picture>
  <source media="(prefers-color-scheme: dark)" srcset=".github/assets/logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset=".github/assets/logo-light.svg">
  <img alt="Steda" src=".github/assets/logo-light.svg" width="100%" height="140px">
</picture>

<p align="center">
  PostgreSQL-backed durable task execution for Rust
</p>

<br/>

<p align="center">
  <a href="https://crates.io/crates/steda"><picture><source media="(prefers-color-scheme: dark)" srcset="https://img.shields.io/crates/v/steda?colorA=21262d&colorB=21262d&style=flat"><img src="https://img.shields.io/crates/v/steda?colorA=f6f8fa&colorB=f6f8fa&style=flat" alt="Version"></picture></a>
  <a href="#license"><picture><source media="(prefers-color-scheme: dark)" srcset="https://img.shields.io/crates/l/steda?colorA=21262d&colorB=21262d&style=flat"><img src="https://img.shields.io/crates/l/steda?colorA=f6f8fa&colorB=f6f8fa&style=flat" alt="MIT OR Apache-2.0"></picture></a>
</p>

Steda is a durable task queue for Rust applications that already depend on PostgreSQL. Tasks are
ordinary async Rust handlers; PostgreSQL stores queues, attempts, retries, checkpoints, durable
sleeps, cancellation state, and results so work can survive process restarts and worker failure.

The model is deliberately small:

- a [`Task`](https://docs.rs/steda/latest/steda/struct.Task.html) defines a stable name and typed input/output pair;
- queues persist logical tasks and their attempts;
- workers declare which task definitions they can execute;
- checkpointed steps preserve successful work across retries;
- durable sleeps suspend work without occupying a worker;
- idempotency keys make repeated external delivery safe to submit.

## Installation

Steda requires PostgreSQL 18+. See [MSRV](#msrv) for supported Rust versions.

```sh
cargo add steda
```

Download [`sql/steda.sql`](sql/steda.sql) and apply it to your PostgreSQL database before starting producers or workers.

When upgrading Steda, apply the `steda.sql` file from the new release again.

## Quick start

Define a task:

```rust
use serde::{Deserialize, Serialize};
use steda::{Task, TaskContext};

#[derive(Deserialize, Serialize)]
struct ResizeImageInput {
    object_key: String,
    width: u32,
}

#[derive(Deserialize, Serialize)]
struct ResizeImageOutput {
    resized_key: String,
}

const RESIZE_IMAGE: Task<ResizeImageInput, ResizeImageOutput> = Task::new("resize-image");
```

Create a queue and register a handler:

```rust
let queue = steda.queue("media")?;
queue.create().await?;

let worker = queue
    .worker()
    .concurrency(8)
    .task(RESIZE_IMAGE, async |input: ResizeImageInput, _ctx: TaskContext| {
        let resized_key = format!("resized/{}", input.object_key);
        Ok(ResizeImageOutput { resized_key })
    })
    .build()?;
worker.run().await?;
```

Producers use the same task definition:

```rust
let task = queue
    .spawn(RESIZE_IMAGE, ResizeImageInput {
        object_key: "uploads/photo.jpg".to_owned(),
        width: 1600,
    })
    .await?;

let output = task.result().await?;
println!("{}", output.resized_key);
```

The returned task keeps the output type attached, so results and snapshots remain typed. `task.task_ref()` produces a serializable reference that keeps the queue, task name, task ID, and Rust input/output types together across restarts. Producer and worker processes share the same `Task` constant and PostgreSQL state; no runtime task registry is required.

## Durable workflows

### Idempotent submission

Queue-scoped idempotency keys make repeated external delivery safe to submit:

```rust
let task = payments
    .spawn(FULFILL_ORDER, payment)
    .idempotency_key(format!("payment-captured:{payment_id}"))
    .await?;
```

Replaying the same request returns the existing logical task. Reusing the key for a different
request returns `Error::IdempotencyConflict`.

### Retries

Tasks can use bounded retry policies with configurable backoff:

```rust
use std::time::Duration;
use steda::RetryStrategy;

let task = queue
    .spawn(DELIVER_DOCUMENT, input)
    .max_attempts(5)
    .retry_strategy(RetryStrategy::exponential(
        Duration::from_secs(1),
        2.0,
        Some(Duration::from_secs(60)),
    ))
    .await?;
```

`max_attempts` includes the first attempt. Without explicit configuration, Steda defaults to five attempts with exponential backoff.

### Checkpointed steps

`TaskContext::step` persists a successful result under a typed stable identity:

```rust
use steda::Step;

const RESERVE_INVENTORY: Step<Reservation> = Step::new("reserve-inventory");

let reservation = ctx
    .step(RESERVE_INVENTORY, async || {
        inventory.reserve(&input.order_id).await
    })
    .await?;
```

If the task runs again, Steda replays the stored result rather than executing the step body again.

See [`multistep_workflow`](examples/multistep_workflow.rs) for a complete task composed from several typed steps.

A checkpoint makes the Steda step replayable; it cannot make an external side effect exactly once. Use the external system's idempotency or fencing mechanism when that property is required.

### Durable sleeps

A durable sleep persists its wake time and releases the worker claim:

```rust
use std::time::Duration;
use steda::Sleep;

const SETTLEMENT_WINDOW: Sleep = Sleep::new("settlement-window");

ctx.sleep_for(SETTLEMENT_WINDOW, Duration::from_secs(30)).await?;
```

When the wake time arrives, execution starts again from the handler entry point. Earlier
checkpoints and sleeps replay until execution reaches new work.

### Provisioned execution

[`TaskExecutor`](https://docs.rs/steda/latest/steda/trait.TaskExecutor.html) allows a worker to
execute a claimed attempt in a separate process, container, sandbox, Kubernetes Job, or remote
environment without introducing another task model.

The Steda worker still owns claiming, leases, cancellation, retries, checkpoints, and terminal
state. Custom executors are responsible for terminating or fencing external work when their
execution future is cancelled.

See [`provisioned_executor`](examples/provisioned_executor.rs) for a complete example.

## Runnable examples

The repository contains standalone programs that use only Steda's public API. Point
`DATABASE_URL` at PostgreSQL and run any of them with Cargo:

| Example | Demonstrates |
| --- | --- |
| [`basic_task`](examples/basic_task.rs) | Typed producer/worker flow and typed results |
| [`idempotent_webhook`](examples/idempotent_webhook.rs) | Deduplicating repeated webhook delivery |
| [`retrying_delivery`](examples/retrying_delivery.rs) | Bounded retries after transient failures |
| [`cancellation`](examples/cancellation.rs) | Explicit and deadline-driven cancellation |
| [`multistep_workflow`](examples/multistep_workflow.rs) | A task composed from several typed durable steps |
| [`checkpointed_order`](examples/checkpointed_order.rs) | Multi-step work replayed across a retry |
| [`durable_delay`](examples/durable_delay.rs) | Suspending without holding a worker claim |
| [`cross_queue_workflow`](examples/cross_queue_workflow.rs) | Parent/child work across separate queues |
| [`provisioned_executor`](examples/provisioned_executor.rs) | Fresh execution environments under the normal Steda worker/retry path |
| [`metrics`](examples/metrics.rs) | Exporter-agnostic queue and worker execution counters |
| [`tower_layer`](examples/tower_layer.rs) | Custom Tower middleware around registered executor invocation |

See [`examples/README.md`](examples/README.md) for the full walkthrough.

## Operations

Queues have persisted cleanup policies, workers use finite PostgreSQL leases, and Steda exposes exporter-agnostic queue and worker metrics. See the [crate documentation](https://docs.rs/steda) for
lifecycle, graceful shutdown, cleanup, metrics, middleware, and database-pool behavior.

### Database roles

Steda uses PostgreSQL invoker privileges and creates queue storage dynamically. The simplest
deployment uses one database role to run migrations, provision queues, and execute Steda
operations. Deployments that separate migration, provisioning, and runtime roles must grant the
required schema, function, and queue-table privileges explicitly; Steda does not install a
least-privilege role split automatically.

## Tower middleware

Task handler invocation can be wrapped at the root with ordinary [Tower](https://github.com/tower-rs/tower) layers through
`Steda::builder(pool)`. Middleware receives task metadata and `TaskContext`; PostgreSQL remains
authoritative for claims, leases, retries, cancellation, and terminal state.

See [`tower_layer`](examples/tower_layer.rs) for a complete example.

## Development

Repository tests use PostgreSQL through `DATABASE_URL`. SQLx creates isolated migrated databases for
integration tests, so the configured PostgreSQL user must be allowed to create and drop databases.

```sh
cp .env.template .env
docker compose up -d postgres
make test
make lint
```

`make test` runs deterministic tests, generated state-machine histories, example compilation, and doctests. Run the complete repository verification with `make pr`.

## MSRV

<!--
When updating this, also update:
- Cargo.toml
- .github/workflows/ci.yml
-->

The current MSRV (minimum supported Rust version) is 1.95.

Steda will keep a rolling MSRV policy of **at least** two versions behind the
latest stable release (so if the latest stable release is 1.97, we would
support 1.95).

Note that the MSRV is not increased automatically.

## Contributing

Contributions to Steda are welcome. See the [Contributing Guide](CONTRIBUTING.md) for information on reporting bugs, proposing features, submitting pull requests, and the licensing terms that apply to contributions.

## Security Policy

If you believe you have found a security vulnerability, please do not report it through GitHub Issues. See our [Security Policy](SECURITY.md) for reporting instructions.

## Credit

Steda was inspired in part by [Absurd](https://github.com/earendil-works/absurd), whose work helped shape parts of its durable execution model.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

This software includes third-party components subject to separate license
terms. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Steda by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
