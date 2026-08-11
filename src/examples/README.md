# Steda examples

These examples are complete programs built against Steda's public API. They are intended to be read
in order, but each can be run independently.

## Prerequisites

Set `DATABASE_URL` to a PostgreSQL database that the current user can migrate and use:

```sh
export DATABASE_URL=postgres://postgres:postgres@localhost/steda
```

Every example runs `steda::migrate` and creates its own `example-*` queue. This keeps the examples
self-contained. Production deployments will usually run migrations separately before starting
producers and workers.

## `basic_task`

```sh
cargo run --example basic_task
```

The smallest complete Steda program: define a typed task, register a handler, spawn work, await its
typed result, and shut the worker down cleanly.

## `idempotent_webhook`

```sh
cargo run --example idempotent_webhook
```

Models a payment webhook that may be delivered more than once. Two identical spawns use the same
queue-scoped idempotency key and resolve to one logical task.

The important boundary is submission: an idempotency key deduplicates Steda task creation. If the
handler calls another system, that system may still need its own idempotency key.

## `retrying_delivery`

```sh
cargo run --example retrying_delivery
```

Models a transient remote-service failure. Attempts one and two fail; the third succeeds under a
three-attempt fixed-delay retry policy.

Use retry policies for failures expected to improve when tried again. A terminal application error
should normally be returned without artificially extending its retry budget.

## `checkpointed_order`

```sh
cargo run --example checkpointed_order
```

Models an order workflow with inventory reservation and payment charge steps. The first attempt
fails after both steps have succeeded. On retry, Steda replays the persisted step values instead of
executing those step bodies again.

This demonstrates deterministic Steda replay, not magical exactly-once external effects. Real
inventory and payment APIs should still receive their own idempotency/fencing keys.

## `durable_delay`

```sh
cargo run --example durable_delay
```

Models a delayed trial reminder. `TaskContext::sleep_for` persists the wake time and releases the
worker claim while the task is waiting. When it becomes runnable again, the handler starts from the
beginning and replays its checkpoints.

## `cross_queue_workflow`

```sh
cargo run --example cross_queue_workflow
```

Models an order task that spawns an email receipt in another queue and waits for its result. The
child spawn and result wait are checkpointed, so retrying the parent does not create another child
or repeat a completed wait.

Steda intentionally rejects same-queue task waits because they can deadlock a finite worker pool.
Use a separate queue when one task synchronously depends on another task's result.

## `provisioned_executor`

```sh
cargo run --example provisioned_executor
```

Models a long-lived Steda worker backed by a reusable provisioner. Each task attempt receives a
fresh simulated sandbox, while Steda keeps the claim, lease, retry, checkpoint, and result lifecycle
on the normal worker path. The first sandbox exits after a durable preparation step; the retry gets
a new run and a new sandbox, but replays the already committed step.

The example intentionally does not depend on Docker or Kubernetes. A production `TaskExecutor` can
launch either of those (or a process, VM, or remote job) and bridge the supplied `TaskContext` over
IPC/RPC when code inside the execution environment needs checkpointing, durable sleep, or task
result waits.

## `metrics`

```sh
cargo run --example metrics
```

Runs one successful attempt and one terminal failure, then reads the queue lineage's cloned
`QueueMetrics` handle. The example prints claim, outcome, lease-loss, suspension, unhandled, and
cumulative execution-time counters in the shape an exporter would poll.

These counters are process-local observations, not durable queue statistics. PostgreSQL remains the
source of truth for task state, and a separately constructed queue handle for the same database
queue intentionally starts a separate metric lineage.

## `tower_layer`

```sh
cargo run --example tower_layer
```

Installs a custom Tower layer once on `Steda::builder`, times registered executor invocation, and
prints metadata from the already-claimed `middleware::Request`. The same composed execution stack
is shared by queues and workers created from that root handle.

The timer deliberately does not include claiming or the final durable complete/fail transition.
Those operations, along with leases, retries, cancellation, and checkpoints, stay outside the
middleware boundary and remain Steda-owned PostgreSQL behavior.