//! Wrap registered task execution with a custom Tower layer.
//!
//! Steda installs Tower middleware once on the root `Steda` handle and shares that composed
//! execution service with every queue and worker created from it. The layer sees an already-claimed
//! attempt and can inspect its task, queue, run, attempt, headers, and `TaskContext` before
//! invoking the registered handler or `TaskExecutor`.
//!
//! The boundary is intentionally narrow: middleware wraps executor invocation only. `PostgreSQL`
//! claiming, lease supervision, retry scheduling, checkpoint semantics, cancellation, and the final
//! complete/fail transition remain Steda-owned durable behavior.

/// Shared setup and finite-worker helpers.
mod common;

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use common::RunningWorker;
use serde::{Deserialize, Serialize};
use steda::{
    Error, Result, Steda, Task, TaskContext,
    middleware::{Layer, Request, Response, Service},
};

/// Input handled behind the timing middleware.
#[derive(Debug, Deserialize, Serialize)]
struct RenderPreviewInput {
    /// Document identifier included in the resulting preview path.
    document_id: String,
}

/// Typed result produced by the preview task.
#[derive(Debug, Deserialize, Serialize)]
struct RenderPreviewOutput {
    /// Simulated object-store path for the generated preview.
    preview_path: String,
}

/// Task definition used by the Tower example.
const RENDER_PREVIEW: Task<RenderPreviewInput, RenderPreviewOutput> = Task::new("render-preview");

/// Tower layer that times each registered executor invocation.
#[derive(Clone, Debug)]
struct ExecutionTimingLayer {
    /// Number of attempts observed after their executor future resolves.
    completed_calls: Arc<AtomicU64>,
}

impl<S> Layer<S> for ExecutionTimingLayer {
    type Service = ExecutionTimingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ExecutionTimingService { inner, completed_calls: Arc::clone(&self.completed_calls) }
    }
}

/// Service produced by [`ExecutionTimingLayer`].
#[derive(Clone, Debug)]
struct ExecutionTimingService<S> {
    /// Next service in the Tower stack.
    inner: S,
    /// Shared observation counter owned by the example.
    completed_calls: Arc<AtomicU64>,
}

impl<S> Service<Request> for ExecutionTimingService<S>
where
    S: Service<Request, Response = Response, Error = Error>,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Copy observation metadata before passing ownership of the request to the inner service.
        let task_name = request.task_name().to_owned();
        let attempt = request.attempt();
        let started = Instant::now();
        let future = self.inner.call(request);
        let completed_calls = Arc::clone(&self.completed_calls);

        Box::pin(async move {
            let result = future.await;
            let _ = completed_calls.fetch_add(1, Ordering::Relaxed);
            println!(
                "middleware observed {task_name} attempt {attempt}: {} in {} ms",
                if result.is_ok() { "succeeded" } else { "failed" },
                started.elapsed().as_millis()
            );
            result
        })
    }
}

/// Run the Tower execution-layer example.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // `common::connect` performs migrations. Rebuild the lightweight root handle around the same
    // pool so the execution layer is frozen before queues/workers are created.
    let base = common::connect().await?;
    let completed_calls = Arc::new(AtomicU64::new(0));
    let steda = Steda::builder(base.pool().clone())
        .layer(ExecutionTimingLayer { completed_calls: Arc::clone(&completed_calls) })
        .build();

    let queue = steda.queue("example-tower-layer")?;
    queue.create().await?;

    let worker = queue
        .worker()
        .task(RENDER_PREVIEW, async |input: RenderPreviewInput, _ctx: TaskContext| {
            // This delay is inside the handler, so the layer's timer observes it. Claiming and the
            // final durable completion transition happen outside this middleware boundary.
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(RenderPreviewOutput { preview_path: format!("previews/{}.png", input.document_id) })
        })
        .build()?;
    let worker = RunningWorker::start(worker);

    let task = queue
        .spawn(RENDER_PREVIEW, RenderPreviewInput { document_id: "document-1001".to_owned() })
        .await?;
    let output = task.result_with_timeout(Duration::from_secs(10)).await?;

    worker.stop().await?;
    println!("preview created: {}", output.preview_path);
    println!("middleware calls: {}", completed_calls.load(Ordering::Relaxed));

    Ok(())
}
