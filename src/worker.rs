//! Background task worker.

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{FutureExt, future::BoxFuture};
use log::{error, info};
use sqlx::PgPool;
use tokio::{task::JoinSet, time::sleep};
use uuid::Uuid;

use crate::{
    context::TaskContext,
    db::{claim_tasks, duration_seconds},
    error::{Error, Result},
    execution::SharedExecutionService,
    executor::{ExecutionContext, execute_task},
    metrics::QueueMetrics,
    queue::Queue,
    task::{Task, validate_task_name},
    types::Json,
};

/// Default delay between worker polling attempts.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Type-erased task executor stored in a worker's frozen capability registry.
pub(crate) type ErasedTaskExecutor =
    Arc<dyn Fn(Json, TaskContext) -> BoxFuture<'static, Result<Json>> + Send + Sync>;

/// Local execution capability for one task type.
pub(crate) struct RegisteredTask {
    /// Type-erased executor for one claimed attempt.
    pub executor: ErasedTaskExecutor,
}

impl std::fmt::Debug for RegisteredTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredTask").field("executor", &"<task executor>").finish()
    }
}

/// Local runtime configuration whose fields all have observable worker semantics.
#[derive(Clone, Copy, Debug)]
struct WorkerRuntime {
    /// Finite task lease duration requested from `PostgreSQL`.
    lease_duration: Duration,
    /// Maximum number of task attempts executing concurrently.
    concurrency: usize,
}

impl Default for WorkerRuntime {
    fn default() -> Self {
        Self { lease_duration: Duration::from_secs(120), concurrency: 1 }
    }
}

/// Runs the polling worker loop until shutdown, draining in-flight tasks before returning.
async fn run_worker_loop<S>(
    pool: PgPool,
    queue_name: String,
    registry: Arc<HashMap<String, RegisteredTask>>,
    metrics: QueueMetrics,
    execution_service: SharedExecutionService,
    runtime: WorkerRuntime,
    shutdown: impl Future<Output = S> + Send,
) -> Result<()>
where
    S: Send,
{
    let worker_id = default_worker_id();
    let lease_seconds = worker_lease_seconds(runtime.lease_duration)?;
    let supported_tasks = registered_task_names(&registry);
    let log_queue_name = queue_name.as_str();
    let log_worker_id = worker_id.as_str();
    let mut executing: JoinSet<Result<()>> = JoinSet::new();
    let mut terminal_error = None;
    let execution_context = ExecutionContext::new(
        pool.clone(),
        queue_name.clone(),
        Arc::clone(&registry),
        metrics.clone(),
        execution_service,
    );
    tokio::pin!(shutdown);

    loop {
        while let Some(joined) = executing.try_join_next() {
            if let Some(err) = terminal_join_error(joined) {
                terminal_error = Some(err);
                break;
            }
        }
        if terminal_error.is_some() {
            break;
        }

        let available = runtime.concurrency.saturating_sub(executing.len());
        if available == 0 {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Worker shutting down (queue={log_queue_name}, worker_id={log_worker_id})");
                    break;
                }
                joined = executing.join_next() => {
                    if let Some(joined) = joined
                        && let Some(err) = terminal_join_error(joined)
                    {
                        terminal_error = Some(err);
                        break;
                    }
                }
            }
            continue;
        }

        // Do not start a new claim after shutdown is already observable. Once a
        // claim query has started, however, let it complete: PostgreSQL may have
        // committed ownership even if the client future were cancelled. Any runs
        // returned here become in-flight work and are drained before shutdown.
        if shutdown.as_mut().now_or_never().is_some() {
            info!("Worker shutting down (queue={log_queue_name}, worker_id={log_worker_id})");
            break;
        }

        let batch_size = i32::try_from(available).map_err(|_| {
            Error::InvalidOptions("worker concurrency exceeds PostgreSQL integer range".to_owned())
        })?;
        let tasks = match claim_tasks(
            &pool,
            &queue_name,
            &worker_id,
            lease_seconds,
            batch_size,
            &supported_tasks,
        )
        .await
        {
            Ok(tasks) => {
                metrics.record_claimed(tasks.len());
                tasks
            }
            Err(err) => {
                metrics.record_claim_error();
                if !is_transient_worker_error(&err) {
                    terminal_error = Some(err);
                    break;
                }
                error!(
                    "Transient worker claim error (queue={log_queue_name}, worker_id={log_worker_id}): {err:?}"
                );
                tokio::select! {
                    _ = &mut shutdown => {
                        info!("Worker shutting down (queue={log_queue_name}, worker_id={log_worker_id})");
                        break;
                    }
                    _ = sleep(DEFAULT_POLL_INTERVAL) => {}
                }
                continue;
            }
        };

        if tasks.is_empty() {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Worker shutting down (queue={log_queue_name}, worker_id={log_worker_id})");
                    break;
                }
                _ = sleep(DEFAULT_POLL_INTERVAL) => {}
            }
            continue;
        }

        for task in tasks {
            let execution_context = execution_context.clone();
            let queue_name = queue_name.clone();
            let worker_id = worker_id.clone();
            executing.spawn(async move {
                match execute_task(execution_context, task, lease_seconds).await {
                    Ok(())
                    | Err(
                        Error::Suspended
                        | Error::Cancelled
                        | Error::FailedRun
                        | Error::LeaseLost,
                    ) => Ok(()),
                    Err(err) if is_transient_worker_error(&err) => {
                        error!(
                            "Transient task execution infrastructure error (queue={queue_name}, worker_id={worker_id}): {err:?}"
                        );
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            });
        }
    }

    while let Some(joined) = executing.join_next().await {
        if let Some(err) = terminal_join_error(joined)
            && terminal_error.is_none()
        {
            terminal_error = Some(err);
        }
    }

    if let Some(err) = terminal_error {
        return Err(err);
    }
    Ok(())
}

/// Converts a joined execution attempt into a worker-fatal error when necessary.
fn terminal_join_error(
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Option<Error> {
    match joined {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(err),
        Err(err) => Some(Error::Other(format!("worker task join failed: {err}"))),
    }
}

/// Returns whether an infrastructure failure is safe for the worker to retry.
fn is_transient_worker_error(error: &Error) -> bool {
    let Error::Database(error) = error else {
        return false;
    };

    match error {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolTimedOut => true,
        sqlx::Error::Database(database) => {
            database.code().is_some_and(|code| is_transient_sqlstate(code.as_ref()))
        }
        _ => false,
    }
}

/// `PostgreSQL` conditions that are expected to become retryable without changing Steda itself.
fn is_transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("40")
        || code.starts_with("53")
        || matches!(code, "55P03" | "57P01" | "57P02" | "57P03")
}

/// Executes one claimed attempt of task `T`.
///
/// `TaskExecutor` is the compute boundary inside Steda's durable worker loop.
/// The worker still owns claiming, lease supervision, cancellation observation,
/// retries, checkpoints, and the final complete/fail transition. An executor
/// only supplies the computation for one already-claimed attempt.
///
/// Ordinary async functions and closures implement this trait automatically.
/// Reusable objects can implement it directly when execution needs its own
/// process, sandbox, or scheduler client:
///
/// ```no_run
/// use std::future::Future;
///
/// use steda::{Result, Task, TaskContext, TaskExecutor};
///
/// struct Double;
///
/// impl Task for Double {
///     const NAME: &'static str = "double";
///     type Input = i64;
///     type Output = i64;
/// }
///
/// async fn in_process(input: i64, _context: TaskContext) -> Result<i64> {
///     Ok(input * 2)
/// }
///
/// struct ProvisionedExecutor;
///
/// impl TaskExecutor<Double> for ProvisionedExecutor {
///     fn execute(
///         &self,
///         input: i64,
///         _context: TaskContext,
///     ) -> impl Future<Output = Result<i64>> + Send {
///         async move { Ok(input * 2) }
///     }
/// }
///
/// fn accepts_executor(_executor: impl TaskExecutor<Double>) {}
/// accepts_executor(in_process);
/// accepts_executor(ProvisionedExecutor);
/// ```
///
/// Reusable executor objects can provision compute per attempt: a
/// process, container, sandbox, Kubernetes Job, or another execution substrate.
/// That does not create a second durable path; returning from `execute` flows
/// through the same Steda supervision and state transitions as an in-process
/// async function. A provisioned runtime that needs checkpoints or durable sleeps can
/// bridge the supplied [`TaskContext`] over its own IPC/RPC protocol.
///
/// # Cancellation
///
/// Steda may drop the future returned by [`TaskExecutor::execute`] when authoritative
/// `PostgreSQL` supervision determines that the attempt has been cancelled, suspended,
/// or has lost its finite lease. In-process async work is cancelled by that drop. An
/// executor that starts a child process, container, remote job, or other external compute
/// must ensure dropping its future also terminates or fences that work so it cannot keep
/// acting as the no-longer-owned attempt.
pub trait TaskExecutor<T: Task>: Send + Sync + 'static {
    /// Execute one typed task attempt.
    ///
    /// # Errors
    ///
    /// Returning an error fails the current attempt through Steda's normal
    /// durable failure and retry transition.
    fn execute(
        &self,
        input: T::Input,
        context: TaskContext,
    ) -> impl Future<Output = Result<T::Output>> + Send;
}

/// Reusable in-process async handler accepted by [`WorkerBuilder::task`].
///
/// The adapter keeps the handler's concrete future type out of the builder method's generic
/// parameter list, so callers can write `.task::<T>(...)`. Matching async functions, async
/// closures, and reusable closures returning futures implement it automatically.
///
/// The handler must implement [`Fn`], not merely `FnOnce`, because one worker registration may
/// execute many tasks and retries. Stateful handlers should keep reusable state outside each
/// invocation and clone the required handles per call. Async closures should annotate their input
/// and [`TaskContext`] parameters at this adapter boundary.
pub trait TaskHandler<T: Task>:
    Fn(T::Input, TaskContext) -> <Self as TaskHandler<T>>::Future + Send + Sync + 'static
{
    /// Future returned by this handler.
    type Future: Future<Output = Result<T::Output>> + Send + 'static;
}

impl<T, F, Fut> TaskHandler<T> for F
where
    T: Task,
    F: Fn(T::Input, TaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T::Output>> + Send + 'static,
{
    type Future = Fut;
}

impl<T, F> TaskExecutor<T> for F
where
    T: Task,
    F: TaskHandler<T>,
{
    fn execute(
        &self,
        input: T::Input,
        context: TaskContext,
    ) -> impl Future<Output = Result<T::Output>> + Send {
        (self)(input, context)
    }
}

/// Staged builder for an immutable queue worker.
///
/// Task registration is a local process capability declaration. `PostgreSQL` remains authoritative
/// for task state and only returns claims whose names match the capabilities frozen into the
/// worker. Producers do not need this registry.
///
/// The default concurrency is one and the default finite lease duration is 120 seconds.
#[derive(Debug)]
pub struct WorkerBuilder {
    /// Queue this worker consumes from.
    queue: Queue,
    /// Local task capabilities under construction.
    registry: HashMap<String, RegisteredTask>,
    /// Local worker runtime configuration.
    runtime: WorkerRuntime,
    /// First registration/configuration error, deferred until `build` to keep the builder fluent.
    error: Option<Error>,
}

impl WorkerBuilder {
    /// Begin building a worker for one queue.
    pub(crate) fn new(queue: Queue) -> Self {
        Self { queue, registry: HashMap::new(), runtime: WorkerRuntime::default(), error: None }
    }

    /// Set the finite lease duration for this worker.
    ///
    /// `PostgreSQL` remains authoritative for lease validity and renewal. The
    /// duration only controls how much lease time each successful supervision
    /// call requests.
    #[must_use]
    pub fn lease_duration(mut self, lease_duration: Duration) -> Self {
        if self.error.is_some() {
            return self;
        }
        match duration_seconds(lease_duration) {
            Ok(0) => {
                self.error = Some(Error::InvalidOptions(
                    "lease duration must round to at least 1 second".to_owned(),
                ));
                return self;
            }
            Ok(_) => {}
            Err(error) => {
                self.error = Some(error);
                return self;
            }
        }
        self.runtime.lease_duration = lease_duration;
        self
    }

    /// Set the maximum number of task attempts this worker executes concurrently.
    ///
    /// Claims are limited to currently available execution capacity, so the worker
    /// never owns more runs than it can begin executing immediately.
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        if self.error.is_some() {
            return self;
        }
        if concurrency == 0 {
            self.error =
                Some(Error::InvalidOptions("worker concurrency must be at least 1".to_owned()));
            return self;
        }
        if i32::try_from(concurrency).is_err() {
            self.error = Some(Error::InvalidOptions(
                "worker concurrency exceeds PostgreSQL integer range".to_owned(),
            ));
            return self;
        }
        self.runtime.concurrency = concurrency;
        self
    }

    /// Declare that this worker executes task `T` with an in-process async handler.
    ///
    /// This is the normal registration path. Async closures are supported directly;
    /// annotate their input and [`TaskContext`] parameters so Rust can resolve the
    /// projected task input type through the handler adapter. Use [`Self::task_executor`]
    /// when one reusable object should decide where each attempt computes.
    #[must_use]
    pub fn task<T>(self, handler: impl TaskHandler<T>) -> Self
    where
        T: Task,
    {
        self.register_task_executor::<T>(handler)
    }

    /// Declare that this worker executes task `T` using a reusable [`TaskExecutor`].
    ///
    /// The executor may run the attempt in-process or provision a process, container,
    /// sandbox, VM, Kubernetes Job, or another execution substrate. This changes only
    /// where one attempt computes: claiming, supervision, checkpoints, retries,
    /// cancellation, and completion still use the same Steda worker path.
    #[must_use]
    pub fn task_executor<T>(self, executor: impl TaskExecutor<T>) -> Self
    where
        T: Task,
    {
        self.register_task_executor::<T>(executor)
    }

    /// Type-erase one typed executor into the worker's single task registry.
    fn register_task_executor<T>(mut self, executor: impl TaskExecutor<T>) -> Self
    where
        T: Task,
    {
        if self.error.is_some() {
            return self;
        }
        if let Err(error) = validate_task_name(T::NAME) {
            self.error = Some(error);
            return self;
        }
        if self.registry.contains_key(T::NAME) {
            self.error =
                Some(Error::InvalidOptions(format!("task {:?} is already registered", T::NAME)));
            return self;
        }

        let executor = Arc::new(executor);
        let erased: ErasedTaskExecutor = Arc::new(move |raw, context| {
            let executor = Arc::clone(&executor);
            Box::pin(async move {
                let input = serde_json::from_value::<T::Input>(raw)?;
                let output = executor.execute(input, context).await?;
                Ok(serde_json::to_value(output)?)
            })
        });
        self.registry.insert(T::NAME.to_owned(), RegisteredTask { executor: erased });
        self
    }

    /// Freeze task capabilities and return a runnable worker.
    ///
    /// # Errors
    ///
    /// Returns an error if registration failed or no task capability was declared.
    pub fn build(self) -> Result<Worker> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.registry.is_empty() {
            return Err(Error::InvalidOptions("worker requires at least one task".to_owned()));
        }

        Ok(Worker { queue: self.queue, registry: Arc::new(self.registry), runtime: self.runtime })
    }
}

/// Immutable long-lived worker for one Steda queue.
///
/// A worker owns process-local task execution capabilities and supervises claimed attempts.
/// `PostgreSQL` remains authoritative for scheduling, leases, retries, cancellation, checkpoints,
/// and results. Multiple workers may safely consume the same queue concurrently.
#[derive(Debug)]
pub struct Worker {
    /// Queue this worker consumes from.
    queue: Queue,
    /// Frozen local task capabilities.
    registry: Arc<HashMap<String, RegisteredTask>>,
    /// Local runtime configuration.
    runtime: WorkerRuntime,
}

impl Worker {
    /// Run until the worker loop returns an error or the process stops it.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker loop cannot continue claiming or executing work.
    pub async fn run(&self) -> Result<()> {
        self.run_until(std::future::pending::<()>()).await
    }

    /// Run until `shutdown` resolves, then stop claiming and drain in-flight attempts.
    ///
    /// Graceful shutdown does not abandon already claimed work. Abrupt process termination remains
    /// recoverable through finite lease expiry, but another worker must wait for ownership to
    /// expire before reclaiming that attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker loop cannot continue claiming or executing work.
    pub async fn run_until<S>(&self, shutdown: impl Future<Output = S> + Send) -> Result<()>
    where
        S: Send,
    {
        run_worker_loop(
            self.queue.pool().clone(),
            self.queue.name().to_owned(),
            Arc::clone(&self.registry),
            self.queue.metrics(),
            self.queue.execution().clone(),
            self.runtime,
            shutdown,
        )
        .await
    }

    /// Return exporter-agnostic metrics for this worker's queue.
    pub fn metrics(&self) -> QueueMetrics {
        self.queue.metrics()
    }
}

/// Returns task names this worker can execute.
fn registered_task_names(registry: &HashMap<String, RegisteredTask>) -> Vec<String> {
    registry.keys().cloned().collect()
}

/// Generates a unique default worker identifier.
fn default_worker_id() -> String {
    format!("worker:{}", Uuid::now_v7())
}

/// Returns the worker lease duration in seconds.
fn worker_lease_seconds(lease_duration: Duration) -> Result<i32> {
    duration_seconds(lease_duration)
}
#[cfg(test)]
mod tests {
    use super::is_transient_sqlstate;

    #[test]
    fn transient_sqlstates_are_narrowly_classified() {
        for code in ["08006", "40001", "40P01", "53300", "55P03", "57P01", "57P02", "57P03"] {
            assert!(is_transient_sqlstate(code), "{code} should be transient");
        }
        for code in ["22023", "23505", "42703", "42P01", "AB001"] {
            assert!(!is_transient_sqlstate(code), "{code} should be terminal");
        }
    }
}
