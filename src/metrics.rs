//! Exporter-agnostic queue metrics.
//!
//! Counters are owned and mutated by Steda. Metrics integrations can retain a
//! cloned [`QueueMetrics`] handle and poll its read-only accessors without Steda
//! depending on a particular exporter.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

/// Monotonic counters recorded by a queue handle.
///
/// Cloning this value preserves the underlying counters, making it suitable for
/// handing to a metrics exporter that outlives a particular [`Queue`](crate::Queue)
/// handle. Values cover activity through that handle and its clones; they are
/// not global counts for other handles or processes using the same database queue.
///
/// ```no_run
/// # async fn example(queue: &steda::Queue) {
/// let metrics = queue.metrics();
/// let claimed = metrics.claimed_runs();
/// let completed = metrics.completed_executions();
/// # let _ = (claimed, completed);
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct QueueMetrics {
    /// Runs successfully claimed by workers.
    claimed_runs: Arc<AtomicU64>,
    /// Failed attempts to claim runs.
    claim_errors: Arc<AtomicU64>,
    /// Run execution attempts, including attempts dropped before a bounded outcome.
    executions: Arc<AtomicU64>,
    /// Successfully completed execution attempts.
    completed_executions: Arc<AtomicU64>,
    /// Failed execution attempts.
    failed_executions: Arc<AtomicU64>,
    /// Execution attempts that lost ownership because their finite lease expired.
    lease_lost_executions: Arc<AtomicU64>,
    /// Cancelled execution attempts.
    cancelled_executions: Arc<AtomicU64>,
    /// Suspended execution attempts.
    suspended_executions: Arc<AtomicU64>,
    /// Execution guards dropped without recording a bounded outcome.
    unhandled_executions: Arc<AtomicU64>,
    /// Cumulative execution time in nanoseconds.
    execution_duration_nanoseconds: Arc<AtomicU64>,
}

impl QueueMetrics {
    /// Create empty counters for a new queue handle lineage.
    pub(crate) fn new() -> Self {
        Self {
            claimed_runs: Arc::new(AtomicU64::new(0)),
            claim_errors: Arc::new(AtomicU64::new(0)),
            executions: Arc::new(AtomicU64::new(0)),
            completed_executions: Arc::new(AtomicU64::new(0)),
            failed_executions: Arc::new(AtomicU64::new(0)),
            lease_lost_executions: Arc::new(AtomicU64::new(0)),
            cancelled_executions: Arc::new(AtomicU64::new(0)),
            suspended_executions: Arc::new(AtomicU64::new(0)),
            unhandled_executions: Arc::new(AtomicU64::new(0)),
            execution_duration_nanoseconds: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return successfully claimed runs.
    pub fn claimed_runs(&self) -> u64 {
        self.claimed_runs.load(Ordering::Relaxed)
    }

    /// Return failed claim attempts.
    pub fn claim_errors(&self) -> u64 {
        self.claim_errors.load(Ordering::Relaxed)
    }

    /// Return total execution attempts.
    pub fn executions(&self) -> u64 {
        self.executions.load(Ordering::Relaxed)
    }

    /// Return successfully completed execution attempts.
    pub fn completed_executions(&self) -> u64 {
        self.completed_executions.load(Ordering::Relaxed)
    }

    /// Return failed execution attempts.
    pub fn failed_executions(&self) -> u64 {
        self.failed_executions.load(Ordering::Relaxed)
    }

    /// Return execution attempts that lost their finite lease.
    pub fn lease_lost_executions(&self) -> u64 {
        self.lease_lost_executions.load(Ordering::Relaxed)
    }

    /// Return cancelled execution attempts.
    pub fn cancelled_executions(&self) -> u64 {
        self.cancelled_executions.load(Ordering::Relaxed)
    }

    /// Return suspended execution attempts.
    pub fn suspended_executions(&self) -> u64 {
        self.suspended_executions.load(Ordering::Relaxed)
    }

    /// Return execution attempts dropped without a bounded outcome.
    pub fn unhandled_executions(&self) -> u64 {
        self.unhandled_executions.load(Ordering::Relaxed)
    }

    /// Return cumulative observed execution time in nanoseconds.
    pub fn execution_duration_nanoseconds(&self) -> u64 {
        self.execution_duration_nanoseconds.load(Ordering::Relaxed)
    }

    /// Add a value to a counter without allowing integer overflow to wrap it.
    fn increment(counter: &AtomicU64, value: u64) {
        let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(value))
        });
    }

    /// Record successfully claimed runs.
    pub(crate) fn record_claimed(&self, count: usize) {
        Self::increment(&self.claimed_runs, u64::try_from(count).unwrap_or(u64::MAX));
    }

    /// Record a failed claim attempt.
    pub(crate) fn record_claim_error(&self) {
        Self::increment(&self.claim_errors, 1);
    }
}

/// Bounded result of an execution attempt.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ExecutionOutcome {
    /// Task completed successfully.
    Completed,
    /// Task failed.
    Failed,
    /// Worker lost ownership because the finite run lease expired.
    LeaseLost,
    /// Task was cancelled.
    Cancelled,
    /// Task suspended its execution.
    Suspended,
}

/// Tracks one worker execution attempt.
#[derive(Debug)]
pub(crate) struct TaskExecution {
    /// Counters shared with the queue that started the execution.
    metrics: QueueMetrics,
    /// Execution start timestamp.
    start: Instant,
    /// Whether this guard has emitted final counters.
    finished: bool,
}

impl TaskExecution {
    /// Start tracking an execution attempt.
    pub(crate) fn start(metrics: QueueMetrics) -> Self {
        Self { metrics, start: Instant::now(), finished: false }
    }

    /// Finish tracking with a bounded outcome.
    pub(crate) fn finish(mut self, outcome: ExecutionOutcome) {
        self.record(outcome);
        self.finished = true;
    }

    /// Emit final counters for this execution.
    fn record(&self, outcome: ExecutionOutcome) {
        QueueMetrics::increment(&self.metrics.executions, 1);
        let outcome_counter = match outcome {
            ExecutionOutcome::Completed => &self.metrics.completed_executions,
            ExecutionOutcome::Failed => &self.metrics.failed_executions,
            ExecutionOutcome::LeaseLost => &self.metrics.lease_lost_executions,
            ExecutionOutcome::Cancelled => &self.metrics.cancelled_executions,
            ExecutionOutcome::Suspended => &self.metrics.suspended_executions,
        };
        QueueMetrics::increment(outcome_counter, 1);

        let elapsed = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        QueueMetrics::increment(&self.metrics.execution_duration_nanoseconds, elapsed);
    }
}

impl Drop for TaskExecution {
    fn drop(&mut self) {
        if !self.finished {
            QueueMetrics::increment(&self.metrics.executions, 1);
            QueueMetrics::increment(&self.metrics.unhandled_executions, 1);
            let elapsed = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
            QueueMetrics::increment(&self.metrics.execution_duration_nanoseconds, elapsed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionOutcome, QueueMetrics, TaskExecution};

    #[test]
    fn cloned_metrics_observe_same_counters() {
        let metrics = QueueMetrics::new();
        let exporter = metrics.clone();

        metrics.record_claimed(3);
        metrics.record_claim_error();

        assert_eq!(exporter.claimed_runs(), 3);
        assert_eq!(exporter.claim_errors(), 1);
    }

    #[test]
    fn executions_record_one_bounded_outcome() {
        let metrics = QueueMetrics::new();
        TaskExecution::start(metrics.clone()).finish(ExecutionOutcome::Completed);

        assert_eq!(metrics.executions(), 1);
        assert_eq!(metrics.completed_executions(), 1);
        assert_eq!(metrics.unhandled_executions(), 0);
    }

    #[test]
    fn lease_loss_is_a_bounded_execution_outcome() {
        let metrics = QueueMetrics::new();
        TaskExecution::start(metrics.clone()).finish(ExecutionOutcome::LeaseLost);

        assert_eq!(metrics.executions(), 1);
        assert_eq!(metrics.lease_lost_executions(), 1);
        assert_eq!(metrics.unhandled_executions(), 0);
    }

    #[test]
    fn dropped_executions_are_counted_as_unhandled() {
        let metrics = QueueMetrics::new();
        drop(TaskExecution::start(metrics.clone()));

        assert_eq!(metrics.executions(), 1);
        assert_eq!(metrics.unhandled_executions(), 1);
    }
}
