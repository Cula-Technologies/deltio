use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds (in seconds) for the push dispatch duration histogram.
/// The final bound is `+Inf`.
pub const DURATION_BUCKETS_SECONDS: [f64; 12] = [
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    f64::INFINITY,
];

/// Collects metrics about HTTP push delivery.
///
/// Push delivery runs in detached tasks rather than in an actor, so these
/// counters are shared atomics instead of actor-owned state. They are global
/// (aggregated across all push subscriptions) to avoid per-subscription
/// locking on the dispatch path.
#[derive(Debug)]
pub struct PushMetrics {
    /// Number of successful push dispatches.
    dispatch_success: AtomicU64,

    /// Number of failed push dispatches.
    dispatch_failure: AtomicU64,

    /// Per-bucket observation counts for the duration histogram (non-cumulative).
    duration_buckets: [AtomicU64; DURATION_BUCKETS_SECONDS.len()],

    /// Sum of all observed durations, in microseconds.
    duration_sum_micros: AtomicU64,
}

impl PushMetrics {
    /// Creates a new, zeroed `PushMetrics`.
    pub fn new() -> Self {
        Self {
            dispatch_success: AtomicU64::new(0),
            dispatch_failure: AtomicU64::new(0),
            duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            duration_sum_micros: AtomicU64::new(0),
        }
    }

    /// Records a single dispatch outcome along with how long it took.
    pub fn record(&self, success: bool, duration: Duration) {
        if success {
            self.dispatch_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.dispatch_failure.fetch_add(1, Ordering::Relaxed);
        }

        let seconds = duration.as_secs_f64();
        let bucket = DURATION_BUCKETS_SECONDS
            .iter()
            .position(|&bound| seconds <= bound)
            .unwrap_or(DURATION_BUCKETS_SECONDS.len() - 1);
        self.duration_buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.duration_sum_micros
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Takes a snapshot of the current values for rendering.
    ///
    /// `duration_count` is derived from the bucket snapshot rather than read
    /// from a separate atomic, so the histogram is internally consistent:
    /// `_count` always equals the `+Inf` cumulative bucket even under
    /// concurrent dispatch.
    pub fn snapshot(&self) -> PushMetricsSnapshot {
        let duration_buckets: [u64; DURATION_BUCKETS_SECONDS.len()] =
            std::array::from_fn(|i| self.duration_buckets[i].load(Ordering::Relaxed));
        PushMetricsSnapshot {
            dispatch_success: self.dispatch_success.load(Ordering::Relaxed),
            dispatch_failure: self.dispatch_failure.load(Ordering::Relaxed),
            duration_count: duration_buckets.iter().sum(),
            duration_buckets,
            duration_sum_micros: self.duration_sum_micros.load(Ordering::Relaxed),
        }
    }
}

impl Default for PushMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time copy of [`PushMetrics`], used for rendering.
#[derive(Debug, Clone)]
pub struct PushMetricsSnapshot {
    pub dispatch_success: u64,
    pub dispatch_failure: u64,
    pub duration_buckets: [u64; DURATION_BUCKETS_SECONDS.len()],
    pub duration_sum_micros: u64,
    pub duration_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_outcomes_and_buckets() {
        let m = PushMetrics::new();
        m.record(true, Duration::from_millis(3)); // <= 0.005 -> bucket 0
        m.record(true, Duration::from_millis(30)); // <= 0.05 -> bucket 3
        m.record(false, Duration::from_secs(20)); // > 10 -> +Inf bucket (last)

        let s = m.snapshot();
        assert_eq!(s.dispatch_success, 2);
        assert_eq!(s.dispatch_failure, 1);
        assert_eq!(s.duration_count, 3);
        assert_eq!(s.duration_buckets[0], 1);
        assert_eq!(s.duration_buckets[3], 1);
        assert_eq!(s.duration_buckets[DURATION_BUCKETS_SECONDS.len() - 1], 1);
    }
}
