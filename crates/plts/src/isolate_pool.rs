use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolateState {
    Fresh,
    Warm,
    Tainted,
    Retired,
}

impl std::fmt::Display for IsolateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolateState::Fresh => write!(f, "fresh"),
            IsolateState::Warm => write!(f, "warm"),
            IsolateState::Tainted => write!(f, "tainted"),
            IsolateState::Retired => write!(f, "retired"),
        }
    }
}

pub struct IsolatePoolMetrics {
    pub pool_hits: AtomicU64,
    pub pool_misses: AtomicU64,
    pub active_isolates: AtomicUsize,
    pub retired_count: AtomicU64,
    pub recycle_reason_max_age: AtomicU64,
    pub recycle_reason_max_invocations: AtomicU64,
    pub recycle_reason_termination: AtomicU64,
    pub recycle_reason_heap_pressure: AtomicU64,
    pub cold_invocations: AtomicU64,
    pub warm_invocations: AtomicU64,
}

impl Default for IsolatePoolMetrics {
    fn default() -> Self {
        Self {
            pool_hits: AtomicU64::new(0),
            pool_misses: AtomicU64::new(0),
            active_isolates: AtomicUsize::new(0),
            retired_count: AtomicU64::new(0),
            recycle_reason_max_age: AtomicU64::new(0),
            recycle_reason_max_invocations: AtomicU64::new(0),
            recycle_reason_termination: AtomicU64::new(0),
            recycle_reason_heap_pressure: AtomicU64::new(0),
            cold_invocations: AtomicU64::new(0),
            warm_invocations: AtomicU64::new(0),
        }
    }
}

pub struct IsolatePoolConfig {
    pub max_age_seconds: u64,
    pub max_invocations: u64,
    pub max_pool_size: usize,
    pub enable_reuse: bool,
}

impl Default for IsolatePoolConfig {
    fn default() -> Self {
        Self { max_age_seconds: 300, max_invocations: 1000, max_pool_size: 4, enable_reuse: true }
    }
}

struct PooledIsolate {
    state: IsolateState,
    created_at: Instant,
    invocation_count: u64,
    termination_count: u64,
    heap_pressure_events: u64,
    last_used_at: Instant,
}

impl PooledIsolate {
    fn new() -> Self {
        Self {
            state: IsolateState::Fresh,
            created_at: Instant::now(),
            invocation_count: 0,
            termination_count: 0,
            heap_pressure_events: 0,
            last_used_at: Instant::now(),
        }
    }

    fn check_out(&mut self, config: &IsolatePoolConfig) -> bool {
        if self.state == IsolateState::Retired {
            return false;
        }

        if self.state == IsolateState::Tainted {
            return false;
        }

        if self.should_recycle(config) {
            return false;
        }

        self.state = IsolateState::Warm;
        self.invocation_count += 1;
        self.last_used_at = Instant::now();
        true
    }

    fn check_in(&mut self, healthy: bool) {
        if !healthy {
            self.state = IsolateState::Tainted;
            self.termination_count += 1;
        } else {
            self.state = IsolateState::Warm;
        }
    }

    fn should_recycle(&self, config: &IsolatePoolConfig) -> bool {
        if !config.enable_reuse {
            return true;
        }

        if self.state == IsolateState::Tainted {
            return true;
        }

        let age_seconds = self.created_at.elapsed().as_secs();
        if age_seconds >= config.max_age_seconds {
            return true;
        }

        if self.invocation_count >= config.max_invocations {
            return true;
        }

        false
    }

    fn mark_heap_pressure(&mut self) {
        self.heap_pressure_events += 1;
    }

    fn recycle_reason(&self, config: &IsolatePoolConfig) -> &'static str {
        if self.state == IsolateState::Tainted {
            return "termination";
        }

        let age_seconds = self.created_at.elapsed().as_secs();
        if age_seconds >= config.max_age_seconds {
            return "max_age";
        }

        if self.invocation_count >= config.max_invocations {
            return "max_invocations";
        }

        if self.heap_pressure_events > 0 {
            return "heap_pressure";
        }

        "none"
    }
}

pub struct IsolatePool {
    config: IsolatePoolConfig,
    metrics: Arc<IsolatePoolMetrics>,
    available: std::sync::Mutex<VecDeque<PooledIsolate>>,
}

impl IsolatePool {
    pub fn new(config: IsolatePoolConfig, metrics: Arc<IsolatePoolMetrics>) -> Self {
        Self { config, metrics, available: std::sync::Mutex::new(VecDeque::new()) }
    }

    pub fn checkout(&self) -> bool {
        let mut pool = self.available.lock().unwrap();

        while let Some(mut isolate) = pool.pop_front() {
            if isolate.check_out(&self.config) {
                self.metrics.active_isolates.fetch_add(1, Ordering::Relaxed);
                if isolate.invocation_count > 1 {
                    self.metrics.pool_hits.fetch_add(1, Ordering::Relaxed);
                    self.metrics.warm_invocations.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.metrics.cold_invocations.fetch_add(1, Ordering::Relaxed);
                }
                return true;
            } else {
                self.metrics.retired_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.metrics.pool_misses.fetch_add(1, Ordering::Relaxed);
        self.metrics.cold_invocations.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_isolates.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn checkin(&self, healthy: bool) {
        let mut pool = self.available.lock().unwrap();

        if healthy && pool.len() < self.config.max_pool_size {
            let mut isolate = PooledIsolate::new();
            isolate.check_in(healthy);
            pool.push_back(isolate);
        } else {
            self.metrics.retired_count.fetch_add(1, Ordering::Relaxed);
        }

        self.metrics.active_isolates.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_recycle(&self) {
        let pool = self.available.lock().unwrap();
        if let Some(isolate) = pool.back() {
            match isolate.recycle_reason(&self.config) {
                "max_age" => {
                    self.metrics.recycle_reason_max_age.fetch_add(1, Ordering::Relaxed);
                }
                "max_invocations" => {
                    self.metrics.recycle_reason_max_invocations.fetch_add(1, Ordering::Relaxed);
                }
                "termination" => {
                    self.metrics.recycle_reason_termination.fetch_add(1, Ordering::Relaxed);
                }
                "heap_pressure" => {
                    self.metrics.recycle_reason_heap_pressure.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }

    pub fn config(&self) -> &IsolatePoolConfig {
        &self.config
    }

    pub fn metrics(&self) -> &Arc<IsolatePoolMetrics> {
        &self.metrics
    }

    pub fn active_count(&self) -> usize {
        self.metrics.active_isolates.load(Ordering::Relaxed)
    }

    pub fn available_count(&self) -> usize {
        self.available.lock().unwrap().len()
    }
}

impl Default for IsolatePool {
    fn default() -> Self {
        Self::new(IsolatePoolConfig::default(), Arc::new(IsolatePoolMetrics::default()))
    }
}

pub fn create_default_isolate_pool() -> IsolatePool {
    IsolatePool::new(IsolatePoolConfig::default(), Arc::new(IsolatePoolMetrics::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_isolate_starts_fresh() {
        let isolate = PooledIsolate::new();
        assert_eq!(isolate.state, IsolateState::Fresh);
        assert_eq!(isolate.invocation_count, 0);
    }

    #[test]
    fn pooled_isolate_checkout_marks_as_warm() {
        let mut isolate = PooledIsolate::new();
        let config = IsolatePoolConfig::default();

        let result = isolate.check_out(&config);
        assert!(result);
        assert_eq!(isolate.state, IsolateState::Warm);
        assert_eq!(isolate.invocation_count, 1);
    }

    #[test]
    fn pooled_isolate_checkout_increments_invocation_count() {
        let mut isolate = PooledIsolate::new();
        let config = IsolatePoolConfig::default();

        isolate.check_out(&config);
        isolate.check_in(true);
        isolate.check_out(&config);

        assert_eq!(isolate.invocation_count, 2);
    }

    #[test]
    fn pooled_isolate_checkout_fails_when_tainted() {
        let mut isolate = PooledIsolate::new();
        let config = IsolatePoolConfig::default();

        isolate.check_out(&config);
        isolate.check_in(false);
        assert_eq!(isolate.state, IsolateState::Tainted);

        let result = isolate.check_out(&config);
        assert!(!result);
    }

    #[test]
    fn pooled_isolate_checkout_fails_when_max_age_exceeded() {
        let mut isolate = PooledIsolate::new();
        let config = IsolatePoolConfig { max_age_seconds: 0, ..Default::default() };

        let result = isolate.check_out(&config);
        assert!(!result);
    }

    #[test]
    fn pooled_isolate_checkout_fails_when_max_invocations_exceeded() {
        let mut isolate = PooledIsolate::new();
        let config = IsolatePoolConfig { max_invocations: 0, ..Default::default() };

        let result = isolate.check_out(&config);
        assert!(!result);
    }

    #[test]
    fn pooled_isolate_recycle_reason_max_age() {
        let isolate = PooledIsolate::new();
        let config = IsolatePoolConfig { max_age_seconds: 0, ..Default::default() };

        assert_eq!(isolate.recycle_reason(&config), "max_age");
    }

    #[test]
    fn pooled_isolate_recycle_reason_max_invocations() {
        let isolate = PooledIsolate::new();
        let config = IsolatePoolConfig { max_invocations: 0, ..Default::default() };

        assert_eq!(isolate.recycle_reason(&config), "max_invocations");
    }

    #[test]
    fn pooled_isolate_recycle_reason_termination() {
        let isolate = PooledIsolate::new();
        let config = IsolatePoolConfig::default();

        let mut isolate = PooledIsolate::new();
        isolate.check_out(&config);
        isolate.check_in(false);

        assert_eq!(isolate.recycle_reason(&config), "termination");
    }

    #[test]
    fn isolate_pool_checkout_returns_true_when_available() {
        let metrics = Arc::new(IsolatePoolMetrics::default());
        let config = IsolatePoolConfig { enable_reuse: false, ..Default::default() };
        let pool = IsolatePool::new(config, metrics);

        let result = pool.checkout();
        assert!(result);
        assert_eq!(pool.active_count(), 1);
    }

    #[test]
    fn isolate_pool_checkin_decrements_active_count() {
        let metrics = Arc::new(IsolatePoolMetrics::default());
        let config = IsolatePoolConfig { enable_reuse: false, ..Default::default() };
        let pool = IsolatePool::new(config, metrics);

        pool.checkout();
        assert_eq!(pool.active_count(), 1);

        pool.checkin(true);
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn isolate_pool_metrics_track_hits_and_misses() {
        let metrics = Arc::new(IsolatePoolMetrics::default());
        let config = IsolatePoolConfig { enable_reuse: false, ..Default::default() };
        let pool = IsolatePool::new(config, metrics);

        pool.checkout();
        assert_eq!(pool.metrics.pool_misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn isolate_pool_respects_max_pool_size() {
        let metrics = Arc::new(IsolatePoolMetrics::default());
        let config =
            IsolatePoolConfig { max_pool_size: 1, enable_reuse: true, ..Default::default() };
        let pool = IsolatePool::new(config, metrics);

        pool.checkout();
        pool.checkin(true);
        assert_eq!(pool.available_count(), 1);

        pool.checkout();
        pool.checkin(true);
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn isolate_state_display() {
        assert_eq!(format!("{}", IsolateState::Fresh), "fresh");
        assert_eq!(format!("{}", IsolateState::Warm), "warm");
        assert_eq!(format!("{}", IsolateState::Tainted), "tainted");
        assert_eq!(format!("{}", IsolateState::Retired), "retired");
    }
}
