use std::collections::VecDeque;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolateState {
    Fresh,
    Warm,
    Tainted,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireReason {
    MaxAge,
    MaxInvocations,
    Termination,
    HeapPressure,
    ReuseDisabled,
    PoolFull,
    ConfigChanged,
    CleanupFailure,
    SetupFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsolatePoolConfig {
    pub max_age_seconds: u64,
    pub max_invocations: u64,
    pub max_pool_size: usize,
    pub enable_reuse: bool,
}

const DEFAULT_MAX_AGE_SECONDS: u64 = 120;
const DEFAULT_MAX_INVOCATIONS: u64 = 250;
const DEFAULT_MAX_POOL_SIZE: usize = 2;

impl Default for IsolatePoolConfig {
    fn default() -> Self {
        Self {
            max_age_seconds: DEFAULT_MAX_AGE_SECONDS,
            max_invocations: DEFAULT_MAX_INVOCATIONS,
            max_pool_size: DEFAULT_MAX_POOL_SIZE,
            enable_reuse: true,
        }
    }
}

#[derive(Debug)]
struct PoolEntry<T> {
    value: T,
    state: IsolateState,
    created_at: Instant,
    invocation_count: u64,
    heap_pressure_events: u64,
    last_used_at: Instant,
}

impl<T> PoolEntry<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            state: IsolateState::Fresh,
            created_at: Instant::now(),
            invocation_count: 0,
            heap_pressure_events: 0,
            last_used_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct CheckedOut<T> {
    entry: PoolEntry<T>,
    was_warm: bool,
}

impl<T> CheckedOut<T> {
    pub fn fresh(value: T) -> Self {
        let mut entry = PoolEntry::new(value);
        entry.state = IsolateState::Warm;
        entry.invocation_count = 1;
        Self { entry, was_warm: false }
    }

    #[cfg(test)]
    pub fn value(&self) -> &T {
        &self.entry.value
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.entry.value
    }

    pub fn was_warm(&self) -> bool {
        self.was_warm
    }

    fn into_entry(self) -> PoolEntry<T> {
        self.entry
    }
}

#[derive(Debug)]
pub struct CheckoutResult<T> {
    pub checked_out: Option<CheckedOut<T>>,
    pub retired: Vec<RetireReason>,
    pub was_miss: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellHealth {
    pub terminated: bool,
    pub heap_pressure: bool,
    pub cleanup_ok: bool,
    pub config_changed: bool,
    pub setup_failed: bool,
}

impl Default for ShellHealth {
    fn default() -> Self {
        Self {
            terminated: false,
            heap_pressure: false,
            cleanup_ok: true,
            config_changed: false,
            setup_failed: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckinOutcome {
    pub returned: bool,
    pub retire_reason: Option<RetireReason>,
}

#[derive(Debug, Default)]
pub struct IsolatePool<T> {
    available: VecDeque<PoolEntry<T>>,
}

impl<T> IsolatePool<T> {
    pub fn new() -> Self {
        Self { available: VecDeque::new() }
    }

    pub fn checkout(&mut self, config: &IsolatePoolConfig) -> CheckoutResult<T> {
        let mut retired = Vec::new();

        while let Some(mut entry) = self.available.pop_front() {
            if let Some(reason) = retire_reason_for_entry(&entry, config) {
                entry.state = IsolateState::Retired;
                retired.push(reason);
                continue;
            }

            let was_warm = !matches!(entry.state, IsolateState::Fresh);
            entry.state = IsolateState::Warm;
            entry.invocation_count += 1;
            entry.last_used_at = Instant::now();
            return CheckoutResult {
                checked_out: Some(CheckedOut { entry, was_warm }),
                retired,
                was_miss: false,
            };
        }

        CheckoutResult { checked_out: None, retired, was_miss: true }
    }

    pub fn checkin(
        &mut self,
        checked_out: CheckedOut<T>,
        config: &IsolatePoolConfig,
        health: ShellHealth,
    ) -> CheckinOutcome {
        if let Some(reason) = retire_reason_for_health(&checked_out.entry, config, health) {
            return CheckinOutcome { returned: false, retire_reason: Some(reason) };
        }

        if self.available.len() >= config.max_pool_size {
            return CheckinOutcome { returned: false, retire_reason: Some(RetireReason::PoolFull) };
        }

        let mut entry = checked_out.into_entry();
        entry.state = IsolateState::Warm;
        self.available.push_back(entry);
        CheckinOutcome { returned: true, retire_reason: None }
    }

    #[cfg(test)]
    pub fn insert_fresh(&mut self, value: T, config: &IsolatePoolConfig) -> CheckinOutcome {
        if !config.enable_reuse {
            return CheckinOutcome {
                returned: false,
                retire_reason: Some(RetireReason::ReuseDisabled),
            };
        }

        if self.available.len() >= config.max_pool_size {
            return CheckinOutcome { returned: false, retire_reason: Some(RetireReason::PoolFull) };
        }

        self.available.push_back(PoolEntry::new(value));
        CheckinOutcome { returned: true, retire_reason: None }
    }

    #[cfg(test)]
    pub fn available_count(&self) -> usize {
        self.available.len()
    }
}

fn retire_reason_for_entry<T>(
    entry: &PoolEntry<T>,
    config: &IsolatePoolConfig,
) -> Option<RetireReason> {
    if !config.enable_reuse {
        return Some(RetireReason::ReuseDisabled);
    }

    if entry.state == IsolateState::Tainted {
        return Some(RetireReason::Termination);
    }

    if entry.created_at.elapsed().as_secs() >= config.max_age_seconds {
        return Some(RetireReason::MaxAge);
    }

    if entry.invocation_count >= config.max_invocations {
        return Some(RetireReason::MaxInvocations);
    }

    None
}

fn retire_reason_for_health<T>(
    entry: &PoolEntry<T>,
    config: &IsolatePoolConfig,
    health: ShellHealth,
) -> Option<RetireReason> {
    if !config.enable_reuse {
        return Some(RetireReason::ReuseDisabled);
    }

    if health.setup_failed {
        return Some(RetireReason::SetupFailure);
    }

    if health.config_changed {
        return Some(RetireReason::ConfigChanged);
    }

    if health.terminated {
        return Some(RetireReason::Termination);
    }

    if health.heap_pressure || entry.heap_pressure_events > 0 {
        return Some(RetireReason::HeapPressure);
    }

    if !health.cleanup_ok {
        return Some(RetireReason::CleanupFailure);
    }

    if entry.created_at.elapsed().as_secs() >= config.max_age_seconds {
        return Some(RetireReason::MaxAge);
    }

    if entry.invocation_count >= config.max_invocations {
        return Some(RetireReason::MaxInvocations);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn checkout_miss_when_empty() {
        let mut pool = IsolatePool::<u32>::new();
        let result = pool.checkout(&IsolatePoolConfig::default());
        assert!(result.checked_out.is_none());
        assert!(result.was_miss);
        assert!(result.retired.is_empty());
    }

    #[test]
    fn fresh_insert_then_checkout_returns_cold_entry() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig::default();
        assert!(pool.insert_fresh(7_u32, &config).returned);

        let result = pool.checkout(&config);
        let checked_out = result.checked_out.expect("entry should be available");
        assert!(!checked_out.was_warm());
        assert_eq!(*checked_out.value(), 7);
    }

    #[test]
    fn checkin_then_checkout_returns_warm_entry() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig::default();
        assert!(pool.insert_fresh(11_u32, &config).returned);

        let first = pool.checkout(&config).checked_out.expect("checkout should succeed");
        assert!(pool.checkin(first, &config, ShellHealth::default()).returned);

        let second = pool.checkout(&config).checked_out.expect("warm checkout should succeed");
        assert!(second.was_warm());
        assert_eq!(*second.value(), 11);
    }

    #[test]
    fn expired_entry_is_retired_on_checkout() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig { max_age_seconds: 0, ..Default::default() };
        assert!(pool.insert_fresh(5_u32, &config).returned);
        thread::sleep(Duration::from_millis(1));

        let result = pool.checkout(&config);
        assert!(result.checked_out.is_none());
        assert!(result.retired.contains(&RetireReason::MaxAge));
    }

    #[test]
    fn terminated_shell_is_not_returned() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig::default();
        assert!(pool.insert_fresh(3_u32, &config).returned);

        let checked_out = pool.checkout(&config).checked_out.expect("checkout should succeed");
        let outcome = pool.checkin(
            checked_out,
            &config,
            ShellHealth { terminated: true, ..Default::default() },
        );
        assert!(!outcome.returned);
        assert_eq!(outcome.retire_reason, Some(RetireReason::Termination));
        assert_eq!(pool.available_count(), 0);
    }

    #[test]
    fn cleanup_failure_retires_shell() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig::default();
        assert!(pool.insert_fresh(9_u32, &config).returned);

        let checked_out = pool.checkout(&config).checked_out.expect("checkout should succeed");
        let outcome = pool.checkin(
            checked_out,
            &config,
            ShellHealth { cleanup_ok: false, ..Default::default() },
        );
        assert_eq!(outcome.retire_reason, Some(RetireReason::CleanupFailure));
    }

    #[test]
    fn pool_full_retires_checkin() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig { max_pool_size: 1, ..Default::default() };
        assert!(pool.insert_fresh(1_u32, &config).returned);
        assert!(!pool.insert_fresh(2_u32, &config).returned);
    }

    #[test]
    fn setup_failure_retires_shell() {
        let mut pool = IsolatePool::new();
        let config = IsolatePoolConfig::default();
        assert!(pool.insert_fresh(19_u32, &config).returned);

        let checked_out = pool.checkout(&config).checked_out.expect("checkout should succeed");
        let outcome = pool.checkin(
            checked_out,
            &config,
            ShellHealth { setup_failed: true, ..Default::default() },
        );
        assert_eq!(outcome.retire_reason, Some(RetireReason::SetupFailure));
    }
}
