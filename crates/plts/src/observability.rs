use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static COMPILE_CALLS: AtomicU64 = AtomicU64::new(0);
static COMPILE_ERRORS: AtomicU64 = AtomicU64::new(0);
static COMPILE_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static COMPILE_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static COMPILE_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static COMPILE_ERROR_DIAGNOSTICS: AtomicU64 = AtomicU64::new(0);
static COMPILE_ERROR_SQL: AtomicU64 = AtomicU64::new(0);
static COMPILE_ERROR_UNKNOWN: AtomicU64 = AtomicU64::new(0);
static EXECUTE_CALLS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERRORS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_TIMEOUT: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_MEMORY: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_CANCEL: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_JS_EXCEPTION: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_SQL: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERROR_UNKNOWN: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_CHECKOUT_HITS: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_CHECKOUT_MISSES: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_CHECKOUT_LAST_US: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_CHECKOUT_MAX_US: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_SETUP_REALM_LAST_US: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_SETUP_REALM_MAX_US: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_COLD_SHELL_CREATES: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_WARM_SHELL_REUSES: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRED: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRE_MAX_AGE: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRE_MAX_INVOCATIONS: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRE_TERMINATION: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRE_HEAP_PRESSURE: AtomicU64 = AtomicU64::new(0);
static RUNTIME_READINESS_RETIRE_OTHER: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_INIT_CALLS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_INIT_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_INIT_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_INIT_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_BUILT_IN_CONFIGURED: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_MANUAL_HITS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_MANUAL_MISSES: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_FALLBACK_COMPILES: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_CONFIG_ERRORS: AtomicU64 = AtomicU64::new(0);
static TSGO_WASM_CACHE_DESERIALIZE_ERRORS: AtomicU64 = AtomicU64::new(0);
static LOG_LEVEL: OnceLock<LogLevel> = OnceLock::new();

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
}

fn parse_log_level(raw: &str) -> LogLevel {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => LogLevel::Off,
        "error" => LogLevel::Error,
        "warn" | "warning" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" | "trace" => LogLevel::Debug,
        _ => LogLevel::Warn,
    }
}

fn read_log_level_from_spi() -> LogLevel {
    let raw = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('plts.log_level', true), 'warn')::text",
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| "warn".to_string());
    parse_log_level(raw.as_str())
}

fn current_log_level() -> LogLevel {
    *LOG_LEVEL.get_or_init(read_log_level_from_spi)
}

pub(crate) fn should_log_info() -> bool {
    current_log_level() >= LogLevel::Info
}

pub(crate) fn should_log_warn() -> bool {
    current_log_level() >= LogLevel::Warn
}

pub(crate) fn log_info(message: &str) {
    if should_log_info() {
        info!("{message}");
    }
}

pub(crate) fn log_warn(message: &str) {
    if should_log_warn() {
        warning!("{message}");
    }
}

pub(crate) fn record_compile_start() -> Instant {
    COMPILE_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_compile_success(started_at: Instant) {
    record_latency(
        started_at,
        &COMPILE_LATENCY_TOTAL_MS,
        &COMPILE_LATENCY_LAST_MS,
        &COMPILE_LATENCY_MAX_MS,
    );
}

pub(crate) fn record_compile_error(started_at: Instant, class: &str) {
    COMPILE_ERRORS.fetch_add(1, Ordering::Relaxed);
    increment_compile_error_class(class);
    record_compile_success(started_at);
}

pub(crate) fn record_execute_start() -> Instant {
    EXECUTE_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_execute_success(started_at: Instant) {
    record_latency(
        started_at,
        &EXECUTE_LATENCY_TOTAL_MS,
        &EXECUTE_LATENCY_LAST_MS,
        &EXECUTE_LATENCY_MAX_MS,
    );
}

pub(crate) fn record_execute_error(started_at: Instant, class: &str) {
    EXECUTE_ERRORS.fetch_add(1, Ordering::Relaxed);
    increment_execute_error_class(class);
    record_execute_success(started_at);
}

pub(crate) fn record_runtime_checkout_hit(elapsed_us: u64) {
    RUNTIME_READINESS_CHECKOUT_HITS.fetch_add(1, Ordering::Relaxed);
    RUNTIME_READINESS_CHECKOUT_LAST_US.store(elapsed_us, Ordering::Relaxed);
    update_max(&RUNTIME_READINESS_CHECKOUT_MAX_US, elapsed_us);
}

pub(crate) fn record_runtime_checkout_miss(elapsed_us: u64) {
    RUNTIME_READINESS_CHECKOUT_MISSES.fetch_add(1, Ordering::Relaxed);
    RUNTIME_READINESS_CHECKOUT_LAST_US.store(elapsed_us, Ordering::Relaxed);
    update_max(&RUNTIME_READINESS_CHECKOUT_MAX_US, elapsed_us);
}

pub(crate) fn record_runtime_setup_realm(elapsed_us: u64) {
    RUNTIME_READINESS_SETUP_REALM_LAST_US.store(elapsed_us, Ordering::Relaxed);
    update_max(&RUNTIME_READINESS_SETUP_REALM_MAX_US, elapsed_us);
}

pub(crate) fn record_runtime_cold_shell_create() {
    RUNTIME_READINESS_COLD_SHELL_CREATES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_runtime_warm_shell_reuse() {
    RUNTIME_READINESS_WARM_SHELL_REUSES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_runtime_retire(reason: &str) {
    RUNTIME_READINESS_RETIRED.fetch_add(1, Ordering::Relaxed);
    match reason {
        "max_age" => {
            RUNTIME_READINESS_RETIRE_MAX_AGE.fetch_add(1, Ordering::Relaxed);
        }
        "max_invocations" => {
            RUNTIME_READINESS_RETIRE_MAX_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        "termination" => {
            RUNTIME_READINESS_RETIRE_TERMINATION.fetch_add(1, Ordering::Relaxed);
        }
        "heap_pressure" => {
            RUNTIME_READINESS_RETIRE_HEAP_PRESSURE.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            RUNTIME_READINESS_RETIRE_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) fn record_tsgo_wasm_init_start() -> Instant {
    TSGO_WASM_INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_tsgo_wasm_init_success(started_at: Instant) {
    record_latency(
        started_at,
        &TSGO_WASM_INIT_LATENCY_TOTAL_MS,
        &TSGO_WASM_INIT_LATENCY_LAST_MS,
        &TSGO_WASM_INIT_LATENCY_MAX_MS,
    );
}

pub(crate) fn record_tsgo_wasm_cache_event(event: &str) {
    match event {
        "built_in_configured" => {
            TSGO_WASM_CACHE_BUILT_IN_CONFIGURED.fetch_add(1, Ordering::Relaxed);
        }
        "manual_hit" => {
            TSGO_WASM_CACHE_MANUAL_HITS.fetch_add(1, Ordering::Relaxed);
        }
        "manual_miss" => {
            TSGO_WASM_CACHE_MANUAL_MISSES.fetch_add(1, Ordering::Relaxed);
        }
        "fallback_compile" => {
            TSGO_WASM_CACHE_FALLBACK_COMPILES.fetch_add(1, Ordering::Relaxed);
        }
        "config_error" => {
            TSGO_WASM_CACHE_CONFIG_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
        "deserialize_error" => {
            TSGO_WASM_CACHE_DESERIALIZE_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

pub(crate) fn classify_compile_error(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("diagnostic") || lowered.contains("typescript") {
        "diagnostics"
    } else if lowered.contains("spi") || lowered.contains("sql") {
        "sql"
    } else {
        "unknown"
    }
}

pub(crate) fn classify_execute_error(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("runtime timeout") || lowered.contains("statement_timeout") {
        "timeout"
    } else if lowered.contains("runtime memory limit") || lowered.contains("heap") {
        "memory"
    } else if lowered.contains("cancel signal") || lowered.contains("interrupted") {
        "cancel"
    } else if lowered.contains("spi") || lowered.contains("sql") {
        "sql"
    } else if lowered.contains("stage=") {
        "js_exception"
    } else {
        "unknown"
    }
}

pub(crate) fn metrics_json() -> Value {
    json!({
        "compile": {
            "calls": COMPILE_CALLS.load(Ordering::Relaxed),
            "errors": COMPILE_ERRORS.load(Ordering::Relaxed),
            "latency_ms": {
                "total": COMPILE_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                "last": COMPILE_LATENCY_LAST_MS.load(Ordering::Relaxed),
                "max": COMPILE_LATENCY_MAX_MS.load(Ordering::Relaxed)
            },
            "error_classes": {
                "diagnostics": COMPILE_ERROR_DIAGNOSTICS.load(Ordering::Relaxed),
                "sql": COMPILE_ERROR_SQL.load(Ordering::Relaxed),
                "unknown": COMPILE_ERROR_UNKNOWN.load(Ordering::Relaxed)
            }
        },
        "execute": {
            "calls": EXECUTE_CALLS.load(Ordering::Relaxed),
            "errors": EXECUTE_ERRORS.load(Ordering::Relaxed),
            "latency_ms": {
                "total": EXECUTE_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                "last": EXECUTE_LATENCY_LAST_MS.load(Ordering::Relaxed),
                "max": EXECUTE_LATENCY_MAX_MS.load(Ordering::Relaxed)
            },
            "error_classes": {
                "timeout": EXECUTE_ERROR_TIMEOUT.load(Ordering::Relaxed),
                "memory": EXECUTE_ERROR_MEMORY.load(Ordering::Relaxed),
                "cancel": EXECUTE_ERROR_CANCEL.load(Ordering::Relaxed),
                "js_exception": EXECUTE_ERROR_JS_EXCEPTION.load(Ordering::Relaxed),
                "sql": EXECUTE_ERROR_SQL.load(Ordering::Relaxed),
                "unknown": EXECUTE_ERROR_UNKNOWN.load(Ordering::Relaxed)
            }
        },
        "runtime": {
            "readiness": {
                "checkout_hits": RUNTIME_READINESS_CHECKOUT_HITS.load(Ordering::Relaxed),
                "checkout_misses": RUNTIME_READINESS_CHECKOUT_MISSES.load(Ordering::Relaxed),
                "checkout_last_us": RUNTIME_READINESS_CHECKOUT_LAST_US.load(Ordering::Relaxed),
                "checkout_max_us": RUNTIME_READINESS_CHECKOUT_MAX_US.load(Ordering::Relaxed),
                "setup_realm_last_us": RUNTIME_READINESS_SETUP_REALM_LAST_US.load(Ordering::Relaxed),
                "setup_realm_max_us": RUNTIME_READINESS_SETUP_REALM_MAX_US.load(Ordering::Relaxed),
                "cold_shell_creates": RUNTIME_READINESS_COLD_SHELL_CREATES.load(Ordering::Relaxed),
                "warm_shell_reuses": RUNTIME_READINESS_WARM_SHELL_REUSES.load(Ordering::Relaxed),
                "retired": RUNTIME_READINESS_RETIRED.load(Ordering::Relaxed),
                "retire_reasons": {
                    "max_age": RUNTIME_READINESS_RETIRE_MAX_AGE.load(Ordering::Relaxed),
                    "max_invocations": RUNTIME_READINESS_RETIRE_MAX_INVOCATIONS.load(Ordering::Relaxed),
                    "termination": RUNTIME_READINESS_RETIRE_TERMINATION.load(Ordering::Relaxed),
                    "heap_pressure": RUNTIME_READINESS_RETIRE_HEAP_PRESSURE.load(Ordering::Relaxed),
                    "other": RUNTIME_READINESS_RETIRE_OTHER.load(Ordering::Relaxed)
                }
            }
        },
        "tsgo_wasm": {
            "init": {
                "calls": TSGO_WASM_INIT_CALLS.load(Ordering::Relaxed),
                "latency_ms": {
                    "total": TSGO_WASM_INIT_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                    "last": TSGO_WASM_INIT_LATENCY_LAST_MS.load(Ordering::Relaxed),
                    "max": TSGO_WASM_INIT_LATENCY_MAX_MS.load(Ordering::Relaxed)
                }
            },
            "cache": {
                "built_in_configured": TSGO_WASM_CACHE_BUILT_IN_CONFIGURED.load(Ordering::Relaxed),
                "manual_hits": TSGO_WASM_CACHE_MANUAL_HITS.load(Ordering::Relaxed),
                "manual_misses": TSGO_WASM_CACHE_MANUAL_MISSES.load(Ordering::Relaxed),
                "fallback_compiles": TSGO_WASM_CACHE_FALLBACK_COMPILES.load(Ordering::Relaxed),
                "config_errors": TSGO_WASM_CACHE_CONFIG_ERRORS.load(Ordering::Relaxed),
                "deserialize_errors": TSGO_WASM_CACHE_DESERIALIZE_ERRORS.load(Ordering::Relaxed)
            }
        }
    })
}

fn increment_compile_error_class(class: &str) {
    match class {
        "diagnostics" => {
            COMPILE_ERROR_DIAGNOSTICS.fetch_add(1, Ordering::Relaxed);
        }
        "sql" => {
            COMPILE_ERROR_SQL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            COMPILE_ERROR_UNKNOWN.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn increment_execute_error_class(class: &str) {
    match class {
        "timeout" => {
            EXECUTE_ERROR_TIMEOUT.fetch_add(1, Ordering::Relaxed);
        }
        "memory" => {
            EXECUTE_ERROR_MEMORY.fetch_add(1, Ordering::Relaxed);
        }
        "cancel" => {
            EXECUTE_ERROR_CANCEL.fetch_add(1, Ordering::Relaxed);
        }
        "js_exception" => {
            EXECUTE_ERROR_JS_EXCEPTION.fetch_add(1, Ordering::Relaxed);
        }
        "sql" => {
            EXECUTE_ERROR_SQL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            EXECUTE_ERROR_UNKNOWN.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn record_latency(
    started_at: Instant,
    total_ms: &AtomicU64,
    last_ms: &AtomicU64,
    max_ms: &AtomicU64,
) {
    let elapsed_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    total_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
    last_ms.store(elapsed_ms, Ordering::Relaxed);
    update_max(max_ms, elapsed_ms);
}

fn update_max(max_metric: &AtomicU64, candidate: u64) {
    let mut current = max_metric.load(Ordering::Relaxed);
    while candidate > current {
        match max_metric.compare_exchange(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(all(test, not(feature = "pg_test")))]
mod tests {
    use serde_json::Value;

    #[test]
    fn parse_log_level_defaults_to_warn_for_unknown_values() {
        assert!(matches!(super::parse_log_level("something-else"), super::LogLevel::Warn));
    }

    #[test]
    fn parse_log_level_accepts_known_values() {
        assert!(matches!(super::parse_log_level("off"), super::LogLevel::Off));
        assert!(matches!(super::parse_log_level("ERROR"), super::LogLevel::Error));
        assert!(matches!(super::parse_log_level("warn"), super::LogLevel::Warn));
        assert!(matches!(super::parse_log_level("info"), super::LogLevel::Info));
        assert!(matches!(super::parse_log_level("debug"), super::LogLevel::Debug));
    }

    #[test]
    fn metrics_include_latency_and_error_class_counters() {
        let before = super::metrics_json();
        let before_compile_errors = metric_u64(&before, &["compile", "errors"]);
        let before_compile_diagnostics =
            metric_u64(&before, &["compile", "error_classes", "diagnostics"]);
        let before_execute_errors = metric_u64(&before, &["execute", "errors"]);
        let before_execute_js = metric_u64(&before, &["execute", "error_classes", "js_exception"]);
        let before_runtime_checkout_hits =
            metric_u64(&before, &["runtime", "readiness", "checkout_hits"]);
        let before_runtime_retired = metric_u64(&before, &["runtime", "readiness", "retired"]);
        let before_tsgo_init_calls = metric_u64(&before, &["tsgo_wasm", "init", "calls"]);
        let before_tsgo_manual_hits = metric_u64(&before, &["tsgo_wasm", "cache", "manual_hits"]);
        let before_tsgo_fallback =
            metric_u64(&before, &["tsgo_wasm", "cache", "fallback_compiles"]);

        let compile_start = super::record_compile_start();
        super::record_compile_error(compile_start, "diagnostics");
        let execute_start = super::record_execute_start();
        super::record_execute_error(execute_start, "js_exception");
        super::record_runtime_checkout_hit(17);
        super::record_runtime_setup_realm(9);
        super::record_runtime_cold_shell_create();
        super::record_runtime_warm_shell_reuse();
        super::record_runtime_retire("termination");
        let tsgo_init_start = super::record_tsgo_wasm_init_start();
        super::record_tsgo_wasm_cache_event("manual_hit");
        super::record_tsgo_wasm_cache_event("fallback_compile");
        super::record_tsgo_wasm_init_success(tsgo_init_start);

        let after = super::metrics_json();
        assert!(metric_u64(&after, &["compile", "errors"]) > before_compile_errors);
        assert!(
            metric_u64(&after, &["compile", "error_classes", "diagnostics"])
                > before_compile_diagnostics
        );
        assert!(metric_u64(&after, &["execute", "errors"]) > before_execute_errors);
        assert!(
            metric_u64(&after, &["execute", "error_classes", "js_exception"]) > before_execute_js
        );
        assert!(
            metric_u64(&after, &["runtime", "readiness", "checkout_hits"])
                > before_runtime_checkout_hits
        );
        assert!(metric_u64(&after, &["runtime", "readiness", "retired"]) > before_runtime_retired);
        assert!(metric_u64(&after, &["tsgo_wasm", "init", "calls"]) > before_tsgo_init_calls);
        assert!(
            metric_u64(&after, &["tsgo_wasm", "cache", "manual_hits"]) > before_tsgo_manual_hits
        );
        assert!(
            metric_u64(&after, &["tsgo_wasm", "cache", "fallback_compiles"]) > before_tsgo_fallback
        );
        let _ = metric_u64(&after, &["compile", "latency_ms", "last"]);
        let _ = metric_u64(&after, &["execute", "latency_ms", "last"]);
        let _ = metric_u64(&after, &["runtime", "readiness", "checkout_last_us"]);
        let _ = metric_u64(&after, &["tsgo_wasm", "init", "latency_ms", "last"]);
    }

    fn metric_u64(root: &Value, path: &[&str]) -> u64 {
        path.iter()
            .fold(Some(root), |current, segment| current.and_then(|value| value.get(*segment)))
            .and_then(Value::as_u64)
            .expect("metrics field should be present and numeric")
    }
}
