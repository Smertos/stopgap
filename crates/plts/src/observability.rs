use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
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

fn current_log_level() -> LogLevel {
    let raw = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('plts.log_level', true), 'warn')::text",
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| "warn".to_string());
    parse_log_level(raw.as_str())
}

pub(crate) fn log_info(message: &str) {
    if current_log_level() >= LogLevel::Info {
        info!("{message}");
    }
}

pub(crate) fn log_warn(message: &str) {
    if current_log_level() >= LogLevel::Warn {
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

#[cfg(test)]
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

        let compile_start = super::record_compile_start();
        super::record_compile_error(compile_start, "diagnostics");
        let execute_start = super::record_execute_start();
        super::record_execute_error(execute_start, "js_exception");

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
        let _ = metric_u64(&after, &["compile", "latency_ms", "last"]);
        let _ = metric_u64(&after, &["execute", "latency_ms", "last"]);
    }

    fn metric_u64(root: &Value, path: &[&str]) -> u64 {
        path.iter()
            .fold(Some(root), |current, segment| current.and_then(|value| value.get(*segment)))
            .and_then(Value::as_u64)
            .expect("metrics field should be present and numeric")
    }
}
