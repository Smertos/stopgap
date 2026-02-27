use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static DEPLOY_CALLS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERRORS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERROR_PERMISSION: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERROR_VALIDATION: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERROR_STATE: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERROR_SQL: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERROR_UNKNOWN: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_CALLS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERRORS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERROR_PERMISSION: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERROR_VALIDATION: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERROR_STATE: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERROR_SQL: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERROR_UNKNOWN: AtomicU64 = AtomicU64::new(0);
static DIFF_CALLS: AtomicU64 = AtomicU64::new(0);
static DIFF_ERRORS: AtomicU64 = AtomicU64::new(0);
static DIFF_LATENCY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static DIFF_LATENCY_LAST_MS: AtomicU64 = AtomicU64::new(0);
static DIFF_LATENCY_MAX_MS: AtomicU64 = AtomicU64::new(0);
static DIFF_ERROR_PERMISSION: AtomicU64 = AtomicU64::new(0);
static DIFF_ERROR_VALIDATION: AtomicU64 = AtomicU64::new(0);
static DIFF_ERROR_STATE: AtomicU64 = AtomicU64::new(0);
static DIFF_ERROR_SQL: AtomicU64 = AtomicU64::new(0);
static DIFF_ERROR_UNKNOWN: AtomicU64 = AtomicU64::new(0);

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
        "SELECT COALESCE(current_setting('stopgap.log_level', true), 'warn')::text",
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

pub(crate) fn record_deploy_start() -> Instant {
    DEPLOY_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_deploy_success(started_at: Instant) {
    record_latency(
        started_at,
        &DEPLOY_LATENCY_TOTAL_MS,
        &DEPLOY_LATENCY_LAST_MS,
        &DEPLOY_LATENCY_MAX_MS,
    );
}

pub(crate) fn record_deploy_error(started_at: Instant, class: &str) {
    DEPLOY_ERRORS.fetch_add(1, Ordering::Relaxed);
    increment_error_class(
        class,
        &DEPLOY_ERROR_PERMISSION,
        &DEPLOY_ERROR_VALIDATION,
        &DEPLOY_ERROR_STATE,
        &DEPLOY_ERROR_SQL,
        &DEPLOY_ERROR_UNKNOWN,
    );
    record_deploy_success(started_at);
}

pub(crate) fn record_rollback_start() -> Instant {
    ROLLBACK_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_rollback_success(started_at: Instant) {
    record_latency(
        started_at,
        &ROLLBACK_LATENCY_TOTAL_MS,
        &ROLLBACK_LATENCY_LAST_MS,
        &ROLLBACK_LATENCY_MAX_MS,
    );
}

pub(crate) fn record_rollback_error(started_at: Instant, class: &str) {
    ROLLBACK_ERRORS.fetch_add(1, Ordering::Relaxed);
    increment_error_class(
        class,
        &ROLLBACK_ERROR_PERMISSION,
        &ROLLBACK_ERROR_VALIDATION,
        &ROLLBACK_ERROR_STATE,
        &ROLLBACK_ERROR_SQL,
        &ROLLBACK_ERROR_UNKNOWN,
    );
    record_rollback_success(started_at);
}

pub(crate) fn record_diff_start() -> Instant {
    DIFF_CALLS.fetch_add(1, Ordering::Relaxed);
    Instant::now()
}

pub(crate) fn record_diff_success(started_at: Instant) {
    record_latency(started_at, &DIFF_LATENCY_TOTAL_MS, &DIFF_LATENCY_LAST_MS, &DIFF_LATENCY_MAX_MS);
}

pub(crate) fn record_diff_error(started_at: Instant, class: &str) {
    DIFF_ERRORS.fetch_add(1, Ordering::Relaxed);
    increment_error_class(
        class,
        &DIFF_ERROR_PERMISSION,
        &DIFF_ERROR_VALIDATION,
        &DIFF_ERROR_STATE,
        &DIFF_ERROR_SQL,
        &DIFF_ERROR_UNKNOWN,
    );
    record_diff_success(started_at);
}

pub(crate) fn classify_operation_error(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("permission") || lowered.contains("must be") {
        "permission"
    } else if lowered.contains("not found")
        || lowered.contains("does not exist")
        || lowered.contains("invalid")
        || lowered.contains("must be positive")
    {
        "validation"
    } else if lowered.contains("status") || lowered.contains("already active") {
        "state"
    } else if lowered.contains("sql") || lowered.contains("spi") || lowered.contains("query") {
        "sql"
    } else {
        "unknown"
    }
}

pub(crate) fn metrics_json() -> Value {
    json!({
        "deploy": {
            "calls": DEPLOY_CALLS.load(Ordering::Relaxed),
            "errors": DEPLOY_ERRORS.load(Ordering::Relaxed),
            "latency_ms": {
                "total": DEPLOY_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                "last": DEPLOY_LATENCY_LAST_MS.load(Ordering::Relaxed),
                "max": DEPLOY_LATENCY_MAX_MS.load(Ordering::Relaxed)
            },
            "error_classes": {
                "permission": DEPLOY_ERROR_PERMISSION.load(Ordering::Relaxed),
                "validation": DEPLOY_ERROR_VALIDATION.load(Ordering::Relaxed),
                "state": DEPLOY_ERROR_STATE.load(Ordering::Relaxed),
                "sql": DEPLOY_ERROR_SQL.load(Ordering::Relaxed),
                "unknown": DEPLOY_ERROR_UNKNOWN.load(Ordering::Relaxed)
            }
        },
        "rollback": {
            "calls": ROLLBACK_CALLS.load(Ordering::Relaxed),
            "errors": ROLLBACK_ERRORS.load(Ordering::Relaxed),
            "latency_ms": {
                "total": ROLLBACK_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                "last": ROLLBACK_LATENCY_LAST_MS.load(Ordering::Relaxed),
                "max": ROLLBACK_LATENCY_MAX_MS.load(Ordering::Relaxed)
            },
            "error_classes": {
                "permission": ROLLBACK_ERROR_PERMISSION.load(Ordering::Relaxed),
                "validation": ROLLBACK_ERROR_VALIDATION.load(Ordering::Relaxed),
                "state": ROLLBACK_ERROR_STATE.load(Ordering::Relaxed),
                "sql": ROLLBACK_ERROR_SQL.load(Ordering::Relaxed),
                "unknown": ROLLBACK_ERROR_UNKNOWN.load(Ordering::Relaxed)
            }
        },
        "diff": {
            "calls": DIFF_CALLS.load(Ordering::Relaxed),
            "errors": DIFF_ERRORS.load(Ordering::Relaxed),
            "latency_ms": {
                "total": DIFF_LATENCY_TOTAL_MS.load(Ordering::Relaxed),
                "last": DIFF_LATENCY_LAST_MS.load(Ordering::Relaxed),
                "max": DIFF_LATENCY_MAX_MS.load(Ordering::Relaxed)
            },
            "error_classes": {
                "permission": DIFF_ERROR_PERMISSION.load(Ordering::Relaxed),
                "validation": DIFF_ERROR_VALIDATION.load(Ordering::Relaxed),
                "state": DIFF_ERROR_STATE.load(Ordering::Relaxed),
                "sql": DIFF_ERROR_SQL.load(Ordering::Relaxed),
                "unknown": DIFF_ERROR_UNKNOWN.load(Ordering::Relaxed)
            }
        }
    })
}

fn increment_error_class(
    class: &str,
    permission: &AtomicU64,
    validation: &AtomicU64,
    state: &AtomicU64,
    sql: &AtomicU64,
    unknown: &AtomicU64,
) {
    match class {
        "permission" => {
            permission.fetch_add(1, Ordering::Relaxed);
        }
        "validation" => {
            validation.fetch_add(1, Ordering::Relaxed);
        }
        "state" => {
            state.fetch_add(1, Ordering::Relaxed);
        }
        "sql" => {
            sql.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            unknown.fetch_add(1, Ordering::Relaxed);
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
    fn metrics_include_latency_and_error_classes_for_all_operations() {
        let before = super::metrics_json();
        let before_deploy_errors = metric_u64(&before, &["deploy", "errors"]);
        let before_deploy_validation =
            metric_u64(&before, &["deploy", "error_classes", "validation"]);
        let before_rollback_errors = metric_u64(&before, &["rollback", "errors"]);
        let before_rollback_state = metric_u64(&before, &["rollback", "error_classes", "state"]);
        let before_diff_errors = metric_u64(&before, &["diff", "errors"]);
        let before_diff_sql = metric_u64(&before, &["diff", "error_classes", "sql"]);

        let deploy_start = super::record_deploy_start();
        super::record_deploy_error(deploy_start, "validation");
        let rollback_start = super::record_rollback_start();
        super::record_rollback_error(rollback_start, "state");
        let diff_start = super::record_diff_start();
        super::record_diff_error(diff_start, "sql");

        let after = super::metrics_json();
        assert!(metric_u64(&after, &["deploy", "errors"]) > before_deploy_errors);
        assert!(
            metric_u64(&after, &["deploy", "error_classes", "validation"])
                > before_deploy_validation
        );
        assert!(metric_u64(&after, &["rollback", "errors"]) > before_rollback_errors);
        assert!(
            metric_u64(&after, &["rollback", "error_classes", "state"]) > before_rollback_state
        );
        assert!(metric_u64(&after, &["diff", "errors"]) > before_diff_errors);
        assert!(metric_u64(&after, &["diff", "error_classes", "sql"]) > before_diff_sql);
        let _ = metric_u64(&after, &["deploy", "latency_ms", "last"]);
        let _ = metric_u64(&after, &["rollback", "latency_ms", "last"]);
        let _ = metric_u64(&after, &["diff", "latency_ms", "last"]);
    }

    fn metric_u64(root: &Value, path: &[&str]) -> u64 {
        path.iter()
            .fold(Some(root), |current, segment| current.and_then(|value| value.get(*segment)))
            .and_then(Value::as_u64)
            .expect("metrics field should be present and numeric")
    }
}
