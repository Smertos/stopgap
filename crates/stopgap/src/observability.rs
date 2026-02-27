use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

static DEPLOY_CALLS: AtomicU64 = AtomicU64::new(0);
static DEPLOY_ERRORS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_CALLS: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_ERRORS: AtomicU64 = AtomicU64::new(0);
static DIFF_CALLS: AtomicU64 = AtomicU64::new(0);
static DIFF_ERRORS: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn record_deploy_start() {
    DEPLOY_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_deploy_error() {
    DEPLOY_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_rollback_start() {
    ROLLBACK_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_rollback_error() {
    ROLLBACK_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_diff_start() {
    DIFF_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_diff_error() {
    DIFF_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn metrics_json() -> Value {
    json!({
        "deploy": {
            "calls": DEPLOY_CALLS.load(Ordering::Relaxed),
            "errors": DEPLOY_ERRORS.load(Ordering::Relaxed)
        },
        "rollback": {
            "calls": ROLLBACK_CALLS.load(Ordering::Relaxed),
            "errors": ROLLBACK_ERRORS.load(Ordering::Relaxed)
        },
        "diff": {
            "calls": DIFF_CALLS.load(Ordering::Relaxed),
            "errors": DIFF_ERRORS.load(Ordering::Relaxed)
        }
    })
}

#[cfg(test)]
mod tests {
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
}
