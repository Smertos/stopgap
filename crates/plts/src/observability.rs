use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

static COMPILE_CALLS: AtomicU64 = AtomicU64::new(0);
static COMPILE_ERRORS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_CALLS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_ERRORS: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn record_compile_start() {
    COMPILE_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_compile_error() {
    COMPILE_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_execute_start() {
    EXECUTE_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_execute_error() {
    EXECUTE_ERRORS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn metrics_json() -> Value {
    json!({
        "compile": {
            "calls": COMPILE_CALLS.load(Ordering::Relaxed),
            "errors": COMPILE_ERRORS.load(Ordering::Relaxed)
        },
        "execute": {
            "calls": EXECUTE_CALLS.load(Ordering::Relaxed),
            "errors": EXECUTE_ERRORS.load(Ordering::Relaxed)
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
