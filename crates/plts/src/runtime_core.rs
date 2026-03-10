use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeExecError {
    stage: &'static str,
    message: String,
    stack: Option<String>,
}

impl RuntimeExecError {
    pub(crate) fn new(stage: &'static str, message: impl Into<String>) -> Self {
        Self { stage, message: message.into(), stack: None }
    }

    pub(crate) fn with_stack(
        stage: &'static str,
        message: impl Into<String>,
        stack: impl Into<Option<String>>,
    ) -> Self {
        Self { stage, message: message.into(), stack: stack.into() }
    }
}

impl fmt::Display for RuntimeExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stage={}; message={}", self.stage, self.message)?;
        if let Some(stack) = &self.stack {
            write!(f, "; stack={stack}")?;
        }
        Ok(())
    }
}

pub(crate) const INLINE_IMPORT_MAP_MARKER: &str = "plts-import-map:";
pub(crate) const STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT_NAME: &str = "plts_runtime_lockdown.js";
pub(crate) const STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT: &str =
    include_str!("runtime_lockdown.js");

pub(crate) fn parse_js_error_details(details: &str) -> (String, Option<String>) {
    let trimmed = details.trim();
    if let Some((first, rest)) = trimmed.split_once('\n') {
        let message = first.trim().to_string();
        let stack = rest.trim();
        if stack.is_empty() { (message, None) } else { (message, Some(stack.to_string())) }
    } else {
        (trimmed.to_string(), None)
    }
}

pub(crate) fn parse_inline_import_map(source: &str) -> HashMap<String, String> {
    let Some(marker_start) = source.find(INLINE_IMPORT_MAP_MARKER) else {
        return HashMap::new();
    };

    let mut cursor = marker_start + INLINE_IMPORT_MAP_MARKER.len();
    while source[cursor..].chars().next().is_some_and(char::is_whitespace) {
        cursor += source[cursor..].chars().next().map(char::len_utf8).unwrap_or(0);
    }

    if source[cursor..].chars().next() != Some('{') {
        return HashMap::new();
    }

    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut end = None;
    for (offset, ch) in source[cursor..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(cursor + offset + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }

    let Some(end) = end else {
        return HashMap::new();
    };

    serde_json::from_str::<HashMap<String, String>>(&source[cursor..end]).unwrap_or_default()
}

pub(crate) fn resolve_runtime_timeout_ms(
    statement_timeout_ms: Option<u64>,
    plts_max_runtime_ms: Option<u64>,
) -> Option<u64> {
    match (statement_timeout_ms, plts_max_runtime_ms) {
        (Some(statement_timeout), Some(runtime_cap)) => Some(statement_timeout.min(runtime_cap)),
        (Some(statement_timeout), None) => Some(statement_timeout),
        (None, Some(runtime_cap)) => Some(runtime_cap),
        (None, None) => None,
    }
}

pub(crate) fn parse_statement_timeout_ms(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "0" {
        return None;
    }

    let unit_start =
        trimmed.find(|ch: char| !(ch.is_ascii_digit() || ch == '.')).unwrap_or(trimmed.len());
    if unit_start == 0 {
        return None;
    }

    let magnitude = trimmed[..unit_start].trim().parse::<f64>().ok()?;
    if !magnitude.is_finite() || magnitude <= 0.0 {
        return None;
    }

    let unit = trimmed[unit_start..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
        "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hour" | "hours" => 3_600_000.0,
        "d" | "day" | "days" => 86_400_000.0,
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" => 0.001,
        _ => return None,
    };

    let timeout_ms = (magnitude * multiplier).ceil();
    if !timeout_ms.is_finite() || timeout_ms <= 0.0 {
        return None;
    }

    Some(timeout_ms as u64)
}

pub(crate) fn parse_runtime_heap_limit_bytes(raw: &str) -> Option<usize> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "0" {
        return None;
    }

    let unit_start =
        trimmed.find(|ch: char| !(ch.is_ascii_digit() || ch == '.')).unwrap_or(trimmed.len());
    if unit_start == 0 {
        return None;
    }

    let magnitude = trimmed[..unit_start].trim().parse::<f64>().ok()?;
    if !magnitude.is_finite() || magnitude <= 0.0 {
        return None;
    }

    let unit = trimmed[unit_start..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "m" | "mb" | "mib" | "megabyte" | "megabytes" => 1_048_576.0,
        "k" | "kb" | "kib" | "kilobyte" | "kilobytes" => 1_024.0,
        "g" | "gb" | "gib" | "gigabyte" | "gigabytes" => 1_073_741_824.0,
        "b" | "byte" | "bytes" => 1.0,
        _ => return None,
    };

    let bytes = (magnitude * multiplier).ceil();
    if !bytes.is_finite() || bytes <= 0.0 || bytes > usize::MAX as f64 {
        return None;
    }

    Some(bytes as usize)
}

pub(crate) fn interrupt_pending_from_flags(
    interrupt_pending: i32,
    query_cancel_pending: i32,
    proc_die_pending: i32,
) -> bool {
    interrupt_pending != 0 || query_cancel_pending != 0 || proc_die_pending != 0
}

pub(crate) fn static_bootstrap_scripts() -> [(&'static str, &'static str); 1] {
    [(STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT_NAME, STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT)]
}

pub(crate) fn build_dynamic_context_setup_script(
    context_json: &str,
    db_mode_js: &str,
    db_read_only_js: bool,
) -> Result<String, RuntimeExecError> {
    let encoded_context = serde_json::to_string(context_json).map_err(|e| {
        RuntimeExecError::new(
            "context encode",
            format!("failed to encode runtime context string: {e}"),
        )
    })?;

    let db_read_only_js = if db_read_only_js { "true" } else { "false" };
    Ok(format!(
        "globalThis.__plts_ctx = JSON.parse({});\
         globalThis.__plts_ctx.db = {{\
           mode: '{}',\
           query(input, params) {{\
             return globalThis.__plts_internal_ops.dbQuery(input, params, {}, arguments.length > 1);\
           }},\
           exec(input, params) {{\
             return globalThis.__plts_internal_ops.dbExec(input, params, {}, arguments.length > 1);\
           }}\
          }};",
        encoded_context, db_mode_js, db_read_only_js, db_read_only_js
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeExecError, build_dynamic_context_setup_script, interrupt_pending_from_flags,
        parse_inline_import_map, parse_js_error_details, parse_runtime_heap_limit_bytes,
        parse_statement_timeout_ms, resolve_runtime_timeout_ms, static_bootstrap_scripts,
    };

    #[test]
    fn parse_js_error_details_with_stack() {
        let details = "Uncaught Error: boom\n    at default (plts_module.js:1:1)\n    at foo";
        let (message, stack) = parse_js_error_details(details);
        assert_eq!(message, "Uncaught Error: boom");
        assert_eq!(stack.as_deref(), Some("at default (plts_module.js:1:1)\n    at foo"));
    }

    #[test]
    fn runtime_exec_error_display() {
        let err = RuntimeExecError::with_stack(
            "entrypoint invocation",
            "Uncaught Error: boom",
            Some("at default (plts_module.js:1:1)".to_string()),
        );
        let rendered = err.to_string();
        assert!(rendered.contains("stage=entrypoint invocation"));
        assert!(rendered.contains("message=Uncaught Error: boom"));
        assert!(rendered.contains("stack=at default"));
    }

    #[test]
    fn parse_inline_import_map_extracts_json_object_after_marker() {
        let source = r#"
            // plts-import-map: {"@app/math":"sha256:abc","@app/time":"data:text/javascript,export const now=1;"}
            import { now } from "@app/time";
            export default () => now;
        "#;

        let import_map = parse_inline_import_map(source);
        assert_eq!(import_map.get("@app/math").map(String::as_str), Some("sha256:abc"));
        assert_eq!(
            import_map.get("@app/time").map(String::as_str),
            Some("data:text/javascript,export const now=1;")
        );
    }

    #[test]
    fn parse_inline_import_map_returns_empty_when_marker_payload_is_invalid_json() {
        let source = r#"
            // plts-import-map: {"@app/math":
            import { now } from "@app/math";
            export default () => now;
        "#;

        assert!(parse_inline_import_map(source).is_empty());
    }

    #[test]
    fn runtime_static_bootstrap_script_stays_invocation_agnostic() {
        let scripts = static_bootstrap_scripts();
        assert!(!scripts.is_empty());
        for (_name, source) in scripts {
            assert!(!source.contains("__plts_ctx"));
            assert!(!source.contains("ctx.fn"));
            assert!(!source.contains("ctx.args"));
        }
    }

    #[test]
    fn runtime_dynamic_context_setup_script_encodes_invocation_specific_data() {
        let script = build_dynamic_context_setup_script(r#"{"id":7}"#, "ro", true)
            .expect("dynamic context setup script should build");
        assert!(script.contains("__plts_ctx"));
        assert!(script.contains("mode: 'ro'"));
        assert!(script.contains("dbQuery"));
        assert!(script.contains("dbExec"));
    }

    #[test]
    fn parse_statement_timeout_ms_parses_common_postgres_units() {
        assert_eq!(parse_statement_timeout_ms("0"), None);
        assert_eq!(parse_statement_timeout_ms("250"), Some(250));
        assert_eq!(parse_statement_timeout_ms("250ms"), Some(250));
        assert_eq!(parse_statement_timeout_ms("2s"), Some(2_000));
        assert_eq!(parse_statement_timeout_ms("1min"), Some(60_000));
        assert_eq!(parse_statement_timeout_ms("1.5s"), Some(1_500));
        assert_eq!(parse_statement_timeout_ms("500us"), Some(1));
    }

    #[test]
    fn parse_statement_timeout_ms_rejects_invalid_values() {
        assert_eq!(parse_statement_timeout_ms(""), None);
        assert_eq!(parse_statement_timeout_ms("off"), None);
        assert_eq!(parse_statement_timeout_ms("-5ms"), None);
        assert_eq!(parse_statement_timeout_ms("12fortnights"), None);
    }

    #[test]
    fn parse_runtime_heap_limit_bytes_parses_expected_units() {
        assert_eq!(parse_runtime_heap_limit_bytes("0"), None);
        assert_eq!(parse_runtime_heap_limit_bytes("32"), Some(32 * 1024 * 1024));
        assert_eq!(parse_runtime_heap_limit_bytes("32mb"), Some(32 * 1024 * 1024));
        assert_eq!(parse_runtime_heap_limit_bytes("1.5mb"), Some(1_572_864));
        assert_eq!(parse_runtime_heap_limit_bytes("512kb"), Some(524_288));
        assert_eq!(parse_runtime_heap_limit_bytes("2gb"), Some(2_147_483_648));
        assert_eq!(parse_runtime_heap_limit_bytes("2048 bytes"), Some(2048));
    }

    #[test]
    fn parse_runtime_heap_limit_bytes_rejects_invalid_values() {
        assert_eq!(parse_runtime_heap_limit_bytes(""), None);
        assert_eq!(parse_runtime_heap_limit_bytes("off"), None);
        assert_eq!(parse_runtime_heap_limit_bytes("-1mb"), None);
        assert_eq!(parse_runtime_heap_limit_bytes("12fortnights"), None);
    }

    #[test]
    fn resolve_runtime_timeout_ms_prefers_most_restrictive_limit() {
        assert_eq!(resolve_runtime_timeout_ms(None, None), None);
        assert_eq!(resolve_runtime_timeout_ms(Some(1_000), None), Some(1_000));
        assert_eq!(resolve_runtime_timeout_ms(None, Some(750)), Some(750));
        assert_eq!(resolve_runtime_timeout_ms(Some(2_000), Some(750)), Some(750));
        assert_eq!(resolve_runtime_timeout_ms(Some(500), Some(3_000)), Some(500));
    }

    #[test]
    fn interrupt_pending_from_flags_detects_pending_signal() {
        assert!(!interrupt_pending_from_flags(0, 0, 0));
        assert!(interrupt_pending_from_flags(1, 0, 0));
        assert!(interrupt_pending_from_flags(0, 1, 0));
        assert!(interrupt_pending_from_flags(0, 0, 1));
    }
}
