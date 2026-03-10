use crate::function_program::FunctionProgram;
#[cfg(feature = "v8_runtime")]
use crate::function_program::load_compiled_artifact_source;
#[cfg(feature = "v8_runtime")]
use crate::isolate_pool::{CheckedOut, IsolatePool, IsolatePoolConfig, RetireReason, ShellHealth};
#[cfg(feature = "v8_runtime")]
use crate::observability::{
    record_runtime_checkout_hit, record_runtime_checkout_miss, record_runtime_cold_shell_create,
    record_runtime_retire, record_runtime_setup_realm, record_runtime_warm_shell_reuse,
};
#[cfg(feature = "v8_runtime")]
use crate::runtime_spi::{exec_sql_with_params, query_json_rows_with_params};
#[cfg(feature = "v8_runtime")]
use crate::{
    isolate_max_age_seconds, isolate_max_invocations, isolate_pool_size, isolate_reuse_enabled,
};
#[cfg(feature = "v8_runtime")]
use base64::Engine;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
#[cfg(feature = "v8_runtime")]
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
#[cfg(feature = "v8_runtime")]
use std::rc::Rc;
#[cfg(feature = "v8_runtime")]
use std::sync::Arc;
#[cfg(feature = "v8_runtime")]
use std::sync::OnceLock;
#[cfg(feature = "v8_runtime")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "v8_runtime")]
use std::thread::{self, JoinHandle};
#[cfg(feature = "v8_runtime")]
use std::time::{Duration, Instant};

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

    #[cfg(any(test, feature = "v8_runtime"))]
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

pub(crate) fn format_runtime_error_for_sql(
    program: &FunctionProgram,
    err: &RuntimeExecError,
) -> String {
    format!(
        "plts runtime error for {}.{} (oid={}): {}; sql_context={{schema={}, name={}, oid={}}}",
        program.schema, program.name, program.oid, err, program.schema, program.name, program.oid
    )
}

#[cfg(any(test, feature = "v8_runtime"))]
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

pub(crate) fn build_runtime_context(program: &FunctionProgram, args_payload: &Value) -> Value {
    json!({
        "db": {
            "mode": "rw",
            "api": ["query", "exec"]
        },
        "args": args_payload,
        "fn": {
            "oid": program.oid.to_u32(),
            "name": program.name,
            "schema": program.schema
        },
        "now": current_timestamp_text()
    })
}

fn current_timestamp_text() -> String {
    Spi::get_one::<String>("SELECT now()::text").ok().flatten().unwrap_or_default()
}

#[cfg(any(test, feature = "v8_runtime"))]
const INLINE_IMPORT_MAP_MARKER: &str = "plts-import-map:";

#[cfg(any(test, feature = "v8_runtime"))]
fn parse_inline_import_map(source: &str) -> HashMap<String, String> {
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

#[cfg(feature = "v8_runtime")]
#[derive(Default)]
struct PltsModuleLoaderState {
    bare_specifier_map: HashMap<String, String>,
}

#[cfg(feature = "v8_runtime")]
#[derive(Clone)]
struct PltsModuleLoader {
    state: Rc<RefCell<PltsModuleLoaderState>>,
}

#[cfg(feature = "v8_runtime")]
fn is_bare_module_specifier(specifier: &str) -> bool {
    !specifier.starts_with("./")
        && !specifier.starts_with("../")
        && !specifier.starts_with('/')
        && !specifier.contains(':')
}

#[cfg(feature = "v8_runtime")]
fn resolve_inline_import_map_target(
    target: &str,
) -> Result<deno_core::ModuleSpecifier, deno_core::error::ModuleLoaderError> {
    if let Ok(specifier) = deno_core::ModuleSpecifier::parse(target) {
        return Ok(specifier);
    }

    if target.starts_with("sha256:") {
        let specifier = format!("plts+artifact:{target}");
        return Ok(deno_core::ModuleSpecifier::parse(&specifier)
            .map_err(deno_error::JsErrorBox::from_err)?);
    }

    Err(deno_error::JsErrorBox::generic(format!(
        "invalid inline import map target `{target}`; expected absolute module specifier or artifact hash"
    ))
    .into())
}

#[cfg(feature = "v8_runtime")]
impl deno_core::ModuleLoader for PltsModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: deno_core::ResolutionKind,
    ) -> Result<deno_core::ModuleSpecifier, deno_core::error::ModuleLoaderError> {
        if specifier == "@stopgap/runtime" {
            return Ok(deno_core::ModuleSpecifier::parse("file:///plts/__stopgap_runtime__.js")
                .map_err(deno_error::JsErrorBox::from_err)?);
        }

        if is_bare_module_specifier(specifier) {
            if let Some(target) = self.state.borrow().bare_specifier_map.get(specifier) {
                return resolve_inline_import_map_target(target);
            }

            return Err(deno_error::JsErrorBox::generic(format!(
                "unsupported bare module import `{specifier}`; add an inline import map comment like `// {INLINE_IMPORT_MAP_MARKER} {{\"{specifier}\":\"plts+artifact:sha256:...\"}}`"
            ))
            .into());
        }

        Ok(deno_core::resolve_import(specifier, referrer)
            .map_err(deno_error::JsErrorBox::from_err)?)
    }

    fn load(
        &self,
        module_specifier: &deno_core::ModuleSpecifier,
        _maybe_referrer: Option<&deno_core::ModuleSpecifier>,
        _is_dyn_import: bool,
        _requested_module_type: deno_core::RequestedModuleType,
    ) -> deno_core::ModuleLoadResponse {
        deno_core::ModuleLoadResponse::Sync(load_module_source(module_specifier))
    }
}

#[cfg(feature = "v8_runtime")]
fn load_module_source(
    module_specifier: &deno_core::ModuleSpecifier,
) -> Result<deno_core::ModuleSource, deno_core::error::ModuleLoaderError> {
    use deno_core::{ModuleSource, ModuleSourceCode, ModuleType};

    let stripped_specifier = strip_invocation_suffix(module_specifier.as_str());
    match deno_core::ModuleSpecifier::parse(stripped_specifier)
        .map_err(deno_error::JsErrorBox::from_err)?
        .scheme()
    {
        "plts+artifact" => {
            let artifact_hash = parse_artifact_module_hash(module_specifier)?;
            let source = load_compiled_artifact_source(&artifact_hash).ok_or_else(|| {
                deno_error::JsErrorBox::generic(format!(
                    "artifact module `{}` could not be loaded: artifact `{}` not found",
                    module_specifier, artifact_hash
                ))
            })?;
            let source = invocation_nonce_from_specifier(module_specifier.as_str())
                .map(|nonce| version_source_module_literals(source.as_str(), nonce))
                .unwrap_or(source);
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(source.into()),
                module_specifier,
                None,
            ))
        }
        "data" => {
            let source = decode_data_url_module_code(module_specifier)?;
            let source = invocation_nonce_from_specifier(module_specifier.as_str())
                .map(|nonce| version_source_module_literals(source.as_str(), nonce))
                .unwrap_or(source);
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(source.into()),
                module_specifier,
                None,
            ))
        }
        "file" if stripped_specifier == "file:///plts/__stopgap_runtime__.js" => Ok(
            ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(
                    include_str!("../../../packages/runtime/dist/embedded_runtime.js")
                        .to_string()
                        .into(),
                ),
                module_specifier,
                None,
            ),
        ),
        _ => Err(deno_error::JsErrorBox::generic(format!(
            "unsupported module import `{}`; allowed imports are `data:`, `plts+artifact:<hash>`, and `@stopgap/runtime`",
            module_specifier
        ))
        .into()),
    }
}

#[cfg(feature = "v8_runtime")]
fn parse_artifact_module_hash(
    module_specifier: &deno_core::ModuleSpecifier,
) -> Result<String, deno_core::error::ModuleLoaderError> {
    let raw = strip_invocation_suffix(module_specifier.as_str());
    let raw_hash = raw.strip_prefix("plts+artifact:").ok_or_else(|| {
        deno_error::JsErrorBox::generic(format!(
            "invalid artifact module specifier `{module_specifier}`"
        ))
    })?;

    let artifact_hash = raw_hash.trim_start_matches('/').trim();
    if artifact_hash.is_empty() {
        return Err(deno_error::JsErrorBox::generic(format!(
            "invalid artifact module specifier `{module_specifier}`: artifact hash is required"
        ))
        .into());
    }

    Ok(artifact_hash.to_string())
}

#[cfg(feature = "v8_runtime")]
fn decode_data_url_module_code(
    module_specifier: &deno_core::ModuleSpecifier,
) -> Result<String, deno_core::error::ModuleLoaderError> {
    let raw = strip_invocation_suffix(module_specifier.as_str());
    let payload = raw.strip_prefix("data:").ok_or_else(|| {
        deno_error::JsErrorBox::generic(format!(
            "module specifier `{module_specifier}` is not a data URL"
        ))
    })?;

    let (metadata, encoded) = payload.split_once(',').ok_or_else(|| {
        deno_error::JsErrorBox::generic(format!(
            "invalid data URL module specifier `{module_specifier}`"
        ))
    })?;

    if metadata.contains(";base64") {
        let decoded = base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|err| {
            deno_error::JsErrorBox::generic(format!(
                "failed to decode base64 data URL module `{module_specifier}`: {err}"
            ))
        })?;
        Ok(String::from_utf8(decoded).map_err(|err| {
            deno_error::JsErrorBox::generic(format!(
                "data URL module `{module_specifier}` is not valid UTF-8: {err}"
            ))
        })?)
    } else {
        Ok(encoded.to_string())
    }
}

#[cfg(feature = "v8_runtime")]
fn current_setting_text(name: &str) -> Option<String> {
    let sql = match name {
        "statement_timeout" => "SELECT current_setting('statement_timeout', true)",
        "plts.max_runtime_ms" => "SELECT current_setting('plts.max_runtime_ms', true)",
        "plts.max_heap_mb" => "SELECT current_setting('plts.max_heap_mb', true)",
        _ => return None,
    };
    Spi::get_one::<String>(&sql).ok().flatten().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
    })
}

#[cfg(feature = "v8_runtime")]
fn current_statement_timeout_ms() -> Option<u64> {
    current_setting_text("statement_timeout")
        .and_then(|raw| parse_statement_timeout_ms(raw.as_str()))
}

#[cfg(feature = "v8_runtime")]
fn current_plts_max_runtime_ms() -> Option<u64> {
    current_setting_text("plts.max_runtime_ms")
        .and_then(|raw| parse_statement_timeout_ms(raw.as_str()))
}

#[cfg(feature = "v8_runtime")]
fn current_plts_max_heap_setting() -> Option<String> {
    current_setting_text("plts.max_heap_mb")
}

#[cfg_attr(not(any(test, feature = "v8_runtime")), allow(dead_code))]
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

#[cfg_attr(not(any(test, feature = "v8_runtime")), allow(dead_code))]
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

#[cfg_attr(not(any(test, feature = "v8_runtime")), allow(dead_code))]
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

#[cfg(feature = "v8_runtime")]
struct RuntimeInterruptGuard {
    cancel: Arc<AtomicBool>,
    timed_out: Arc<AtomicBool>,
    interrupted: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

#[cfg(feature = "v8_runtime")]
impl RuntimeInterruptGuard {
    fn with_statement_timeout(
        runtime: &mut deno_core::JsRuntime,
        timeout_ms: Option<u64>,
    ) -> Option<Self> {
        let timeout_ms = timeout_ms.filter(|value| *value > 0)?;
        let cancel = Arc::new(AtomicBool::new(false));
        let timed_out = Arc::new(AtomicBool::new(false));
        let interrupted = Arc::new(AtomicBool::new(false));
        let cancel_worker = Arc::clone(&cancel);
        let timed_out_worker = Arc::clone(&timed_out);
        let interrupted_worker = Arc::clone(&interrupted);
        let isolate_handle = runtime.v8_isolate().thread_safe_handle();
        let timeout = Duration::from_millis(timeout_ms);

        let worker = thread::spawn(move || {
            let start = Instant::now();
            loop {
                if cancel_worker.load(Ordering::Relaxed) {
                    return;
                }

                if postgres_interrupt_pending() {
                    interrupted_worker.store(true, Ordering::Relaxed);
                    isolate_handle.terminate_execution();
                    return;
                }

                if start.elapsed() >= timeout {
                    timed_out_worker.store(true, Ordering::Relaxed);
                    isolate_handle.terminate_execution();
                    return;
                }

                thread::sleep(Duration::from_millis(5));
            }
        });

        Some(Self { cancel, timed_out, interrupted, worker: Some(worker) })
    }

    fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Relaxed)
    }

    fn interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "v8_runtime")]
fn postgres_interrupt_pending() -> bool {
    unsafe {
        interrupt_pending_from_flags(
            pg_sys::InterruptPending,
            pg_sys::QueryCancelPending,
            pg_sys::ProcDiePending,
        )
    }
}

#[cfg_attr(not(any(test, feature = "v8_runtime")), allow(dead_code))]
pub(crate) fn interrupt_pending_from_flags(
    interrupt_pending: i32,
    query_cancel_pending: i32,
    proc_die_pending: i32,
) -> bool {
    interrupt_pending != 0 || query_cancel_pending != 0 || proc_die_pending != 0
}

#[cfg(feature = "v8_runtime")]
impl Drop for RuntimeInterruptGuard {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(feature = "v8_runtime")]
#[deno_core::op2]
#[serde]
fn op_plts_db_query(
    #[string] sql: String,
    #[serde] params: Vec<serde_json::Value>,
    read_only: bool,
) -> Result<serde_json::Value, deno_error::JsErrorBox> {
    query_json_rows_with_params(&sql, params, read_only).map_err(deno_error::JsErrorBox::generic)
}

#[cfg(feature = "v8_runtime")]
#[deno_core::op2]
#[serde]
fn op_plts_db_exec(
    #[string] sql: String,
    #[serde] params: Vec<serde_json::Value>,
    read_only: bool,
) -> Result<serde_json::Value, deno_error::JsErrorBox> {
    exec_sql_with_params(&sql, params, read_only).map_err(deno_error::JsErrorBox::generic)
}

#[cfg(feature = "v8_runtime")]
deno_core::extension!(plts_runtime_ext, ops = [op_plts_db_query, op_plts_db_exec]);

#[cfg(any(test, feature = "v8_runtime"))]
const STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT_NAME: &str = "plts_runtime_lockdown.js";

#[cfg(any(test, feature = "v8_runtime"))]
const STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT: &str = include_str!("runtime_lockdown.js");

#[cfg(any(test, feature = "v8_runtime"))]
fn static_bootstrap_scripts() -> [(&'static str, &'static str); 1] {
    [(STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT_NAME, STATIC_BOOTSTRAP_RUNTIME_LOCKDOWN_SCRIPT)]
}

#[cfg(any(test, feature = "v8_runtime"))]
fn build_dynamic_context_setup_script(
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

#[cfg(feature = "v8_runtime")]
static RUNTIME_STARTUP_SNAPSHOT: OnceLock<Option<&'static [u8]>> = OnceLock::new();

#[cfg(feature = "v8_runtime")]
fn build_runtime_startup_snapshot() -> Option<&'static [u8]> {
    use deno_core::{JsRuntimeForSnapshot, RuntimeOptions};

    let mut runtime = JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init_ops()],
        skip_op_registration: true,
        ..Default::default()
    });

    for (name, source) in static_bootstrap_scripts() {
        if let Err(err) = runtime.execute_script(name, source) {
            warning!(
                "plts runtime static bootstrap snapshot preparation failed at `{}`: {}",
                name,
                err
            );
            return None;
        }
    }

    Some(Box::leak(runtime.snapshot()))
}

#[cfg(feature = "v8_runtime")]
fn runtime_startup_snapshot() -> Option<&'static [u8]> {
    *RUNTIME_STARTUP_SNAPSHOT.get_or_init(build_runtime_startup_snapshot)
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn bootstrap_v8_isolate() {
    if runtime_startup_snapshot().is_none() {
        let _runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions::default());
    }
}

#[cfg(not(feature = "v8_runtime"))]
pub(crate) fn bootstrap_v8_isolate() {}

#[cfg(feature = "v8_runtime")]
pub(crate) fn runtime_available() -> bool {
    true
}

#[cfg(not(feature = "v8_runtime"))]
pub(crate) fn runtime_available() -> bool {
    false
}

#[cfg(feature = "v8_runtime")]
struct RuntimeShell {
    runtime: deno_core::JsRuntime,
    loader_state: Rc<RefCell<PltsModuleLoaderState>>,
    baseline_globals_json: String,
    heap_limit_setting: Option<String>,
    heap_limit_reached: Arc<AtomicBool>,
    invocation_nonce: u64,
}

#[cfg(feature = "v8_runtime")]
struct RuntimeShellGuard {
    checked_out: Option<CheckedOut<RuntimeShell>>,
    health: ShellHealth,
}

#[cfg(feature = "v8_runtime")]
impl RuntimeShellGuard {
    fn new(shell: RuntimeShell) -> Self {
        Self { checked_out: Some(CheckedOut::fresh(shell)), health: ShellHealth::default() }
    }

    fn from_checked_out(checked_out: CheckedOut<RuntimeShell>) -> Self {
        Self { checked_out: Some(checked_out), health: ShellHealth::default() }
    }

    fn shell_mut(&mut self) -> &mut RuntimeShell {
        self.checked_out.as_mut().expect("runtime shell should be present").value_mut()
    }

    fn set_terminated(&mut self) {
        self.health.terminated = true;
    }

    fn set_heap_pressure(&mut self) {
        self.health.heap_pressure = true;
    }

    fn set_cleanup_failed(&mut self) {
        self.health.cleanup_ok = false;
    }

    fn set_config_changed(&mut self) {
        self.health.config_changed = true;
    }

    fn health(&self) -> ShellHealth {
        self.health
    }

    fn into_checked_out(self) -> CheckedOut<RuntimeShell> {
        self.checked_out.expect("runtime shell should be present")
    }
}

#[cfg(feature = "v8_runtime")]
thread_local! {
    static RUNTIME_POOL: RefCell<IsolatePool<RuntimeShell>> = RefCell::new(IsolatePool::new());
}

#[cfg(feature = "v8_runtime")]
fn current_runtime_pool_config() -> IsolatePoolConfig {
    IsolatePoolConfig {
        enable_reuse: isolate_reuse_enabled(),
        max_pool_size: isolate_pool_size(),
        max_age_seconds: isolate_max_age_seconds(),
        max_invocations: isolate_max_invocations(),
    }
}

#[cfg(feature = "v8_runtime")]
fn record_retire_reason(reason: RetireReason) {
    match reason {
        RetireReason::MaxAge => record_runtime_retire("max_age"),
        RetireReason::MaxInvocations => record_runtime_retire("max_invocations"),
        RetireReason::Termination => record_runtime_retire("termination"),
        RetireReason::HeapPressure => record_runtime_retire("heap_pressure"),
        RetireReason::ReuseDisabled
        | RetireReason::PoolFull
        | RetireReason::ConfigChanged
        | RetireReason::CleanupFailure
        | RetireReason::SetupFailure => record_runtime_retire("other"),
    }
}

#[cfg(feature = "v8_runtime")]
fn checkout_runtime_shell() -> Result<RuntimeShellGuard, RuntimeExecError> {
    let config = current_runtime_pool_config();
    let started_at = Instant::now();

    loop {
        let checkout = RUNTIME_POOL.with(|pool| pool.borrow_mut().checkout(&config));

        for reason in checkout.retired {
            record_retire_reason(reason);
        }

        if let Some(checked_out) = checkout.checked_out {
            let elapsed_us = started_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            record_runtime_checkout_hit(elapsed_us);
            if checked_out.was_warm() {
                record_runtime_warm_shell_reuse();
            }
            let mut guard = RuntimeShellGuard::from_checked_out(checked_out);
            if guard.shell_mut().heap_limit_setting != current_plts_max_heap_setting() {
                guard.set_config_changed();
                let health = guard.health();
                let checked_out = guard.into_checked_out();
                checkin_runtime_shell(checked_out, health);
                continue;
            }
            return Ok(guard);
        }

        if checkout.was_miss {
            let elapsed_us = started_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            record_runtime_checkout_miss(elapsed_us);
        }

        record_runtime_cold_shell_create();
        return build_runtime_shell().map(RuntimeShellGuard::new);
    }
}

#[cfg(feature = "v8_runtime")]
fn checkin_runtime_shell(checked_out: CheckedOut<RuntimeShell>, health: ShellHealth) {
    let config = current_runtime_pool_config();
    let outcome = RUNTIME_POOL.with(|pool| pool.borrow_mut().checkin(checked_out, &config, health));

    if let Some(reason) = outcome.retire_reason {
        record_retire_reason(reason);
    }
}

#[cfg(feature = "v8_runtime")]
fn build_runtime_shell() -> Result<RuntimeShell, RuntimeExecError> {
    use deno_core::{JsRuntime, RuntimeOptions, v8};

    let max_heap_setting = current_plts_max_heap_setting();
    let max_heap_bytes = max_heap_setting.as_deref().and_then(parse_runtime_heap_limit_bytes);
    let startup_snapshot = runtime_startup_snapshot();
    let loader_state = Rc::new(RefCell::new(PltsModuleLoaderState::default()));

    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init_ops()],
        module_loader: Some(Rc::new(PltsModuleLoader { state: Rc::clone(&loader_state) })),
        startup_snapshot,
        skip_op_registration: false,
        create_params: max_heap_bytes
            .map(|bytes| v8::Isolate::create_params().heap_limits(0, bytes)),
        ..Default::default()
    });

    let heap_limit_reached = Arc::new(AtomicBool::new(false));
    if max_heap_bytes.is_some() {
        let heap_limit_reached = Arc::clone(&heap_limit_reached);
        let isolate_handle = runtime.v8_isolate().thread_safe_handle();
        runtime.add_near_heap_limit_callback(move |current_limit, _initial_limit| {
            heap_limit_reached.store(true, Ordering::Relaxed);
            isolate_handle.terminate_execution();
            current_limit
        });
    }

    if startup_snapshot.is_none() {
        for (name, source) in static_bootstrap_scripts() {
            runtime.execute_script(name, source).map_err(|err| {
                RuntimeExecError::new(
                    "runtime static bootstrap",
                    format!("failed to execute `{name}`: {err}"),
                )
            })?;
        }
    }

    let baseline_globals = capture_global_names(&mut runtime)?;
    let baseline_globals_json = serde_json::to_string(&baseline_globals).map_err(|err| {
        RuntimeExecError::new(
            "runtime bootstrap",
            format!("failed to encode baseline globals: {err}"),
        )
    })?;

    Ok(RuntimeShell {
        runtime,
        loader_state,
        baseline_globals_json,
        heap_limit_setting: max_heap_setting,
        heap_limit_reached,
        invocation_nonce: 0,
    })
}

#[cfg(feature = "v8_runtime")]
fn capture_global_names(
    runtime: &mut deno_core::JsRuntime,
) -> Result<Vec<String>, RuntimeExecError> {
    use deno_core::serde_v8;
    use deno_core::v8;

    let names = runtime
        .execute_script(
            "plts_capture_globals.js",
            r#"
            (() => Object.getOwnPropertyNames(globalThis).sort())();
            "#,
        )
        .map_err(|err| RuntimeExecError::new("runtime bootstrap", err.to_string()))?;
    let scope = &mut runtime.handle_scope();
    let local = v8::Local::new(scope, names);
    serde_v8::from_v8::<Vec<String>>(scope, local).map_err(|err| {
        RuntimeExecError::new(
            "runtime bootstrap",
            format!("failed to decode baseline globals: {err}"),
        )
    })
}

#[cfg(feature = "v8_runtime")]
fn reset_runtime_shell(shell: &mut RuntimeShell) -> Result<(), RuntimeExecError> {
    let reset_script = format!(
        r#"
        (() => {{
            const baseline = new Set({});
            for (const key of Object.getOwnPropertyNames(globalThis)) {{
                if (baseline.has(key)) {{
                    continue;
                }}

                try {{
                    delete globalThis[key];
                }} catch (_err) {{
                    try {{
                        Object.defineProperty(globalThis, key, {{
                            value: undefined,
                            configurable: true,
                            enumerable: false,
                            writable: true,
                        }});
                        delete globalThis[key];
                    }} catch (_innerErr) {{
                        // keep the key so verification fails and the shell retires
                    }}
                }}
            }}

            delete globalThis.__plts_ctx;
            delete globalThis.__plts_entrypoint;
            delete globalThis.__plts_invocation_nonce;

            return Object.getOwnPropertyNames(globalThis)
                .filter((key) => !baseline.has(key))
                .sort();
        }})();
        "#,
        shell.baseline_globals_json
    );

    let leaked = shell
        .runtime
        .execute_script("plts_reset.js", reset_script)
        .map_err(|err| RuntimeExecError::new("runtime cleanup", err.to_string()))?;
    let scope = &mut shell.runtime.handle_scope();
    let local = deno_core::v8::Local::new(scope, leaked);
    let leaked = deno_core::serde_v8::from_v8::<Vec<String>>(scope, local).map_err(|err| {
        RuntimeExecError::new("runtime cleanup", format!("failed to decode cleanup result: {err}"))
    })?;
    if leaked.is_empty() {
        Ok(())
    } else {
        Err(RuntimeExecError::new(
            "runtime cleanup",
            format!("global state leaked across calls: {}", leaked.join(", ")),
        ))
    }
}

#[cfg(feature = "v8_runtime")]
fn runtime_main_module_specifier(invocation_nonce: u64) -> String {
    format!("file:///plts/main-{invocation_nonce}.js")
}

#[cfg(feature = "v8_runtime")]
fn versioned_module_target(target: &str, invocation_nonce: u64) -> String {
    if target.starts_with("plts+artifact:") || target.starts_with("data:") {
        format!("{target}#plts-invocation-{invocation_nonce}")
    } else {
        target.to_string()
    }
}

#[cfg(feature = "v8_runtime")]
fn invocation_nonce_from_specifier(specifier: &str) -> Option<u64> {
    let suffix = specifier.split_once("#plts-invocation-")?.1;
    let digits = suffix.chars().take_while(|ch| ch.is_ascii_digit()).collect::<String>();
    digits.parse::<u64>().ok()
}

#[cfg(feature = "v8_runtime")]
fn version_source_module_literals(source: &str, invocation_nonce: u64) -> String {
    const PATTERNS: [(&str, char); 4] =
        [("from \"", '"'), ("from '", '\''), ("import(\"", '"'), ("import('", '\'')];

    let mut out = String::with_capacity(source.len() + 32);
    let mut cursor = 0;

    while cursor < source.len() {
        let next_match = PATTERNS
            .iter()
            .filter_map(|(prefix, quote)| {
                source[cursor..].find(prefix).map(|offset| (cursor + offset, *prefix, *quote))
            })
            .min_by_key(|(offset, _, _)| *offset);

        let Some((match_start, prefix, quote)) = next_match else {
            out.push_str(&source[cursor..]);
            break;
        };

        out.push_str(&source[cursor..match_start]);
        out.push_str(prefix);
        cursor = match_start + prefix.len();

        let mut end = cursor;
        let bytes = source.as_bytes();
        while end < source.len() {
            let ch = bytes[end] as char;
            if ch == quote && (end == cursor || bytes[end - 1] != b'\\') {
                break;
            }
            end += 1;
        }

        if end >= source.len() {
            out.push_str(&source[cursor..]);
            break;
        }

        let literal = &source[cursor..end];
        if literal.starts_with("plts+artifact:") || literal.starts_with("data:") {
            out.push_str(versioned_module_target(literal, invocation_nonce).as_str());
        } else {
            out.push_str(literal);
        }
        out.push(quote);
        cursor = end + 1;
    }

    out
}

#[cfg(feature = "v8_runtime")]
fn strip_invocation_suffix(specifier: &str) -> &str {
    specifier
        .split_once('#')
        .map(|(base, _)| base)
        .or_else(|| specifier.split_once('?').map(|(base, _)| base))
        .unwrap_or(specifier)
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn execute_program(
    source: &str,
    entrypoint_export: &str,
    pointer_import_map: &HashMap<String, String>,
    context: &Value,
) -> Result<Option<Value>, RuntimeExecError> {
    use deno_core::{ModuleSpecifier, PollEventLoopOptions, serde_v8, v8};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum DbAccessMode {
        ReadOnly,
        ReadWrite,
    }

    impl DbAccessMode {
        fn is_read_only(self) -> bool {
            matches!(self, Self::ReadOnly)
        }

        fn as_js_mode(self) -> &'static str {
            match self {
                Self::ReadOnly => "ro",
                Self::ReadWrite => "rw",
            }
        }
    }

    let mut shell_guard = checkout_runtime_shell()?;
    let shell = shell_guard.shell_mut();
    shell.heap_limit_reached.store(false, Ordering::Relaxed);
    shell.invocation_nonce = shell.invocation_nonce.saturating_add(1);
    let invocation_nonce = shell.invocation_nonce;

    let mut bare_specifier_map = pointer_import_map
        .iter()
        .map(|(key, value)| (key.clone(), versioned_module_target(value, invocation_nonce)))
        .collect::<HashMap<_, _>>();
    bare_specifier_map.extend(
        parse_inline_import_map(source)
            .into_iter()
            .map(|(key, value)| (key, versioned_module_target(value.as_str(), invocation_nonce))),
    );
    shell.loader_state.borrow_mut().bare_specifier_map = bare_specifier_map;

    let statement_timeout_ms = current_statement_timeout_ms();
    let max_runtime_ms = current_plts_max_runtime_ms();
    let effective_timeout_ms = resolve_runtime_timeout_ms(statement_timeout_ms, max_runtime_ms);
    let interrupt_guard =
        RuntimeInterruptGuard::with_statement_timeout(&mut shell.runtime, effective_timeout_ms);
    let heap_limit_setting = shell.heap_limit_setting.clone();
    let heap_limit_reached = Arc::clone(&shell.heap_limit_reached);
    let setup_started_at = Instant::now();

    let execution_result = (|| {
        let runtime = &mut shell.runtime;

        let map_runtime_error = |stage: &'static str, details: &str| {
            if heap_limit_reached.load(Ordering::Relaxed) {
                let configured_limit = heap_limit_setting.as_deref().unwrap_or("unknown");
                RuntimeExecError::new(
                    "memory limit",
                    format!(
                        "execution exceeded configured runtime memory limit (plts.max_heap_mb={}) while in stage `{}`",
                        configured_limit, stage
                    ),
                )
            } else if interrupt_guard.as_ref().is_some_and(RuntimeInterruptGuard::timed_out) {
                let configured_ms = effective_timeout_ms.unwrap_or_default();
                RuntimeExecError::new(
                    "statement timeout",
                    format!(
                        "execution exceeded configured runtime timeout ({}ms) while in stage `{}`",
                        configured_ms, stage
                    ),
                )
            } else if interrupt_guard.as_ref().is_some_and(RuntimeInterruptGuard::interrupted) {
                RuntimeExecError::new(
                    "postgres interrupt",
                    format!(
                        "execution interrupted by pending PostgreSQL cancel signal while in stage `{}`",
                        stage
                    ),
                )
            } else {
                format_js_error(stage, details)
            }
        };

        let main_specifier =
            ModuleSpecifier::parse(runtime_main_module_specifier(invocation_nonce).as_str())
                .map_err(|err| {
                    RuntimeExecError::new(
                        "module bootstrap",
                        format!(
                            "invalid main module specifier for invocation {invocation_nonce}: {err}"
                        ),
                    )
                })?;
        let versioned_source = version_source_module_literals(source, invocation_nonce);

        let module_id = deno_core::futures::executor::block_on(
            runtime.load_side_es_module_from_code(&main_specifier, versioned_source),
        )
        .map_err(|e| map_runtime_error("module load", &e.to_string()))?;

        let module_result = runtime.mod_evaluate(module_id);
        deno_core::futures::executor::block_on(async {
            runtime.run_event_loop(PollEventLoopOptions::default()).await?;
            module_result.await
        })
        .map_err(|e| map_runtime_error("module evaluation", &e.to_string()))?;

        {
            let namespace = runtime
                .get_module_namespace(module_id)
                .map_err(|e| map_runtime_error("module namespace", &e.to_string()))?;

            let scope = &mut runtime.handle_scope();
            let namespace = v8::Local::new(scope, namespace);
            let entrypoint_key = v8::String::new(scope, entrypoint_export).ok_or_else(|| {
                RuntimeExecError::new("entrypoint resolution", "failed to intern key")
            })?;
            let resolved_export = namespace.get(scope, entrypoint_key.into()).ok_or_else(|| {
                RuntimeExecError::new(
                    "entrypoint resolution",
                    format!("module export '{}' is missing", entrypoint_export),
                )
            })?;

            if !resolved_export.is_function() {
                return Err(RuntimeExecError::new(
                    "entrypoint resolution",
                    format!("module export '{}' must be a function", entrypoint_export),
                ));
            }

            let global = scope.get_current_context().global(scope);
            let global_key = v8::String::new(scope, "__plts_entrypoint").ok_or_else(|| {
                RuntimeExecError::new("entrypoint resolution", "failed to intern key")
            })?;
            if !global.set(scope, global_key.into(), resolved_export).unwrap_or(false) {
                return Err(RuntimeExecError::new(
                    "entrypoint resolution",
                    format!("failed to install module export '{}' entrypoint", entrypoint_export),
                ));
            }
        }

        let db_mode = {
            let handler_kind_value = runtime
                .execute_script(
                    "plts_handler_kind.js",
                    r#"
                    (() => {
                        const kind = globalThis.__plts_entrypoint?.__stopgap_kind;
                        return typeof kind === "string" ? kind : null;
                    })();
                    "#,
                )
                .map_err(|e| map_runtime_error("handler metadata", &e.to_string()))?;

            let scope = &mut runtime.handle_scope();
            let local = v8::Local::new(scope, handler_kind_value);
            let handler_kind = serde_v8::from_v8::<Option<String>>(scope, local).map_err(|e| {
                RuntimeExecError::new(
                    "handler metadata",
                    format!("failed to decode stopgap handler kind: {e}"),
                )
            })?;

            match handler_kind.as_deref() {
                Some("query") => DbAccessMode::ReadOnly,
                _ => DbAccessMode::ReadWrite,
            }
        };

        let context_json = serde_json::to_string(context).map_err(|e| {
            RuntimeExecError::new(
                "context serialize",
                format!("failed to serialize runtime context: {e}"),
            )
        })?;

        let set_ctx_script = build_dynamic_context_setup_script(
            &context_json,
            db_mode.as_js_mode(),
            db_mode.is_read_only(),
        )?;

        runtime
            .execute_script("plts_ctx.js", set_ctx_script)
            .map_err(|e| map_runtime_error("context setup", &e.to_string()))?;
        let setup_elapsed_us =
            setup_started_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        record_runtime_setup_realm(setup_elapsed_us);

        let invoke_script = r#"
            if (typeof globalThis.__plts_entrypoint !== "function") {
                throw new Error("configured module export must be a function");
            }
            globalThis.__plts_entrypoint(globalThis.__plts_ctx);
        "#;

        let value = runtime
            .execute_script("plts_invoke.js", invoke_script)
            .map_err(|e| map_runtime_error("entrypoint invocation", &e.to_string()))?;

        #[allow(deprecated)]
        let value = deno_core::futures::executor::block_on(runtime.resolve_value(value))
            .map_err(|e| map_runtime_error("entrypoint await", &e.to_string()))?;

        let scope = &mut runtime.handle_scope();
        let local = v8::Local::new(scope, value);
        if local.is_null_or_undefined() {
            return Ok(None);
        }

        let value = serde_v8::from_v8::<Value>(scope, local).map_err(|e| {
            RuntimeExecError::new("result decode", format!("failed to decode JS result value: {e}"))
        })?;

        if value.is_null() { Ok(None) } else { Ok(Some(value)) }
    })();

    if shell.heap_limit_reached.load(Ordering::Relaxed) {
        shell_guard.set_heap_pressure();
        shell_guard.set_terminated();
    } else if interrupt_guard.as_ref().is_some_and(RuntimeInterruptGuard::timed_out)
        || interrupt_guard.as_ref().is_some_and(RuntimeInterruptGuard::interrupted)
    {
        shell_guard.set_terminated();
    }

    if !shell_guard.health().terminated && !shell_guard.health().heap_pressure {
        if let Err(_err) = reset_runtime_shell(shell_guard.shell_mut()) {
            shell_guard.set_cleanup_failed();
        }
    }

    let health = shell_guard.health();
    let checked_out = shell_guard.into_checked_out();
    checkin_runtime_shell(checked_out, health);
    execution_result
}

#[cfg(not(feature = "v8_runtime"))]
pub(crate) fn execute_program(
    _source: &str,
    _entrypoint_export: &str,
    _pointer_import_map: &HashMap<String, String>,
    _context: &Value,
) -> Result<Option<Value>, RuntimeExecError> {
    Err(RuntimeExecError::new("runtime bootstrap", "v8_runtime feature is disabled"))
}

#[cfg(feature = "v8_runtime")]
fn format_js_error(stage: &'static str, details: &str) -> RuntimeExecError {
    let (message, stack) = parse_js_error_details(details);
    RuntimeExecError::with_stack(stage, message, stack)
}
