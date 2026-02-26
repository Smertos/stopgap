use crate::function_program::FunctionProgram;
#[cfg(feature = "v8_runtime")]
use crate::runtime_spi::{exec_sql_with_params, query_json_rows_with_params};
#[cfg(feature = "v8_runtime")]
use base64::Engine;
use pgrx::prelude::*;
use serde_json::json;
use serde_json::Value;
use std::fmt;
#[cfg(feature = "v8_runtime")]
use std::rc::Rc;
#[cfg(feature = "v8_runtime")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "v8_runtime")]
use std::sync::Arc;
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
        if stack.is_empty() {
            (message, None)
        } else {
            (message, Some(stack.to_string()))
        }
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
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
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
pub(crate) fn bootstrap_v8_isolate() {
    let _runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions::default());
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
pub(crate) fn execute_program(
    source: &str,
    context: &Value,
) -> Result<Option<Value>, RuntimeExecError> {
    use deno_core::{
        op2, serde_v8, v8, JsRuntime, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse,
        ModuleLoader, ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType,
        PollEventLoopOptions, ResolutionKind, RuntimeOptions,
    };

    const MAIN_MODULE_SPECIFIER: &str = "file:///plts/main.js";
    const STOPGAP_RUNTIME_BARE_SPECIFIER: &str = "@stopgap/runtime";
    const STOPGAP_RUNTIME_SPECIFIER: &str = "file:///plts/__stopgap_runtime__.js";
    const STOPGAP_RUNTIME_SOURCE: &str = include_str!("../../../packages/runtime/src/embedded.ts");

    struct PltsModuleLoader;

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

    impl ModuleLoader for PltsModuleLoader {
        fn resolve(
            &self,
            specifier: &str,
            referrer: &str,
            _kind: ResolutionKind,
        ) -> Result<ModuleSpecifier, deno_core::error::ModuleLoaderError> {
            if specifier == STOPGAP_RUNTIME_BARE_SPECIFIER {
                return ModuleSpecifier::parse(STOPGAP_RUNTIME_SPECIFIER)
                    .map_err(deno_error::JsErrorBox::from_err);
            }
            deno_core::resolve_import(specifier, referrer).map_err(deno_error::JsErrorBox::from_err)
        }

        fn load(
            &self,
            module_specifier: &ModuleSpecifier,
            _maybe_referrer: Option<&ModuleLoadReferrer>,
            _options: ModuleLoadOptions,
        ) -> ModuleLoadResponse {
            ModuleLoadResponse::Sync(load_module_source(module_specifier))
        }
    }

    fn load_module_source(
        module_specifier: &ModuleSpecifier,
    ) -> Result<ModuleSource, deno_core::error::ModuleLoaderError> {
        match module_specifier.scheme() {
            "data" => {
                let source = decode_data_url_module_code(module_specifier)?;
                Ok(ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(source.into()),
                    module_specifier,
                    None,
                ))
            }
            "file" if module_specifier.as_str() == STOPGAP_RUNTIME_SPECIFIER => {
                Ok(ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(STOPGAP_RUNTIME_SOURCE.to_string().into()),
                    module_specifier,
                    None,
                ))
            }
            _ => Err(deno_error::JsErrorBox::generic(format!(
                "unsupported module import `{}`; only `data:` imports and `@stopgap/runtime` are currently allowed",
                module_specifier
            ))),
        }
    }

    fn decode_data_url_module_code(
        module_specifier: &ModuleSpecifier,
    ) -> Result<String, deno_core::error::ModuleLoaderError> {
        let raw = module_specifier.as_str();
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
            let decoded =
                base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|err| {
                    deno_error::JsErrorBox::generic(format!(
                        "failed to decode base64 data URL module `{module_specifier}`: {err}"
                    ))
                })?;
            String::from_utf8(decoded).map_err(|err| {
                deno_error::JsErrorBox::generic(format!(
                    "data URL module `{module_specifier}` is not valid UTF-8: {err}"
                ))
            })
        } else {
            Ok(encoded.to_string())
        }
    }

    #[op2]
    #[serde]
    fn op_plts_db_query(
        #[string] sql: String,
        #[serde] params: Vec<serde_json::Value>,
        read_only: bool,
    ) -> Result<serde_json::Value, deno_error::JsErrorBox> {
        query_json_rows_with_params(&sql, params, read_only)
            .map_err(deno_error::JsErrorBox::generic)
    }

    #[op2]
    #[serde]
    fn op_plts_db_exec(
        #[string] sql: String,
        #[serde] params: Vec<serde_json::Value>,
        read_only: bool,
    ) -> Result<serde_json::Value, deno_error::JsErrorBox> {
        exec_sql_with_params(&sql, params, read_only).map_err(deno_error::JsErrorBox::generic)
    }

    deno_core::extension!(plts_runtime_ext, ops = [op_plts_db_query, op_plts_db_exec]);

    const LOCKDOWN_RUNTIME_SURFACE_SCRIPT: &str = r#"
        (() => {
            const normalizeParams = (raw, opName) => {
                if (raw === undefined) {
                    return [];
                }

                if (!Array.isArray(raw)) {
                    throw new TypeError(`${opName} params must be an array`);
                }

                return raw;
            };

            const normalizeDbCall = (input, params, paramsProvided, opName) => {
                if (typeof input === "string") {
                    return { sql: input, params: normalizeParams(paramsProvided ? params : [], opName) };
                }

                if (typeof input === "object" && input !== null) {
                    let resolved = input;
                    if (typeof resolved.toSQL === "function") {
                        resolved = resolved.toSQL();
                    }

                    if (typeof resolved === "object" && resolved !== null && typeof resolved.sql === "string") {
                        const resolvedParams = paramsProvided ? params : resolved.params;
                        return { sql: resolved.sql, params: normalizeParams(resolvedParams, opName) };
                    }
                }

                throw new TypeError(
                    `${opName} expects SQL input as string, { sql, params }, or object with toSQL()`
                );
            };

            const ops = {
                dbQuery(input, params, readOnly = false, paramsProvided = false) {
                    const call = normalizeDbCall(input, params, paramsProvided, "db.query");
                    return Deno.core.ops.op_plts_db_query(call.sql, call.params, readOnly);
                },
                dbExec(input, params, readOnly = false, paramsProvided = false) {
                    const call = normalizeDbCall(input, params, paramsProvided, "db.exec");
                    return Deno.core.ops.op_plts_db_exec(call.sql, call.params, readOnly);
                },
            };

            Object.defineProperty(globalThis, "__plts_internal_ops", {
                value: Object.freeze(ops),
                configurable: false,
                enumerable: false,
                writable: false,
            });

            const stripGlobal = (key) => {
                try {
                    delete globalThis[key];
                } catch (_err) {
                    Object.defineProperty(globalThis, key, {
                        value: undefined,
                        configurable: true,
                        enumerable: false,
                        writable: false,
                    });
                }
            };

            stripGlobal("Deno");
            stripGlobal("fetch");
            stripGlobal("Request");
            stripGlobal("Response");
            stripGlobal("Headers");
            stripGlobal("WebSocket");
        })();
    "#;

    let max_heap_setting = current_plts_max_heap_setting();
    let max_heap_bytes = max_heap_setting.as_deref().and_then(parse_runtime_heap_limit_bytes);

    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init()],
        module_loader: Some(Rc::new(PltsModuleLoader)),
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

    let statement_timeout_ms = current_statement_timeout_ms();
    let max_runtime_ms = current_plts_max_runtime_ms();
    let effective_timeout_ms = resolve_runtime_timeout_ms(statement_timeout_ms, max_runtime_ms);
    let interrupt_guard =
        RuntimeInterruptGuard::with_statement_timeout(&mut runtime, effective_timeout_ms);

    let map_runtime_error = |stage: &'static str, details: &str| {
        if heap_limit_reached.load(Ordering::Relaxed) {
            let configured_limit = max_heap_setting.as_deref().unwrap_or("unknown");
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

    runtime
        .execute_script("plts_runtime_lockdown.js", LOCKDOWN_RUNTIME_SURFACE_SCRIPT)
        .map_err(|e| map_runtime_error("runtime lockdown", &e.to_string()))?;

    let main_specifier = ModuleSpecifier::parse(MAIN_MODULE_SPECIFIER).map_err(|err| {
        RuntimeExecError::new(
            "module bootstrap",
            format!("invalid main module specifier `{MAIN_MODULE_SPECIFIER}`: {err}"),
        )
    })?;

    let module_id = deno_core::futures::executor::block_on(
        runtime.load_main_es_module_from_code(&main_specifier, source.to_string()),
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

        deno_core::scope!(scope, runtime);
        let namespace = v8::Local::new(scope, namespace);
        let default_key = v8::String::new(scope, "default").ok_or_else(|| {
            RuntimeExecError::new("entrypoint resolution", "failed to intern key")
        })?;
        let default_export = namespace.get(scope, default_key.into()).ok_or_else(|| {
            RuntimeExecError::new("entrypoint resolution", "module default export is missing")
        })?;

        if !default_export.is_function() {
            return Err(RuntimeExecError::new(
                "entrypoint resolution",
                "default export must be a function",
            ));
        }

        let global = scope.get_current_context().global(scope);
        let global_key = v8::String::new(scope, "__plts_default").ok_or_else(|| {
            RuntimeExecError::new("entrypoint resolution", "failed to intern key")
        })?;
        if !global.set(scope, global_key.into(), default_export).unwrap_or(false) {
            return Err(RuntimeExecError::new(
                "entrypoint resolution",
                "failed to install default export entrypoint",
            ));
        }
    }

    let db_mode = {
        let handler_kind_value = runtime
            .execute_script(
                "plts_handler_kind.js",
                r#"
                (() => {
                    const kind = globalThis.__plts_default?.__stopgap_kind;
                    return typeof kind === "string" ? kind : null;
                })();
                "#,
            )
            .map_err(|e| map_runtime_error("handler metadata", &e.to_string()))?;

        deno_core::scope!(scope, runtime);
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

    let db_mode_js = db_mode.as_js_mode();
    let db_read_only_js = if db_mode.is_read_only() { "true" } else { "false" };
    let set_ctx_script = format!(
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
        serde_json::to_string(&context_json).map_err(|e| {
            RuntimeExecError::new(
                "context encode",
                format!("failed to encode runtime context string: {e}"),
            )
        })?,
        db_mode_js,
        db_read_only_js,
        db_read_only_js
    );

    runtime
        .execute_script("plts_ctx.js", set_ctx_script)
        .map_err(|e| map_runtime_error("context setup", &e.to_string()))?;

    let invoke_script = r#"
        if (typeof globalThis.__plts_default !== "function") {
            throw new Error("default export must be a function");
        }
        globalThis.__plts_default(globalThis.__plts_ctx);
    "#;

    let value = runtime
        .execute_script("plts_invoke.js", invoke_script)
        .map_err(|e| map_runtime_error("entrypoint invocation", &e.to_string()))?;

    #[allow(deprecated)]
    let value = deno_core::futures::executor::block_on(runtime.resolve_value(value))
        .map_err(|e| map_runtime_error("entrypoint await", &e.to_string()))?;

    deno_core::scope!(scope, runtime);
    let local = v8::Local::new(scope, value);
    if local.is_null_or_undefined() {
        return Ok(None);
    }

    let value = serde_v8::from_v8::<Value>(scope, local).map_err(|e| {
        RuntimeExecError::new("result decode", format!("failed to decode JS result value: {e}"))
    })?;

    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

#[cfg(not(feature = "v8_runtime"))]
pub(crate) fn execute_program(
    _source: &str,
    _context: &Value,
) -> Result<Option<Value>, RuntimeExecError> {
    Err(RuntimeExecError::new("runtime bootstrap", "v8_runtime feature is disabled"))
}

#[cfg(feature = "v8_runtime")]
fn format_js_error(stage: &'static str, details: &str) -> RuntimeExecError {
    let (message, stack) = parse_js_error_details(details);
    RuntimeExecError::with_stack(stage, message, stack)
}
