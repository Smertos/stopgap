#[cfg(feature = "v8_runtime")]
use base64::Engine;
use common::sql::quote_literal;
use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::JsonB;
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

mod compiler;
mod function_program;
mod runtime_spi;

use function_program::{load_function_program, FunctionProgram};

#[cfg(feature = "v8_runtime")]
use runtime_spi::{exec_sql_with_params, query_json_rows_with_params};

#[cfg(feature = "v8_runtime")]
pub(crate) use runtime_spi::{bind_json_params, is_read_only_sql, BoundParam};

::pgrx::pg_module_magic!(name, version);

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS plts;

    CREATE TABLE IF NOT EXISTS plts.artifact (
        artifact_hash text PRIMARY KEY,
        source_ts text NOT NULL,
        compiled_js text NOT NULL,
        compiler_opts jsonb NOT NULL,
        compiler_fingerprint text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        source_map text,
        diagnostics jsonb
    );

    ALTER TABLE plts.artifact
    ADD COLUMN IF NOT EXISTS source_map text;

    CREATE FUNCTION plts_call_handler()
    RETURNS language_handler
    AS 'MODULE_PATHNAME', 'plts_call_handler'
    LANGUAGE C STRICT;

    CREATE FUNCTION plts_validator(oid)
    RETURNS void
    AS 'MODULE_PATHNAME', 'plts_validator'
    LANGUAGE C STRICT;

    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_language WHERE lanname = 'plts') THEN
            CREATE LANGUAGE plts HANDLER plts_call_handler VALIDATOR plts_validator;
        END IF;
    END;
    $$;
    "#,
    name = "plts_sql_bootstrap"
);

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn plts_call_handler(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    if fcinfo.is_null() {
        return pg_sys::Datum::from(0);
    }

    let flinfo = (*fcinfo).flinfo;
    if flinfo.is_null() {
        (*fcinfo).isnull = true;
        return pg_sys::Datum::from(0);
    }

    let fn_oid = (*flinfo).fn_oid;
    let args_payload = build_args_payload(fcinfo, fn_oid);

    if runtime_available() {
        if let Some(program) = load_function_program(fn_oid) {
            let context = build_runtime_context(&program, &args_payload);
            match execute_program(&program.source, &context) {
                Ok(Some(value)) => {
                    if let Some(datum) = JsonB(value).into_datum() {
                        return datum;
                    }
                }
                Ok(None) => {
                    (*fcinfo).isnull = true;
                    return pg_sys::Datum::from(0);
                }
                Err(err) => {
                    error!("{}", format_runtime_error_for_sql(&program, &err));
                }
            }
        }
    }

    let is_jsonb_single_arg = is_single_jsonb_arg_function(fn_oid);
    if is_jsonb_single_arg && (*fcinfo).nargs == 1 {
        let arg0 = (*fcinfo).args.as_ptr();
        if !arg0.is_null() && !(*arg0).isnull {
            return (*arg0).value;
        }
    }

    if let Some(datum) = JsonB(args_payload).into_datum() {
        return datum;
    }

    (*fcinfo).isnull = true;
    pg_sys::Datum::from(0)
}

#[no_mangle]
pub extern "C" fn pg_finfo_plts_call_handler() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeExecError {
    stage: &'static str,
    message: String,
    stack: Option<String>,
}

impl RuntimeExecError {
    fn new(stage: &'static str, message: impl Into<String>) -> Self {
        Self { stage, message: message.into(), stack: None }
    }

    #[cfg(any(test, feature = "v8_runtime"))]
    fn with_stack(
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

fn format_runtime_error_for_sql(program: &FunctionProgram, err: &RuntimeExecError) -> String {
    format!(
        "plts runtime error for {}.{} (oid={}): {}; sql_context={{schema={}, name={}, oid={}}}",
        program.schema, program.name, program.oid, err, program.schema, program.name, program.oid
    )
}

#[cfg(any(test, feature = "v8_runtime"))]
fn parse_js_error_details(details: &str) -> (String, Option<String>) {
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

fn build_runtime_context(program: &FunctionProgram, args_payload: &Value) -> Value {
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
fn current_statement_timeout_ms() -> Option<u64> {
    let raw =
        Spi::get_one::<String>("SELECT current_setting('statement_timeout')").ok().flatten()?;
    parse_statement_timeout_ms(raw.as_str())
}

#[cfg_attr(not(any(test, feature = "v8_runtime")), allow(dead_code))]
fn parse_statement_timeout_ms(raw: &str) -> Option<u64> {
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
fn interrupt_pending_from_flags(
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

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn plts_validator(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    pg_sys::Datum::from(0)
}

#[no_mangle]
pub extern "C" fn pg_finfo_plts_validator() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[pg_schema]
mod plts {
    use super::*;

    #[pg_extern]
    fn version() -> &'static str {
        "0.1.0"
    }

    #[pg_extern]
    fn compile_ts(
        source_ts: &str,
        compiler_opts: default!(JsonB, "'{}'::jsonb"),
    ) -> TableIterator<
        'static,
        (
            name!(compiled_js, String),
            name!(diagnostics, JsonB),
            name!(compiler_fingerprint, String),
        ),
    > {
        bootstrap_v8_isolate();
        let (compiled_js, diagnostics) = transpile_typescript(source_ts, &compiler_opts.0);
        TableIterator::once((compiled_js, JsonB(diagnostics), compiler_fingerprint().to_string()))
    }

    #[pg_extern]
    fn upsert_artifact(
        source_ts: &str,
        compiled_js: &str,
        compiler_opts: default!(JsonB, "'{}'::jsonb"),
    ) -> String {
        let compiler_fingerprint = compiler_fingerprint();
        let hash =
            compute_artifact_hash(source_ts, compiled_js, &compiler_opts.0, compiler_fingerprint);
        let source_map_sql = maybe_extract_source_map(compiled_js, &compiler_opts.0)
            .as_deref()
            .map(quote_literal)
            .unwrap_or_else(|| "NULL".to_string());

        let sql = format!(
            "
            INSERT INTO plts.artifact (
                artifact_hash,
                source_ts,
                compiled_js,
                compiler_opts,
                compiler_fingerprint,
                source_map
            )
            VALUES ({}, {}, {}, {}::jsonb, {}, {})
            ON CONFLICT (artifact_hash) DO UPDATE
            SET source_ts = EXCLUDED.source_ts,
                compiled_js = EXCLUDED.compiled_js,
                compiler_opts = EXCLUDED.compiler_opts,
                compiler_fingerprint = EXCLUDED.compiler_fingerprint,
                source_map = EXCLUDED.source_map
            ",
            quote_literal(&hash),
            quote_literal(source_ts),
            quote_literal(compiled_js),
            quote_literal(&compiler_opts.0.to_string()),
            quote_literal(compiler_fingerprint),
            source_map_sql
        );

        let _ = Spi::run(&sql);

        hash
    }

    #[pg_extern]
    fn compile_and_store(source_ts: &str, compiler_opts: default!(JsonB, "'{}'::jsonb")) -> String {
        let opts = compiler_opts.0;
        let mut rows = compile_ts(source_ts, JsonB(opts.clone()));
        let (compiled_js, diagnostics, _compiler_fingerprint) =
            rows.next().expect("compile_ts must always return one row");

        if diagnostics
            .0
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .any(|entry| entry.get("severity").and_then(|v| v.as_str()) == Some("error"))
            })
            .unwrap_or(false)
        {
            error!(
                "plts.compile_and_store aborted due to TypeScript diagnostics: {}",
                diagnostics.0
            );
        }

        upsert_artifact(source_ts, &compiled_js, JsonB(opts))
    }

    #[pg_extern]
    fn get_artifact(artifact_hash: &str) -> Option<JsonB> {
        let sql = format!(
            "
            SELECT jsonb_build_object(
                'source_ts', source_ts,
                'compiled_js', compiled_js,
                'compiler_opts', compiler_opts,
                'compiler_fingerprint', compiler_fingerprint,
                'source_map', source_map,
                'created_at', created_at
            )
            FROM plts.artifact
            WHERE artifact_hash = {}
            ",
            quote_literal(artifact_hash)
        );

        Spi::get_one::<JsonB>(&sql).ok().flatten()
    }
}

fn compute_artifact_hash(
    source_ts: &str,
    compiled_js: &str,
    compiler_opts: &serde_json::Value,
    compiler_fingerprint: &str,
) -> String {
    compiler::compute_artifact_hash(source_ts, compiled_js, compiler_opts, compiler_fingerprint)
}

fn compiler_fingerprint() -> &'static str {
    compiler::compiler_fingerprint()
}

fn transpile_typescript(source_ts: &str, compiler_opts: &Value) -> (String, Value) {
    compiler::transpile_typescript(source_ts, compiler_opts)
}

fn maybe_extract_source_map(compiled_js: &str, compiler_opts: &Value) -> Option<String> {
    compiler::maybe_extract_source_map(compiled_js, compiler_opts)
}

#[cfg(feature = "v8_runtime")]
fn bootstrap_v8_isolate() {
    let _runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions::default());
}

#[cfg(not(feature = "v8_runtime"))]
fn bootstrap_v8_isolate() {}

fn is_single_jsonb_arg_function(fn_oid: pg_sys::Oid) -> bool {
    let sql = format!(
        "
        SELECT (array_length(p.proargtypes::oid[], 1) = 1 AND p.proargtypes[0] = 'jsonb'::regtype::oid)
        FROM pg_proc p
        WHERE p.oid = {}
        ",
        fn_oid
    );

    Spi::get_one::<bool>(&sql).ok().flatten().unwrap_or(false)
}

unsafe fn build_args_payload(fcinfo: pg_sys::FunctionCallInfo, fn_oid: pg_sys::Oid) -> Value {
    let arg_oids = get_arg_type_oids(fn_oid);
    if arg_oids.is_empty() {
        return json!({ "positional": [], "named": {} });
    }

    let nargs = (*fcinfo).nargs as usize;
    let mut positional = Vec::with_capacity(nargs);
    let mut named = serde_json::Map::with_capacity(nargs);

    for i in 0..nargs {
        let arg = *(*fcinfo).args.as_ptr().add(i);
        let oid = arg_oids.get(i).copied().unwrap_or(pg_sys::UNKNOWNOID);
        let value = if arg.isnull { Value::Null } else { datum_to_json_value(arg.value, oid) };

        positional.push(value.clone());
        named.insert(i.to_string(), value);
    }

    json!({ "positional": positional, "named": named })
}

unsafe fn datum_to_json_value(datum: pg_sys::Datum, oid: pg_sys::Oid) -> Value {
    match oid {
        pg_sys::TEXTOID => {
            String::from_datum(datum, false).map(Value::String).unwrap_or(Value::Null)
        }
        pg_sys::INT4OID => i32::from_datum(datum, false).map(|v| json!(v)).unwrap_or(Value::Null),
        pg_sys::BOOLOID => bool::from_datum(datum, false).map(|v| json!(v)).unwrap_or(Value::Null),
        pg_sys::JSONBOID => JsonB::from_datum(datum, false).map(|v| v.0).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn get_arg_type_oids(fn_oid: pg_sys::Oid) -> Vec<pg_sys::Oid> {
    let sql = format!(
        "
        SELECT COALESCE(array_to_string(p.proargtypes::oid[], ','), '')
        FROM pg_proc p
        WHERE p.oid = {}
        ",
        fn_oid
    );

    let csv = Spi::get_one::<String>(&sql).ok().flatten().unwrap_or_default();
    if csv.is_empty() {
        return Vec::new();
    }

    csv.split(',').filter_map(|raw| raw.trim().parse::<u32>().ok()).map(pg_sys::Oid::from).collect()
}

#[cfg(feature = "v8_runtime")]
fn runtime_available() -> bool {
    true
}

#[cfg(not(feature = "v8_runtime"))]
fn runtime_available() -> bool {
    false
}

#[cfg(feature = "v8_runtime")]
fn execute_program(source: &str, context: &Value) -> Result<Option<Value>, RuntimeExecError> {
    use deno_core::{
        op2, serde_v8, v8, JsRuntime, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse,
        ModuleLoader, ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType,
        PollEventLoopOptions, ResolutionKind, RuntimeOptions,
    };

    const MAIN_MODULE_SPECIFIER: &str = "file:///plts/main.js";
    const STOPGAP_RUNTIME_BARE_SPECIFIER: &str = "@stopgap/runtime";
    const STOPGAP_RUNTIME_SPECIFIER: &str = "file:///plts/__stopgap_runtime__.js";
    const STOPGAP_RUNTIME_SOURCE: &str = r#"
        const isPlainObject = (value) =>
            typeof value === "object" && value !== null && !Array.isArray(value);

        const typeMatches = (expectedType, value) => {
            switch (expectedType) {
                case "object":
                    return isPlainObject(value);
                case "array":
                    return Array.isArray(value);
                case "string":
                    return typeof value === "string";
                case "boolean":
                    return typeof value === "boolean";
                case "number":
                    return typeof value === "number" && Number.isFinite(value);
                case "integer":
                    return typeof value === "number" && Number.isInteger(value);
                case "null":
                    return value === null;
                default:
                    return true;
            }
        };

        const describeValue = (value) => {
            if (value === null) return "null";
            if (Array.isArray(value)) return "array";
            return typeof value;
        };

        const sameJson = (left, right) => JSON.stringify(left) === JSON.stringify(right);

        const validateJsonSchema = (schema, value, path = "$") => {
            if (schema == null || schema === true) {
                return;
            }

            if (schema === false) {
                throw new TypeError(`stopgap args validation failed at ${path}: schema forbids all values`);
            }

            if (!isPlainObject(schema)) {
                throw new TypeError(`stopgap args validation failed at ${path}: schema must be an object`);
            }

            if (Array.isArray(schema.enum)) {
                const matched = schema.enum.some((allowed) => sameJson(allowed, value));
                if (!matched) {
                    throw new TypeError(`stopgap args validation failed at ${path}: value is not in enum`);
                }
            }

            if (Array.isArray(schema.anyOf) && schema.anyOf.length > 0) {
                let matched = false;
                for (const branch of schema.anyOf) {
                    try {
                        validateJsonSchema(branch, value, path);
                        matched = true;
                        break;
                    } catch (_err) {
                        // continue trying other branches
                    }
                }

                if (!matched) {
                    throw new TypeError(`stopgap args validation failed at ${path}: value does not match anyOf branches`);
                }
            }

            if (schema.type !== undefined) {
                const expected = Array.isArray(schema.type) ? schema.type : [schema.type];
                const matches = expected.some((entry) => typeMatches(entry, value));
                if (!matches) {
                    throw new TypeError(
                        `stopgap args validation failed at ${path}: expected ${expected.join("|")}, got ${describeValue(value)}`
                    );
                }
            }

            if (isPlainObject(value)) {
                const properties = isPlainObject(schema.properties) ? schema.properties : {};
                const required = Array.isArray(schema.required) ? schema.required : [];

                for (const key of required) {
                    if (!Object.prototype.hasOwnProperty.call(value, key)) {
                        throw new TypeError(`stopgap args validation failed at ${path}.${key}: missing required property`);
                    }
                }

                for (const [key, propertySchema] of Object.entries(properties)) {
                    if (Object.prototype.hasOwnProperty.call(value, key)) {
                        validateJsonSchema(propertySchema, value[key], `${path}.${key}`);
                    }
                }

                if (schema.additionalProperties === false) {
                    for (const key of Object.keys(value)) {
                        if (!Object.prototype.hasOwnProperty.call(properties, key)) {
                            throw new TypeError(`stopgap args validation failed at ${path}.${key}: additional properties are not allowed`);
                        }
                    }
                }
            }

            if (Array.isArray(value) && schema.items !== undefined) {
                for (let i = 0; i < value.length; i += 1) {
                    validateJsonSchema(schema.items, value[i], `${path}[${i}]`);
                }
            }
        };

        const normalizeWrapperArgs = (kind, argsSchema, handler) => {
            if (typeof argsSchema === "function" && handler === undefined) {
                return { argsSchema: null, handler: argsSchema };
            }

            if (typeof handler !== "function") {
                throw new TypeError(`stopgap.${kind} expects a function handler`);
            }

            return { argsSchema: argsSchema ?? null, handler };
        };

        const wrap = (kind, argsSchema, handler) => {
            const normalized = normalizeWrapperArgs(kind, argsSchema, handler);

            const wrapped = async (ctx) => {
                const args = ctx?.args ?? null;
                validateJsonSchema(normalized.argsSchema, args);
                return await normalized.handler(args, ctx);
            };

            wrapped.__stopgap_kind = kind;
            wrapped.__stopgap_args_schema = normalized.argsSchema;
            return wrapped;
        };

        export const query = (argsSchema, handler) => wrap("query", argsSchema, handler);
        export const mutation = (argsSchema, handler) => wrap("mutation", argsSchema, handler);
        export default { query, mutation };
    "#;

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
            .map_err(|e| deno_error::JsErrorBox::generic(e))
    }

    #[op2]
    #[serde]
    fn op_plts_db_exec(
        #[string] sql: String,
        #[serde] params: Vec<serde_json::Value>,
        read_only: bool,
    ) -> Result<serde_json::Value, deno_error::JsErrorBox> {
        exec_sql_with_params(&sql, params, read_only)
            .map_err(|e| deno_error::JsErrorBox::generic(e))
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

    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init()],
        module_loader: Some(Rc::new(PltsModuleLoader)),
        ..Default::default()
    });

    let statement_timeout_ms = current_statement_timeout_ms();
    let interrupt_guard =
        RuntimeInterruptGuard::with_statement_timeout(&mut runtime, statement_timeout_ms);

    let map_runtime_error = |stage: &'static str, details: &str| {
        if interrupt_guard.as_ref().is_some_and(RuntimeInterruptGuard::timed_out) {
            let configured_ms = statement_timeout_ms.unwrap_or_default();
            RuntimeExecError::new(
                "statement timeout",
                format!(
                    "execution exceeded current statement_timeout ({}ms) while in stage `{}`",
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
fn execute_program(_source: &str, _context: &Value) -> Result<Option<Value>, RuntimeExecError> {
    Err(RuntimeExecError::new("runtime bootstrap", "v8_runtime feature is disabled"))
}

#[cfg(feature = "v8_runtime")]
fn format_js_error(stage: &'static str, details: &str) -> RuntimeExecError {
    let (message, stack) = parse_js_error_details(details);
    RuntimeExecError::with_stack(stage, message, stack)
}

#[cfg(test)]
mod unit_tests {
    #[test]
    fn test_hash_prefix() {
        let hash = crate::compute_artifact_hash(
            "export default () => ({ ok: true })",
            "export default () => ({ ok: true })",
            &serde_json::json!({}),
            "v8-deno_core-p0",
        );
        assert!(hash.starts_with("sha256:"));
    }

    #[test]
    fn test_parse_artifact_ptr() {
        let ptr = crate::function_program::parse_artifact_ptr(
            r#"{"plts":1,"kind":"artifact_ptr","artifact_hash":"sha256:abc"}"#,
        )
        .expect("expected pointer metadata");
        assert_eq!(ptr.artifact_hash, "sha256:abc");
    }

    #[test]
    fn test_parse_js_error_details_with_stack() {
        let details = "Uncaught Error: boom\n    at default (plts_module.js:1:1)\n    at foo";
        let (message, stack) = crate::parse_js_error_details(details);
        assert_eq!(message, "Uncaught Error: boom");
        assert_eq!(stack.as_deref(), Some("at default (plts_module.js:1:1)\n    at foo"));
    }

    #[test]
    fn test_runtime_exec_error_display() {
        let err = crate::RuntimeExecError::with_stack(
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
    fn test_transpile_typescript_emits_js() {
        let source =
            "export default (ctx: { args: { id: number } }) => ({ id: ctx.args.id as number });";
        let (compiled, diagnostics) = crate::transpile_typescript(source, &serde_json::json!({}));
        assert!(diagnostics.as_array().is_some_and(|items| items.is_empty()));
        assert!(compiled.contains("export default"));
        assert!(!compiled.contains(": { args:"));
    }

    #[test]
    fn test_transpile_typescript_returns_diagnostic_on_parse_error() {
        let (compiled, diagnostics) =
            crate::transpile_typescript("export default (ctx => ctx", &serde_json::json!({}));
        assert!(compiled.is_empty());
        assert_eq!(
            diagnostics
                .as_array()
                .and_then(|items| items.first())
                .and_then(|entry| entry.get("severity"))
                .and_then(|value| value.as_str()),
            Some("error")
        );
    }

    #[test]
    fn test_dependency_version_from_lock_finds_known_crate() {
        let version = crate::compiler::dependency_version_from_lock("serde_json");
        assert!(version.is_some());
    }

    #[test]
    fn test_extract_inline_source_map_decodes_payload() {
        let compiled =
            "console.log('x');\n//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozfQ==";
        let source_map = crate::compiler::extract_inline_source_map(compiled)
            .expect("inline source map should decode from base64 payload");
        assert!(source_map.contains("\"version\":3"));
    }

    #[test]
    fn test_transpile_typescript_optionally_emits_source_map_payload() {
        let source =
            "export default (ctx: { args: { id: number } }) => ({ id: ctx.args.id as number });";
        let (compiled, diagnostics) =
            crate::transpile_typescript(source, &serde_json::json!({ "source_map": true }));
        assert!(diagnostics.as_array().is_some_and(|items| items.is_empty()));

        let source_map =
            crate::maybe_extract_source_map(&compiled, &serde_json::json!({ "source_map": true }))
                .expect("source_map=true should persist an inline source map payload");

        assert!(source_map.contains("\"version\""));
    }

    #[test]
    fn test_compiler_fingerprint_includes_dependency_versions() {
        let fingerprint = crate::compiler_fingerprint();
        assert!(fingerprint.contains("deno_ast@"));
        assert!(fingerprint.contains("deno_core@"));
    }

    #[test]
    fn test_artifact_source_cache_evicts_least_recently_used_entry() {
        let mut cache = crate::function_program::ArtifactSourceCache::default();
        for i in 0..crate::function_program::artifact_source_cache_capacity() {
            cache.insert(format!("hash-{i}"), format!("src-{i}"));
        }

        assert_eq!(cache.get("hash-0").as_deref(), Some("src-0"));
        cache.insert("hash-overflow".to_string(), "src-overflow".to_string());

        assert_eq!(cache.get("hash-1"), None);
        assert_eq!(cache.get("hash-0").as_deref(), Some("src-0"));
        assert_eq!(cache.get("hash-overflow").as_deref(), Some("src-overflow"));
    }

    #[test]
    fn test_parse_statement_timeout_ms_parses_common_postgres_units() {
        assert_eq!(crate::parse_statement_timeout_ms("0"), None);
        assert_eq!(crate::parse_statement_timeout_ms("250"), Some(250));
        assert_eq!(crate::parse_statement_timeout_ms("250ms"), Some(250));
        assert_eq!(crate::parse_statement_timeout_ms("2s"), Some(2_000));
        assert_eq!(crate::parse_statement_timeout_ms("1min"), Some(60_000));
        assert_eq!(crate::parse_statement_timeout_ms("1.5s"), Some(1_500));
        assert_eq!(crate::parse_statement_timeout_ms("500us"), Some(1));
    }

    #[test]
    fn test_parse_statement_timeout_ms_rejects_invalid_values() {
        assert_eq!(crate::parse_statement_timeout_ms(""), None);
        assert_eq!(crate::parse_statement_timeout_ms("off"), None);
        assert_eq!(crate::parse_statement_timeout_ms("-5ms"), None);
        assert_eq!(crate::parse_statement_timeout_ms("12fortnights"), None);
    }

    #[test]
    fn test_interrupt_pending_from_flags_detects_pending_signal() {
        assert!(!crate::interrupt_pending_from_flags(0, 0, 0));
        assert!(crate::interrupt_pending_from_flags(1, 0, 0));
        assert!(crate::interrupt_pending_from_flags(0, 1, 0));
        assert!(crate::interrupt_pending_from_flags(0, 0, 1));
    }

    #[cfg(feature = "v8_runtime")]
    #[test]
    fn test_bind_json_params_maps_common_value_types() {
        let params = crate::bind_json_params(vec![
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!("hello"),
            serde_json::json!({ "ok": true }),
            serde_json::Value::Null,
        ]);

        assert!(matches!(params[0], crate::BoundParam::Bool(true)));
        assert!(matches!(params[1], crate::BoundParam::Int(42)));
        assert!(matches!(params[2], crate::BoundParam::Text(ref v) if v == "hello"));
        assert!(matches!(params[3], crate::BoundParam::Json(_)));
        assert!(matches!(params[4], crate::BoundParam::NullText));
    }

    #[cfg(feature = "v8_runtime")]
    #[test]
    fn test_is_read_only_sql_accepts_select_and_rejects_writes() {
        assert!(crate::is_read_only_sql("SELECT 1"));
        assert!(crate::is_read_only_sql("-- comment\nSELECT now()"));
        assert!(crate::is_read_only_sql("/* leading */ SELECT * FROM pg_class"));
        assert!(crate::is_read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte"));

        assert!(!crate::is_read_only_sql("INSERT INTO t(id) VALUES (1)"));
        assert!(!crate::is_read_only_sql(
            "WITH x AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT * FROM x"
        ));
        assert!(!crate::is_read_only_sql("DELETE FROM t"));
    }
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    include!("../tests/pg/mod.rs");
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
