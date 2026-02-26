use base64::Engine;
use deno_ast::EmitOptions;
use deno_ast::MediaType;
use deno_ast::ModuleSpecifier;
use deno_ast::ParseParams;
use deno_ast::SourceMapOption;
use deno_ast::TranspileModuleOptions;
use deno_ast::TranspileOptions;
#[cfg(feature = "v8_runtime")]
use pgrx::datum::DatumWithOid;
use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::OnceLock;

::pgrx::pg_module_magic!(name, version);

const CARGO_LOCK_CONTENT: &str = include_str!("../../../Cargo.lock");
static TS_COMPILER_FINGERPRINT: OnceLock<String> = OnceLock::new();

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

#[derive(Debug)]
struct FunctionProgram {
    oid: pg_sys::Oid,
    schema: String,
    name: String,
    source: String,
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
#[derive(Debug)]
enum BoundParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(Value),
    NullText,
}

#[cfg(feature = "v8_runtime")]
impl BoundParam {
    fn from_json(value: Value) -> Self {
        match value {
            Value::Bool(v) => Self::Bool(v),
            Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    Self::Int(v)
                } else if let Some(v) = n.as_f64() {
                    Self::Float(v)
                } else {
                    Self::Json(Value::Number(n))
                }
            }
            Value::String(v) => Self::Text(v),
            Value::Array(_) | Value::Object(_) => Self::Json(value),
            Value::Null => Self::NullText,
        }
    }

    fn as_datum_with_oid(&self) -> DatumWithOid<'_> {
        match self {
            Self::Bool(v) => (*v).into(),
            Self::Int(v) => (*v).into(),
            Self::Float(v) => (*v).into(),
            Self::Text(v) => v.as_str().into(),
            Self::Json(v) => JsonB(v.clone()).into(),
            Self::NullText => Option::<&str>::None.into(),
        }
    }
}

#[cfg(feature = "v8_runtime")]
fn bind_json_params(params: Vec<Value>) -> Vec<BoundParam> {
    params.into_iter().map(BoundParam::from_json).collect()
}

#[cfg(feature = "v8_runtime")]
fn query_json_rows_with_params(sql: &str, params: Vec<Value>) -> Result<Value, String> {
    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    let wrapped_sql =
        format!("SELECT COALESCE(jsonb_agg(to_jsonb(q)), '[]'::jsonb) FROM ({}) q", sql);

    let rows = Spi::get_one_with_args::<JsonB>(&wrapped_sql, &args)
        .map_err(|e| format!("db.query SPI error: {e}"))?
        .map(|v| v.0)
        .unwrap_or_else(|| json!([]));

    Ok(rows)
}

#[cfg(feature = "v8_runtime")]
fn exec_sql_with_params(sql: &str, params: Vec<Value>) -> Result<Value, String> {
    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    Spi::run_with_args(sql, &args).map_err(|e| format!("db.exec SPI error: {e}"))?;
    Ok(json!({ "ok": true }))
}

fn load_function_program(fn_oid: pg_sys::Oid) -> Option<FunctionProgram> {
    let sql = format!(
        "
        SELECT n.nspname::text AS fn_schema,
               p.proname::text AS fn_name,
               p.prosrc::text AS prosrc
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.oid = {}
        ",
        fn_oid
    );

    let row = Spi::connect(|client| {
        let mut rows = client.select(&sql, None, &[])?;
        if let Some(row) = rows.next() {
            let schema = row.get_by_name::<String, _>("fn_schema")?.unwrap_or_default();
            let name = row.get_by_name::<String, _>("fn_name")?.unwrap_or_default();
            let prosrc = row.get_by_name::<String, _>("prosrc")?.unwrap_or_default();
            Ok::<Option<(String, String, String)>, pgrx::spi::Error>(Some((schema, name, prosrc)))
        } else {
            Ok::<Option<(String, String, String)>, pgrx::spi::Error>(None)
        }
    })
    .ok()
    .flatten()?;

    let source = resolve_program_source(&row.2)?;
    Some(FunctionProgram { oid: fn_oid, schema: row.0, name: row.1, source })
}

fn resolve_program_source(prosrc: &str) -> Option<String> {
    if let Some(ptr) = parse_artifact_ptr(prosrc) {
        let sql = format!(
            "SELECT compiled_js FROM plts.artifact WHERE artifact_hash = {}",
            quote_literal(&ptr.artifact_hash)
        );
        return Spi::get_one::<String>(&sql).ok().flatten();
    }

    Some(prosrc.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArtifactPtr {
    artifact_hash: String,
}

fn parse_artifact_ptr(prosrc: &str) -> Option<ArtifactPtr> {
    let parsed = serde_json::from_str::<Value>(prosrc).ok()?;
    let kind = parsed.get("kind")?.as_str()?;
    if kind != "artifact_ptr" {
        return None;
    }

    let artifact_hash = parsed.get("artifact_hash")?.as_str()?.to_string();
    if artifact_hash.is_empty() {
        return None;
    }

    Some(ArtifactPtr { artifact_hash })
}

#[pg_guard]
pub unsafe extern "C-unwind" fn plts_validator(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    pg_sys::Datum::from(0)
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
    let mut hasher = Sha256::new();
    hasher.update(compiler_fingerprint.as_bytes());
    hasher.update([0]);
    hasher.update(source_ts.as_bytes());
    hasher.update([0]);
    hasher.update(compiled_js.as_bytes());
    hasher.update([0]);
    hasher.update(compiler_opts.to_string().as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn compiler_fingerprint() -> &'static str {
    TS_COMPILER_FINGERPRINT
        .get_or_init(|| {
            let deno_ast = dependency_version_from_lock("deno_ast").unwrap_or("unknown");
            let deno_core = dependency_version_from_lock("deno_core").unwrap_or("disabled");
            format!("deno_ast@{};deno_core@{}", deno_ast, deno_core)
        })
        .as_str()
}

fn dependency_version_from_lock(crate_name: &str) -> Option<&'static str> {
    let mut in_package = false;
    for line in CARGO_LOCK_CONTENT.lines() {
        let trimmed = line.trim();

        if trimmed == "[[package]]" {
            in_package = false;
            continue;
        }

        if let Some(name) = trimmed.strip_prefix("name = ") {
            in_package = name.trim_matches('"') == crate_name;
            continue;
        }

        if in_package {
            if let Some(version) = trimmed.strip_prefix("version = ") {
                return Some(version.trim_matches('"'));
            }
            if trimmed.starts_with("checksum = ") {
                in_package = false;
            }
        }
    }

    None
}

fn transpile_typescript(source_ts: &str, compiler_opts: &Value) -> (String, Value) {
    let source_map = compiler_opts.get("source_map").and_then(Value::as_bool).unwrap_or(false);

    let specifier = ModuleSpecifier::parse("file:///plts_module.ts")
        .expect("static module specifier must parse");

    let parsed = deno_ast::parse_module(ParseParams {
        specifier,
        text: source_ts.to_string().into(),
        media_type: MediaType::TypeScript,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    });

    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(err) => {
            let diagnostics = json!([diagnostic_from_message("error", &err.to_string())]);
            return (String::new(), diagnostics);
        }
    };

    let transpiled = parsed.transpile(
        &TranspileOptions::default(),
        &TranspileModuleOptions::default(),
        &EmitOptions {
            source_map: if source_map { SourceMapOption::Inline } else { SourceMapOption::None },
            inline_sources: source_map,
            ..Default::default()
        },
    );

    match transpiled {
        Ok(result) => (result.into_source().text, json!([])),
        Err(err) => {
            let diagnostics = json!([diagnostic_from_message("error", &err.to_string())]);
            (String::new(), diagnostics)
        }
    }
}

fn diagnostic_from_message(severity: &str, message: &str) -> Value {
    let mut line = Value::Null;
    let mut column = Value::Null;
    if let Some((parsed_line, parsed_column)) = extract_line_column(message) {
        line = json!(parsed_line);
        column = json!(parsed_column);
    }

    json!({
        "severity": severity,
        "message": message,
        "line": line,
        "column": column
    })
}

fn extract_line_column(message: &str) -> Option<(u32, u32)> {
    let open = message.rfind('(')?;
    let close = message[open..].find(')')? + open;
    let coords = &message[(open + 1)..close];
    let mut pieces = coords.rsplitn(3, ':');
    let col = pieces.next()?.parse::<u32>().ok()?;
    let line = pieces.next()?.parse::<u32>().ok()?;
    Some((line, col))
}

fn maybe_extract_source_map(compiled_js: &str, compiler_opts: &Value) -> Option<String> {
    let source_map_enabled =
        compiler_opts.get("source_map").and_then(Value::as_bool).unwrap_or(false);
    if !source_map_enabled {
        return None;
    }

    extract_inline_source_map(compiled_js)
}

fn extract_inline_source_map(compiled_js: &str) -> Option<String> {
    const SOURCE_MAP_PREFIX: &str = "//# sourceMappingURL=data:application/json;base64,";

    let marker = compiled_js.rfind(SOURCE_MAP_PREFIX)?;
    let encoded = compiled_js[(marker + SOURCE_MAP_PREFIX.len())..].lines().next()?.trim();
    if encoded.is_empty() {
        return None;
    }

    let decoded = base64::engine::general_purpose::STANDARD.decode(encoded).ok()?;
    String::from_utf8(decoded).ok()
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

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
    use deno_core::{op2, serde_v8, v8, JsRuntime, RuntimeOptions};

    #[op2]
    #[serde]
    fn op_plts_db_query(
        #[string] sql: String,
        #[serde] params: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, deno_error::JsErrorBox> {
        query_json_rows_with_params(&sql, params).map_err(|e| deno_error::JsErrorBox::generic(e))
    }

    #[op2]
    #[serde]
    fn op_plts_db_exec(
        #[string] sql: String,
        #[serde] params: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, deno_error::JsErrorBox> {
        exec_sql_with_params(&sql, params).map_err(|e| deno_error::JsErrorBox::generic(e))
    }

    deno_core::extension!(plts_runtime_ext, ops = [op_plts_db_query, op_plts_db_exec]);

    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init()],
        ..Default::default()
    });
    let rewritten = rewrite_default_export(source)?;

    runtime
        .execute_script("plts_module.js", rewritten)
        .map_err(|e| format_js_error("module evaluation", &e.to_string()))?;

    let context_json = serde_json::to_string(context).map_err(|e| {
        RuntimeExecError::new(
            "context serialize",
            format!("failed to serialize runtime context: {e}"),
        )
    })?;
    let set_ctx_script = format!(
        "globalThis.__plts_ctx = JSON.parse({});\
         globalThis.__plts_ctx.db = {{\
           mode: 'rw',\
           query: (sql, params = []) => Deno.core.ops.op_plts_db_query(sql, params),\
           exec: (sql, params = []) => Deno.core.ops.op_plts_db_exec(sql, params)\
         }};",
        serde_json::to_string(&context_json).map_err(|e| {
            RuntimeExecError::new(
                "context encode",
                format!("failed to encode runtime context string: {e}"),
            )
        })?
    );

    runtime
        .execute_script("plts_ctx.js", set_ctx_script)
        .map_err(|e| format_js_error("context setup", &e.to_string()))?;

    let invoke_script = r#"
        if (typeof globalThis.__plts_default !== "function") {
            throw new Error("default export must be a function");
        }
        const __plts_out = globalThis.__plts_default(globalThis.__plts_ctx);
        if (__plts_out && typeof __plts_out.then === "function") {
            throw new Error("async default export is not supported yet");
        }
        if (__plts_out === undefined || __plts_out === null) {
            globalThis.__plts_result_json = null;
        } else {
            globalThis.__plts_result_json = JSON.stringify(__plts_out);
        }
    "#;

    runtime
        .execute_script("plts_invoke.js", invoke_script)
        .map_err(|e| format_js_error("entrypoint invocation", &e.to_string()))?;

    let value = runtime
        .execute_script("plts_result.js", "globalThis.__plts_result_json")
        .map_err(|e| format_js_error("result extraction", &e.to_string()))?;

    deno_core::scope!(scope, runtime);
    let local = v8::Local::new(scope, value);
    let maybe_json = serde_v8::from_v8::<Option<String>>(scope, local).map_err(|e| {
        RuntimeExecError::new("result decode", format!("failed to decode JS result string: {e}"))
    })?;

    match maybe_json {
        None => Ok(None),
        Some(raw) => {
            let value = serde_json::from_str::<Value>(&raw).map_err(|e| {
                RuntimeExecError::new("result parse", format!("failed to decode JSON result: {e}"))
            })?;
            if value.is_null() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
    }
}

#[cfg(not(feature = "v8_runtime"))]
fn execute_program(_source: &str, _context: &Value) -> Result<Option<Value>, RuntimeExecError> {
    Err(RuntimeExecError::new("runtime bootstrap", "v8_runtime feature is disabled"))
}

#[cfg(any(test, feature = "v8_runtime"))]
fn rewrite_default_export(source: &str) -> Result<String, RuntimeExecError> {
    let token = "export default";
    if let Some(idx) = source.find(token) {
        let mut rewritten = String::with_capacity(source.len() + 32);
        rewritten.push_str(&source[..idx]);
        rewritten.push_str("globalThis.__plts_default =");
        rewritten.push_str(&source[idx + token.len()..]);
        Ok(rewritten)
    } else {
        Err(RuntimeExecError::new("module rewrite", "module must include `export default`"))
    }
}

#[cfg(feature = "v8_runtime")]
fn format_js_error(stage: &'static str, details: &str) -> RuntimeExecError {
    let (message, stack) = parse_js_error_details(details);
    RuntimeExecError::with_stack(stage, message, stack)
}

#[cfg(test)]
mod tests {
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
        let ptr = crate::parse_artifact_ptr(
            r#"{"plts":1,"kind":"artifact_ptr","artifact_hash":"sha256:abc"}"#,
        )
        .expect("expected pointer metadata");
        assert_eq!(ptr.artifact_hash, "sha256:abc");
    }

    #[test]
    fn test_rewrite_default_export() {
        let src = "export default (ctx) => ({ ok: true, args: ctx.args })";
        let rewritten = crate::rewrite_default_export(src).expect("rewrite should succeed");
        assert!(rewritten.contains("globalThis.__plts_default ="));
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
        let version = crate::dependency_version_from_lock("serde_json");
        assert!(version.is_some());
    }

    #[test]
    fn test_extract_inline_source_map_decodes_payload() {
        let compiled =
            "console.log('x');\n//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozfQ==";
        let source_map = crate::extract_inline_source_map(compiled)
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
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
