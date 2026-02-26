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
#[cfg(feature = "v8_runtime")]
use std::rc::Rc;
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
fn query_json_rows_with_params(
    sql: &str,
    params: Vec<Value>,
    read_only: bool,
) -> Result<Value, String> {
    if read_only && !is_read_only_sql(sql) {
        return Err(
            "db.query is read-only for stopgap.query handlers; use a SELECT-only statement"
                .to_string(),
        );
    }

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
fn exec_sql_with_params(sql: &str, params: Vec<Value>, read_only: bool) -> Result<Value, String> {
    if read_only {
        return Err("db.exec is disabled for stopgap.query handlers; switch to stopgap.mutation"
            .to_string());
    }

    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    Spi::run_with_args(sql, &args).map_err(|e| format!("db.exec SPI error: {e}"))?;
    Ok(json!({ "ok": true }))
}

#[cfg(feature = "v8_runtime")]
fn is_read_only_sql(sql: &str) -> bool {
    let normalized = strip_leading_sql_comments(sql).to_ascii_lowercase();
    if !(normalized.starts_with("select") || normalized.starts_with("with")) {
        return false;
    }

    let forbidden = [
        "insert", "update", "delete", "merge", "create", "alter", "drop", "truncate", "grant",
        "revoke", "vacuum", "analyze", "reindex", "cluster", "call", "copy",
    ];

    let mut token = String::new();
    for ch in normalized.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch);
            continue;
        }

        if !token.is_empty() {
            if forbidden.contains(&token.as_str()) {
                return false;
            }
            token.clear();
        }
    }

    if !token.is_empty() && forbidden.contains(&token.as_str()) {
        return false;
    }

    true
}

#[cfg(feature = "v8_runtime")]
fn strip_leading_sql_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if let Some(line_comment) = rest.strip_prefix("--") {
            if let Some(newline_idx) = line_comment.find('\n') {
                rest = line_comment[(newline_idx + 1)..].trim_start();
                continue;
            }
            return "";
        }

        if let Some(block_comment) = rest.strip_prefix("/*") {
            if let Some(end_idx) = block_comment.find("*/") {
                rest = block_comment[(end_idx + 2)..].trim_start();
                continue;
            }
            return "";
        }

        return rest;
    }
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

    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![plts_runtime_ext::init()],
        module_loader: Some(Rc::new(PltsModuleLoader)),
        ..Default::default()
    });

    let main_specifier = ModuleSpecifier::parse(MAIN_MODULE_SPECIFIER).map_err(|err| {
        RuntimeExecError::new(
            "module bootstrap",
            format!("invalid main module specifier `{MAIN_MODULE_SPECIFIER}`: {err}"),
        )
    })?;

    let module_id = deno_core::futures::executor::block_on(
        runtime.load_main_es_module_from_code(&main_specifier, source.to_string()),
    )
    .map_err(|e| format_js_error("module load", &e.to_string()))?;

    let module_result = runtime.mod_evaluate(module_id);
    deno_core::futures::executor::block_on(async {
        runtime.run_event_loop(PollEventLoopOptions::default()).await?;
        module_result.await
    })
    .map_err(|e| format_js_error("module evaluation", &e.to_string()))?;

    {
        let namespace = runtime
            .get_module_namespace(module_id)
            .map_err(|e| format_js_error("module namespace", &e.to_string()))?;

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
            .map_err(|e| format_js_error("handler metadata", &e.to_string()))?;

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
           query: (sql, params = []) => Deno.core.ops.op_plts_db_query(sql, params, {}),\
           exec: (sql, params = []) => Deno.core.ops.op_plts_db_exec(sql, params, {})\
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
        .map_err(|e| format_js_error("context setup", &e.to_string()))?;

    let invoke_script = r#"
        if (typeof globalThis.__plts_default !== "function") {
            throw new Error("default export must be a function");
        }
        globalThis.__plts_default(globalThis.__plts_ctx);
    "#;

    let value = runtime
        .execute_script("plts_invoke.js", invoke_script)
        .map_err(|e| format_js_error("entrypoint invocation", &e.to_string()))?;

    #[allow(deprecated)]
    let value = deno_core::futures::executor::block_on(runtime.resolve_value(value))
        .map_err(|e| format_js_error("entrypoint await", &e.to_string()))?;

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
        let ptr = crate::parse_artifact_ptr(
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
    use super::*;

    #[pg_test]
    fn test_compile_and_store_round_trip() {
        let source = "export default (ctx) => ({ ok: true, args: ctx.args })";
        let artifact_hash = Spi::get_one_with_args::<String>(
            "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
            &[source.into()],
        )
        .expect("compile_and_store query should succeed")
        .expect("compile_and_store should return an artifact hash");

        assert!(artifact_hash.starts_with("sha256:"));

        let artifact = Spi::get_one_with_args::<JsonB>(
            "SELECT plts.get_artifact($1)",
            &[artifact_hash.into()],
        )
        .expect("get_artifact query should succeed")
        .expect("artifact must exist after compile_and_store");

        assert_eq!(
            artifact.0.get("source_ts").and_then(Value::as_str),
            Some(source),
            "stored artifact should preserve source_ts"
        );
        assert!(
            artifact
                .0
                .get("compiled_js")
                .and_then(Value::as_str)
                .is_some_and(|compiled| !compiled.is_empty()),
            "stored artifact should include compiled_js"
        );
    }

    #[pg_test]
    fn test_regular_args_conversion_for_common_types() {
        Spi::run(
            "
            DROP SCHEMA IF EXISTS plts_it CASCADE;
            CREATE SCHEMA plts_it;
            CREATE OR REPLACE FUNCTION plts_it.arg_echo(t text, i int4, b boolean, j jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            export default () => null;
            $$;
            ",
        )
        .expect("test setup SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "
            SELECT plts_it.arg_echo('hello', 42, true, '{\"ok\": true}'::jsonb)
            ",
        )
        .expect("arg_echo query should succeed")
        .expect("arg_echo should return a json payload in non-runtime mode");

        assert_eq!(
            payload
                .0
                .get("positional")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_str),
            Some("hello")
        );
        assert_eq!(
            payload
                .0
                .get("positional")
                .and_then(Value::as_array)
                .and_then(|items| items.get(1))
                .and_then(Value::as_i64),
            Some(42)
        );
        assert_eq!(
            payload
                .0
                .get("positional")
                .and_then(Value::as_array)
                .and_then(|items| items.get(2))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .0
                .get("positional")
                .and_then(Value::as_array)
                .and_then(|items| items.get(3))
                .and_then(|json| json.get("ok"))
                .and_then(Value::as_bool),
            Some(true)
        );

        assert_eq!(
            payload.0.get("named").and_then(|named| named.get("0")).and_then(Value::as_str),
            Some("hello")
        );
        assert_eq!(
            payload.0.get("named").and_then(|named| named.get("1")).and_then(Value::as_i64),
            Some(42)
        );
        assert_eq!(
            payload.0.get("named").and_then(|named| named.get("2")).and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .0
                .get("named")
                .and_then(|named| named.get("3"))
                .and_then(|json| json.get("ok"))
                .and_then(Value::as_bool),
            Some(true)
        );

        Spi::run("DROP SCHEMA IF EXISTS plts_it CASCADE;")
            .expect("test teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_runtime_normalizes_null_and_undefined_to_sql_null() {
        Spi::run(
            "
            DROP SCHEMA IF EXISTS plts_runtime_it CASCADE;
            CREATE SCHEMA plts_runtime_it;
            CREATE OR REPLACE FUNCTION plts_runtime_it.return_null(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            export default () => null;
            $$;
            CREATE OR REPLACE FUNCTION plts_runtime_it.return_undefined(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            export default () => undefined;
            $$;
            CREATE OR REPLACE FUNCTION plts_runtime_it.return_object(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            export default () => ({ ok: true });
            $$;
            ",
        )
        .expect("runtime null-normalization setup SQL should succeed");

        let null_is_sql_null =
            Spi::get_one::<bool>("SELECT plts_runtime_it.return_null('{}'::jsonb) IS NULL")
                .expect("return_null query should succeed")
                .expect("return_null IS NULL predicate should return a row");
        assert!(null_is_sql_null, "runtime should map JS null to SQL NULL");

        let undefined_is_sql_null =
            Spi::get_one::<bool>("SELECT plts_runtime_it.return_undefined('{}'::jsonb) IS NULL")
                .expect("return_undefined query should succeed")
                .expect("return_undefined IS NULL predicate should return a row");
        assert!(undefined_is_sql_null, "runtime should map JS undefined to SQL NULL");

        let object = Spi::get_one::<JsonB>("SELECT plts_runtime_it.return_object('{}'::jsonb)")
            .expect("return_object query should succeed")
            .expect("return_object should return jsonb for non-null result");
        assert_eq!(object.0.get("ok").and_then(Value::as_bool), Some(true));

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_it CASCADE;")
            .expect("runtime null-normalization teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_artifact_pointer_executes_compiled_program() {
        Spi::run(
            "
            DROP SCHEMA IF EXISTS plts_runtime_ptr_it CASCADE;
            CREATE SCHEMA plts_runtime_ptr_it;
            ",
        )
        .expect("artifact-pointer setup schema SQL should succeed");

        let source =
            "export default (ctx) => ({ mode: 'artifact', echoed: ctx.args.positional[0] });";
        let artifact_hash = Spi::get_one_with_args::<String>(
            "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
            &[source.into()],
        )
        .expect("compile_and_store query should succeed")
        .expect("compile_and_store should return artifact hash");

        let pointer = json!({
            "plts": 1,
            "kind": "artifact_ptr",
            "artifact_hash": artifact_hash,
            "export": "default",
            "mode": "stopgap_deployed"
        })
        .to_string()
        .replace('\'', "''");

        let create_sql = format!(
            "
            CREATE OR REPLACE FUNCTION plts_runtime_ptr_it.ptr_fn(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$ {} $$;
            ",
            pointer
        );
        Spi::run(create_sql.as_str()).expect("pointer function creation SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "SELECT plts_runtime_ptr_it.ptr_fn('{\"id\": 42, \"tag\": \"ok\"}'::jsonb)",
        )
        .expect("pointer function invocation should succeed")
        .expect("pointer function should return jsonb");

        assert_eq!(payload.0.get("mode").and_then(Value::as_str), Some("artifact"));
        assert_eq!(
            payload.0.get("echoed").and_then(|value| value.get("id")).and_then(Value::as_i64),
            Some(42)
        );

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_ptr_it CASCADE;")
            .expect("artifact-pointer teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_runtime_supports_async_default_export() {
        Spi::run(
            "
            DROP SCHEMA IF EXISTS plts_runtime_async_it CASCADE;
            CREATE SCHEMA plts_runtime_async_it;
            CREATE OR REPLACE FUNCTION plts_runtime_async_it.return_async(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            export default async (ctx) => {
                const row = await Promise.resolve({ ok: true, id: ctx.args.id });
                return row;
            };
            $$;
            ",
        )
        .expect("runtime async setup SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "SELECT plts_runtime_async_it.return_async('{\"id\": 7}'::jsonb)",
        )
        .expect("async function invocation should succeed")
        .expect("async function should return jsonb");

        assert_eq!(payload.0.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(7));

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_async_it CASCADE;")
            .expect("runtime async teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_runtime_supports_module_imports_via_data_url() {
        Spi::run(
            "
            DROP SCHEMA IF EXISTS plts_runtime_module_it CASCADE;
            CREATE SCHEMA plts_runtime_module_it;
            CREATE OR REPLACE FUNCTION plts_runtime_module_it.imported(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { imported } from \"data:text/javascript;base64,ZXhwb3J0IGNvbnN0IGltcG9ydGVkID0gOTs=\";
            export default (ctx) => ({ imported, id: ctx.args.id });
            $$;
            ",
        )
        .expect("runtime module import setup SQL should succeed");

        let payload =
            Spi::get_one::<JsonB>("SELECT plts_runtime_module_it.imported('{\"id\": 11}'::jsonb)")
                .expect("imported function invocation should succeed")
                .expect("imported function should return jsonb");

        assert_eq!(payload.0.get("imported").and_then(Value::as_i64), Some(9));
        assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(11));

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_it CASCADE;")
            .expect("runtime module import teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_runtime_supports_stopgap_runtime_bare_import() {
        Spi::run(
            r#"
            DROP SCHEMA IF EXISTS plts_runtime_stopgap_import_it CASCADE;
            CREATE SCHEMA plts_runtime_stopgap_import_it;
            CREATE OR REPLACE FUNCTION plts_runtime_stopgap_import_it.wrapped(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { query } from "@stopgap/runtime";

            export default query({ type: "object" }, async (args, ctx) => ({
                kind: "query",
                id: args.id,
                dbMode: ctx.db.mode,
            }));
            $$;
            "#,
        )
        .expect("runtime stopgap import setup SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "SELECT plts_runtime_stopgap_import_it.wrapped('{\"id\": 13}'::jsonb)",
        )
        .expect("wrapped function invocation should succeed")
        .expect("wrapped function should return jsonb");

        assert_eq!(payload.0.get("kind").and_then(Value::as_str), Some("query"));
        assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(13));
        assert_eq!(payload.0.get("dbMode").and_then(Value::as_str), Some("ro"));

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_import_it CASCADE;")
            .expect("runtime stopgap import teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_stopgap_query_wrapper_rejects_db_exec() {
        Spi::run(
            r#"
            DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_exec_it CASCADE;
            CREATE SCHEMA plts_runtime_stopgap_query_exec_it;
            CREATE OR REPLACE FUNCTION plts_runtime_stopgap_query_exec_it.wrapped(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { query } from "@stopgap/runtime";

            export default query({ type: "object" }, async (_args, ctx) => {
                await ctx.db.exec("SELECT 1", []);
                return { ok: true };
            });
            $$;
            "#,
        )
        .expect("stopgap query exec rejection setup SQL should succeed");

        Spi::run(
            r#"
            DO $$
            BEGIN
                PERFORM plts_runtime_stopgap_query_exec_it.wrapped('{}'::jsonb);
                RAISE EXCEPTION 'expected db.exec rejection for query wrapper';
            EXCEPTION
                WHEN OTHERS THEN
                    IF POSITION('db.exec is disabled for stopgap.query handlers' IN SQLERRM) = 0 THEN
                        RAISE;
                    END IF;
            END;
            $$;
            "#,
        )
        .expect("query wrapper should reject db.exec");

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_exec_it CASCADE;")
            .expect("stopgap query exec rejection teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_stopgap_query_wrapper_validates_json_schema() {
        Spi::run(
            r#"
            DROP SCHEMA IF EXISTS plts_runtime_stopgap_schema_it CASCADE;
            CREATE SCHEMA plts_runtime_stopgap_schema_it;
            CREATE OR REPLACE FUNCTION plts_runtime_stopgap_schema_it.wrapped(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { query } from "@stopgap/runtime";

            const schema = {
                type: "object",
                required: ["id"],
                additionalProperties: false,
                properties: {
                    id: { type: "integer" }
                }
            };

            export default query(schema, async (args, _ctx) => ({ id: args.id }));
            $$;
            "#,
        )
        .expect("stopgap schema validation setup SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "SELECT plts_runtime_stopgap_schema_it.wrapped('{\"id\": 22}'::jsonb)",
        )
        .expect("wrapped function invocation should succeed")
        .expect("wrapped function should return jsonb");

        assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(22));

        Spi::run(
            r#"
            DO $$
            BEGIN
                PERFORM plts_runtime_stopgap_schema_it.wrapped('{}'::jsonb);
                RAISE EXCEPTION 'expected schema validation failure for missing id';
            EXCEPTION
                WHEN OTHERS THEN
                    IF POSITION('args validation failed at $.id: missing required property' IN SQLERRM) = 0 THEN
                        RAISE;
                    END IF;
            END;
            $$;
            "#,
        )
        .expect("query wrapper should reject invalid args schema payload");

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_schema_it CASCADE;")
            .expect("stopgap schema validation teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_stopgap_query_wrapper_rejects_write_sql_in_db_query() {
        Spi::run(
            r#"
            DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_write_it CASCADE;
            CREATE SCHEMA plts_runtime_stopgap_query_write_it;
            CREATE TABLE plts_runtime_stopgap_query_write_it.items(id int4);
            CREATE OR REPLACE FUNCTION plts_runtime_stopgap_query_write_it.wrapped(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { query } from "@stopgap/runtime";

            export default query({ type: "object" }, async (_args, ctx) => {
                await ctx.db.query("WITH w AS (INSERT INTO plts_runtime_stopgap_query_write_it.items(id) VALUES (1) RETURNING id) SELECT id FROM w", []);
                return { ok: true };
            });
            $$;
            "#,
        )
        .expect("stopgap query write rejection setup SQL should succeed");

        Spi::run(
            r#"
            DO $$
            BEGIN
                PERFORM plts_runtime_stopgap_query_write_it.wrapped('{}'::jsonb);
                RAISE EXCEPTION 'expected write SQL rejection for query wrapper';
            EXCEPTION
                WHEN OTHERS THEN
                    IF POSITION('db.query is read-only for stopgap.query handlers' IN SQLERRM) = 0 THEN
                        RAISE;
                    END IF;
            END;
            $$;
            "#,
        )
        .expect("query wrapper should reject write SQL through db.query");

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_write_it CASCADE;")
            .expect("stopgap query write rejection teardown SQL should succeed");
    }

    #[cfg(feature = "v8_runtime")]
    #[pg_test]
    fn test_stopgap_mutation_wrapper_allows_db_exec() {
        Spi::run(
            r#"
            DROP SCHEMA IF EXISTS plts_runtime_stopgap_mutation_it CASCADE;
            CREATE SCHEMA plts_runtime_stopgap_mutation_it;
            CREATE TABLE plts_runtime_stopgap_mutation_it.items(id int4);
            CREATE OR REPLACE FUNCTION plts_runtime_stopgap_mutation_it.wrapped(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $$
            import { mutation } from "@stopgap/runtime";

            export default mutation({ type: "object" }, async (args, ctx) => {
                await ctx.db.exec("INSERT INTO plts_runtime_stopgap_mutation_it.items(id) VALUES ($1)", [args.id]);
                const rows = await ctx.db.query("SELECT id FROM plts_runtime_stopgap_mutation_it.items ORDER BY id", []);
                return { kind: "mutation", dbMode: ctx.db.mode, count: rows.length };
            });
            $$;
            "#,
        )
        .expect("stopgap mutation setup SQL should succeed");

        let payload = Spi::get_one::<JsonB>(
            "SELECT plts_runtime_stopgap_mutation_it.wrapped('{\"id\": 17}'::jsonb)",
        )
        .expect("mutation wrapper invocation should succeed")
        .expect("mutation wrapper should return jsonb");

        assert_eq!(payload.0.get("kind").and_then(Value::as_str), Some("mutation"));
        assert_eq!(payload.0.get("dbMode").and_then(Value::as_str), Some("rw"));
        assert_eq!(payload.0.get("count").and_then(Value::as_i64), Some(1));

        Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_mutation_it CASCADE;")
            .expect("stopgap mutation teardown SQL should succeed");
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
