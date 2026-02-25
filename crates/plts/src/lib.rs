use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt;

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
        diagnostics jsonb
    );

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
        let _opts = compiler_opts.0;
        let diagnostics = JsonB(json!([]));
        let compiler_fingerprint = "v8-deno_core-p0".to_string();
        TableIterator::once((source_ts.to_string(), diagnostics, compiler_fingerprint))
    }

    #[pg_extern]
    fn upsert_artifact(
        source_ts: &str,
        compiled_js: &str,
        compiler_opts: default!(JsonB, "'{}'::jsonb"),
    ) -> String {
        let compiler_fingerprint = "v8-deno_core-p0";
        let hash =
            compute_artifact_hash(source_ts, compiled_js, &compiler_opts.0, compiler_fingerprint);

        let sql = format!(
            "
            INSERT INTO plts.artifact (artifact_hash, source_ts, compiled_js, compiler_opts, compiler_fingerprint)
            VALUES ({}, {}, {}, {}::jsonb, {})
            ON CONFLICT (artifact_hash) DO UPDATE
            SET source_ts = EXCLUDED.source_ts,
                compiled_js = EXCLUDED.compiled_js,
                compiler_opts = EXCLUDED.compiler_opts,
                compiler_fingerprint = EXCLUDED.compiler_fingerprint
            ",
            quote_literal(&hash),
            quote_literal(source_ts),
            quote_literal(compiled_js),
            quote_literal(&compiler_opts.0.to_string()),
            quote_literal(compiler_fingerprint)
        );

        let _ = Spi::run(&sql);

        hash
    }

    #[pg_extern]
    fn compile_and_store(source_ts: &str, compiler_opts: default!(JsonB, "'{}'::jsonb")) -> String {
        let opts = compiler_opts.0;
        let mut rows = compile_ts(source_ts, JsonB(opts.clone()));
        let (compiled_js, _diagnostics, _compiler_fingerprint) =
            rows.next().expect("compile_ts must always return one row");

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
    use deno_core::{serde_v8, v8, JsRuntime, RuntimeOptions};

    let mut runtime = JsRuntime::new(RuntimeOptions::default());
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
        "globalThis.__plts_ctx = JSON.parse({});",
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
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
