use base64::Engine as Base64Engine;
use deno_ast::EmitOptions;
use deno_ast::MediaType;
use deno_ast::ModuleSpecifier;
use deno_ast::ParseParams;
use deno_ast::SourceMapOption;
use deno_ast::TranspileModuleOptions;
use deno_ast::TranspileOptions;
use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use wasmtime::{Engine as WasmtimeEngine, Linker, Module, Store};
use wasmtime_wasi::I32Exit;
use wasmtime_wasi::WasiCtxBuilder;
use wasmtime_wasi::pipe::{MemoryInputPipe, MemoryOutputPipe};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};

const CARGO_LOCK_CONTENT: &str = include_str!("../../../Cargo.lock");
const STOPGAP_TSGO_API_WASM: &[u8] =
    include_bytes!("../../../third_party/stopgap-tsgo-api/dist/stopgap-tsgo-api.wasm");
static TS_COMPILER_FINGERPRINT: OnceLock<String> = OnceLock::new();
static TSGO_WASM_RUNTIME: OnceLock<Result<TsgoWasmRuntime, String>> = OnceLock::new();
static TSGO_TRANSPILE_ENABLED: OnceLock<bool> = OnceLock::new();

#[derive(serde::Serialize)]
struct TsgoTypecheckRequest<'a> {
    source_ts: &'a str,
}

#[derive(serde::Serialize)]
struct TsgoTranspileRequest<'a> {
    source_ts: &'a str,
    source_map: bool,
}

#[derive(serde::Deserialize)]
struct TsgoDiagnostic {
    severity: String,
    #[serde(default)]
    phase: Option<String>,
    message: String,
    #[serde(default)]
    line: Option<u32>,
    #[serde(default)]
    column: Option<u32>,
}

#[derive(serde::Deserialize)]
struct TsgoTypecheckResponse {
    diagnostics: Vec<TsgoDiagnostic>,
}

#[derive(serde::Deserialize)]
struct TsgoTranspileResponse {
    compiled_js: String,
    diagnostics: Vec<TsgoDiagnostic>,
}

struct TsgoWasmRuntime {
    engine: WasmtimeEngine,
    module: Module,
}

pub(crate) fn compute_artifact_hash(
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

pub(crate) fn compiler_fingerprint() -> &'static str {
    TS_COMPILER_FINGERPRINT
        .get_or_init(|| {
            let deno_ast = dependency_version_from_lock("deno_ast").unwrap_or("unknown");
            let deno_core = dependency_version_from_lock("deno_core").unwrap_or("disabled");
            let tsgo_api_wasm_hash = hex::encode(Sha256::digest(tsgo_api_wasm_bytes()));
            format!(
                "deno_ast@{};deno_core@{};tsgo_api_wasm_sha256@{}",
                deno_ast, deno_core, tsgo_api_wasm_hash
            )
        })
        .as_str()
}

pub(crate) fn tsgo_api_wasm_bytes() -> &'static [u8] {
    STOPGAP_TSGO_API_WASM
}

pub(crate) fn dependency_version_from_lock(crate_name: &str) -> Option<&'static str> {
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

pub(crate) fn transpile_typescript(source_ts: &str, compiler_opts: &Value) -> (String, Value) {
    if tsgo_transpile_enabled() {
        if let Ok((compiled_js, diagnostics)) =
            transpile_typescript_via_tsgo_wasm(source_ts, compiler_opts)
        {
            if !contains_error_diagnostics(&diagnostics) && !compiled_js.is_empty() {
                return (compiled_js, diagnostics);
            }
        }
    }

    transpile_typescript_via_deno_ast(source_ts, compiler_opts)
}

fn transpile_typescript_via_deno_ast(source_ts: &str, compiler_opts: &Value) -> (String, Value) {
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

fn transpile_typescript_via_tsgo_wasm(
    source_ts: &str,
    compiler_opts: &Value,
) -> Result<(String, Value), String> {
    let source_map = compiler_opts.get("source_map").and_then(Value::as_bool).unwrap_or(false);
    let request_json = serde_json::to_vec(&TsgoTranspileRequest { source_ts, source_map })
        .map_err(|err| format!("failed to encode tsgo transpile request: {err}"))?;

    let stdout_bytes = execute_tsgo_wasm_command("transpile", request_json)?;
    let decoded: TsgoTranspileResponse = serde_json::from_slice(&stdout_bytes)
        .map_err(|err| format!("failed to decode tsgo transpile response: {err}"))?;

    let diagnostics =
        decoded.diagnostics.into_iter().map(tsgo_diagnostic_to_json).collect::<Vec<_>>();

    Ok((decoded.compiled_js, Value::Array(diagnostics)))
}

pub(crate) fn semantic_typecheck_typescript(source_ts: &str) -> Value {
    semantic_typecheck_typescript_via_tsgo_wasm(source_ts).unwrap_or_else(|err| {
        json!([diagnostic_from_message(
            "error",
            &format!("failed to execute TypeScript checker: {err}"),
        )])
    })
}

fn semantic_typecheck_typescript_via_tsgo_wasm(source_ts: &str) -> Result<Value, String> {
    let request_json = serde_json::to_vec(&TsgoTypecheckRequest { source_ts })
        .map_err(|err| format!("failed to encode tsgo typecheck request: {err}"))?;

    let stdout_bytes = execute_tsgo_wasm_command("typecheck", request_json)?;

    let decoded: TsgoTypecheckResponse = serde_json::from_slice(&stdout_bytes)
        .map_err(|err| format!("failed to decode tsgo typecheck response: {err}"))?;

    let diagnostics =
        decoded.diagnostics.into_iter().map(tsgo_diagnostic_to_json).collect::<Vec<_>>();

    Ok(Value::Array(diagnostics))
}

fn execute_tsgo_wasm_command(command: &str, request_json: Vec<u8>) -> Result<Vec<u8>, String> {
    let runtime = tsgo_wasm_runtime()?;

    let stdout = MemoryOutputPipe::new(1024 * 1024);
    let stderr = MemoryOutputPipe::new(128 * 1024);

    let mut linker = Linker::new(&runtime.engine);
    preview1::add_to_linker_sync(&mut linker, |ctx: &mut WasiP1Ctx| ctx)
        .map_err(|err| format!("failed to wire tsgo wasi linker: {err}"))?;

    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.args(&["stopgap-tsgo-api", command]);

    let wasi = wasi_builder
        .stdin(MemoryInputPipe::new(request_json))
        .stdout(stdout.clone())
        .stderr(stderr.clone())
        .build_p1();

    let mut store = Store::new(&runtime.engine, wasi);

    let instance = linker
        .instantiate(&mut store, &runtime.module)
        .map_err(|err| format!("failed to instantiate tsgo wasm module: {err}"))?;

    let start = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .map_err(|err| format!("failed to locate tsgo wasm _start export: {err}"))?;
    if let Err(err) = start.call(&mut store, ()) {
        if err.downcast_ref::<I32Exit>().map(|exit| exit.0) != Some(0) {
            return Err(format!("failed to execute tsgo wasm `{command}` command: {err}"));
        }
    }

    let stdout_bytes = stdout.contents();

    let stderr_bytes = stderr.contents();
    if !stderr_bytes.is_empty() {
        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        return Err(format!("tsgo wasm stderr output: {stderr_text}"));
    }

    Ok(stdout_bytes.to_vec())
}

fn tsgo_diagnostic_to_json(diag: TsgoDiagnostic) -> Value {
    json!({
        "severity": diag.severity,
        "phase": diag.phase,
        "message": diag.message,
        "line": diag.line,
        "column": diag.column,
    })
}

fn tsgo_wasm_runtime() -> Result<&'static TsgoWasmRuntime, String> {
    let runtime = TSGO_WASM_RUNTIME.get_or_init(|| {
        let engine = WasmtimeEngine::default();
        let module = Module::new(&engine, tsgo_api_wasm_bytes())
            .map_err(|err| format!("failed to compile embedded tsgo wasm module: {err}"))?;
        Ok(TsgoWasmRuntime { engine, module })
    });

    match runtime {
        Ok(runtime) => Ok(runtime),
        Err(err) => Err(err.clone()),
    }
}

fn tsgo_transpile_enabled() -> bool {
    *TSGO_TRANSPILE_ENABLED.get_or_init(|| {
        std::env::var("PLTS_EXPERIMENTAL_TSGO_TRANSPILE")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

pub(crate) fn contains_error_diagnostics(diagnostics: &Value) -> bool {
    diagnostics
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .any(|entry| entry.get("severity").and_then(|v| v.as_str()) == Some("error"))
        })
        .unwrap_or(false)
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

#[cfg(test)]
mod tests {
    use super::{semantic_typecheck_typescript_via_tsgo_wasm, tsgo_api_wasm_bytes};

    #[test]
    fn embeds_tsgo_wasm_artifact() {
        let wasm = tsgo_api_wasm_bytes();
        assert!(wasm.len() > 8, "embedded tsgo wasm must not be empty");
        assert_eq!(&wasm[0..4], b"\0asm", "embedded tsgo payload must be wasm");
    }

    #[test]
    fn tsgo_wasm_typecheck_reports_app_import_diagnostic() {
        let diagnostics = semantic_typecheck_typescript_via_tsgo_wasm(
            "import { base } from '@app/math';\nexport default () => base;",
        )
        .expect("tsgo wasm typecheck call should succeed");

        let entries = diagnostics.as_array().expect("diagnostics must be an array");
        assert!(!entries.is_empty(), "@app import should produce diagnostics");
        let first = &entries[0];
        let message =
            first.get("message").and_then(|value| value.as_str()).expect("message string");
        assert!(message.contains("unsupported bare module import `@app/math`"));
    }
}

pub(crate) fn maybe_extract_source_map(compiled_js: &str, compiler_opts: &Value) -> Option<String> {
    let source_map_enabled =
        compiler_opts.get("source_map").and_then(Value::as_bool).unwrap_or(false);
    if !source_map_enabled {
        return None;
    }

    extract_inline_source_map(compiled_js)
}

pub(crate) fn extract_inline_source_map(compiled_js: &str) -> Option<String> {
    const SOURCE_MAP_PREFIX: &str = "//# sourceMappingURL=data:application/json;base64,";

    let marker = compiled_js.rfind(SOURCE_MAP_PREFIX)?;
    let encoded = compiled_js[(marker + SOURCE_MAP_PREFIX.len())..].lines().next()?.trim();
    if encoded.is_empty() {
        return None;
    }

    let decoded = base64::engine::general_purpose::STANDARD.decode(encoded).ok()?;
    String::from_utf8(decoded).ok()
}
