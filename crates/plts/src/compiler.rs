use crate::observability::{
    log_info, log_warn, record_tsgo_wasm_cache_event, record_tsgo_wasm_init_start,
    record_tsgo_wasm_init_success,
};
use base64::Engine as Base64Engine;
use directories_next::ProjectDirs;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use wasmtime::{
    Config as WasmtimeConfig, Engine as WasmtimeEngine, Linker, Module, OptLevel,
    RegallocAlgorithm, Store,
};
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
static TSGO_WASM_TEMPFILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(serde::Serialize)]
struct TsgoTypecheckRequest<'a> {
    source_ts: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    declarations: Vec<TsgoVirtualDeclaration>,
}

#[derive(serde::Serialize)]
struct TsgoTranspileRequest<'a> {
    source_ts: &'a str,
    source_map: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    declarations: Vec<TsgoVirtualDeclaration>,
}

#[derive(serde::Serialize)]
struct TsgoVirtualDeclaration {
    file_name: String,
    content: String,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TsgoWasmCacheMode {
    Auto,
    ManualOnly,
    Off,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TsgoWasmCachePaths {
    root: PathBuf,
    wasmtime_config: PathBuf,
    wasmtime_cache_dir: PathBuf,
    manual_dir: PathBuf,
    quarantine_dir: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TsgoWasmEngineProfile {
    wasmtime_version: &'static str,
    opt_level: &'static str,
    regalloc_algorithm: &'static str,
    parallel_compilation: bool,
    target_arch: &'static str,
    target_os: &'static str,
    target_env: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TsgoWasmInitOutcome {
    BuiltInCache,
    ManualHit { artifact_path: PathBuf },
    ManualMiss { artifact_path: PathBuf },
    DirectCompile,
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

fn current_setting_text(name: &str) -> Option<String> {
    let sql = format!("SELECT NULLIF(current_setting('{}', true), '')", name);
    Spi::get_one::<String>(&sql)
        .ok()
        .flatten()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_tsgo_wasm_cache_mode() -> TsgoWasmCacheMode {
    parse_tsgo_wasm_cache_mode(current_setting_text("plts.tsgo_wasm_cache_mode").as_deref())
}

fn parse_tsgo_wasm_cache_mode(raw: Option<&str>) -> TsgoWasmCacheMode {
    match raw.unwrap_or("auto").trim().to_ascii_lowercase().as_str() {
        "manual-only" => TsgoWasmCacheMode::ManualOnly,
        "off" => TsgoWasmCacheMode::Off,
        _ => TsgoWasmCacheMode::Auto,
    }
}

fn read_tsgo_wasm_cache_dir() -> Option<PathBuf> {
    current_setting_text("plts.tsgo_wasm_cache_dir").map(PathBuf::from)
}

fn resolve_tsgo_wasm_cache_paths() -> Result<Option<TsgoWasmCachePaths>, String> {
    let root = resolve_tsgo_wasm_cache_root(
        read_tsgo_wasm_cache_dir().as_deref(),
        ProjectDirs::from("", "Stopgap", "plts").map(|dirs| dirs.cache_dir().to_path_buf()),
        std::env::temp_dir(),
    );
    bootstrap_tsgo_wasm_cache_paths(root).map(Some)
}

fn resolve_tsgo_wasm_cache_root(
    explicit_root: Option<&Path>,
    project_cache_dir: Option<PathBuf>,
    temp_dir: PathBuf,
) -> PathBuf {
    explicit_root
        .map(Path::to_path_buf)
        .or_else(|| project_cache_dir.map(|path| path.join("tsgo-wasm")))
        .unwrap_or_else(|| temp_dir.join("stopgap").join("plts").join("tsgo-wasm"))
}

fn bootstrap_tsgo_wasm_cache_paths(root: PathBuf) -> Result<TsgoWasmCachePaths, String> {
    ensure_directory(&root)?;
    let canonical_root = fs::canonicalize(&root).map_err(|err| {
        format!("failed to canonicalize tsgo cache root `{}`: {err}", root.display())
    })?;
    let paths = TsgoWasmCachePaths {
        root: canonical_root.clone(),
        wasmtime_config: canonical_root.join("wasmtime-config.toml"),
        wasmtime_cache_dir: canonical_root.join("wasmtime-cache"),
        manual_dir: canonical_root.join("manual"),
        quarantine_dir: canonical_root.join("quarantine"),
    };
    ensure_directory(&paths.wasmtime_cache_dir)?;
    ensure_directory(&paths.manual_dir)?;
    ensure_directory(&paths.quarantine_dir)?;
    Ok(paths)
}

fn ensure_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|err| {
        format!("failed to create tsgo cache directory `{}`: {err}", path.display())
    })?;
    set_owner_only_dir_permissions(path);
    Ok(())
}

fn build_tsgo_wasm_engine(
    enable_builtin_cache: bool,
    paths: Option<&TsgoWasmCachePaths>,
    profile: &TsgoWasmEngineProfile,
) -> Result<WasmtimeEngine, String> {
    let mut config = WasmtimeConfig::new();
    apply_tsgo_wasm_engine_profile(&mut config, profile);
    if enable_builtin_cache {
        let cache_paths =
            paths.ok_or_else(|| "missing tsgo wasm cache paths for built-in cache".to_string())?;
        ensure_wasmtime_cache_config(cache_paths)?;
        config.cache_config_load(&cache_paths.wasmtime_config).map_err(|err| {
            format!(
                "failed to load tsgo wasmtime cache config `{}`: {err}",
                cache_paths.wasmtime_config.display()
            )
        })?;
    }
    WasmtimeEngine::new(&config)
        .map_err(|err| format!("failed to initialize tsgo wasm engine: {err}"))
}

fn apply_tsgo_wasm_engine_profile(config: &mut WasmtimeConfig, profile: &TsgoWasmEngineProfile) {
    let _ = profile;
    config
        .cranelift_opt_level(OptLevel::None)
        .cranelift_regalloc_algorithm(RegallocAlgorithm::SinglePass)
        .parallel_compilation(true);
}

fn ensure_wasmtime_cache_config(paths: &TsgoWasmCachePaths) -> Result<(), String> {
    let config = format!(
        "[cache]\nenabled = true\ndirectory = {}\n",
        toml_string(paths.wasmtime_cache_dir.to_string_lossy().as_ref())
    );
    atomic_write_file(&paths.wasmtime_config, config.as_bytes())
}

fn toml_string(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len() + 2);
    escaped.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

fn tsgo_wasm_engine_profile() -> TsgoWasmEngineProfile {
    TsgoWasmEngineProfile {
        wasmtime_version: dependency_version_from_lock("wasmtime").unwrap_or("unknown"),
        opt_level: "none",
        regalloc_algorithm: "single-pass",
        parallel_compilation: true,
        target_arch: std::env::consts::ARCH,
        target_os: std::env::consts::OS,
        target_env: target_env_identity(),
    }
}

fn target_env_identity() -> &'static str {
    if cfg!(target_env = "gnu") {
        "gnu"
    } else if cfg!(target_env = "musl") {
        "musl"
    } else if cfg!(target_env = "msvc") {
        "msvc"
    } else if cfg!(target_env = "sgx") {
        "sgx"
    } else {
        "unknown"
    }
}

fn load_tsgo_wasm_module(
    engine: &WasmtimeEngine,
    paths: Option<&TsgoWasmCachePaths>,
    mode: TsgoWasmCacheMode,
    profile: &TsgoWasmEngineProfile,
) -> Result<(Module, TsgoWasmInitOutcome), String> {
    load_tsgo_wasm_module_from_bytes(engine, paths, mode, profile, tsgo_api_wasm_bytes())
}

fn load_tsgo_wasm_module_from_bytes(
    engine: &WasmtimeEngine,
    paths: Option<&TsgoWasmCachePaths>,
    mode: TsgoWasmCacheMode,
    profile: &TsgoWasmEngineProfile,
    wasm_bytes: &[u8],
) -> Result<(Module, TsgoWasmInitOutcome), String> {
    if mode != TsgoWasmCacheMode::Off {
        if let Some(paths) = paths {
            let fingerprint = tsgo_wasm_manual_fingerprint(profile, wasm_bytes);
            let artifact_path = tsgo_wasm_manual_artifact_path(paths, &fingerprint);
            return load_manual_tsgo_wasm_module(
                engine,
                paths,
                wasm_bytes,
                &fingerprint,
                artifact_path,
            );
        }
    }

    Module::new(engine, wasm_bytes)
        .map(|module| (module, TsgoWasmInitOutcome::DirectCompile))
        .map_err(|err| format!("failed to compile embedded tsgo wasm module: {err}"))
}

fn load_tsgo_wasm_module_with_fallback(
    engine: &WasmtimeEngine,
    paths: Option<&TsgoWasmCachePaths>,
    mode: TsgoWasmCacheMode,
    profile: &TsgoWasmEngineProfile,
) -> Result<(Module, TsgoWasmInitOutcome), String> {
    match load_tsgo_wasm_module(engine, paths, mode, profile) {
        Ok(result) => Ok(result),
        Err(err) if mode != TsgoWasmCacheMode::Off => {
            log_warn(&format!(
                "plts.tsgo_wasm persistent cache load failed; falling back to direct compile error={err}"
            ));
            load_tsgo_wasm_module(engine, None, TsgoWasmCacheMode::Off, profile)
        }
        Err(err) => Err(err),
    }
}

fn load_manual_tsgo_wasm_module(
    engine: &WasmtimeEngine,
    paths: &TsgoWasmCachePaths,
    wasm_bytes: &[u8],
    fingerprint: &str,
    artifact_path: PathBuf,
) -> Result<(Module, TsgoWasmInitOutcome), String> {
    if artifact_path.exists() {
        match unsafe { Module::deserialize_file(engine, &artifact_path) } {
            Ok(module) => {
                return Ok((module, TsgoWasmInitOutcome::ManualHit { artifact_path }));
            }
            Err(err) => {
                record_tsgo_wasm_cache_event("deserialize_error");
                log_warn(&format!(
                    "plts.tsgo_wasm manual cache deserialize failed artifact={} error={err}",
                    artifact_path.display()
                ));
                quarantine_manual_artifact(paths, fingerprint, &artifact_path)?;
            }
        }
    }

    let precompiled = engine.precompile_module(wasm_bytes).map_err(|err| {
        format!(
            "failed to precompile embedded tsgo wasm module for manual cache `{}`: {err}",
            artifact_path.display()
        )
    })?;
    atomic_write_new_file(&artifact_path, &precompiled)?;

    match unsafe { Module::deserialize_file(engine, &artifact_path) } {
        Ok(module) => Ok((module, TsgoWasmInitOutcome::ManualMiss { artifact_path })),
        Err(err) => {
            record_tsgo_wasm_cache_event("deserialize_error");
            log_warn(&format!(
                "plts.tsgo_wasm manual cache deserialize failed after rebuild artifact={} error={err}",
                artifact_path.display()
            ));
            quarantine_manual_artifact(paths, fingerprint, &artifact_path)?;
            Err(format!(
                "failed to deserialize rebuilt tsgo wasm manual cache artifact `{}`: {err}",
                artifact_path.display()
            ))
        }
    }
}

fn tsgo_wasm_manual_artifact_path(paths: &TsgoWasmCachePaths, fingerprint: &str) -> PathBuf {
    paths.manual_dir.join(format!("{fingerprint}.cwasm"))
}

fn tsgo_wasm_manual_fingerprint(profile: &TsgoWasmEngineProfile, wasm_bytes: &[u8]) -> String {
    let wasm_hash = Sha256::digest(wasm_bytes);
    let mut hasher = Sha256::new();
    hasher.update(hex::encode(wasm_hash).as_bytes());
    hasher.update([0]);
    hasher.update(profile.wasmtime_version.as_bytes());
    hasher.update([0]);
    hasher.update(profile.opt_level.as_bytes());
    hasher.update([0]);
    hasher.update(profile.regalloc_algorithm.as_bytes());
    hasher.update([0]);
    hasher.update(if profile.parallel_compilation { b"parallel=1" } else { b"parallel=0" });
    hasher.update([0]);
    hasher.update(profile.target_arch.as_bytes());
    hasher.update([0]);
    hasher.update(profile.target_os.as_bytes());
    hasher.update([0]);
    hasher.update(profile.target_env.as_bytes());
    hex::encode(hasher.finalize())
}

fn quarantine_manual_artifact(
    paths: &TsgoWasmCachePaths,
    fingerprint: &str,
    artifact_path: &Path,
) -> Result<(), String> {
    if !artifact_path.exists() {
        return Ok(());
    }

    let quarantine_path = paths.quarantine_dir.join(format!(
        "{fingerprint}.{}.bad",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
    ));
    match fs::rename(artifact_path, &quarantine_path) {
        Ok(()) => {
            log_warn(&format!(
                "plts.tsgo_wasm quarantined manual cache artifact from {} to {}",
                artifact_path.display(),
                quarantine_path.display()
            ));
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to quarantine tsgo wasm manual cache artifact `{}` -> `{}`: {err}",
            artifact_path.display(),
            quarantine_path.display()
        )),
    }
}

fn atomic_write_file(path: &Path, contents: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        ensure_directory(parent)?;
    }

    let temp_path = unique_temp_path(path);
    fs::write(&temp_path, contents).map_err(|err| {
        format!("failed to write temp tsgo cache file `{}`: {err}", temp_path.display())
    })?;
    set_owner_only_file_permissions(&temp_path);
    match fs::rename(&temp_path, path) {
        Ok(()) => {
            set_owner_only_file_permissions(path);
            Ok(())
        }
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            Err(format!("failed to atomically replace tsgo cache file `{}`: {err}", path.display()))
        }
    }
}

fn atomic_write_new_file(path: &Path, contents: &[u8]) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        ensure_directory(parent)?;
    }

    let temp_path = unique_temp_path(path);
    fs::write(&temp_path, contents).map_err(|err| {
        format!("failed to write temp tsgo cache file `{}`: {err}", temp_path.display())
    })?;
    set_owner_only_file_permissions(&temp_path);
    match fs::hard_link(&temp_path, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temp_path);
            set_owner_only_file_permissions(path);
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temp_path);
            Ok(())
        }
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            Err(format!(
                "failed to publish tsgo cache file `{}` without overwrite: {err}",
                path.display()
            ))
        }
    }
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let suffix = TSGO_WASM_TEMPFILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let extension = format!(
        "tmp.{}.{}.{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos(),
        suffix
    );
    path.with_extension(extension)
}

#[cfg(unix)]
fn set_owner_only_dir_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn set_owner_only_dir_permissions(_path: &Path) {}

#[cfg(unix)]
fn set_owner_only_file_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn set_owner_only_file_permissions(_path: &Path) {}

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

    let specifier = deno_ast::ModuleSpecifier::parse("file:///plts_module.ts")
        .expect("static module specifier must parse");

    let parsed = deno_ast::parse_module(deno_ast::ParseParams {
        specifier,
        text: source_ts.to_string().into(),
        media_type: deno_ast::MediaType::TypeScript,
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
        &deno_ast::TranspileOptions::default(),
        &deno_ast::TranspileModuleOptions::default(),
        &deno_ast::EmitOptions {
            source_map: if source_map {
                deno_ast::SourceMapOption::Inline
            } else {
                deno_ast::SourceMapOption::None
            },
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
    let declarations = tsgo_virtual_declarations(compiler_opts);
    let request_json =
        serde_json::to_vec(&TsgoTranspileRequest { source_ts, source_map, declarations })
            .map_err(|err| format!("failed to encode tsgo transpile request: {err}"))?;

    let stdout_bytes = execute_tsgo_wasm_command("transpile", request_json)?;
    let decoded: TsgoTranspileResponse = serde_json::from_slice(&stdout_bytes)
        .map_err(|err| format!("failed to decode tsgo transpile response: {err}"))?;

    let diagnostics =
        decoded.diagnostics.into_iter().map(tsgo_diagnostic_to_json).collect::<Vec<_>>();

    Ok((decoded.compiled_js, Value::Array(diagnostics)))
}

pub(crate) fn semantic_typecheck_typescript(source_ts: &str, compiler_opts: &Value) -> Value {
    semantic_typecheck_typescript_via_tsgo_wasm(source_ts, compiler_opts).unwrap_or_else(|err| {
        json!([diagnostic_from_message(
            "error",
            &format!("failed to execute TypeScript checker: {err}"),
        )])
    })
}

fn semantic_typecheck_typescript_via_tsgo_wasm(
    source_ts: &str,
    compiler_opts: &Value,
) -> Result<Value, String> {
    let declarations = tsgo_virtual_declarations(compiler_opts);
    let request_json = serde_json::to_vec(&TsgoTypecheckRequest { source_ts, declarations })
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
        let started_at = record_tsgo_wasm_init_start();
        let profile = tsgo_wasm_engine_profile();
        let mode = read_tsgo_wasm_cache_mode();
        let cache_paths = if mode == TsgoWasmCacheMode::Off {
            None
        } else {
            match resolve_tsgo_wasm_cache_paths() {
                Ok(paths) => paths,
                Err(err) => {
                    record_tsgo_wasm_cache_event("config_error");
                    log_warn(&format!("plts.tsgo_wasm cache bootstrap failed: {err}"));
                    None
                }
            }
        };

        let result = match mode {
            TsgoWasmCacheMode::Auto => {
                if let Some(paths) = cache_paths.as_ref() {
                    match build_tsgo_wasm_engine(true, Some(paths), &profile) {
                        Ok(engine) => match Module::new(&engine, tsgo_api_wasm_bytes()) {
                            Ok(module) => {
                                record_tsgo_wasm_init_outcome(
                                    &TsgoWasmInitOutcome::BuiltInCache,
                                    Some(paths),
                                );
                                Ok(TsgoWasmRuntime { engine, module })
                            }
                            Err(err) => {
                                log_warn(&format!(
                                    "plts.tsgo_wasm built-in cache compile failed; falling back to manual/direct cache root={} error={err}",
                                    paths.root.display()
                                ));
                                let engine = build_tsgo_wasm_engine(false, Some(paths), &profile)?;
                                let (module, outcome) = load_tsgo_wasm_module_with_fallback(
                                    &engine,
                                    Some(paths),
                                    TsgoWasmCacheMode::ManualOnly,
                                    &profile,
                                )?;
                                record_tsgo_wasm_init_outcome(&outcome, Some(paths));
                                Ok(TsgoWasmRuntime { engine, module })
                            }
                        },
                        Err(err) => {
                            record_tsgo_wasm_cache_event("config_error");
                            log_warn(&format!(
                                "plts.tsgo_wasm built-in cache configuration failed; falling back to manual/direct cache root={} error={err}",
                                paths.root.display()
                            ));
                            let engine = build_tsgo_wasm_engine(false, Some(paths), &profile)?;
                            let (module, outcome) = load_tsgo_wasm_module_with_fallback(
                                &engine,
                                Some(paths),
                                TsgoWasmCacheMode::ManualOnly,
                                &profile,
                            )?;
                            record_tsgo_wasm_init_outcome(&outcome, Some(paths));
                            Ok(TsgoWasmRuntime { engine, module })
                        }
                    }
                } else {
                    let engine = build_tsgo_wasm_engine(false, None, &profile)?;
                    let (module, outcome) = load_tsgo_wasm_module_with_fallback(
                        &engine,
                        None,
                        TsgoWasmCacheMode::Off,
                        &profile,
                    )?;
                    record_tsgo_wasm_init_outcome(&outcome, None);
                    Ok(TsgoWasmRuntime { engine, module })
                }
            }
            TsgoWasmCacheMode::ManualOnly => {
                let engine = build_tsgo_wasm_engine(false, cache_paths.as_ref(), &profile)?;
                let (module, outcome) =
                    load_tsgo_wasm_module_with_fallback(&engine, cache_paths.as_ref(), mode, &profile)?;
                record_tsgo_wasm_init_outcome(&outcome, cache_paths.as_ref());
                Ok(TsgoWasmRuntime { engine, module })
            }
            TsgoWasmCacheMode::Off => {
                let engine = build_tsgo_wasm_engine(false, None, &profile)?;
                let (module, outcome) = load_tsgo_wasm_module_with_fallback(
                    &engine,
                    None,
                    TsgoWasmCacheMode::Off,
                    &profile,
                )?;
                record_tsgo_wasm_init_outcome(&outcome, None);
                Ok(TsgoWasmRuntime { engine, module })
            }
        };

        record_tsgo_wasm_init_success(started_at);
        result
    });

    match runtime {
        Ok(runtime) => Ok(runtime),
        Err(err) => Err(err.clone()),
    }
}

fn record_tsgo_wasm_init_outcome(
    outcome: &TsgoWasmInitOutcome,
    paths: Option<&TsgoWasmCachePaths>,
) {
    match outcome {
        TsgoWasmInitOutcome::BuiltInCache => {
            record_tsgo_wasm_cache_event("built_in_configured");
            if let Some(paths) = paths {
                log_info(&format!(
                    "plts.tsgo_wasm init cache=built-in root={}",
                    paths.root.display()
                ));
            } else {
                log_info("plts.tsgo_wasm init cache=built-in");
            }
        }
        TsgoWasmInitOutcome::ManualHit { artifact_path } => {
            record_tsgo_wasm_cache_event("manual_hit");
            log_info(&format!(
                "plts.tsgo_wasm init cache=manual-hit artifact={}",
                artifact_path.display()
            ));
        }
        TsgoWasmInitOutcome::ManualMiss { artifact_path } => {
            record_tsgo_wasm_cache_event("manual_miss");
            log_info(&format!(
                "plts.tsgo_wasm init cache=manual-miss artifact={}",
                artifact_path.display()
            ));
        }
        TsgoWasmInitOutcome::DirectCompile => {
            record_tsgo_wasm_cache_event("fallback_compile");
            log_info("plts.tsgo_wasm init cache=direct-compile");
        }
    }
}

fn tsgo_virtual_declarations(compiler_opts: &Value) -> Vec<TsgoVirtualDeclaration> {
    let Some(meta) = compiler_opts.get("stopgap_function").and_then(Value::as_object) else {
        return Vec::new();
    };

    let function_path = meta.get("function_path").and_then(Value::as_str).unwrap_or("");
    let module_path = meta.get("module_path").and_then(Value::as_str).unwrap_or("");
    let export_name = meta.get("export_name").and_then(Value::as_str).unwrap_or("");
    let kind = meta.get("kind").and_then(Value::as_str).unwrap_or("mutation");

    if function_path.is_empty() || module_path.is_empty() || export_name.is_empty() {
        return Vec::new();
    }

    let function_path_literal =
        serde_json::to_string(function_path).unwrap_or_else(|_| "\"\"".to_string());
    let module_path_literal =
        serde_json::to_string(module_path).unwrap_or_else(|_| "\"\"".to_string());
    let export_name_literal =
        serde_json::to_string(export_name).unwrap_or_else(|_| "\"\"".to_string());
    let kind_literal = serde_json::to_string(kind).unwrap_or_else(|_| "\"mutation\"".to_string());

    let content = format!(
        "declare namespace StopgapGenerated {{\n  interface FunctionMetadata {{\n    readonly functionPath: {function_path_literal};\n    readonly modulePath: {module_path_literal};\n    readonly exportName: {export_name_literal};\n    readonly kind: {kind_literal};\n    readonly args: unknown;\n    readonly ctx: import(\"@stopgap/runtime\").StopgapContext<unknown>;\n  }}\n}}\n"
    );

    let mut sanitized = function_path
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized.push_str("function");
    }

    vec![TsgoVirtualDeclaration {
        file_name: format!("/stopgap/generated/{sanitized}.d.ts"),
        content,
    }]
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

#[cfg(all(test, not(feature = "pg_test")))]
mod tests {
    use super::{
        TsgoWasmCacheMode, TsgoWasmEngineProfile, TsgoWasmInitOutcome,
        bootstrap_tsgo_wasm_cache_paths, build_tsgo_wasm_engine, contains_error_diagnostics,
        ensure_wasmtime_cache_config, load_tsgo_wasm_module_from_bytes, parse_tsgo_wasm_cache_mode,
        resolve_tsgo_wasm_cache_root, semantic_typecheck_typescript_via_tsgo_wasm, toml_string,
        transpile_typescript_via_tsgo_wasm, tsgo_api_wasm_bytes, tsgo_virtual_declarations,
        tsgo_wasm_engine_profile, tsgo_wasm_manual_artifact_path, tsgo_wasm_manual_fingerprint,
    };
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_WASM_BYTES: &[u8] = b"\0asm\x01\0\0\0";
    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let unique = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "plts-tsgo-wasm-test-{label}-{}-{unique}",
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
            ));
            if path.exists() {
                let _ = fs::remove_dir_all(&path);
            }
            fs::create_dir_all(&path).expect("test temp dir should be creatable");
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn embeds_tsgo_wasm_artifact() {
        let wasm = tsgo_api_wasm_bytes();
        assert!(wasm.len() > 8, "embedded tsgo wasm must not be empty");
        assert_eq!(&wasm[0..4], b"\0asm", "embedded tsgo payload must be wasm");
    }

    #[test]
    fn cache_mode_parsing_defaults_to_auto() {
        assert_eq!(parse_tsgo_wasm_cache_mode(None), TsgoWasmCacheMode::Auto);
        assert_eq!(parse_tsgo_wasm_cache_mode(Some("manual-only")), TsgoWasmCacheMode::ManualOnly);
        assert_eq!(parse_tsgo_wasm_cache_mode(Some("off")), TsgoWasmCacheMode::Off);
        assert_eq!(parse_tsgo_wasm_cache_mode(Some("unexpected-value")), TsgoWasmCacheMode::Auto);
    }

    #[test]
    fn cache_root_resolution_prefers_explicit_then_project_then_temp() {
        let explicit = PathBuf::from("/tmp/plts-explicit-cache");
        assert_eq!(
            resolve_tsgo_wasm_cache_root(
                Some(explicit.as_path()),
                Some(PathBuf::from("/ignored")),
                PathBuf::from("/also-ignored"),
            ),
            explicit
        );

        assert_eq!(
            resolve_tsgo_wasm_cache_root(
                None,
                Some(PathBuf::from("/var/cache/plts")),
                PathBuf::from("/tmp/fallback"),
            ),
            PathBuf::from("/var/cache/plts/tsgo-wasm")
        );

        assert_eq!(
            resolve_tsgo_wasm_cache_root(None, None, PathBuf::from("/tmp/fallback")),
            PathBuf::from("/tmp/fallback/stopgap/plts/tsgo-wasm")
        );
    }

    #[test]
    fn wasmtime_cache_config_bootstrap_is_idempotent() {
        let dir = TestDir::new("config");
        let paths = bootstrap_tsgo_wasm_cache_paths(dir.path.join("cache-root"))
            .expect("cache paths should bootstrap");

        ensure_wasmtime_cache_config(&paths).expect("first config bootstrap should work");
        let first = fs::read_to_string(&paths.wasmtime_config).expect("config file should exist");
        ensure_wasmtime_cache_config(&paths).expect("second config bootstrap should work");
        let second = fs::read_to_string(&paths.wasmtime_config).expect("config file should exist");

        assert_eq!(first, second);
        assert!(first.contains("[cache]"));
        assert!(first.contains("enabled = true"));
        assert!(first.contains(&toml_string(paths.wasmtime_cache_dir.to_string_lossy().as_ref())));
    }

    #[test]
    fn manual_cache_creates_then_reuses_serialized_artifact() {
        let dir = TestDir::new("manual-hit");
        let paths = bootstrap_tsgo_wasm_cache_paths(dir.path.join("cache-root"))
            .expect("cache paths should bootstrap");
        let profile = tsgo_wasm_engine_profile();
        let fingerprint = tsgo_wasm_manual_fingerprint(&profile, TEST_WASM_BYTES);
        let engine = build_tsgo_wasm_engine(false, Some(&paths), &profile)
            .expect("engine should initialize");

        let (module, first_outcome) = load_tsgo_wasm_module_from_bytes(
            &engine,
            Some(&paths),
            TsgoWasmCacheMode::ManualOnly,
            &profile,
            TEST_WASM_BYTES,
        )
        .expect("first load should precompile and deserialize");
        drop(module);
        let artifact_path = match first_outcome {
            TsgoWasmInitOutcome::ManualMiss { artifact_path } => artifact_path,
            other => panic!("expected manual miss on first load, got {other:?}"),
        };
        assert!(artifact_path.exists(), "manual cache artifact should be created");

        let (_module, second_outcome) = load_tsgo_wasm_module_from_bytes(
            &engine,
            Some(&paths),
            TsgoWasmCacheMode::ManualOnly,
            &profile,
            TEST_WASM_BYTES,
        )
        .expect("second load should reuse serialized artifact");
        match second_outcome {
            TsgoWasmInitOutcome::ManualHit { artifact_path: reused } => {
                assert_eq!(reused, artifact_path);
            }
            other => panic!("expected manual hit on second load, got {other:?}"),
        }

        assert_eq!(artifact_path, tsgo_wasm_manual_artifact_path(&paths, &fingerprint));
    }

    #[test]
    fn corrupted_manual_cache_artifact_is_quarantined_and_rebuilt() {
        let dir = TestDir::new("manual-quarantine");
        let paths = bootstrap_tsgo_wasm_cache_paths(dir.path.join("cache-root"))
            .expect("cache paths should bootstrap");
        let profile = tsgo_wasm_engine_profile();
        let fingerprint = tsgo_wasm_manual_fingerprint(&profile, TEST_WASM_BYTES);
        let engine = build_tsgo_wasm_engine(false, Some(&paths), &profile)
            .expect("engine should initialize");

        let (module, first_outcome) = load_tsgo_wasm_module_from_bytes(
            &engine,
            Some(&paths),
            TsgoWasmCacheMode::ManualOnly,
            &profile,
            TEST_WASM_BYTES,
        )
        .expect("first load should succeed");
        drop(module);
        let artifact_path = match first_outcome {
            TsgoWasmInitOutcome::ManualMiss { artifact_path } => artifact_path,
            other => panic!("expected manual miss on first load, got {other:?}"),
        };

        fs::write(&artifact_path, b"corrupted").expect("corrupt artifact write should succeed");

        let (_module, second_outcome) = load_tsgo_wasm_module_from_bytes(
            &engine,
            Some(&paths),
            TsgoWasmCacheMode::ManualOnly,
            &profile,
            TEST_WASM_BYTES,
        )
        .expect("corrupted artifact should be rebuilt");
        match second_outcome {
            TsgoWasmInitOutcome::ManualMiss { artifact_path: rebuilt } => {
                assert_eq!(rebuilt, artifact_path);
            }
            other => panic!("expected manual miss after quarantine, got {other:?}"),
        }

        let mut quarantined = fs::read_dir(&paths.quarantine_dir)
            .expect("quarantine dir should be readable")
            .map(|entry| entry.expect("quarantine entry should read").path())
            .collect::<Vec<_>>();
        quarantined.sort();
        assert_eq!(quarantined.len(), 1, "expected exactly one quarantined artifact");
        assert!(
            quarantined[0]
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&fingerprint)),
            "quarantined artifact should include the manual fingerprint"
        );
        assert!(artifact_path.exists(), "rebuilt artifact should be present");
    }

    #[test]
    fn manual_fingerprint_changes_when_inputs_change() {
        let profile = tsgo_wasm_engine_profile();
        let same = tsgo_wasm_manual_fingerprint(&profile, TEST_WASM_BYTES);
        assert_eq!(same, tsgo_wasm_manual_fingerprint(&profile, TEST_WASM_BYTES));

        let changed_wasm = tsgo_wasm_manual_fingerprint(&profile, b"\0asm\x01\0\0\0\0");
        assert_ne!(same, changed_wasm);

        let changed_profile =
            TsgoWasmEngineProfile { parallel_compilation: false, ..profile.clone() };
        assert_ne!(same, tsgo_wasm_manual_fingerprint(&changed_profile, TEST_WASM_BYTES));
    }

    #[test]
    fn tsgo_wasm_typecheck_reports_app_import_diagnostic() {
        let diagnostics = semantic_typecheck_typescript_via_tsgo_wasm(
            "import { base } from '@app/math';\nexport default () => base;",
            &json!({}),
        )
        .expect("tsgo wasm typecheck call should succeed");

        let entries = diagnostics.as_array().expect("diagnostics must be an array");
        assert!(!entries.is_empty(), "@app import should produce diagnostics");
        let first = &entries[0];
        let message =
            first.get("message").and_then(|value| value.as_str()).expect("message string");
        assert!(message.contains("unsupported bare module import `@app/math`"));
    }

    #[test]
    fn builds_stopgap_function_declaration_from_compiler_opts() {
        let declarations = tsgo_virtual_declarations(&json!({
            "stopgap_function": {
                "function_path": "api.admin.users.list",
                "module_path": "admin/users.ts",
                "export_name": "list",
                "kind": "query"
            }
        }));
        assert_eq!(declarations.len(), 1);
        let declaration = &declarations[0];
        assert!(declaration.file_name.contains("api_admin_users_list"));
        assert!(declaration.content.contains("functionPath: \"api.admin.users.list\""));
        assert!(declaration.content.contains("kind: \"query\""));
        assert!(declaration.content.contains("StopgapContext<unknown>"));
    }

    #[test]
    fn tsgo_wasm_transpile_emits_javascript() {
        let (compiled, diagnostics) =
            transpile_typescript_via_tsgo_wasm("export const value: number = 1;", &json!({}))
                .expect("tsgo wasm transpile call should succeed");

        assert!(
            !contains_error_diagnostics(&diagnostics),
            "transpile diagnostics should not contain errors: {diagnostics}"
        );
        assert_eq!(compiled, "export const value = 1;\n");
    }

    #[test]
    fn tsgo_wasm_transpile_can_emit_inline_source_map() {
        let (compiled, diagnostics) = transpile_typescript_via_tsgo_wasm(
            "export const value: number = 1;",
            &json!({ "source_map": true }),
        )
        .expect("tsgo wasm transpile source_map call should succeed");

        assert!(
            !contains_error_diagnostics(&diagnostics),
            "transpile diagnostics should not contain errors: {diagnostics}"
        );
        assert!(compiled.contains("//# sourceMappingURL=data:application/json;base64,"));
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
