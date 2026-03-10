use base64::Engine as Base64Engine;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use wasmtime::{
    Config as WasmtimeConfig, Engine as WasmtimeEngine, Module, OptLevel, RegallocAlgorithm,
};

const CARGO_LOCK_CONTENT: &str = include_str!("../../../Cargo.lock");
const STOPGAP_TSGO_API_WASM: &[u8] =
    include_bytes!("../../../third_party/stopgap-tsgo-api/dist/stopgap-tsgo-api.wasm");
const STOPGAP_TSGO_RUNTIME_DECLARATIONS: &str = include_str!("tsgo_runtime.d.ts");

static TS_COMPILER_FINGERPRINT: OnceLock<String> = OnceLock::new();
static TSGO_WASM_TEMPFILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TsgoWasmCacheMode {
    Auto,
    ManualOnly,
    Off,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TsgoWasmCachePaths {
    pub(crate) root: PathBuf,
    pub(crate) wasmtime_config: PathBuf,
    pub(crate) wasmtime_cache_dir: PathBuf,
    pub(crate) manual_dir: PathBuf,
    pub(crate) quarantine_dir: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TsgoWasmEngineProfile {
    pub(crate) wasmtime_version: &'static str,
    pub(crate) opt_level: &'static str,
    pub(crate) regalloc_algorithm: &'static str,
    pub(crate) parallel_compilation: bool,
    pub(crate) target_arch: &'static str,
    pub(crate) target_os: &'static str,
    pub(crate) target_env: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TsgoWasmInitOutcome {
    BuiltInCache,
    ManualHit { artifact_path: PathBuf },
    ManualMiss { artifact_path: PathBuf },
    DirectCompile,
}

#[derive(serde::Serialize)]
pub(crate) struct TsgoVirtualDeclaration {
    pub(crate) file_name: String,
    pub(crate) content: String,
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
            let deno_core = dependency_version_from_lock("deno_core").unwrap_or("disabled");
            let tsgo_api_wasm_hash = hex::encode(Sha256::digest(tsgo_api_wasm_bytes()));
            format!("deno_core@{};tsgo_api_wasm_sha256@{}", deno_core, tsgo_api_wasm_hash)
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

pub(crate) fn parse_tsgo_wasm_cache_mode(raw: Option<&str>) -> TsgoWasmCacheMode {
    match raw.unwrap_or("auto").trim().to_ascii_lowercase().as_str() {
        "manual-only" => TsgoWasmCacheMode::ManualOnly,
        "off" => TsgoWasmCacheMode::Off,
        _ => TsgoWasmCacheMode::Auto,
    }
}

pub(crate) fn resolve_tsgo_wasm_cache_root(
    explicit_root: Option<&Path>,
    project_cache_dir: Option<PathBuf>,
    temp_dir: PathBuf,
) -> PathBuf {
    explicit_root
        .map(Path::to_path_buf)
        .or_else(|| project_cache_dir.map(|path| path.join("tsgo-wasm")))
        .unwrap_or_else(|| temp_dir.join("stopgap").join("plts").join("tsgo-wasm"))
}

pub(crate) fn bootstrap_tsgo_wasm_cache_paths(root: PathBuf) -> Result<TsgoWasmCachePaths, String> {
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

pub(crate) fn build_tsgo_wasm_engine(
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

pub(crate) fn tsgo_wasm_engine_profile() -> TsgoWasmEngineProfile {
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

pub(crate) fn ensure_wasmtime_cache_config(paths: &TsgoWasmCachePaths) -> Result<(), String> {
    let config = format!(
        "[cache]\nenabled = true\ndirectory = {}\n",
        toml_string(paths.wasmtime_cache_dir.to_string_lossy().as_ref())
    );
    atomic_write_file(&paths.wasmtime_config, config.as_bytes())
}

pub(crate) fn toml_string(raw: &str) -> String {
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

pub(crate) fn load_tsgo_wasm_module_from_bytes(
    engine: &WasmtimeEngine,
    paths: Option<&TsgoWasmCachePaths>,
    mode: TsgoWasmCacheMode,
    profile: &TsgoWasmEngineProfile,
    wasm_bytes: &[u8],
) -> Result<(Module, TsgoWasmInitOutcome), String> {
    if mode != TsgoWasmCacheMode::Off
        && let Some(paths) = paths
    {
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

    Module::new(engine, wasm_bytes)
        .map(|module| (module, TsgoWasmInitOutcome::DirectCompile))
        .map_err(|err| format!("failed to compile embedded tsgo wasm module: {err}"))
}

pub(crate) fn tsgo_wasm_manual_artifact_path(
    paths: &TsgoWasmCachePaths,
    fingerprint: &str,
) -> PathBuf {
    paths.manual_dir.join(format!("{fingerprint}.cwasm"))
}

pub(crate) fn tsgo_wasm_manual_fingerprint(
    profile: &TsgoWasmEngineProfile,
    wasm_bytes: &[u8],
) -> String {
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

pub(crate) fn tsgo_virtual_declarations(compiler_opts: &Value) -> Vec<TsgoVirtualDeclaration> {
    let mut declarations = vec![TsgoVirtualDeclaration {
        file_name: "/stopgap/runtime/index.d.ts".to_string(),
        content: STOPGAP_TSGO_RUNTIME_DECLARATIONS.to_string(),
    }];

    let Some(meta) = compiler_opts.get("stopgap_function").and_then(Value::as_object) else {
        return declarations;
    };

    let function_path = meta.get("function_path").and_then(Value::as_str).unwrap_or("");
    let module_path = meta.get("module_path").and_then(Value::as_str).unwrap_or("");
    let export_name = meta.get("export_name").and_then(Value::as_str).unwrap_or("");
    let kind = meta.get("kind").and_then(Value::as_str).unwrap_or("mutation");

    if function_path.is_empty() || module_path.is_empty() || export_name.is_empty() {
        return declarations;
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

    declarations.push(TsgoVirtualDeclaration {
        file_name: format!("/stopgap/generated/{sanitized}.d.ts"),
        content,
    });

    declarations
}

fn apply_tsgo_wasm_engine_profile(config: &mut WasmtimeConfig, _profile: &TsgoWasmEngineProfile) {
    config
        .cranelift_opt_level(OptLevel::None)
        .cranelift_regalloc_algorithm(RegallocAlgorithm::SinglePass)
        .parallel_compilation(true);
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
            Err(_) => {
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
            quarantine_manual_artifact(paths, fingerprint, &artifact_path)?;
            Err(format!(
                "failed to deserialize rebuilt tsgo wasm manual cache artifact `{}`: {err}",
                artifact_path.display()
            ))
        }
    }
}

fn ensure_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|err| {
        format!("failed to create tsgo cache directory `{}`: {err}", path.display())
    })?;
    set_owner_only_dir_permissions(path);
    Ok(())
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
        Ok(()) => Ok(()),
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

#[cfg(test)]
mod tests {
    use super::{
        TsgoWasmCacheMode, TsgoWasmEngineProfile, TsgoWasmInitOutcome,
        bootstrap_tsgo_wasm_cache_paths, build_tsgo_wasm_engine, compiler_fingerprint,
        compute_artifact_hash, contains_error_diagnostics, dependency_version_from_lock,
        ensure_wasmtime_cache_config, extract_inline_source_map, load_tsgo_wasm_module_from_bytes,
        parse_tsgo_wasm_cache_mode, resolve_tsgo_wasm_cache_root, toml_string, tsgo_api_wasm_bytes,
        tsgo_virtual_declarations, tsgo_wasm_engine_profile, tsgo_wasm_manual_artifact_path,
        tsgo_wasm_manual_fingerprint,
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
        assert!(wasm.len() > 8);
        assert_eq!(&wasm[0..4], b"\0asm");
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
        assert!(artifact_path.exists());

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
        assert_eq!(quarantined.len(), 1);
        assert!(
            quarantined[0]
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&fingerprint))
        );
        assert!(artifact_path.exists());
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
    fn dependency_version_from_lock_finds_known_crate() {
        let version = dependency_version_from_lock("serde_json");
        assert!(version.is_some());
    }

    #[test]
    fn extract_inline_source_map_decodes_payload() {
        let compiled = "console.log('x');\n//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozfQ==";
        let source_map = extract_inline_source_map(compiled)
            .expect("inline source map should decode from base64 payload");
        assert!(source_map.contains("\"version\":3"));
    }

    #[test]
    fn compiler_fingerprint_includes_dependency_versions() {
        let fingerprint = compiler_fingerprint();
        assert!(fingerprint.contains("deno_core@"));
        assert!(fingerprint.contains("tsgo_api_wasm_sha256@"));
    }

    #[test]
    fn artifact_hash_is_stable() {
        let hash = compute_artifact_hash(
            "export default () => ({ ok: true })",
            "export default () => ({ ok: true })",
            &json!({}),
            "v8-deno_core-p0",
        );
        assert!(hash.starts_with("sha256:"));
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
        assert_eq!(declarations.len(), 2);
        assert!(declarations.iter().any(|declaration| {
            declaration.content.contains("declare module \"@stopgap/runtime\"")
        }));
        let declaration = declarations
            .iter()
            .find(|declaration| declaration.file_name.contains("api_admin_users_list"))
            .expect("generated function declaration should be present");
        assert!(declaration.content.contains("functionPath: \"api.admin.users.list\""));
        assert!(declaration.content.contains("kind: \"query\""));
        assert!(declaration.content.contains("StopgapContext<unknown>"));
    }

    #[test]
    fn tsgo_virtual_declarations_include_runtime_module_without_metadata() {
        let declarations = tsgo_virtual_declarations(&json!({}));
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].file_name, "/stopgap/runtime/index.d.ts");
        assert!(declarations[0].content.contains("declare module \"@stopgap/runtime\""));
    }

    #[test]
    fn contains_error_diagnostics_detects_error_entries() {
        assert!(contains_error_diagnostics(&json!([{ "severity": "error" }])));
        assert!(!contains_error_diagnostics(&json!([{ "severity": "warning" }])));
    }
}
