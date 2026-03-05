use base64::Engine;
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
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const CARGO_LOCK_CONTENT: &str = include_str!("../../../Cargo.lock");
const RUNTIME_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../packages/runtime");
const STOPGAP_TSGO_API_WASM: &[u8] =
    include_bytes!("../../../third_party/stopgap-tsgo-api/dist/stopgap-tsgo-api.wasm");
const RUNTIME_TYPECHECK_D_TS: &str = r#"export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [k: string]: JsonValue };

export type JsonSchema = {
  type?: "object" | "array" | "string" | "number" | "integer" | "boolean" | "null";
  properties?: Record<string, JsonSchema>;
  required?: readonly string[];
  additionalProperties?: boolean;
  items?: JsonSchema;
  enum?: readonly JsonValue[];
  anyOf?: readonly JsonSchema[];
};

export type SchemaIssue = { message?: string };
export type SchemaSafeParseResult<T> =
  | { success: true; data: T }
  | { success: false; error: { issues?: SchemaIssue[] } };

export type SchemaLike<T = unknown> = {
  safeParse?: (value: unknown) => SchemaSafeParseResult<T>;
  parse?: (value: unknown) => T;
};

type InferSchemaValue<S> = S extends SchemaLike<infer T> ? T : JsonValue;

export type InferArgsSchema<S> = S extends SchemaLike<infer T>
  ? T
  : S extends JsonSchema
    ? JsonValue
    : JsonValue;

type StopgapSchema = JsonSchema | SchemaLike<unknown>;
type StopgapWrapped = ((ctx: unknown) => Promise<unknown>) & {
  __stopgap_kind: "query" | "mutation";
  __stopgap_args_schema: unknown;
};

export type DbMode = "ro" | "rw";
export type DbApi = {
  mode: DbMode;
  query: (sql: string, params?: JsonValue[]) => Promise<JsonValue[]>;
  exec: (sql: string, params?: JsonValue[]) => Promise<{ ok: true }>;
};

export type StopgapContext<TArgs> = {
  args: TArgs;
  db: DbApi;
  fn: {
    oid: number;
    schema: string;
    name: string;
  };
  now: string;
};

export type StopgapHandler<TArgs, TResult> = (
  args: TArgs,
  ctx: StopgapContext<TArgs>
) => TResult | Promise<TResult>;

type MiniSchema<T> = SchemaLike<T>;

type InferObjectShape<T extends Record<string, MiniSchema<unknown>>> = {
  [K in keyof T]: InferSchemaValue<T[K]>;
};

export declare const v: {
  string: () => MiniSchema<string>;
  number: () => MiniSchema<number>;
  int: () => MiniSchema<number>;
  boolean: () => MiniSchema<boolean>;
  null: () => MiniSchema<null>;
  literal: <const T extends JsonPrimitive>(value: T) => MiniSchema<T>;
  array: <T>(schema: MiniSchema<T>) => MiniSchema<T[]>;
  object: <T extends Record<string, MiniSchema<unknown>>>(shape: T) => MiniSchema<InferObjectShape<T>>;
  union: <T extends readonly [MiniSchema<unknown>, ...MiniSchema<unknown>[]]>(schemas: T) => MiniSchema<InferSchemaValue<T[number]>>;
  enum: <const T extends readonly [string, ...string[]]>(values: T) => MiniSchema<T[number]>;
};

export declare const validateArgs: (
  schema: StopgapSchema | null | undefined,
  value: unknown,
  path?: string
) => void;

export declare function query<S, TResult>(
  argsSchema: S,
  handler: StopgapHandler<InferArgsSchema<S>, TResult>
): StopgapWrapped;
export declare function query<TResult>(handler: StopgapHandler<JsonValue, TResult>): StopgapWrapped;

export declare function mutation<S, TResult>(
  argsSchema: S,
  handler: StopgapHandler<InferArgsSchema<S>, TResult>
): StopgapWrapped;
export declare function mutation<TResult>(handler: StopgapHandler<JsonValue, TResult>): StopgapWrapped;

declare const runtimeApi: {
  v: typeof v;
  query: typeof query;
  mutation: typeof mutation;
  validateArgs: typeof validateArgs;
};

export default runtimeApi;
"#;
const PLTS_RUNTIME_TYPECHECK_STUBS: &str = r#"declare module "plts+artifact:*" {
  const value: unknown;
  export default value;
  export const imported: unknown;
  export const base: unknown;
}

declare module "data:*" {
  const value: unknown;
  export default value;
  export const imported: unknown;
  export const base: unknown;
}

interface GlobalThis {
  [key: string]: unknown;
}

declare const Deno: unknown;
declare const fetch: unknown;
declare const Request: unknown;
declare const WebSocket: unknown;
"#;
static TS_COMPILER_FINGERPRINT: OnceLock<String> = OnceLock::new();

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

pub(crate) fn semantic_typecheck_typescript(source_ts: &str) -> Value {
    let Ok(workspace) = TypecheckWorkspace::prepare(source_ts) else {
        return json!([diagnostic_from_message(
            "error",
            "failed to prepare TypeScript typecheck workspace",
        )]);
    };

    let output = Command::new("pnpm")
        .arg("--dir")
        .arg(RUNTIME_DIR)
        .arg("exec")
        .arg("tsc")
        .arg("--project")
        .arg(workspace.tsconfig_path.as_os_str())
        .arg("--pretty")
        .arg("false")
        .arg("--noEmit")
        .output();

    let mut diagnostics: Vec<Value> = match output {
        Ok(result) => {
            let mut parsed = parse_tsc_diagnostics(&result.stdout);
            parsed.extend(parse_tsc_diagnostics(&result.stderr));
            if !result.status.success() && parsed.is_empty() {
                parsed.push(diagnostic_from_message(
                    "error",
                    "TypeScript typecheck failed with unknown diagnostics output",
                ));
            }
            parsed
        }
        Err(err) => vec![diagnostic_from_message(
            "error",
            &format!("failed to execute TypeScript checker: {err}"),
        )],
    };

    if let Err(err) = workspace.cleanup() {
        diagnostics.push(diagnostic_from_message(
            "warning",
            &format!("failed to remove temporary typecheck workspace: {err}"),
        ));
    }

    Value::Array(diagnostics)
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

#[derive(Debug)]
struct TypecheckWorkspace {
    root: PathBuf,
    tsconfig_path: PathBuf,
}

impl TypecheckWorkspace {
    fn prepare(source_ts: &str) -> Result<Self, std::io::Error> {
        let stamp =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
        let root = env::temp_dir().join(format!("plts-typecheck-{}-{stamp}", std::process::id()));
        let runtime_dir = root.join("node_modules/@stopgap/runtime");
        fs::create_dir_all(&runtime_dir)?;

        fs::write(
            runtime_dir.join("package.json"),
            "{\n  \"name\": \"@stopgap/runtime\",\n  \"types\": \"index.d.ts\"\n}\n",
        )?;
        fs::write(runtime_dir.join("index.d.ts"), RUNTIME_TYPECHECK_D_TS)?;
        fs::write(root.join("plts_typecheck_stubs.d.ts"), PLTS_RUNTIME_TYPECHECK_STUBS)?;
        fs::write(root.join("plts_module.ts"), source_ts)?;

        let tsconfig_path = root.join("tsconfig.json");
        fs::write(
            &tsconfig_path,
            "{\n  \"compilerOptions\": {\n    \"strict\": true,\n    \"target\": \"ES2022\",\n    \"module\": \"ES2022\",\n    \"moduleResolution\": \"Bundler\",\n    \"skipLibCheck\": true,\n    \"noEmit\": true,\n    \"noImplicitAny\": true\n  },\n  \"files\": [\"./plts_typecheck_stubs.d.ts\", \"./plts_module.ts\"]\n}\n",
        )?;

        Ok(Self { root, tsconfig_path })
    }

    fn cleanup(&self) -> Result<(), std::io::Error> {
        fs::remove_dir_all(&self.root)
    }
}

fn parse_tsc_diagnostics(raw: &[u8]) -> Vec<Value> {
    let text = String::from_utf8_lossy(raw);
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| line.contains("error TS"))
        .map(parse_tsc_diagnostic_line)
        .collect()
}

fn parse_tsc_diagnostic_line(line: &str) -> Value {
    let mut line_number = Value::Null;
    let mut column_number = Value::Null;
    let message = rewrite_tsc_diagnostic_message(line);

    if let Some((line_pos, col_pos)) = extract_tsc_line_column(line) {
        line_number = json!(line_pos);
        column_number = json!(col_pos);
    }

    json!({
        "severity": "error",
        "phase": "semantic",
        "message": message,
        "line": line_number,
        "column": column_number,
    })
}

fn rewrite_tsc_diagnostic_message(line: &str) -> String {
    let unresolved_prefix = "Cannot find module '@app/";
    let message_prefix = ": error TS2307: ";
    let Some(message_start) = line.find(message_prefix).map(|idx| idx + message_prefix.len())
    else {
        return line.to_string();
    };
    let message = &line[message_start..];
    if !message.starts_with(unresolved_prefix) {
        return line.to_string();
    }

    let Some(spec_start) = message.find('\'').map(|idx| idx + 1) else {
        return line.to_string();
    };
    let Some(spec_end_rel) = message[spec_start..].find('\'') else {
        return line.to_string();
    };
    let specifier = &message[spec_start..(spec_start + spec_end_rel)];
    format!(
        "unsupported bare module import `{specifier}`: `@app/*` imports are not supported yet during plts typecheck"
    )
}

fn extract_tsc_line_column(message: &str) -> Option<(u32, u32)> {
    let end = message.find("): error TS")?;
    let start = message[..end].rfind('(')?;
    let raw = &message[(start + 1)..end];
    let mut parts = raw.split(',');
    let line = parts.next()?.trim().parse::<u32>().ok()?;
    let column = parts.next()?.trim().parse::<u32>().ok()?;
    Some((line, column))
}

#[cfg(test)]
mod tests {
    use super::{parse_tsc_diagnostic_line, rewrite_tsc_diagnostic_message, tsgo_api_wasm_bytes};

    #[test]
    fn rewrites_unresolved_app_import_diagnostic() {
        let line = "plts_module.ts(2,22): error TS2307: Cannot find module '@app/math' or its corresponding type declarations.";
        let rewritten = rewrite_tsc_diagnostic_message(line);
        assert_eq!(
            rewritten,
            "unsupported bare module import `@app/math`: `@app/*` imports are not supported yet during plts typecheck"
        );
    }

    #[test]
    fn keeps_other_tsc_diagnostics_unchanged() {
        let line = "plts_module.ts(3,11): error TS2345: Argument of type 'string' is not assignable to parameter of type 'number'.";
        let parsed = parse_tsc_diagnostic_line(line);
        assert_eq!(parsed.get("message").and_then(|value| value.as_str()), Some(line));
        assert_eq!(parsed.get("line").and_then(|value| value.as_u64()), Some(3));
        assert_eq!(parsed.get("column").and_then(|value| value.as_u64()), Some(11));
    }

    #[test]
    fn embeds_tsgo_wasm_artifact() {
        let wasm = tsgo_api_wasm_bytes();
        assert!(wasm.len() > 8, "embedded tsgo wasm must not be empty");
        assert_eq!(&wasm[0..4], b"\0asm", "embedded tsgo payload must be wasm");
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
