use pgrx::prelude::*;

mod api;
mod arg_mapping;
mod compiler;
mod function_program;
mod handler;
mod observability;
mod runtime;
mod runtime_spi;

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

#[cfg(test)]
mod unit_tests {
    #[test]
    fn test_hash_prefix() {
        let hash = crate::compiler::compute_artifact_hash(
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
        let (message, stack) = crate::runtime::parse_js_error_details(details);
        assert_eq!(message, "Uncaught Error: boom");
        assert_eq!(stack.as_deref(), Some("at default (plts_module.js:1:1)\n    at foo"));
    }

    #[test]
    fn test_runtime_exec_error_display() {
        let err = crate::runtime::RuntimeExecError::with_stack(
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
        let (compiled, diagnostics) =
            crate::compiler::transpile_typescript(source, &serde_json::json!({}));
        assert!(diagnostics.as_array().is_some_and(|items| items.is_empty()));
        assert!(compiled.contains("export default"));
        assert!(!compiled.contains(": { args:"));
    }

    #[test]
    fn test_transpile_typescript_returns_diagnostic_on_parse_error() {
        let (compiled, diagnostics) = crate::compiler::transpile_typescript(
            "export default (ctx => ctx",
            &serde_json::json!({}),
        );
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
        let compiled = "console.log('x');\n//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozfQ==";
        let source_map = crate::compiler::extract_inline_source_map(compiled)
            .expect("inline source map should decode from base64 payload");
        assert!(source_map.contains("\"version\":3"));
    }

    #[test]
    fn test_transpile_typescript_optionally_emits_source_map_payload() {
        let source =
            "export default (ctx: { args: { id: number } }) => ({ id: ctx.args.id as number });";
        let (compiled, diagnostics) = crate::compiler::transpile_typescript(
            source,
            &serde_json::json!({ "source_map": true }),
        );
        assert!(diagnostics.as_array().is_some_and(|items| items.is_empty()));

        let source_map = crate::compiler::maybe_extract_source_map(
            &compiled,
            &serde_json::json!({ "source_map": true }),
        )
        .expect("source_map=true should persist an inline source map payload");

        assert!(source_map.contains("\"version\""));
    }

    #[test]
    fn test_compiler_fingerprint_includes_dependency_versions() {
        let fingerprint = crate::compiler::compiler_fingerprint();
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
        assert_eq!(crate::runtime::parse_statement_timeout_ms("0"), None);
        assert_eq!(crate::runtime::parse_statement_timeout_ms("250"), Some(250));
        assert_eq!(crate::runtime::parse_statement_timeout_ms("250ms"), Some(250));
        assert_eq!(crate::runtime::parse_statement_timeout_ms("2s"), Some(2_000));
        assert_eq!(crate::runtime::parse_statement_timeout_ms("1min"), Some(60_000));
        assert_eq!(crate::runtime::parse_statement_timeout_ms("1.5s"), Some(1_500));
        assert_eq!(crate::runtime::parse_statement_timeout_ms("500us"), Some(1));
    }

    #[test]
    fn test_parse_statement_timeout_ms_rejects_invalid_values() {
        assert_eq!(crate::runtime::parse_statement_timeout_ms(""), None);
        assert_eq!(crate::runtime::parse_statement_timeout_ms("off"), None);
        assert_eq!(crate::runtime::parse_statement_timeout_ms("-5ms"), None);
        assert_eq!(crate::runtime::parse_statement_timeout_ms("12fortnights"), None);
    }

    #[test]
    fn test_parse_runtime_heap_limit_bytes_parses_expected_units() {
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("0"), None);
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("32"), Some(32 * 1024 * 1024));
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("32mb"), Some(32 * 1024 * 1024));
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("1.5mb"), Some(1_572_864));
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("512kb"), Some(524_288));
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("2gb"), Some(2_147_483_648));
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("2048 bytes"), Some(2048));
    }

    #[test]
    fn test_parse_runtime_heap_limit_bytes_rejects_invalid_values() {
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes(""), None);
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("off"), None);
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("-1mb"), None);
        assert_eq!(crate::runtime::parse_runtime_heap_limit_bytes("12fortnights"), None);
    }

    #[test]
    fn test_resolve_runtime_timeout_ms_prefers_most_restrictive_limit() {
        assert_eq!(crate::runtime::resolve_runtime_timeout_ms(None, None), None);
        assert_eq!(crate::runtime::resolve_runtime_timeout_ms(Some(1_000), None), Some(1_000));
        assert_eq!(crate::runtime::resolve_runtime_timeout_ms(None, Some(750)), Some(750));
        assert_eq!(crate::runtime::resolve_runtime_timeout_ms(Some(2_000), Some(750)), Some(750));
        assert_eq!(crate::runtime::resolve_runtime_timeout_ms(Some(500), Some(3_000)), Some(500));
    }

    #[test]
    fn test_interrupt_pending_from_flags_detects_pending_signal() {
        assert!(!crate::runtime::interrupt_pending_from_flags(0, 0, 0));
        assert!(crate::runtime::interrupt_pending_from_flags(1, 0, 0));
        assert!(crate::runtime::interrupt_pending_from_flags(0, 1, 0));
        assert!(crate::runtime::interrupt_pending_from_flags(0, 0, 1));
    }

    #[test]
    fn test_parse_positive_usize_parses_valid_limits() {
        assert_eq!(crate::runtime_spi::parse_positive_usize("1"), Some(1));
        assert_eq!(crate::runtime_spi::parse_positive_usize("2048"), Some(2048));
        assert_eq!(crate::runtime_spi::parse_positive_usize(" 512 "), Some(512));
    }

    #[test]
    fn test_parse_positive_usize_rejects_non_positive_or_invalid_values() {
        assert_eq!(crate::runtime_spi::parse_positive_usize(""), None);
        assert_eq!(crate::runtime_spi::parse_positive_usize("0"), None);
        assert_eq!(crate::runtime_spi::parse_positive_usize("-5"), None);
        assert_eq!(crate::runtime_spi::parse_positive_usize("abc"), None);
    }

    #[cfg(feature = "v8_runtime")]
    #[test]
    fn test_bind_json_params_maps_common_value_types() {
        let params = crate::runtime_spi::bind_json_params(vec![
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!("hello"),
            serde_json::json!({ "ok": true }),
            serde_json::Value::Null,
        ]);

        assert!(matches!(params[0], crate::runtime_spi::BoundParam::Bool(true)));
        assert!(matches!(params[1], crate::runtime_spi::BoundParam::Int(42)));
        assert!(matches!(params[2], crate::runtime_spi::BoundParam::Text(ref v) if v == "hello"));
        assert!(matches!(params[3], crate::runtime_spi::BoundParam::Json(_)));
        assert!(matches!(params[4], crate::runtime_spi::BoundParam::NullText));
    }

    #[cfg(feature = "v8_runtime")]
    #[test]
    fn test_is_read_only_sql_accepts_select_and_rejects_writes() {
        assert!(crate::runtime_spi::is_read_only_sql("SELECT 1"));
        assert!(crate::runtime_spi::is_read_only_sql("-- comment\nSELECT now()"));
        assert!(crate::runtime_spi::is_read_only_sql("/* leading */ SELECT * FROM pg_class"));
        assert!(crate::runtime_spi::is_read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(crate::runtime_spi::is_read_only_sql("SELECT 'update' AS verb"));
        assert!(crate::runtime_spi::is_read_only_sql("SELECT $$delete from users$$ AS sql_text"));
        assert!(crate::runtime_spi::is_read_only_sql(
            "SELECT \"drop\" FROM (SELECT 1 AS \"drop\") t"
        ));

        assert!(!crate::runtime_spi::is_read_only_sql("INSERT INTO t(id) VALUES (1)"));
        assert!(!crate::runtime_spi::is_read_only_sql(
            "WITH x AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT * FROM x"
        ));
        assert!(!crate::runtime_spi::is_read_only_sql("DELETE FROM t"));
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
