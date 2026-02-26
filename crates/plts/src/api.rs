use crate::compiler::{
    compiler_fingerprint, compute_artifact_hash, maybe_extract_source_map, transpile_typescript,
};
use crate::runtime::bootstrap_v8_isolate;
use common::sql::quote_literal;
use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::JsonB;

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
        let fingerprint = compiler_fingerprint();
        let hash = compute_artifact_hash(source_ts, compiled_js, &compiler_opts.0, fingerprint);
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
            quote_literal(fingerprint),
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

        if contains_error_diagnostics(&diagnostics.0) {
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

fn contains_error_diagnostics(diagnostics: &serde_json::Value) -> bool {
    diagnostics
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .any(|entry| entry.get("severity").and_then(|v| v.as_str()) == Some("error"))
        })
        .unwrap_or(false)
}
