fn ensure_mock_plts_runtime() {
    Spi::run(
        "
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_language WHERE lanname = 'plts') THEN
                CREATE LANGUAGE plts
                HANDLER plpgsql_call_handler;
            END IF;
        END;
        $$;

        CREATE SCHEMA IF NOT EXISTS plts;

        CREATE TABLE IF NOT EXISTS plts.artifact (
            artifact_hash text PRIMARY KEY,
            source_ts text NOT NULL,
            compiled_js text NOT NULL,
            compiler_opts jsonb NOT NULL
        );

        CREATE OR REPLACE FUNCTION plts.compile_and_store(source_ts text, compiler_opts jsonb)
        RETURNS text
        LANGUAGE plpgsql
        AS $$
        DECLARE
            hash text;
        BEGIN
            hash := 'sha256:' || md5(COALESCE(source_ts, '') || COALESCE(compiler_opts::text, ''));

            INSERT INTO plts.artifact(artifact_hash, source_ts, compiled_js, compiler_opts)
            VALUES (hash, source_ts, source_ts, compiler_opts)
            ON CONFLICT (artifact_hash) DO UPDATE
            SET source_ts = EXCLUDED.source_ts,
                compiled_js = EXCLUDED.compiled_js,
                compiler_opts = EXCLUDED.compiler_opts;

            RETURN hash;
        END;
        $$;
        ",
    )
    .expect("mock plts runtime setup should succeed");
}

fn create_deployable_function(schema: &str, fn_name: &str, source: &str) {
    let sql = format!(
        "
        CREATE OR REPLACE FUNCTION {}.{}(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {} $$;
        ",
        crate::quote_ident(schema),
        crate::quote_ident(fn_name),
        source
    );
    Spi::run(sql.as_str()).expect("deployable function should be created");
}

fn pointer_artifact_hash(live_schema: &str, fn_name: &str) -> String {
    let pointer = Spi::get_one_with_args::<String>(
        "
        SELECT p.prosrc::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1
          AND p.proname = $2
          AND p.prorettype = 'jsonb'::regtype::oid
          AND array_length(p.proargtypes::oid[], 1) = 1
          AND p.proargtypes[0] = 'jsonb'::regtype::oid
        ",
        &[live_schema.into(), fn_name.into()],
    )
    .expect("live pointer function lookup should succeed")
    .expect("live pointer function should exist");

    serde_json::from_str::<Value>(&pointer)
        .expect("live pointer body should be valid json")
        .get("artifact_hash")
        .and_then(Value::as_str)
        .expect("live pointer body should include artifact_hash")
        .to_string()
}

fn fn_version_artifact_hash(deployment_id: i64, fn_name: &str) -> String {
    Spi::get_one_with_args::<String>(
        "
        SELECT artifact_hash
        FROM stopgap.fn_version
        WHERE deployment_id = $1
          AND fn_name = $2
        ",
        &[deployment_id.into(), fn_name.into()],
    )
    .expect("fn_version lookup should succeed")
    .expect("fn_version row should exist")
}
