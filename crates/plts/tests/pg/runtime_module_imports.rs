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
        export default (ctx: any) => ({ imported, id: ctx.args.id });
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

#[pg_test]
fn test_runtime_supports_module_imports_via_artifact_specifier() {
    let artifact_hash = Spi::get_one::<String>(
        r#"
        SELECT plts.compile_and_store(
            $$export const imported = 23;$$,
            '{}'::jsonb
        )
        "#,
    )
    .expect("artifact compile should succeed")
    .expect("artifact hash should be present");

    let setup_sql = format!(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_artifact_it CASCADE;
        CREATE SCHEMA plts_runtime_module_artifact_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_artifact_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import {{ imported }} from "plts+artifact:{artifact_hash}";
        export default (ctx: any) => ({{ imported, id: ctx.args.id }});
        $$;
        "#,
    );
    Spi::run(&setup_sql).expect("runtime artifact module import setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_module_artifact_it.imported('{\"id\": 17}'::jsonb)",
    )
    .expect("artifact-imported function invocation should succeed")
    .expect("artifact-imported function should return jsonb");

    assert_eq!(payload.0.get("imported").and_then(Value::as_i64), Some(23));
    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(17));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_artifact_it CASCADE;")
        .expect("runtime artifact module import teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_supports_nested_module_graph_with_artifact_imports() {
    let artifact_hash = Spi::get_one::<String>(
        r#"
        SELECT plts.compile_and_store(
            $$
            import { factor } from "data:text/javascript;base64,ZXhwb3J0IGNvbnN0IGZhY3RvciA9IDQ7";
            export const imported = factor * 3;
            $$,
            '{}'::jsonb
        )
        "#,
    )
    .expect("nested artifact compile should succeed")
    .expect("nested artifact hash should be present");

    let setup_sql = format!(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_graph_it CASCADE;
        CREATE SCHEMA plts_runtime_module_graph_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_graph_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import {{ imported }} from "plts+artifact:{artifact_hash}";
        export default () => ({{ imported }});
        $$;
        "#,
    );
    Spi::run(&setup_sql).expect("nested module graph setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_module_graph_it.imported('{}'::jsonb)")
            .expect("nested module graph invocation should succeed")
            .expect("nested module graph invocation should return jsonb");

    assert_eq!(payload.0.get("imported").and_then(Value::as_i64), Some(12));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_graph_it CASCADE;")
        .expect("nested module graph teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_supports_bare_imports_via_inline_import_map() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_bare_map_it CASCADE;
        CREATE SCHEMA plts_runtime_module_bare_map_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_bare_map_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        // plts-import-map: {"@pkg/math":"data:text/javascript;base64,ZXhwb3J0IGNvbnN0IGJhc2UgPSA0MDs="}
        // @ts-ignore runtime import-map coverage test
        import { base } from "@pkg/math";
        export default (ctx: any) => ({ total: base + ctx.args.delta });
        $$;
        "#,
    )
    .expect("bare import map setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_module_bare_map_it.imported('{\"delta\": 2}'::jsonb)",
    )
    .expect("bare import map invocation should succeed")
    .expect("bare import map invocation should return jsonb");

    assert_eq!(payload.0.get("total").and_then(Value::as_i64), Some(42));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_bare_map_it CASCADE;")
        .expect("bare import map teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_supports_bare_imports_via_pointer_import_map() {
    let dependency_hash = Spi::get_one::<String>(
        r#"
        SELECT plts.compile_and_store(
            $$export const base = 40;$$,
            '{}'::jsonb
        )
        "#,
    )
    .expect("dependency artifact compile should succeed")
    .expect("dependency artifact hash should be present");

    let main_hash = Spi::get_one::<String>(
        r#"
        SELECT plts.compile_and_store(
            $$
            import { base } from "@pkg/math";
            export default (ctx: any) => ({ total: base + ctx.args.delta });
            $$,
            '{}'::jsonb
        )
        "#,
    )
    .expect("main artifact compile should succeed")
    .expect("main artifact hash should be present");

    let pointer_body = serde_json::json!({
        "plts": 1,
        "kind": "artifact_ptr",
        "artifact_hash": main_hash,
        "export": "default",
        "mode": "stopgap_deployed",
        "import_map": {
            "@pkg/math": format!("plts+artifact:{dependency_hash}")
        }
    })
    .to_string();

    let setup_sql = format!(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_pointer_map_it CASCADE;
        CREATE SCHEMA plts_runtime_module_pointer_map_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_pointer_map_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {pointer_body} $$;
        "#,
    );
    Spi::run(&setup_sql).expect("pointer import map setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_module_pointer_map_it.imported('{\"delta\": 2}'::jsonb)",
    )
    .expect("pointer import map invocation should succeed")
    .expect("pointer import map invocation should return jsonb");

    assert_eq!(payload.0.get("total").and_then(Value::as_i64), Some(42));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_pointer_map_it CASCADE;")
        .expect("pointer import map teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_does_not_leak_imported_module_state_across_calls() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_state_it CASCADE;
        CREATE SCHEMA plts_runtime_module_state_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_state_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { bump } from "data:text/javascript;base64,bGV0IGNvdW50ZXIgPSAwOwpleHBvcnQgY29uc3QgYnVtcCA9ICgpID0+IHsKICBjb3VudGVyICs9IDE7CiAgcmV0dXJuIGNvdW50ZXI7Cn07";
        export default () => ({ counter: bump() });
        $$;
        "#,
    )
    .expect("module-state isolation setup SQL should succeed");

    let first =
        Spi::get_one::<JsonB>("SELECT plts_runtime_module_state_it.imported('{}'::jsonb)")
            .expect("first module-state invocation should succeed")
            .expect("first module-state invocation should return jsonb");
    let second =
        Spi::get_one::<JsonB>("SELECT plts_runtime_module_state_it.imported('{}'::jsonb)")
            .expect("second module-state invocation should succeed")
            .expect("second module-state invocation should return jsonb");

    assert_eq!(first.0.get("counter").and_then(Value::as_i64), Some(1));
    assert_eq!(second.0.get("counter").and_then(Value::as_i64), Some(1));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_state_it CASCADE;")
        .expect("module-state isolation teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_rejects_unmapped_bare_import_with_actionable_error() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_bare_missing_it CASCADE;
        CREATE SCHEMA plts_runtime_module_bare_missing_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_bare_missing_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        // @ts-ignore runtime unmapped bare import test
        import { base } from "@pkg/math";
        export default () => ({ base });
        $$;
        "#,
    )
    .expect("unmapped bare import setup SQL should succeed");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM plts_runtime_module_bare_missing_it.imported('{}'::jsonb);
            RAISE EXCEPTION 'expected unmapped bare import failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('unsupported bare module import `@pkg/math`' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
                IF POSITION('plts-import-map' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        "#,
    )
    .expect("unmapped bare import should fail with actionable error");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_bare_missing_it CASCADE;")
        .expect("unmapped bare import teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_typecheck_rejects_app_imports() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_app_import_it CASCADE;
        CREATE SCHEMA plts_runtime_module_app_import_it;
        "#,
    )
    .expect("app import schema setup should succeed");

    Spi::run(
        r#"
        DO $outer$
        BEGIN
            CREATE OR REPLACE FUNCTION plts_runtime_module_app_import_it.imported(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $fn$
            import { base } from "@app/math";
            export default () => ({ base });
            $fn$;
            RAISE EXCEPTION 'expected @app import typecheck failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('unsupported bare module import `@app/math`' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
                IF POSITION('@app/*` imports are not supported yet during plts typecheck' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $outer$;
        "#,
    )
    .expect("@app imports should fail with explicit typecheck diagnostics");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_app_import_it CASCADE;")
        .expect("app import schema teardown should succeed");
}

#[pg_test]
fn test_runtime_typecheck_infers_wrapper_args_from_v_schema() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_type_infer_it CASCADE;
        CREATE SCHEMA plts_runtime_module_type_infer_it;
        "#,
    )
    .expect("type inference schema setup should succeed");

    Spi::run(
        r#"
        DO $outer$
        BEGIN
            CREATE OR REPLACE FUNCTION plts_runtime_module_type_infer_it.imported(args jsonb)
            RETURNS jsonb
            LANGUAGE plts
            AS $fn$
            import { query, v } from "@stopgap/runtime";
            export default query(v.object({ id: v.int() }), async (args, _ctx) => {
                return { bad: args.id.toUpperCase() };
            });
            $fn$;
            RAISE EXCEPTION 'expected wrapper arg typecheck failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('Property ''toUpperCase'' does not exist on type ''number''' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $outer$;
        "#,
    )
    .expect("wrapper schema inference should enforce typed args during typecheck");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_type_infer_it CASCADE;")
        .expect("type inference schema teardown should succeed");
}

#[pg_test]
fn test_runtime_rejects_unknown_artifact_module_specifier() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_module_missing_artifact_it CASCADE;
        CREATE SCHEMA plts_runtime_module_missing_artifact_it;
        CREATE OR REPLACE FUNCTION plts_runtime_module_missing_artifact_it.imported(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { imported } from "plts+artifact:sha256:missing";
        export default () => ({ imported });
        $$;
        "#,
    )
    .expect("missing artifact module setup SQL should succeed");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM plts_runtime_module_missing_artifact_it.imported('{}'::jsonb);
            RAISE EXCEPTION 'expected missing artifact module import failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('artifact `sha256:missing` not found' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        "#,
    )
    .expect("missing artifact module should fail with clear error");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_module_missing_artifact_it CASCADE;")
        .expect("missing artifact module teardown SQL should succeed");
}
