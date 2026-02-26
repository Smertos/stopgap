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
