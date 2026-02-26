#[pg_test]
fn test_runtime_does_not_expose_network_or_fs_globals() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_surface_it CASCADE;
        CREATE SCHEMA plts_runtime_surface_it;
        CREATE OR REPLACE FUNCTION plts_runtime_surface_it.globals(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => ({
            denoType: typeof Deno,
            fetchType: typeof fetch,
            requestType: typeof Request,
            websocketType: typeof WebSocket,
        });
        $$;
        "#,
    )
    .expect("runtime surface lockdown setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>("SELECT plts_runtime_surface_it.globals('{}'::jsonb)")
        .expect("runtime globals invocation should succeed")
        .expect("runtime globals should return jsonb payload");

    assert_eq!(payload.0.get("denoType").and_then(Value::as_str), Some("undefined"));
    assert_eq!(payload.0.get("fetchType").and_then(Value::as_str), Some("undefined"));
    assert_eq!(payload.0.get("requestType").and_then(Value::as_str), Some("undefined"));
    assert_eq!(payload.0.get("websocketType").and_then(Value::as_str), Some("undefined"));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_surface_it CASCADE;")
        .expect("runtime surface lockdown teardown SQL should succeed");
}
