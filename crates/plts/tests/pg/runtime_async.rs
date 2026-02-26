#[pg_test]
fn test_runtime_supports_async_default_export() {
    Spi::run(
        "
        DROP SCHEMA IF EXISTS plts_runtime_async_it CASCADE;
        CREATE SCHEMA plts_runtime_async_it;
        CREATE OR REPLACE FUNCTION plts_runtime_async_it.return_async(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default async (ctx) => {
            const row = await Promise.resolve({ ok: true, id: ctx.args.id });
            return row;
        };
        $$;
        ",
    )
    .expect("runtime async setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_async_it.return_async('{\"id\": 7}'::jsonb)")
            .expect("async function invocation should succeed")
            .expect("async function should return jsonb");

    assert_eq!(payload.0.get("ok").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(7));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_async_it CASCADE;")
        .expect("runtime async teardown SQL should succeed");
}
