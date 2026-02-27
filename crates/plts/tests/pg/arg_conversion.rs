#[pg_test]
fn test_regular_args_conversion_for_common_types() {
    Spi::run(
        "
        DROP SCHEMA IF EXISTS plts_it CASCADE;
        CREATE SCHEMA plts_it;
        CREATE OR REPLACE FUNCTION plts_it.arg_echo(t text, i int4, b boolean, j jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default (ctx) => ({ positional: ctx.args.positional, named: ctx.args.named });
        $$;
        ",
    )
    .expect("test setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "
        SELECT plts_it.arg_echo('hello', 42, true, '{\"ok\": true}'::jsonb)
        ",
    )
    .expect("arg_echo query should succeed")
    .expect("arg_echo should return a json payload in non-runtime mode");

    assert_eq!(
        payload
            .0
            .get("positional")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(Value::as_str),
        Some("hello")
    );
    assert_eq!(
        payload
            .0
            .get("positional")
            .and_then(Value::as_array)
            .and_then(|items| items.get(1))
            .and_then(Value::as_i64),
        Some(42)
    );
    assert_eq!(
        payload
            .0
            .get("positional")
            .and_then(Value::as_array)
            .and_then(|items| items.get(2))
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload
            .0
            .get("positional")
            .and_then(Value::as_array)
            .and_then(|items| items.get(3))
            .and_then(|entry| entry.get("ok"))
            .and_then(Value::as_bool),
        Some(true)
    );

    assert_eq!(
        payload.0.get("named").and_then(|named| named.get("0")).and_then(Value::as_str),
        Some("hello")
    );
    assert_eq!(
        payload.0.get("named").and_then(|named| named.get("1")).and_then(Value::as_i64),
        Some(42)
    );
    assert_eq!(
        payload.0.get("named").and_then(|named| named.get("2")).and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload
            .0
            .get("named")
            .and_then(|named| named.get("3"))
            .and_then(|entry| entry.get("ok"))
            .and_then(Value::as_bool),
        Some(true)
    );

    Spi::run("DROP SCHEMA IF EXISTS plts_it CASCADE;").expect("test teardown SQL should succeed");
}
