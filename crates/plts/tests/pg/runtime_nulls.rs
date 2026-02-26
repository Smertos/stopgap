#[pg_test]
fn test_runtime_normalizes_null_and_undefined_to_sql_null() {
    Spi::run(
        "
        DROP SCHEMA IF EXISTS plts_runtime_it CASCADE;
        CREATE SCHEMA plts_runtime_it;
        CREATE OR REPLACE FUNCTION plts_runtime_it.return_null(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => null;
        $$;
        CREATE OR REPLACE FUNCTION plts_runtime_it.return_undefined(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => undefined;
        $$;
        CREATE OR REPLACE FUNCTION plts_runtime_it.return_object(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => ({ ok: true });
        $$;
        ",
    )
    .expect("runtime null-normalization setup SQL should succeed");

    let null_is_sql_null =
        Spi::get_one::<bool>("SELECT plts_runtime_it.return_null('{}'::jsonb) IS NULL")
            .expect("return_null query should succeed")
            .expect("return_null IS NULL predicate should return a row");
    assert!(null_is_sql_null, "runtime should map JS null to SQL NULL");

    let undefined_is_sql_null =
        Spi::get_one::<bool>("SELECT plts_runtime_it.return_undefined('{}'::jsonb) IS NULL")
            .expect("return_undefined query should succeed")
            .expect("return_undefined IS NULL predicate should return a row");
    assert!(undefined_is_sql_null, "runtime should map JS undefined to SQL NULL");

    let object = Spi::get_one::<JsonB>("SELECT plts_runtime_it.return_object('{}'::jsonb)")
        .expect("return_object query should succeed")
        .expect("return_object should return jsonb for non-null result");
    assert_eq!(object.0.get("ok").and_then(Value::as_bool), Some(true));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_it CASCADE;")
        .expect("runtime null-normalization teardown SQL should succeed");
}
