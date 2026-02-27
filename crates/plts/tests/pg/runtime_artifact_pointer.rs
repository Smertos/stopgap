#[pg_test]
fn test_artifact_pointer_executes_compiled_program() {
    Spi::run(
        "
        DROP SCHEMA IF EXISTS plts_runtime_ptr_it CASCADE;
        CREATE SCHEMA plts_runtime_ptr_it;
        ",
    )
    .expect("artifact-pointer setup schema SQL should succeed");

    let source = "export default (ctx) => ({ mode: 'artifact', echoed: ctx.args });";
    let artifact_hash = Spi::get_one_with_args::<String>(
        "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
        &[source.into()],
    )
    .expect("compile_and_store query should succeed")
    .expect("compile_and_store should return artifact hash");

    let pointer = json!({
        "plts": 1,
        "kind": "artifact_ptr",
        "artifact_hash": artifact_hash,
        "export": "default",
        "mode": "stopgap_deployed"
    })
    .to_string()
    .replace('\'', "''");

    let create_sql = format!(
        "
        CREATE OR REPLACE FUNCTION plts_runtime_ptr_it.ptr_fn(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {} $$;
        ",
        pointer
    );
    Spi::run(create_sql.as_str()).expect("pointer function creation SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_ptr_it.ptr_fn('{\"id\": 42, \"tag\": \"ok\"}'::jsonb)",
    )
    .expect("pointer function invocation should succeed")
    .expect("pointer function should return jsonb");

    assert_eq!(payload.0.get("mode").and_then(Value::as_str), Some("artifact"));
    assert_eq!(
        payload.0.get("echoed").and_then(|value| value.get("id")).and_then(Value::as_i64),
        Some(42)
    );

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_ptr_it CASCADE;")
        .expect("artifact-pointer teardown SQL should succeed");
}
