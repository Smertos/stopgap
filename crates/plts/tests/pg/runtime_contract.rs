#[pg_test]
fn test_runtime_contract_exposes_fn_identity_and_now() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_contract_ctx_it CASCADE;
        CREATE SCHEMA plts_runtime_contract_ctx_it;
        CREATE OR REPLACE FUNCTION plts_runtime_contract_ctx_it.ctx_shape(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default (ctx) => ({
            schema: ctx.fn.schema,
            name: ctx.fn.name,
            oid: ctx.fn.oid,
            now: ctx.now,
        });
        $$;
        "#,
    )
    .expect("runtime contract ctx setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_contract_ctx_it.ctx_shape('{\"id\": 1}'::jsonb)",
    )
    .expect("runtime contract ctx invocation should succeed")
    .expect("runtime contract ctx function should return jsonb");

    assert_eq!(
        payload.0.get("schema").and_then(Value::as_str),
        Some("plts_runtime_contract_ctx_it")
    );
    assert_eq!(payload.0.get("name").and_then(Value::as_str), Some("ctx_shape"));
    assert!(
        payload.0.get("oid").and_then(Value::as_i64).unwrap_or_default() > 0,
        "runtime contract should expose a positive function oid"
    );
    assert!(
        payload.0.get("now").and_then(Value::as_str).is_some_and(|v| !v.is_empty()),
        "runtime contract should expose a non-empty now timestamp"
    );

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_contract_ctx_it CASCADE;")
        .expect("runtime contract ctx teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_contract_regular_handler_db_exec_returns_ok() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_contract_exec_it CASCADE;
        CREATE SCHEMA plts_runtime_contract_exec_it;
        CREATE TABLE plts_runtime_contract_exec_it.items(id int4);
        CREATE OR REPLACE FUNCTION plts_runtime_contract_exec_it.exec_shape(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default async (ctx) => {
            const execResult = await ctx.db.exec(
                "INSERT INTO plts_runtime_contract_exec_it.items(id) VALUES ($1)",
                [9]
            );
            const rows = await ctx.db.query(
                "SELECT id FROM plts_runtime_contract_exec_it.items ORDER BY id",
                []
            );
            return {
                mode: ctx.db.mode,
                execResult,
                inserted: rows[0]?.id ?? null,
            };
        };
        $$;
        "#,
    )
    .expect("runtime contract exec setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_contract_exec_it.exec_shape('{}'::jsonb)")
            .expect("runtime contract exec invocation should succeed")
            .expect("runtime contract exec function should return jsonb");

    assert_eq!(payload.0.get("mode").and_then(Value::as_str), Some("rw"));
    assert_eq!(
        payload.0.get("execResult").and_then(|v| v.get("ok")).and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(payload.0.get("inserted").and_then(Value::as_i64), Some(9));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_contract_exec_it CASCADE;")
        .expect("runtime contract exec teardown SQL should succeed");
}

#[pg_test]
fn test_runtime_contract_invocation_state_is_isolated() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_contract_isolation_it CASCADE;
        CREATE SCHEMA plts_runtime_contract_isolation_it;
        CREATE OR REPLACE FUNCTION plts_runtime_contract_isolation_it.ctx_isolation(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default (ctx) => {
            const previousId = globalThis.__plts_last_seen_id ?? null;
            globalThis.__plts_last_seen_id = ctx.args.id;
            return {
                previousId,
                currentId: ctx.args.id,
                fnOid: ctx.fn.oid,
            };
        };
        $$;
        "#,
    )
    .expect("runtime contract isolation setup SQL should succeed");

    let first = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_contract_isolation_it.ctx_isolation('{\"id\": 1}'::jsonb)",
    )
    .expect("first runtime contract isolation invocation should succeed")
    .expect("first runtime contract isolation invocation should return jsonb");

    let second = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_contract_isolation_it.ctx_isolation('{\"id\": 2}'::jsonb)",
    )
    .expect("second runtime contract isolation invocation should succeed")
    .expect("second runtime contract isolation invocation should return jsonb");

    assert_eq!(first.0.get("previousId"), Some(&Value::Null));
    assert_eq!(second.0.get("previousId"), Some(&Value::Null));
    assert_eq!(first.0.get("currentId").and_then(Value::as_i64), Some(1));
    assert_eq!(second.0.get("currentId").and_then(Value::as_i64), Some(2));
    assert!(
        first.0.get("fnOid").and_then(Value::as_i64).unwrap_or_default() > 0,
        "runtime contract isolation should expose function oid"
    );
    assert_eq!(first.0.get("fnOid"), second.0.get("fnOid"));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_contract_isolation_it CASCADE;")
        .expect("runtime contract isolation teardown SQL should succeed");
}
