use pgrx::prelude::*;
use pgrx::JsonB;
#[cfg(feature = "v8_runtime")]
use serde_json::json;
use serde_json::Value;

#[pg_test]
fn test_compile_and_store_round_trip() {
    let source = "export default (ctx) => ({ ok: true, args: ctx.args })";
    let artifact_hash = Spi::get_one_with_args::<String>(
        "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
        &[source.into()],
    )
    .expect("compile_and_store query should succeed")
    .expect("compile_and_store should return an artifact hash");

    assert!(artifact_hash.starts_with("sha256:"));

    let artifact =
        Spi::get_one_with_args::<JsonB>("SELECT plts.get_artifact($1)", &[artifact_hash.into()])
            .expect("get_artifact query should succeed")
            .expect("artifact must exist after compile_and_store");

    assert_eq!(
        artifact.0.get("source_ts").and_then(Value::as_str),
        Some(source),
        "stored artifact should preserve source_ts"
    );
    assert!(
        artifact
            .0
            .get("compiled_js")
            .and_then(Value::as_str)
            .is_some_and(|compiled| !compiled.is_empty()),
        "stored artifact should include compiled_js"
    );
}

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
        export default () => null;
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

#[cfg(feature = "v8_runtime")]
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

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_artifact_pointer_executes_compiled_program() {
    Spi::run(
        "
        DROP SCHEMA IF EXISTS plts_runtime_ptr_it CASCADE;
        CREATE SCHEMA plts_runtime_ptr_it;
        ",
    )
    .expect("artifact-pointer setup schema SQL should succeed");

    let source = "export default (ctx) => ({ mode: 'artifact', echoed: ctx.args.positional[0] });";
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

#[cfg(feature = "v8_runtime")]
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

#[cfg(feature = "v8_runtime")]
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

#[cfg(feature = "v8_runtime")]
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

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_runtime_supports_stopgap_runtime_bare_import() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_import_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_import_it;
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_import_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { query } from "@stopgap/runtime";

        export default query({ type: "object" }, async (args, ctx) => ({
            kind: "query",
            id: args.id,
            dbMode: ctx.db.mode,
        }));
        $$;
        "#,
    )
    .expect("runtime stopgap import setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_stopgap_import_it.wrapped('{\"id\": 13}'::jsonb)",
    )
    .expect("wrapped function invocation should succeed")
    .expect("wrapped function should return jsonb");

    assert_eq!(payload.0.get("kind").and_then(Value::as_str), Some("query"));
    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(13));
    assert_eq!(payload.0.get("dbMode").and_then(Value::as_str), Some("ro"));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_import_it CASCADE;")
        .expect("runtime stopgap import teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_stopgap_query_wrapper_rejects_db_exec() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_exec_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_query_exec_it;
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_query_exec_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { query } from "@stopgap/runtime";

        export default query({ type: "object" }, async (_args, ctx) => {
            await ctx.db.exec("SELECT 1", []);
            return { ok: true };
        });
        $$;
        "#,
    )
    .expect("stopgap query exec rejection setup SQL should succeed");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM plts_runtime_stopgap_query_exec_it.wrapped('{}'::jsonb);
            RAISE EXCEPTION 'expected db.exec rejection for query wrapper';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('db.exec is disabled for stopgap.query handlers' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        "#,
    )
    .expect("query wrapper should reject db.exec");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_exec_it CASCADE;")
        .expect("stopgap query exec rejection teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_stopgap_query_wrapper_validates_json_schema() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_schema_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_schema_it;
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_schema_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { query } from "@stopgap/runtime";

        const schema = {
            type: "object",
            required: ["id"],
            additionalProperties: false,
            properties: {
                id: { type: "integer" }
            }
        };

        export default query(schema, async (args, _ctx) => ({ id: args.id }));
        $$;
        "#,
    )
    .expect("stopgap schema validation setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_stopgap_schema_it.wrapped('{\"id\": 22}'::jsonb)",
    )
    .expect("wrapped function invocation should succeed")
    .expect("wrapped function should return jsonb");

    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(22));

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM plts_runtime_stopgap_schema_it.wrapped('{}'::jsonb);
            RAISE EXCEPTION 'expected schema validation failure for missing id';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('args validation failed at $.id: missing required property' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        "#,
    )
    .expect("query wrapper should reject invalid args schema payload");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_schema_it CASCADE;")
        .expect("stopgap schema validation teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_stopgap_query_wrapper_rejects_write_sql_in_db_query() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_write_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_query_write_it;
        CREATE TABLE plts_runtime_stopgap_query_write_it.items(id int4);
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_query_write_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { query } from "@stopgap/runtime";

        export default query({ type: "object" }, async (_args, ctx) => {
            await ctx.db.query("WITH w AS (INSERT INTO plts_runtime_stopgap_query_write_it.items(id) VALUES (1) RETURNING id) SELECT id FROM w", []);
            return { ok: true };
        });
        $$;
        "#,
    )
    .expect("stopgap query write rejection setup SQL should succeed");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM plts_runtime_stopgap_query_write_it.wrapped('{}'::jsonb);
            RAISE EXCEPTION 'expected write SQL rejection for query wrapper';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('db.query is read-only for stopgap.query handlers' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        "#,
    )
    .expect("query wrapper should reject write SQL through db.query");

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_write_it CASCADE;")
        .expect("stopgap query write rejection teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_stopgap_mutation_wrapper_allows_db_exec() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_mutation_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_mutation_it;
        CREATE TABLE plts_runtime_stopgap_mutation_it.items(id int4);
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_mutation_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { mutation } from "@stopgap/runtime";

        export default mutation({ type: "object" }, async (args, ctx) => {
            await ctx.db.exec("INSERT INTO plts_runtime_stopgap_mutation_it.items(id) VALUES ($1)", [args.id]);
            const rows = await ctx.db.query("SELECT id FROM plts_runtime_stopgap_mutation_it.items ORDER BY id", []);
            return { kind: "mutation", dbMode: ctx.db.mode, count: rows.length };
        });
        $$;
        "#,
    )
    .expect("stopgap mutation setup SQL should succeed");

    let payload = Spi::get_one::<JsonB>(
        "SELECT plts_runtime_stopgap_mutation_it.wrapped('{\"id\": 17}'::jsonb)",
    )
    .expect("mutation wrapper invocation should succeed")
    .expect("mutation wrapper should return jsonb");

    assert_eq!(payload.0.get("kind").and_then(Value::as_str), Some("mutation"));
    assert_eq!(payload.0.get("dbMode").and_then(Value::as_str), Some("rw"));
    assert_eq!(payload.0.get("count").and_then(Value::as_i64), Some(1));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_mutation_it CASCADE;")
        .expect("stopgap mutation teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_runtime_db_query_accepts_sql_object_input() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_db_object_query_it CASCADE;
        CREATE SCHEMA plts_runtime_db_object_query_it;
        CREATE OR REPLACE FUNCTION plts_runtime_db_object_query_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default async (_ctx) => {
            const rows = await _ctx.db.query({
                sql: "SELECT $1::int4 AS id",
                params: [41]
            });
            return { id: rows[0]?.id ?? null };
        };
        $$;
        "#,
    )
    .expect("runtime sql object query setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_db_object_query_it.wrapped('{}'::jsonb)")
            .expect("sql object query invocation should succeed")
            .expect("sql object query should return jsonb");

    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(41));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_db_object_query_it CASCADE;")
        .expect("runtime sql object query teardown SQL should succeed");
}

#[cfg(feature = "v8_runtime")]
#[pg_test]
fn test_runtime_db_exec_accepts_to_sql_input() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_db_to_sql_exec_it CASCADE;
        CREATE SCHEMA plts_runtime_db_to_sql_exec_it;
        CREATE TABLE plts_runtime_db_to_sql_exec_it.items(id int4);
        CREATE OR REPLACE FUNCTION plts_runtime_db_to_sql_exec_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default async (_ctx) => {
            const insert = {
                toSQL() {
                    return {
                        sql: "INSERT INTO plts_runtime_db_to_sql_exec_it.items(id) VALUES ($1)",
                        params: [7]
                    };
                }
            };

            const selectRows = {
                toSQL() {
                    return {
                        sql: "SELECT id FROM plts_runtime_db_to_sql_exec_it.items ORDER BY id",
                        params: []
                    };
                }
            };

            await _ctx.db.exec(insert);
            const rows = await _ctx.db.query(selectRows);
            return { count: rows.length, id: rows[0]?.id ?? null };
        };
        $$;
        "#,
    )
    .expect("runtime toSQL exec setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_db_to_sql_exec_it.wrapped('{}'::jsonb)")
            .expect("toSQL exec invocation should succeed")
            .expect("toSQL exec should return jsonb");

    assert_eq!(payload.0.get("count").and_then(Value::as_i64), Some(1));
    assert_eq!(payload.0.get("id").and_then(Value::as_i64), Some(7));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_db_to_sql_exec_it CASCADE;")
        .expect("runtime toSQL exec teardown SQL should succeed");
}
