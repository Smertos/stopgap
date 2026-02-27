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

#[pg_test]
fn test_stopgap_query_wrapper_allows_keyword_literals() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_literal_it CASCADE;
        CREATE SCHEMA plts_runtime_stopgap_query_literal_it;
        CREATE OR REPLACE FUNCTION plts_runtime_stopgap_query_literal_it.wrapped(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import { query } from "@stopgap/runtime";

        export default query({ type: "object" }, async (_args, ctx) => {
            const rows = await ctx.db.query("SELECT 'update' AS verb, $$delete$$ AS body", []);
            return { verb: rows[0].verb, body: rows[0].body, dbMode: ctx.db.mode };
        });
        $$;
        "#,
    )
    .expect("stopgap query literal keyword setup SQL should succeed");

    let payload =
        Spi::get_one::<JsonB>("SELECT plts_runtime_stopgap_query_literal_it.wrapped('{}'::jsonb)")
            .expect("query wrapper literal keyword invocation should succeed")
            .expect("query wrapper literal keyword invocation should return jsonb");

    assert_eq!(payload.0.get("verb").and_then(Value::as_str), Some("update"));
    assert_eq!(payload.0.get("body").and_then(Value::as_str), Some("delete"));
    assert_eq!(payload.0.get("dbMode").and_then(Value::as_str), Some("ro"));

    Spi::run("DROP SCHEMA IF EXISTS plts_runtime_stopgap_query_literal_it CASCADE;")
        .expect("stopgap query literal keyword teardown SQL should succeed");
}

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
