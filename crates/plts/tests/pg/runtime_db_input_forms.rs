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
