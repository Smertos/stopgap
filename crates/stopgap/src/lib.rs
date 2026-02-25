use pgrx::prelude::*;
use serde_json::json;

::pgrx::pg_module_magic!(name, version);

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS stopgap;

    CREATE TABLE IF NOT EXISTS stopgap.environment (
        env text PRIMARY KEY,
        live_schema name NOT NULL,
        active_deployment_id bigint,
        updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS stopgap.deployment (
        id bigserial PRIMARY KEY,
        env text NOT NULL REFERENCES stopgap.environment(env),
        label text,
        created_at timestamptz NOT NULL DEFAULT now(),
        created_by name NOT NULL DEFAULT current_user,
        source_schema name NOT NULL,
        status text NOT NULL,
        manifest jsonb NOT NULL
    );

    CREATE TABLE IF NOT EXISTS stopgap.fn_version (
        deployment_id bigint NOT NULL REFERENCES stopgap.deployment(id),
        fn_name name NOT NULL,
        fn_schema name NOT NULL,
        live_fn_schema name NOT NULL,
        kind text NOT NULL,
        artifact_hash text NOT NULL,
        PRIMARY KEY (deployment_id, fn_schema, fn_name)
    );

    CREATE TABLE IF NOT EXISTS stopgap.activation_log (
        id bigserial PRIMARY KEY,
        env text NOT NULL,
        from_deployment_id bigint,
        to_deployment_id bigint NOT NULL,
        activated_at timestamptz NOT NULL DEFAULT now(),
        activated_by name NOT NULL DEFAULT current_user
    );
    "#,
    name = "stopgap_sql_bootstrap"
);

#[pg_extern]
fn hello_stopgap() -> &'static str {
    "Hello, stopgap"
}

#[pg_schema]
mod stopgap {
    use super::*;

    #[pg_extern]
    fn version() -> &'static str {
        "0.1.0"
    }

    #[pg_extern]
    fn deploy(env: &str, from_schema: &str, label: default!(Option<&str>, "NULL")) -> i64 {
        let lock_key = hash_lock_key(env);
        let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", lock_key));

        let live_schema = resolve_live_schema();
        let label_sql = label
            .map(quote_literal)
            .unwrap_or_else(|| "NULL".to_string());

        let _ = Spi::run(&format!(
            "
            INSERT INTO stopgap.environment (env, live_schema)
            VALUES ({}, {})
            ON CONFLICT (env) DO UPDATE
            SET live_schema = EXCLUDED.live_schema,
                updated_at = now()
            ",
            quote_literal(env),
            quote_literal(&live_schema)
        ));

        ensure_no_overloaded_plts_functions(from_schema);

        let manifest_sql =
            quote_literal(&json!({ "env": env, "source_schema": from_schema }).to_string());
        let deployment_id = Spi::get_one::<i64>(&format!(
            "
            INSERT INTO stopgap.deployment (env, label, source_schema, status, manifest)
            VALUES ({}, {}, {}, 'open', {}::jsonb)
            RETURNING id
            ",
            quote_literal(env),
            label_sql,
            quote_literal(from_schema),
            manifest_sql
        ))
        .ok()
        .flatten()
        .expect("failed to create deployment");

        let fns = fetch_deployable_functions(from_schema);
        let _ = Spi::run(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(&live_schema)
        ));

        for item in &fns {
            let artifact_hash = Spi::get_one::<String>(&format!(
                "SELECT plts.compile_and_store({}::text, '{{}}'::jsonb)",
                quote_literal(&item.prosrc)
            ))
            .ok()
            .flatten()
            .expect("plts.compile_and_store returned no hash");

            let _ = Spi::run(&format!(
                "
                INSERT INTO stopgap.fn_version
                    (deployment_id, fn_name, fn_schema, live_fn_schema, kind, artifact_hash)
                VALUES ({}, {}, {}, {}, 'mutation', {})
                ",
                deployment_id,
                quote_literal(&item.fn_name),
                quote_literal(from_schema),
                quote_literal(&live_schema),
                quote_literal(&artifact_hash)
            ));

            materialize_live_pointer(&live_schema, &item.fn_name, &artifact_hash);
        }

        let previous_active = Spi::get_one::<i64>(&format!(
            "SELECT active_deployment_id FROM stopgap.environment WHERE env = {}",
            quote_literal(env)
        ))
        .ok()
        .flatten();

        let _ = Spi::run(&format!(
            "UPDATE stopgap.deployment SET status = 'sealed' WHERE id = {}",
            deployment_id
        ));

        let _ = Spi::run(&format!(
            "
            UPDATE stopgap.environment
            SET active_deployment_id = {},
                updated_at = now()
            WHERE env = {}
            ",
            deployment_id,
            quote_literal(env)
        ));

        let _ = Spi::run(&format!(
            "UPDATE stopgap.deployment SET status = 'active' WHERE id = {}",
            deployment_id
        ));

        let _ = Spi::run(&format!(
            "
            INSERT INTO stopgap.activation_log (env, from_deployment_id, to_deployment_id)
            VALUES ({}, {}, {})
            ",
            quote_literal(env),
            previous_active
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            deployment_id
        ));

        deployment_id
    }
}

#[derive(Debug)]
struct DeployableFn {
    fn_name: String,
    prosrc: String,
}

fn fetch_deployable_functions(from_schema: &str) -> Vec<DeployableFn> {
    Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "
                SELECT p.proname::text AS fn_name, p.prosrc
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                JOIN pg_language l ON l.oid = p.prolang
                WHERE n.nspname = {}
                  AND l.lanname = 'plts'
                  AND p.prorettype = 'jsonb'::regtype::oid
                  AND array_length(p.proargtypes::oid[], 1) = 1
                  AND p.proargtypes[0] = 'jsonb'::regtype::oid
                ORDER BY p.proname
                ",
                quote_literal(from_schema)
            ),
            None,
            &[],
        )?;

        let mut out = Vec::new();
        for row in rows {
            let fn_name = row
                .get_by_name::<String, _>("fn_name")
                .expect("fn_name must be text")
                .expect("fn_name cannot be null");
            let prosrc = row
                .get_by_name::<String, _>("prosrc")
                .expect("prosrc must be text")
                .expect("prosrc cannot be null");
            out.push(DeployableFn { fn_name, prosrc });
        }

        Ok::<Vec<DeployableFn>, pgrx::spi::Error>(out)
    })
    .expect("failed to scan deployable functions")
}

fn ensure_no_overloaded_plts_functions(from_schema: &str) {
    let overloaded = Spi::get_one::<String>(&format!(
        "
        SELECT proname::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname = {}
          AND l.lanname = 'plts'
        GROUP BY proname
        HAVING count(*) > 1
        LIMIT 1
        ",
        quote_literal(from_schema)
    ))
    .ok()
    .flatten();

    if let Some(name) = overloaded {
        error!(
            "stopgap deploy forbids overloaded plts functions in schema {}; offending function: {}",
            from_schema, name
        );
    }
}

fn materialize_live_pointer(live_schema: &str, fn_name: &str, artifact_hash: &str) {
    let body = json!({
        "plts": 1,
        "kind": "artifact_ptr",
        "artifact_hash": artifact_hash,
        "export": "default",
        "mode": "stopgap_deployed"
    })
    .to_string()
    .replace('\'', "''");

    let sql = format!(
        "
        CREATE OR REPLACE FUNCTION {}.{}(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {} $$
        ",
        quote_ident(live_schema),
        quote_ident(fn_name),
        body
    );

    let _ = Spi::run(&sql);
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn resolve_live_schema() -> String {
    let live = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('stopgap.live_schema', true), 'live_deployment')",
    )
    .ok()
    .flatten();
    live.unwrap_or_else(|| "live_deployment".to_string())
}

fn hash_lock_key(env: &str) -> i64 {
    let mut hash: i64 = 1469598103934665603;
    for b in env.as_bytes() {
        hash ^= i64::from(*b);
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_lock_hash_is_stable() {
        assert_eq!(crate::hash_lock_key("prod"), crate::hash_lock_key("prod"));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
