use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;
use serde_json::Value;

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
        run_sql(
            &format!("SELECT pg_advisory_xact_lock({})", lock_key),
            "failed to acquire deploy lock",
        )
        .unwrap_or_else(|err| error!("{err}"));

        let live_schema = resolve_live_schema();
        ensure_deploy_permissions(from_schema, &live_schema).unwrap_or_else(|err| error!("{err}"));
        let label_sql = label.map(quote_literal).unwrap_or_else(|| "NULL".to_string());

        run_sql(
            &format!(
                "
            INSERT INTO stopgap.environment (env, live_schema)
            VALUES ({}, {})
            ON CONFLICT (env) DO UPDATE
            SET live_schema = EXCLUDED.live_schema,
                updated_at = now()
            ",
                quote_literal(env),
                quote_literal(&live_schema)
            ),
            "failed to upsert stopgap.environment",
        )
        .unwrap_or_else(|err| error!("{err}"));

        ensure_no_overloaded_plts_functions(from_schema);

        let manifest_sql = quote_literal(
            &json!({
                "env": env,
                "source_schema": from_schema,
                "live_schema": live_schema,
                "label": label,
                "functions": []
            })
            .to_string(),
        );
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

        if let Err(err) = run_deploy_flow(deployment_id, env, from_schema, &live_schema) {
            let _ = transition_deployment_status(deployment_id, DeploymentStatus::Failed);
            let _ = update_failed_manifest(deployment_id, &err);
            error!(
                "stopgap deploy failed for env={} schema={} deployment_id={}: {}",
                env, from_schema, deployment_id, err
            );
        }

        deployment_id
    }

    #[pg_extern]
    fn status(env: &str) -> Option<JsonB> {
        load_status(env).map(JsonB)
    }

    #[pg_extern]
    fn deployments(env: &str) -> JsonB {
        JsonB(load_deployments(env))
    }
}

#[derive(Debug)]
struct DeployableFn {
    fn_name: String,
    prosrc: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeploymentStatus {
    Open,
    Sealed,
    Active,
    RolledBack,
    Failed,
}

impl DeploymentStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Sealed => "sealed",
            Self::Active => "active",
            Self::RolledBack => "rolled_back",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "sealed" => Some(Self::Sealed),
            "active" => Some(Self::Active),
            "rolled_back" => Some(Self::RolledBack),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

fn run_deploy_flow(
    deployment_id: i64,
    env: &str,
    from_schema: &str,
    live_schema: &str,
) -> Result<(), String> {
    let fns = fetch_deployable_functions(from_schema)?;
    run_sql(
        &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(live_schema)),
        "failed to create live schema",
    )?;

    let mut manifest_functions: Vec<Value> = Vec::with_capacity(fns.len());

    for item in &fns {
        let artifact_hash = Spi::get_one::<String>(&format!(
            "SELECT plts.compile_and_store({}::text, '{{}}'::jsonb)",
            quote_literal(&item.prosrc)
        ))
        .map_err(|e| format!("compile_and_store SPI error for {}: {e}", item.fn_name))?
        .ok_or_else(|| {
            format!(
                "compile_and_store returned no artifact hash for {}.{}",
                from_schema, item.fn_name
            )
        })?;

        run_sql(
            &format!(
                "
                INSERT INTO stopgap.fn_version
                    (deployment_id, fn_name, fn_schema, live_fn_schema, kind, artifact_hash)
                VALUES ({}, {}, {}, {}, 'mutation', {})
                ",
                deployment_id,
                quote_literal(&item.fn_name),
                quote_literal(from_schema),
                quote_literal(live_schema),
                quote_literal(&artifact_hash)
            ),
            "failed to insert stopgap.fn_version",
        )?;

        materialize_live_pointer(live_schema, &item.fn_name, &artifact_hash)?;
        manifest_functions.push(fn_manifest_item(
            from_schema,
            live_schema,
            &item.fn_name,
            "mutation",
            &artifact_hash,
        ));
    }

    update_deployment_manifest(deployment_id, json!({ "functions": manifest_functions }))?;

    let previous_active = Spi::get_one::<i64>(&format!(
        "SELECT active_deployment_id FROM stopgap.environment WHERE env = {}",
        quote_literal(env)
    ))
    .map_err(|e| format!("failed to read environment active deployment: {e}"))?;

    transition_deployment_status(deployment_id, DeploymentStatus::Sealed)?;

    run_sql(
        &format!(
            "
            UPDATE stopgap.environment
            SET active_deployment_id = {},
                updated_at = now()
            WHERE env = {}
            ",
            deployment_id,
            quote_literal(env)
        ),
        "failed to set active deployment",
    )?;

    transition_deployment_status(deployment_id, DeploymentStatus::Active)?;

    run_sql(
        &format!(
            "
            INSERT INTO stopgap.activation_log (env, from_deployment_id, to_deployment_id)
            VALUES ({}, {}, {})
            ",
            quote_literal(env),
            previous_active.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
            deployment_id
        ),
        "failed to insert activation log",
    )?;

    Ok(())
}

fn ensure_deploy_permissions(from_schema: &str, live_schema: &str) -> Result<(), String> {
    let can_use_source = Spi::get_one::<bool>(&format!(
        "SELECT has_schema_privilege(current_user, {}, 'USAGE')",
        quote_literal(from_schema)
    ))
    .map_err(|e| format!("failed to check source schema privileges: {e}"))?
    .unwrap_or(false);

    if !can_use_source {
        return Err(format!(
            "permission denied for stopgap deploy: current_user lacks USAGE on source schema {}",
            from_schema
        ));
    }

    let live_schema_exists = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = {})",
        quote_literal(live_schema)
    ))
    .map_err(|e| format!("failed to check live schema existence: {e}"))?
    .unwrap_or(false);

    if live_schema_exists {
        let can_write_live = Spi::get_one::<bool>(&format!(
            "SELECT has_schema_privilege(current_user, {}, 'USAGE,CREATE')",
            quote_literal(live_schema)
        ))
        .map_err(|e| format!("failed to check live schema privileges: {e}"))?
        .unwrap_or(false);

        if !can_write_live {
            return Err(format!(
                "permission denied for stopgap deploy: current_user lacks USAGE,CREATE on live schema {}",
                live_schema
            ));
        }
    } else {
        let can_create_schema = Spi::get_one::<bool>(
            "SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
        )
        .map_err(|e| format!("failed to check database CREATE privilege: {e}"))?
        .unwrap_or(false);

        if !can_create_schema {
            return Err(format!(
                "permission denied for stopgap deploy: current_user cannot create live schema {}",
                live_schema
            ));
        }
    }

    let can_compile = Spi::get_one::<bool>(
        "SELECT has_function_privilege(current_user, 'plts.compile_and_store(text, jsonb)', 'EXECUTE')",
    )
    .map_err(|e| format!("failed to check plts.compile_and_store execute privilege: {e}"))?
    .unwrap_or(false);

    if !can_compile {
        return Err(
            "permission denied for stopgap deploy: current_user lacks EXECUTE on plts.compile_and_store(text, jsonb)"
                .to_string(),
        );
    }

    Ok(())
}

fn load_status(env: &str) -> Option<Value> {
    let sql = format!(
        "
        SELECT jsonb_build_object(
            'env', e.env,
            'live_schema', e.live_schema,
            'active_deployment_id', e.active_deployment_id,
            'updated_at', e.updated_at,
            'active_deployment', CASE
                WHEN d.id IS NULL THEN NULL
                ELSE jsonb_build_object(
                    'id', d.id,
                    'label', d.label,
                    'status', d.status,
                    'created_at', d.created_at,
                    'created_by', d.created_by,
                    'source_schema', d.source_schema,
                    'manifest', d.manifest
                )
            END
        )
        FROM stopgap.environment e
        LEFT JOIN stopgap.deployment d ON d.id = e.active_deployment_id
        WHERE e.env = {}
        ",
        quote_literal(env)
    );

    Spi::get_one::<JsonB>(&sql).ok().flatten().map(|json| json.0)
}

fn load_deployments(env: &str) -> Value {
    let sql = format!(
        "
        SELECT COALESCE(jsonb_agg(deploy_row ORDER BY created_at DESC), '[]'::jsonb)
        FROM (
            SELECT jsonb_build_object(
                'id', d.id,
                'env', d.env,
                'label', d.label,
                'status', d.status,
                'created_at', d.created_at,
                'created_by', d.created_by,
                'source_schema', d.source_schema,
                'manifest', d.manifest,
                'is_active', (e.active_deployment_id = d.id)
            ) AS deploy_row,
            d.created_at
            FROM stopgap.deployment d
            JOIN stopgap.environment e ON e.env = d.env
            WHERE d.env = {}
        ) rows
        ",
        quote_literal(env)
    );

    Spi::get_one::<JsonB>(&sql).ok().flatten().map(|json| json.0).unwrap_or_else(|| json!([]))
}

fn fetch_deployable_functions(from_schema: &str) -> Result<Vec<DeployableFn>, String> {
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
    .map_err(|e| format!("failed to scan deployable functions in schema {from_schema}: {e}"))
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

fn materialize_live_pointer(
    live_schema: &str,
    fn_name: &str,
    artifact_hash: &str,
) -> Result<(), String> {
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

    run_sql(&sql, "failed to materialize live pointer function")
}

fn fn_manifest_item(
    source_schema: &str,
    live_schema: &str,
    fn_name: &str,
    kind: &str,
    artifact_hash: &str,
) -> Value {
    json!({
        "fn_name": fn_name,
        "source_schema": source_schema,
        "live_schema": live_schema,
        "kind": kind,
        "artifact_hash": artifact_hash,
        "pointer": {
            "plts": 1,
            "kind": "artifact_ptr",
            "artifact_hash": artifact_hash,
            "export": "default",
            "mode": "stopgap_deployed"
        }
    })
}

fn update_deployment_manifest(deployment_id: i64, patch: Value) -> Result<(), String> {
    run_sql(
        &format!(
            "
            UPDATE stopgap.deployment
            SET manifest = manifest || {}::jsonb
            WHERE id = {}
            ",
            quote_literal(&patch.to_string()),
            deployment_id
        ),
        "failed to update deployment manifest",
    )
}

fn update_failed_manifest(deployment_id: i64, err: &str) -> Result<(), String> {
    update_deployment_manifest(
        deployment_id,
        json!({
            "error": {
                "message": err,
                "at": "stopgap.deploy"
            }
        }),
    )
}

fn transition_deployment_status(deployment_id: i64, to: DeploymentStatus) -> Result<(), String> {
    let current = Spi::get_one::<String>(&format!(
        "SELECT status FROM stopgap.deployment WHERE id = {}",
        deployment_id
    ))
    .map_err(|e| format!("failed to load deployment status for id {}: {e}", deployment_id))?
    .ok_or_else(|| format!("deployment id {} does not exist", deployment_id))?;

    let from = DeploymentStatus::from_str(&current)
        .ok_or_else(|| format!("deployment id {} has unknown status {current}", deployment_id))?;

    if !is_allowed_transition(from, to) {
        return Err(format!(
            "invalid deployment status transition {} -> {} for id {}",
            from.as_str(),
            to.as_str(),
            deployment_id
        ));
    }

    run_sql(
        &format!(
            "UPDATE stopgap.deployment SET status = {} WHERE id = {}",
            quote_literal(to.as_str()),
            deployment_id
        ),
        "failed to update deployment status",
    )
}

fn is_allowed_transition(from: DeploymentStatus, to: DeploymentStatus) -> bool {
    matches!(
        (from, to),
        (DeploymentStatus::Open, DeploymentStatus::Sealed)
            | (DeploymentStatus::Open, DeploymentStatus::Failed)
            | (DeploymentStatus::Sealed, DeploymentStatus::Active)
            | (DeploymentStatus::Sealed, DeploymentStatus::Failed)
            | (DeploymentStatus::Active, DeploymentStatus::RolledBack)
            | (DeploymentStatus::Active, DeploymentStatus::Failed)
            | (DeploymentStatus::RolledBack, DeploymentStatus::Active)
    )
}

fn run_sql(sql: &str, context: &str) -> Result<(), String> {
    Spi::run(sql).map_err(|e| format!("{context}: {e}"))
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

    #[test]
    fn test_allowed_deployment_status_transitions() {
        assert!(crate::is_allowed_transition(
            crate::DeploymentStatus::Open,
            crate::DeploymentStatus::Sealed
        ));
        assert!(crate::is_allowed_transition(
            crate::DeploymentStatus::Open,
            crate::DeploymentStatus::Failed
        ));
        assert!(crate::is_allowed_transition(
            crate::DeploymentStatus::Sealed,
            crate::DeploymentStatus::Active
        ));
        assert!(!crate::is_allowed_transition(
            crate::DeploymentStatus::Open,
            crate::DeploymentStatus::Active
        ));
        assert!(!crate::is_allowed_transition(
            crate::DeploymentStatus::Failed,
            crate::DeploymentStatus::Active
        ));
    }

    #[test]
    fn test_fn_manifest_item_shape() {
        let item =
            crate::fn_manifest_item("app", "live_deployment", "do_work", "mutation", "sha256:abc");
        assert_eq!(item.get("fn_name").and_then(|v| v.as_str()), Some("do_work"));
        assert_eq!(item.get("source_schema").and_then(|v| v.as_str()), Some("app"));
        assert_eq!(item.get("live_schema").and_then(|v| v.as_str()), Some("live_deployment"));
        assert_eq!(item.get("artifact_hash").and_then(|v| v.as_str()), Some("sha256:abc"));
        assert_eq!(
            item.get("pointer").and_then(|v| v.get("kind")).and_then(|v| v.as_str()),
            Some("artifact_ptr")
        );
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
