use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;
use serde_json::Value;
use std::collections::BTreeSet;

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

    CREATE OR REPLACE VIEW stopgap.activation_audit AS
    SELECT l.id AS activation_id,
           l.env,
           l.from_deployment_id,
           l.to_deployment_id,
           l.activated_at,
           l.activated_by,
           d.status AS to_status,
           d.label AS to_label,
           d.source_schema AS to_source_schema,
           d.created_at AS to_created_at,
           d.created_by AS to_created_by
    FROM stopgap.activation_log l
    JOIN stopgap.deployment d ON d.id = l.to_deployment_id;

    CREATE OR REPLACE VIEW stopgap.environment_overview AS
    SELECT e.env,
           e.live_schema,
           e.active_deployment_id,
           e.updated_at,
           d.status AS active_status,
           d.label AS active_label,
           d.created_at AS active_created_at,
           d.created_by AS active_created_by
    FROM stopgap.environment e
    LEFT JOIN stopgap.deployment d ON d.id = e.active_deployment_id;
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
        run_sql_with_args(
            "SELECT pg_advisory_xact_lock($1)",
            &[lock_key.into()],
            "failed to acquire deploy lock",
        )
        .unwrap_or_else(|err| error!("{err}"));

        let live_schema = resolve_live_schema();
        ensure_deploy_permissions(from_schema, &live_schema).unwrap_or_else(|err| error!("{err}"));

        run_sql_with_args(
            "
            INSERT INTO stopgap.environment (env, live_schema)
            VALUES ($1, $2)
            ON CONFLICT (env) DO UPDATE
            SET live_schema = EXCLUDED.live_schema,
                updated_at = now()
            ",
            &[env.into(), live_schema.as_str().into()],
            "failed to upsert stopgap.environment",
        )
        .unwrap_or_else(|err| error!("{err}"));

        ensure_no_overloaded_plts_functions(from_schema);

        let manifest = JsonB(json!({
            "env": env,
            "source_schema": from_schema,
            "live_schema": live_schema,
            "label": label,
            "functions": []
        }));
        let deployment_id = Spi::get_one_with_args::<i64>(
            "
            INSERT INTO stopgap.deployment (env, label, source_schema, status, manifest)
            VALUES ($1, $2, $3, 'open', $4)
            RETURNING id
            ",
            &[env.into(), label.into(), from_schema.into(), manifest.into()],
        )
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

    #[pg_extern]
    fn rollback(env: &str, steps: default!(i32, "1"), to_id: default!(Option<i64>, "NULL")) -> i64 {
        rollback_steps_to_offset(steps).unwrap_or_else(|err| error!("{err}"));

        let lock_key = hash_lock_key(env);
        run_sql_with_args(
            "SELECT pg_advisory_xact_lock($1)",
            &[lock_key.into()],
            "failed to acquire rollback lock",
        )
        .unwrap_or_else(|err| error!("{err}"));

        let (live_schema, current_active) =
            load_environment_state(env).unwrap_or_else(|err| error!("{err}"));

        let target_deployment_id = match to_id {
            Some(explicit_id) => {
                ensure_deployment_belongs_to_env(env, explicit_id)
                    .unwrap_or_else(|err| error!("{err}"));
                explicit_id
            }
            None => find_rollback_target_by_steps(env, current_active, steps)
                .unwrap_or_else(|err| error!("{err}")),
        };

        if target_deployment_id == current_active {
            error!(
                "stopgap rollback target {} is already active for env {}",
                target_deployment_id, env
            );
        }

        let target_status =
            load_deployment_status(target_deployment_id).unwrap_or_else(|err| error!("{err}"));
        if target_status != DeploymentStatus::Active
            && target_status != DeploymentStatus::RolledBack
        {
            error!(
                "stopgap rollback target {} has invalid status {}; expected active or rolled_back",
                target_deployment_id,
                target_status.as_str()
            );
        }

        reactivate_deployment(live_schema.as_str(), target_deployment_id)
            .unwrap_or_else(|err| error!("{err}"));

        transition_if_active(current_active, DeploymentStatus::RolledBack)
            .unwrap_or_else(|err| error!("{err}"));
        if target_status == DeploymentStatus::RolledBack {
            transition_deployment_status(target_deployment_id, DeploymentStatus::Active)
                .unwrap_or_else(|err| error!("{err}"));
        }

        run_sql_with_args(
            "
            UPDATE stopgap.environment
            SET active_deployment_id = $1,
                updated_at = now()
            WHERE env = $2
            ",
            &[target_deployment_id.into(), env.into()],
            "failed to update active deployment during rollback",
        )
        .unwrap_or_else(|err| error!("{err}"));

        run_sql_with_args(
            "
            INSERT INTO stopgap.activation_log (env, from_deployment_id, to_deployment_id)
            VALUES ($1, $2, $3)
            ",
            &[env.into(), current_active.into(), target_deployment_id.into()],
            "failed to write rollback activation log",
        )
        .unwrap_or_else(|err| error!("{err}"));

        target_deployment_id
    }

    #[pg_extern]
    fn diff(env: &str, from_schema: &str) -> JsonB {
        JsonB(load_diff(env, from_schema).unwrap_or_else(|err| error!("{err}")))
    }
}

#[derive(Debug)]
struct DeployableFn {
    fn_name: String,
    prosrc: String,
}

#[derive(Debug)]
struct FnVersionRow {
    fn_name: String,
    live_fn_schema: String,
    artifact_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveFnRow {
    oid: i64,
    fn_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PruneReport {
    enabled: bool,
    dropped: Vec<String>,
    skipped_with_dependents: Vec<String>,
}

#[derive(Debug, Clone)]
struct CandidateFn {
    fn_name: String,
    artifact_hash: String,
}

#[derive(Debug, Clone)]
struct DiffRow {
    fn_name: String,
    change: &'static str,
    active_artifact_hash: Option<String>,
    candidate_artifact_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DiffSummary {
    added: usize,
    changed: usize,
    removed: usize,
    unchanged: usize,
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
    let prune_enabled = resolve_prune_enabled();
    run_sql(
        &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(live_schema)),
        "failed to create live schema",
    )?;

    let mut manifest_functions: Vec<Value> = Vec::with_capacity(fns.len());

    for item in &fns {
        let artifact_hash = Spi::get_one_with_args::<String>(
            "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
            &[item.prosrc.as_str().into()],
        )
        .map_err(|e| format!("compile_and_store SPI error for {}: {e}", item.fn_name))?
        .ok_or_else(|| {
            format!(
                "compile_and_store returned no artifact hash for {}.{}",
                from_schema, item.fn_name
            )
        })?;

        run_sql_with_args(
            "
                INSERT INTO stopgap.fn_version
                    (deployment_id, fn_name, fn_schema, live_fn_schema, kind, artifact_hash)
                VALUES ($1, $2, $3, $4, 'mutation', $5)
                ",
            &[
                deployment_id.into(),
                item.fn_name.as_str().into(),
                from_schema.into(),
                live_schema.into(),
                artifact_hash.as_str().into(),
            ],
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

    let deployed_fn_names = fns.iter().map(|item| item.fn_name.clone()).collect::<BTreeSet<_>>();
    let prune_report = if prune_enabled {
        prune_stale_live_functions(live_schema, &deployed_fn_names)?
    } else {
        PruneReport { enabled: false, dropped: Vec::new(), skipped_with_dependents: Vec::new() }
    };

    update_deployment_manifest(
        deployment_id,
        json!({
            "functions": manifest_functions,
            "prune": prune_manifest_item(&prune_report)
        }),
    )?;

    let previous_active = Spi::get_one_with_args::<i64>(
        "SELECT active_deployment_id FROM stopgap.environment WHERE env = $1",
        &[env.into()],
    )
    .map_err(|e| format!("failed to read environment active deployment: {e}"))?;

    transition_deployment_status(deployment_id, DeploymentStatus::Sealed)?;

    run_sql_with_args(
        "
            UPDATE stopgap.environment
            SET active_deployment_id = $1,
                updated_at = now()
            WHERE env = $2
            ",
        &[deployment_id.into(), env.into()],
        "failed to set active deployment",
    )?;

    transition_deployment_status(deployment_id, DeploymentStatus::Active)?;

    run_sql_with_args(
        "
            INSERT INTO stopgap.activation_log (env, from_deployment_id, to_deployment_id)
            VALUES ($1, $2, $3)
            ",
        &[env.into(), previous_active.into(), deployment_id.into()],
        "failed to insert activation log",
    )?;

    Ok(())
}

fn prune_stale_live_functions(
    live_schema: &str,
    deployed_fn_names: &BTreeSet<String>,
) -> Result<PruneReport, String> {
    let live_rows = fetch_live_deployable_functions(live_schema)?;
    let mut dropped = Vec::new();
    let mut skipped_with_dependents = Vec::new();

    for row in live_rows {
        if deployed_fn_names.contains(row.fn_name.as_str()) {
            continue;
        }

        if live_function_has_dependents(row.oid)? {
            skipped_with_dependents.push(row.fn_name);
            continue;
        }

        let drop_sql = format!(
            "DROP FUNCTION IF EXISTS {}.{}(jsonb)",
            quote_ident(live_schema),
            quote_ident(&row.fn_name)
        );
        run_sql(&drop_sql, "failed to prune stale live function")?;
        dropped.push(row.fn_name);
    }

    dropped.sort();
    skipped_with_dependents.sort();

    Ok(PruneReport { enabled: true, dropped, skipped_with_dependents })
}

fn fetch_live_deployable_functions(live_schema: &str) -> Result<Vec<LiveFnRow>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "
            SELECT p.oid::bigint AS fn_oid,
                   p.proname::text AS fn_name
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = $1
              AND l.lanname = 'plts'
              AND p.prorettype = 'jsonb'::regtype::oid
              AND array_length(p.proargtypes::oid[], 1) = 1
              AND p.proargtypes[0] = 'jsonb'::regtype::oid
            ORDER BY p.proname
            ",
            None,
            &[live_schema.into()],
        )?;

        let mut out = Vec::new();
        for row in rows {
            let oid = row
                .get_by_name::<i64, _>("fn_oid")
                .expect("fn_oid must be bigint")
                .expect("fn_oid cannot be null");
            let fn_name = row
                .get_by_name::<String, _>("fn_name")
                .expect("fn_name must be text")
                .expect("fn_name cannot be null");
            out.push(LiveFnRow { oid, fn_name });
        }

        Ok::<Vec<LiveFnRow>, pgrx::spi::Error>(out)
    })
    .map_err(|e| format!("failed to load live deployable functions in schema {live_schema}: {e}"))
}

fn live_function_has_dependents(function_oid: i64) -> Result<bool, String> {
    Spi::get_one_with_args::<bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM pg_depend d
            WHERE d.refclassid = 'pg_proc'::regclass
              AND d.refobjid = $1
              AND d.deptype IN ('n', 'a', 'i')
              AND NOT (d.classid = 'pg_proc'::regclass AND d.objid = $1)
        )
        ",
        &[function_oid.into()],
    )
    .map_err(|e| {
        format!("failed to inspect dependencies for live function oid {}: {e}", function_oid)
    })
    .map(|value| value.unwrap_or(false))
}

fn prune_manifest_item(report: &PruneReport) -> Value {
    json!({
        "enabled": report.enabled,
        "dropped": report.dropped,
        "skipped_with_dependents": report.skipped_with_dependents
    })
}

fn ensure_deploy_permissions(from_schema: &str, live_schema: &str) -> Result<(), String> {
    let can_use_source = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege(current_user, $1, 'USAGE')",
        &[from_schema.into()],
    )
    .map_err(|e| format!("failed to check source schema privileges: {e}"))?
    .unwrap_or(false);

    if !can_use_source {
        return Err(format!(
            "permission denied for stopgap deploy: current_user lacks USAGE on source schema {}",
            from_schema
        ));
    }

    let live_schema_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
        &[live_schema.into()],
    )
    .map_err(|e| format!("failed to check live schema existence: {e}"))?
    .unwrap_or(false);

    if live_schema_exists {
        let can_write_live = Spi::get_one_with_args::<bool>(
            "SELECT has_schema_privilege(current_user, $1, 'USAGE,CREATE')",
            &[live_schema.into()],
        )
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
    let sql = "
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
        WHERE e.env = $1
        ";

    Spi::get_one_with_args::<JsonB>(sql, &[env.into()]).ok().flatten().map(|json| json.0)
}

fn load_deployments(env: &str) -> Value {
    let sql = "
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
            WHERE d.env = $1
        ) rows
        ";

    Spi::get_one_with_args::<JsonB>(sql, &[env.into()])
        .ok()
        .flatten()
        .map(|json| json.0)
        .unwrap_or_else(|| json!([]))
}

fn load_diff(env: &str, from_schema: &str) -> Result<Value, String> {
    let (live_schema, active_deployment_id) = load_environment_state(env)?;
    ensure_diff_permissions(from_schema)?;

    let active = fetch_fn_versions(active_deployment_id)?;
    let candidate = compile_candidate_functions(from_schema)?;
    let (rows, summary) = compute_diff_rows(&active, &candidate);

    let functions = rows
        .into_iter()
        .map(|row| {
            json!({
                "fn_name": row.fn_name,
                "change": row.change,
                "active_artifact_hash": row.active_artifact_hash,
                "candidate_artifact_hash": row.candidate_artifact_hash
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "env": env,
        "source_schema": from_schema,
        "live_schema": live_schema,
        "active_deployment_id": active_deployment_id,
        "summary": {
            "added": summary.added,
            "changed": summary.changed,
            "removed": summary.removed,
            "unchanged": summary.unchanged
        },
        "functions": functions
    }))
}

fn ensure_diff_permissions(from_schema: &str) -> Result<(), String> {
    let can_use_source = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege(current_user, $1, 'USAGE')",
        &[from_schema.into()],
    )
    .map_err(|e| format!("failed to check source schema privileges: {e}"))?
    .unwrap_or(false);

    if !can_use_source {
        return Err(format!(
            "permission denied for stopgap diff: current_user lacks USAGE on source schema {}",
            from_schema
        ));
    }

    let can_compile = Spi::get_one::<bool>(
        "SELECT has_function_privilege(current_user, 'plts.compile_and_store(text, jsonb)', 'EXECUTE')",
    )
    .map_err(|e| format!("failed to check plts.compile_and_store execute privilege: {e}"))?
    .unwrap_or(false);

    if can_compile {
        Ok(())
    } else {
        Err(
            "permission denied for stopgap diff: current_user lacks EXECUTE on plts.compile_and_store(text, jsonb)"
                .to_string(),
        )
    }
}

fn compile_candidate_functions(from_schema: &str) -> Result<Vec<CandidateFn>, String> {
    let deployables = fetch_deployable_functions(from_schema)?;
    let mut out = Vec::with_capacity(deployables.len());

    for item in deployables {
        let artifact_hash = Spi::get_one_with_args::<String>(
            "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
            &[item.prosrc.as_str().into()],
        )
        .map_err(|e| format!("compile_and_store SPI error for {}: {e}", item.fn_name))?
        .ok_or_else(|| {
            format!(
                "compile_and_store returned no artifact hash for {}.{}",
                from_schema, item.fn_name
            )
        })?;
        out.push(CandidateFn { fn_name: item.fn_name, artifact_hash });
    }

    Ok(out)
}

fn compute_diff_rows(
    active: &[FnVersionRow],
    candidate: &[CandidateFn],
) -> (Vec<DiffRow>, DiffSummary) {
    let active_by_name = active
        .iter()
        .map(|row| (row.fn_name.as_str(), row.artifact_hash.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let candidate_by_name = candidate
        .iter()
        .map(|row| (row.fn_name.as_str(), row.artifact_hash.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();

    let all_names =
        active_by_name.keys().chain(candidate_by_name.keys()).copied().collect::<BTreeSet<_>>();

    let mut rows = Vec::with_capacity(all_names.len());
    let mut summary = DiffSummary::default();

    for fn_name in all_names {
        let active_hash = active_by_name.get(fn_name).copied();
        let candidate_hash = candidate_by_name.get(fn_name).copied();

        let change = match (active_hash, candidate_hash) {
            (None, Some(_)) => {
                summary.added += 1;
                "added"
            }
            (Some(_), None) => {
                summary.removed += 1;
                "removed"
            }
            (Some(a), Some(c)) if a != c => {
                summary.changed += 1;
                "changed"
            }
            (Some(_), Some(_)) => {
                summary.unchanged += 1;
                "unchanged"
            }
            (None, None) => continue,
        };

        rows.push(DiffRow {
            fn_name: fn_name.to_string(),
            change,
            active_artifact_hash: active_hash.map(str::to_string),
            candidate_artifact_hash: candidate_hash.map(str::to_string),
        });
    }

    (rows, summary)
}

fn load_environment_state(env: &str) -> Result<(String, i64), String> {
    Spi::connect(|client| {
        let mut rows = client.select(
            "
            SELECT live_schema::text AS live_schema,
                   active_deployment_id
            FROM stopgap.environment
            WHERE env = $1
            ",
            None,
            &[env.into()],
        )?;

        let row = rows.next().ok_or_else(|| pgrx::spi::Error::NoTupleTable)?;

        let live_schema = row
            .get_by_name::<String, _>("live_schema")?
            .ok_or_else(|| pgrx::spi::Error::NoTupleTable)?;

        let active = row
            .get_by_name::<i64, _>("active_deployment_id")?
            .ok_or_else(|| pgrx::spi::Error::NoTupleTable)?;

        Ok::<(String, i64), pgrx::spi::Error>((live_schema, active))
    })
    .map_err(|_| {
        format!("cannot rollback env {}: environment missing or has no active deployment", env)
    })
}

fn find_rollback_target_by_steps(
    env: &str,
    current_active: i64,
    steps: i32,
) -> Result<i64, String> {
    let offset = rollback_steps_to_offset(steps)?;
    Spi::get_one_with_args::<i64>(
        "
        SELECT id
        FROM stopgap.deployment
        WHERE env = $1
          AND id < $2
          AND status IN ('active', 'rolled_back')
        ORDER BY id DESC
        OFFSET $3
        LIMIT 1
        ",
        &[env.into(), current_active.into(), offset.into()],
    )
    .map_err(|e| format!("failed to find rollback target for env {}: {e}", env))?
    .ok_or_else(|| {
        format!("cannot rollback env {} by {} step(s): no prior deployment available", env, steps)
    })
}

fn rollback_steps_to_offset(steps: i32) -> Result<i64, String> {
    if steps < 1 {
        return Err("stopgap.rollback requires steps >= 1".to_string());
    }

    Ok(i64::from(steps - 1))
}

fn ensure_deployment_belongs_to_env(env: &str, deployment_id: i64) -> Result<(), String> {
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM stopgap.deployment WHERE id = $1 AND env = $2)",
        &[deployment_id.into(), env.into()],
    )
    .map_err(|e| format!("failed to validate rollback target deployment {}: {e}", deployment_id))?
    .unwrap_or(false);

    if exists {
        Ok(())
    } else {
        Err(format!("rollback target deployment {} does not belong to env {}", deployment_id, env))
    }
}

fn load_deployment_status(deployment_id: i64) -> Result<DeploymentStatus, String> {
    let status = Spi::get_one_with_args::<String>(
        "SELECT status FROM stopgap.deployment WHERE id = $1",
        &[deployment_id.into()],
    )
    .map_err(|e| format!("failed to load deployment status for id {}: {e}", deployment_id))?
    .ok_or_else(|| format!("deployment id {} does not exist", deployment_id))?;

    DeploymentStatus::from_str(&status)
        .ok_or_else(|| format!("deployment id {} has unknown status {}", deployment_id, status))
}

fn transition_if_active(deployment_id: i64, to: DeploymentStatus) -> Result<(), String> {
    let status = load_deployment_status(deployment_id)?;
    if status == DeploymentStatus::Active {
        transition_deployment_status(deployment_id, to)?;
    }
    Ok(())
}

fn reactivate_deployment(live_schema: &str, deployment_id: i64) -> Result<(), String> {
    let rows = fetch_fn_versions(deployment_id)?;
    for row in rows {
        let schema =
            if row.live_fn_schema.is_empty() { live_schema } else { row.live_fn_schema.as_str() };
        materialize_live_pointer(schema, row.fn_name.as_str(), row.artifact_hash.as_str())?;
    }

    Ok(())
}

fn fetch_fn_versions(deployment_id: i64) -> Result<Vec<FnVersionRow>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "
            SELECT fn_name::text AS fn_name,
                   live_fn_schema::text AS live_fn_schema,
                   artifact_hash::text AS artifact_hash
            FROM stopgap.fn_version
            WHERE deployment_id = $1
            ORDER BY fn_name
            ",
            None,
            &[deployment_id.into()],
        )?;

        let mut out = Vec::new();
        for row in rows {
            let fn_name = row
                .get_by_name::<String, _>("fn_name")
                .expect("fn_name must be text")
                .expect("fn_name cannot be null");
            let live_fn_schema = row
                .get_by_name::<String, _>("live_fn_schema")
                .expect("live_fn_schema must be text")
                .expect("live_fn_schema cannot be null");
            let artifact_hash = row
                .get_by_name::<String, _>("artifact_hash")
                .expect("artifact_hash must be text")
                .expect("artifact_hash cannot be null");
            out.push(FnVersionRow { fn_name, live_fn_schema, artifact_hash });
        }

        Ok::<Vec<FnVersionRow>, pgrx::spi::Error>(out)
    })
    .map_err(|e| format!("failed to load function versions for deployment {}: {e}", deployment_id))
}

fn fetch_deployable_functions(from_schema: &str) -> Result<Vec<DeployableFn>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "
                SELECT p.proname::text AS fn_name, p.prosrc
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                JOIN pg_language l ON l.oid = p.prolang
                WHERE n.nspname = $1
                  AND l.lanname = 'plts'
                  AND p.prorettype = 'jsonb'::regtype::oid
                  AND array_length(p.proargtypes::oid[], 1) = 1
                  AND p.proargtypes[0] = 'jsonb'::regtype::oid
                ORDER BY p.proname
                ",
            None,
            &[from_schema.into()],
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
    let overloaded = Spi::get_one_with_args::<String>(
        "
        SELECT proname::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname = $1
          AND l.lanname = 'plts'
        GROUP BY proname
        HAVING count(*) > 1
        LIMIT 1
        ",
        &[from_schema.into()],
    )
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
    run_sql_with_args(
        "
            UPDATE stopgap.deployment
            SET manifest = manifest || $1::jsonb
            WHERE id = $2
            ",
        &[JsonB(patch).into(), deployment_id.into()],
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
    let from = load_deployment_status(deployment_id)?;

    if !is_allowed_transition(from, to) {
        return Err(format!(
            "invalid deployment status transition {} -> {} for id {}",
            from.as_str(),
            to.as_str(),
            deployment_id
        ));
    }

    run_sql_with_args(
        "UPDATE stopgap.deployment SET status = $1 WHERE id = $2",
        &[to.as_str().into(), deployment_id.into()],
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

fn run_sql_with_args<'a>(
    sql: &str,
    args: &[DatumWithOid<'a>],
    context: &str,
) -> Result<(), String> {
    Spi::run_with_args(sql, args).map_err(|e| format!("{context}: {e}"))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn resolve_live_schema() -> String {
    let live = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('stopgap.live_schema', true), 'live_deployment')",
    )
    .ok()
    .flatten();
    live.unwrap_or_else(|| "live_deployment".to_string())
}

fn resolve_prune_enabled() -> bool {
    let raw = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('stopgap.prune', true), 'false')::text",
    )
    .ok()
    .flatten();

    raw.as_deref().and_then(parse_bool_setting).unwrap_or(false)
}

fn parse_bool_setting(value: &str) -> Option<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "on" | "true" | "t" | "yes" | "y" => Some(true),
        "0" | "off" | "false" | "f" | "no" | "n" => Some(false),
        _ => None,
    }
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

    #[test]
    fn test_rollback_steps_must_be_positive() {
        assert_eq!(crate::rollback_steps_to_offset(1).expect("steps=1 should be valid"), 0);
        assert_eq!(crate::rollback_steps_to_offset(2).expect("steps=2 should be valid"), 1);
        assert!(crate::rollback_steps_to_offset(0).is_err());
    }

    #[test]
    fn test_compute_diff_rows_covers_added_changed_removed_and_unchanged() {
        let active = vec![
            crate::FnVersionRow {
                fn_name: "alpha".to_string(),
                live_fn_schema: "live_deployment".to_string(),
                artifact_hash: "sha256:1".to_string(),
            },
            crate::FnVersionRow {
                fn_name: "beta".to_string(),
                live_fn_schema: "live_deployment".to_string(),
                artifact_hash: "sha256:2".to_string(),
            },
            crate::FnVersionRow {
                fn_name: "delta".to_string(),
                live_fn_schema: "live_deployment".to_string(),
                artifact_hash: "sha256:4".to_string(),
            },
        ];
        let candidate = vec![
            crate::CandidateFn {
                fn_name: "alpha".to_string(),
                artifact_hash: "sha256:1".to_string(),
            },
            crate::CandidateFn {
                fn_name: "beta".to_string(),
                artifact_hash: "sha256:3".to_string(),
            },
            crate::CandidateFn {
                fn_name: "gamma".to_string(),
                artifact_hash: "sha256:5".to_string(),
            },
        ];

        let (rows, summary) = crate::compute_diff_rows(&active, &candidate);
        assert_eq!(summary, crate::DiffSummary { added: 1, changed: 1, removed: 1, unchanged: 1 });

        let changes = rows
            .iter()
            .map(|row| (row.fn_name.as_str(), row.change))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(changes.get("alpha").copied(), Some("unchanged"));
        assert_eq!(changes.get("beta").copied(), Some("changed"));
        assert_eq!(changes.get("gamma").copied(), Some("added"));
        assert_eq!(changes.get("delta").copied(), Some("removed"));
    }

    #[test]
    fn test_parse_bool_setting_accepts_common_values() {
        assert_eq!(crate::parse_bool_setting("true"), Some(true));
        assert_eq!(crate::parse_bool_setting("on"), Some(true));
        assert_eq!(crate::parse_bool_setting("1"), Some(true));
        assert_eq!(crate::parse_bool_setting("false"), Some(false));
        assert_eq!(crate::parse_bool_setting("off"), Some(false));
        assert_eq!(crate::parse_bool_setting("0"), Some(false));
    }

    #[test]
    fn test_parse_bool_setting_rejects_unknown_values() {
        assert_eq!(crate::parse_bool_setting("maybe"), None);
    }

    #[test]
    fn test_prune_manifest_item_shape() {
        let report = crate::PruneReport {
            enabled: true,
            dropped: vec!["old_fn".to_string()],
            skipped_with_dependents: vec!["kept_fn".to_string()],
        };

        let payload = crate::prune_manifest_item(&report);
        assert_eq!(payload.get("enabled").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            payload
                .get("dropped")
                .and_then(|v| v.as_array())
                .and_then(|values| values.first())
                .and_then(|v| v.as_str()),
            Some("old_fn")
        );
        assert_eq!(
            payload
                .get("skipped_with_dependents")
                .and_then(|v| v.as_array())
                .and_then(|values| values.first())
                .and_then(|v| v.as_str()),
            Some("kept_fn")
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
