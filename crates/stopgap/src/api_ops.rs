use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CandidateFn, DeploymentStatus, PruneReport, compute_diff_rows, deployment_import_map,
    ensure_diff_permissions, fetch_deployable_functions, fetch_fn_versions,
    fetch_live_deployable_functions, harden_live_schema, live_function_has_dependents,
    load_environment_state, materialize_live_pointer, prune_manifest_item, quote_ident,
    resolve_prune_enabled, run_sql, run_sql_with_args, transition_deployment_status,
    update_deployment_manifest,
};

#[derive(Clone, Debug)]
struct DeployExportOverride {
    function_path: String,
    module_path: String,
    export_name: String,
    kind: String,
}

#[derive(Debug)]
struct DeployedFunction {
    fn_name: String,
    artifact_hash: String,
    function_path: String,
    module_path: String,
    export_name: String,
    kind: String,
}

fn compiler_opts_for_export(override_meta: Option<&DeployExportOverride>) -> Value {
    override_meta.map_or_else(
        || json!({}),
        |meta| {
            json!({
                "stopgap_function": {
                    "function_path": meta.function_path,
                    "module_path": meta.module_path,
                    "export_name": meta.export_name,
                    "kind": meta.kind,
                }
            })
        },
    )
}

fn deploy_export_overrides() -> Result<BTreeMap<String, DeployExportOverride>, String> {
    let Some(raw) = crate::resolve_deploy_exports_json() else {
        return Ok(BTreeMap::new());
    };

    let parsed = serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| format!("stopgap.deploy invalid stopgap.deploy_exports JSON: {e}"))?;
    let entries = parsed.as_array().ok_or_else(|| {
        "stopgap.deploy expected stopgap.deploy_exports to be a JSON array of exports".to_string()
    })?;

    let mut overrides = BTreeMap::new();
    let mut used_paths = BTreeSet::new();
    for entry in entries {
        let export_name = entry
            .get("export_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                "stopgap.deploy expected each stopgap.deploy_exports item to include export_name"
                    .to_string()
            })?
            .to_string();
        let function_path = entry
            .get("function_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "stopgap.deploy export '{}' missing function_path in stopgap.deploy_exports",
                    export_name
                )
            })?
            .to_string();
        let module_path = entry
            .get("module_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "stopgap.deploy export '{}' missing module_path in stopgap.deploy_exports",
                    export_name
                )
            })?
            .to_string();
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("mutation").to_string();

        if overrides.contains_key(&export_name) {
            return Err(format!(
                "stopgap.deploy duplicate export_name '{}' in stopgap.deploy_exports",
                export_name
            ));
        }

        if !used_paths.insert(function_path.clone()) {
            return Err(format!(
                "stopgap.deploy duplicate function_path '{}' in stopgap.deploy_exports",
                function_path
            ));
        }

        overrides.insert(
            export_name.clone(),
            DeployExportOverride { function_path, module_path, export_name, kind },
        );
    }

    Ok(overrides)
}

fn validate_deploy_export_coverage(
    deployable_functions: &[crate::deployment_utils::DeployableFn],
    export_overrides: &BTreeMap<String, DeployExportOverride>,
) -> Result<(), String> {
    if export_overrides.is_empty() {
        return Ok(());
    }

    let deployable_names =
        deployable_functions.iter().map(|item| item.fn_name.as_str()).collect::<BTreeSet<_>>();
    let override_names = export_overrides.keys().map(String::as_str).collect::<BTreeSet<_>>();

    let missing = deployable_names
        .difference(&override_names)
        .copied()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown = override_names
        .difference(&deployable_names)
        .copied()
        .map(str::to_string)
        .collect::<Vec<_>>();

    if missing.is_empty() && unknown.is_empty() {
        return Ok(());
    }

    let missing_msg = if missing.is_empty() { "none".to_string() } else { missing.join(", ") };
    let unknown_msg = if unknown.is_empty() { "none".to_string() } else { unknown.join(", ") };

    Err(format!(
        "stopgap.deploy export metadata drift: missing deploy_exports entries for [{}]; unknown deploy_exports entries [{}]",
        missing_msg, unknown_msg
    ))
}

fn compatibility_export_defaults(fn_name: &str) -> DeployExportOverride {
    DeployExportOverride {
        function_path: format!("api.legacy.{fn_name}"),
        module_path: "legacy".to_string(),
        export_name: "default".to_string(),
        kind: "mutation".to_string(),
    }
}

fn resolve_export_metadata(
    fn_name: &str,
    override_meta: Option<&DeployExportOverride>,
) -> DeployExportOverride {
    // TS-first CLI deploys should supply explicit route metadata. These defaults only
    // preserve extension-managed compatibility for legacy SQL-scan deploy paths.
    override_meta.cloned().unwrap_or_else(|| compatibility_export_defaults(fn_name))
}

pub(crate) fn run_deploy_flow(
    deployment_id: i64,
    env: &str,
    from_schema: &str,
    live_schema: &str,
) -> Result<(), String> {
    let fns = fetch_deployable_functions(from_schema)?;
    let export_overrides = deploy_export_overrides()?;
    validate_deploy_export_coverage(&fns, &export_overrides)?;
    let prune_enabled = resolve_prune_enabled();
    run_sql(
        &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(live_schema)),
        "failed to create live schema",
    )?;
    harden_live_schema(live_schema)?;

    let mut manifest_functions: Vec<Value> = Vec::with_capacity(fns.len());
    let mut manifest_functions_by_path = serde_json::Map::new();
    let mut deployed_functions: Vec<DeployedFunction> = Vec::with_capacity(fns.len());

    for item in &fns {
        let override_meta = export_overrides.get(item.fn_name.as_str());
        let export_meta = resolve_export_metadata(item.fn_name.as_str(), override_meta);
        let compiler_opts = compiler_opts_for_export(override_meta);
        let artifact_hash = compile_checked_artifact_hash(
            item.prosrc.as_str(),
            item.fn_name.as_str(),
            &compiler_opts,
        )?;

        run_sql_with_args(
            "
                INSERT INTO stopgap.fn_version
                    (
                        deployment_id,
                        fn_name,
                        fn_schema,
                        live_fn_schema,
                        live_fn_name,
                        function_path,
                        module_path,
                        export_name,
                        kind,
                        artifact_hash
                    )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ",
            &[
                deployment_id.into(),
                item.fn_name.as_str().into(),
                from_schema.into(),
                live_schema.into(),
                item.fn_name.as_str().into(),
                export_meta.function_path.as_str().into(),
                export_meta.module_path.as_str().into(),
                export_meta.export_name.as_str().into(),
                export_meta.kind.as_str().into(),
                artifact_hash.as_str().into(),
            ],
            "failed to insert stopgap.fn_version",
        )?;

        deployed_functions.push(DeployedFunction {
            fn_name: item.fn_name.clone(),
            artifact_hash,
            function_path: export_meta.function_path,
            module_path: export_meta.module_path,
            export_name: export_meta.export_name,
            kind: export_meta.kind,
        });
    }

    let compiled_functions = deployed_functions
        .iter()
        .map(|item| CandidateFn {
            fn_name: item.fn_name.clone(),
            artifact_hash: item.artifact_hash.clone(),
        })
        .collect::<Vec<_>>();

    let import_map = deployment_import_map(from_schema, &compiled_functions);

    for item in &deployed_functions {
        materialize_live_pointer(
            live_schema,
            &item.fn_name,
            &item.artifact_hash,
            &item.export_name,
            &import_map,
        )?;
        let manifest_item = crate::fn_manifest_item(
            from_schema,
            live_schema,
            &item.fn_name,
            &item.function_path,
            &item.module_path,
            &item.export_name,
            &item.kind,
            &item.artifact_hash,
            &import_map,
        );
        manifest_functions_by_path.insert(item.function_path.clone(), manifest_item.clone());
        manifest_functions.push(manifest_item);
    }

    let deployed_fn_names =
        deployed_functions.iter().map(|item| item.fn_name.clone()).collect::<BTreeSet<_>>();
    let prune_report = if prune_enabled {
        prune_stale_live_functions(live_schema, &deployed_fn_names)?
    } else {
        PruneReport { enabled: false, dropped: Vec::new(), skipped_with_dependents: Vec::new() }
    };

    update_deployment_manifest(
        deployment_id,
        json!({
            "functions": manifest_functions,
            "functions_by_path": Value::Object(manifest_functions_by_path),
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

pub(crate) fn load_status(env: &str) -> Option<Value> {
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

pub(crate) fn load_deployments(env: &str) -> Value {
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

pub(crate) fn load_diff(env: &str, from_schema: &str) -> Result<Value, String> {
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

fn compile_candidate_functions(from_schema: &str) -> Result<Vec<CandidateFn>, String> {
    let deployables = fetch_deployable_functions(from_schema)?;
    let mut out = Vec::with_capacity(deployables.len());

    for item in deployables {
        let compiler_opts = json!({});
        let artifact_hash = compile_checked_artifact_hash(
            item.prosrc.as_str(),
            item.fn_name.as_str(),
            &compiler_opts,
        )?;
        out.push(CandidateFn { fn_name: item.fn_name, artifact_hash });
    }

    Ok(out)
}

fn compile_checked_artifact_hash(
    source_ts: &str,
    fn_name: &str,
    compiler_opts: &Value,
) -> Result<String, String> {
    let compiled_row = Spi::get_one_with_args::<JsonB>(
        "SELECT to_jsonb(t) FROM plts.compile_ts_checked($1::text, $2::jsonb) AS t",
        &[source_ts.into(), JsonB(compiler_opts.clone()).into()],
    )
    .map_err(|e| format!("compile_ts_checked SPI error for {fn_name}: {e}"))?
    .map(|value| value.0)
    .ok_or_else(|| format!("compile_ts_checked returned no row for {fn_name}"))?;

    let compiled_js =
        compiled_row.get("compiled_js").and_then(Value::as_str).unwrap_or_default().to_string();
    let diagnostics = compiled_row.get("diagnostics").cloned().unwrap_or_else(|| json!([]));

    let has_error = diagnostics.as_array().is_some_and(|items| {
        items.iter().any(|entry| entry.get("severity").and_then(Value::as_str) == Some("error"))
    });

    if has_error {
        return Err(format!("TypeScript checked compile failed for {}: {}", fn_name, diagnostics));
    }

    Spi::get_one_with_args::<String>(
        "SELECT plts.upsert_artifact($1::text, $2::text, $3::jsonb)",
        &[source_ts.into(), compiled_js.into(), JsonB(compiler_opts.clone()).into()],
    )
    .map_err(|e| format!("upsert_artifact SPI error for {fn_name}: {e}"))?
    .ok_or_else(|| format!("upsert_artifact returned no artifact hash for {fn_name}"))
}
