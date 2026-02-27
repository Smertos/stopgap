use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeSet;

use crate::{
    CandidateFn, DeploymentStatus, PruneReport, compute_diff_rows, ensure_diff_permissions,
    fetch_deployable_functions, fetch_fn_versions, fetch_live_deployable_functions,
    harden_live_schema, live_function_has_dependents, load_environment_state,
    materialize_live_pointer, prune_manifest_item, quote_ident, resolve_prune_enabled, run_sql,
    run_sql_with_args, transition_deployment_status, update_deployment_manifest,
};

pub(crate) fn run_deploy_flow(
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
    harden_live_schema(live_schema)?;

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
        manifest_functions.push(crate::fn_manifest_item(
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
