use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::Value;
use serde_json::json;

use crate::deployment_utils::materialize_live_pointer;
use crate::domain::{
    CandidateFn, DeploymentStatus, FnVersionRow, deployment_import_map, is_allowed_transition,
    rollback_steps_to_offset,
};
use crate::runtime_config::run_sql_with_args;

pub(crate) fn load_environment_state(env: &str) -> Result<(String, i64), String> {
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

pub(crate) fn find_rollback_target_by_steps(
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

pub(crate) fn ensure_deployment_belongs_to_env(
    env: &str,
    deployment_id: i64,
) -> Result<(), String> {
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

pub(crate) fn load_deployment_status(deployment_id: i64) -> Result<DeploymentStatus, String> {
    let status = Spi::get_one_with_args::<String>(
        "SELECT status FROM stopgap.deployment WHERE id = $1",
        &[deployment_id.into()],
    )
    .map_err(|e| format!("failed to load deployment status for id {}: {e}", deployment_id))?
    .ok_or_else(|| format!("deployment id {} does not exist", deployment_id))?;

    DeploymentStatus::from_str(&status)
        .ok_or_else(|| format!("deployment id {} has unknown status {}", deployment_id, status))
}

pub(crate) fn transition_if_active(deployment_id: i64, to: DeploymentStatus) -> Result<(), String> {
    let status = load_deployment_status(deployment_id)?;
    if status == DeploymentStatus::Active {
        transition_deployment_status(deployment_id, to)?;
    }
    Ok(())
}

pub(crate) fn reactivate_deployment(live_schema: &str, deployment_id: i64) -> Result<(), String> {
    let rows = fetch_fn_versions(deployment_id)?;
    let source_schema = load_deployment_source_schema(deployment_id)?;
    let candidates = rows
        .iter()
        .map(|row| CandidateFn {
            fn_name: row.fn_name.clone(),
            artifact_hash: row.artifact_hash.clone(),
        })
        .collect::<Vec<_>>();
    let import_map = deployment_import_map(source_schema.as_str(), &candidates);

    for row in rows {
        let schema =
            if row.live_fn_schema.is_empty() { live_schema } else { row.live_fn_schema.as_str() };
        materialize_live_pointer(
            schema,
            row.fn_name.as_str(),
            row.artifact_hash.as_str(),
            &import_map,
        )?;
    }

    Ok(())
}

fn load_deployment_source_schema(deployment_id: i64) -> Result<String, String> {
    Spi::get_one_with_args::<String>(
        "SELECT source_schema::text FROM stopgap.deployment WHERE id = $1",
        &[deployment_id.into()],
    )
    .map_err(|e| format!("failed to load source schema for deployment {}: {e}", deployment_id))?
    .ok_or_else(|| format!("deployment {} is missing source schema", deployment_id))
}

pub(crate) fn fetch_fn_versions(deployment_id: i64) -> Result<Vec<FnVersionRow>, String> {
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

pub(crate) fn update_deployment_manifest(deployment_id: i64, patch: Value) -> Result<(), String> {
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

pub(crate) fn update_failed_manifest(deployment_id: i64, err: &str) -> Result<(), String> {
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

pub(crate) fn transition_deployment_status(
    deployment_id: i64,
    to: DeploymentStatus,
) -> Result<(), String> {
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
