use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;

use crate::{
    ensure_deploy_permissions, ensure_deployment_belongs_to_env,
    ensure_no_overloaded_plts_functions, ensure_role_membership, find_rollback_target_by_steps,
    hash_lock_key, load_deployment_status, load_deployments, load_diff, load_environment_state,
    load_status, reactivate_deployment, resolve_live_schema, rollback_steps_to_offset,
    run_deploy_flow, run_sql_with_args, transition_deployment_status, transition_if_active,
    update_failed_manifest, DeploymentStatus, STOPGAP_DEPLOYER_ROLE,
};

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

    #[pg_extern(security_definer)]
    fn deploy(env: &str, from_schema: &str, label: default!(Option<&str>, "NULL")) -> i64 {
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap deploy")
            .unwrap_or_else(|err| error!("{err}"));
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

    #[pg_extern(security_definer)]
    fn rollback(env: &str, steps: default!(i32, "1"), to_id: default!(Option<i64>, "NULL")) -> i64 {
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap rollback")
            .unwrap_or_else(|err| error!("{err}"));
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

    #[pg_extern(security_definer)]
    fn diff(env: &str, from_schema: &str) -> JsonB {
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap diff")
            .unwrap_or_else(|err| error!("{err}"));
        JsonB(load_diff(env, from_schema).unwrap_or_else(|err| error!("{err}")))
    }
}
