use pgrx::JsonB;
use pgrx::prelude::*;
use serde_json::json;

use crate::{
    DeploymentStatus, STOPGAP_DEPLOYER_ROLE, ensure_deploy_permissions,
    ensure_deployment_belongs_to_env, ensure_no_overloaded_plts_functions, ensure_role_membership,
    find_rollback_target_by_steps, hash_lock_key, load_deployment_status, load_deployments,
    load_diff, load_environment_state, load_status, observability, reactivate_deployment,
    resolve_default_env, resolve_live_schema, rollback_steps_to_offset, run_deploy_flow,
    run_sql_with_args, transition_deployment_status, transition_if_active, update_failed_manifest,
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

    #[pg_extern]
    fn metrics() -> JsonB {
        JsonB(observability::metrics_json())
    }

    #[pg_extern(security_definer)]
    fn deploy(env: &str, from_schema: &str, label: default!(Option<&str>, "NULL")) -> i64 {
        let started_at = observability::record_deploy_start();
        observability::log_info(&format!(
            "stopgap.deploy start env={} source_schema={}",
            env, from_schema
        ));
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap deploy").unwrap_or_else(|err| {
            observability::record_deploy_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });
        let lock_key = hash_lock_key(env);
        run_sql_with_args(
            "SELECT pg_advisory_xact_lock($1)",
            &[lock_key.into()],
            "failed to acquire deploy lock",
        )
        .unwrap_or_else(|err| {
            observability::record_deploy_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        let live_schema = resolve_live_schema();
        ensure_deploy_permissions(from_schema, &live_schema).unwrap_or_else(|err| {
            observability::record_deploy_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

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
        .unwrap_or_else(|err| {
            observability::record_deploy_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

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
            observability::record_deploy_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            observability::log_warn(&format!(
                "stopgap.deploy failed env={} source_schema={} deployment_id={} err={}",
                env, from_schema, deployment_id, err
            ));
            let _ = transition_deployment_status(deployment_id, DeploymentStatus::Failed);
            let _ = update_failed_manifest(deployment_id, &err);
            error!(
                "stopgap deploy failed for env={} schema={} deployment_id={}: {}",
                env, from_schema, deployment_id, err
            );
        }

        observability::log_info(&format!(
            "stopgap.deploy success env={} source_schema={} deployment_id={}",
            env, from_schema, deployment_id
        ));
        observability::record_deploy_success(started_at);

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
    fn call_fn(path: &str, args: JsonB) -> Option<JsonB> {
        if !path.starts_with("api.")
            || path.split('.').any(str::is_empty)
            || path.matches('.').count() < 2
        {
            error!(
                "stopgap.call_fn invalid path '{}'; expected format api.<module_path>.<export_name>",
                path
            );
        }

        let export_name = path.rsplit('.').next().expect("validated non-empty path");
        let env = resolve_default_env();

        let env_row = Spi::connect(|client| {
            let mut rows = client
                .select(
                    "
                    SELECT live_schema::text AS live_schema,
                           active_deployment_id
                    FROM stopgap.environment
                    WHERE env = $1
                    ",
                    None,
                    &[env.as_str().into()],
                )?
                .into_iter();

            let row = rows.next().map(|row| {
                let live_schema = row
                    .get_by_name::<String, _>("live_schema")?
                    .expect("live_schema must not be null");
                let active_deployment_id = row.get_by_name::<i64, _>("active_deployment_id")?;
                Ok::<(String, Option<i64>), pgrx::spi::Error>((live_schema, active_deployment_id))
            });

            row.transpose()
        })
        .unwrap_or_else(|e| error!("stopgap.call_fn failed to load environment '{}': {e}", env));

        let (live_schema, active_deployment_id) = env_row.unwrap_or_else(|| {
            error!(
                "stopgap.call_fn missing deployment environment '{}'; run stopgap.deploy first",
                env
            )
        });

        let deployment_id = active_deployment_id.unwrap_or_else(|| {
            error!(
                "stopgap.call_fn environment '{}' has no active deployment; run stopgap.deploy first",
                env
            )
        });

        let route_exists = Spi::get_one_with_args::<bool>(
            "
            SELECT EXISTS (
                SELECT 1
                FROM stopgap.fn_version fv
                WHERE fv.deployment_id = $1
                  AND fv.fn_name = $2
            )
            ",
            &[deployment_id.into(), export_name.into()],
        )
        .unwrap_or_else(|e| {
            error!(
                "stopgap.call_fn failed to resolve path '{}' in deployment {}: {e}",
                path, deployment_id
            )
        })
        .unwrap_or(false);

        if !route_exists {
            error!(
                "stopgap.call_fn unknown path '{}' for env '{}' deployment {}",
                path, env, deployment_id
            );
        }

        let invoke_sql = format!(
            "SELECT {}.{}($1::jsonb)",
            crate::quote_ident(&live_schema),
            crate::quote_ident(export_name)
        );

        Spi::get_one_with_args::<JsonB>(invoke_sql.as_str(), &[args.into()])
            .unwrap_or_else(|e| error!("stopgap.call_fn execution failed for '{}': {e}", path))
    }

    #[pg_extern(security_definer)]
    fn rollback(env: &str, steps: default!(i32, "1"), to_id: default!(Option<i64>, "NULL")) -> i64 {
        let started_at = observability::record_rollback_start();
        observability::log_info(&format!(
            "stopgap.rollback start env={} steps={} to_id={}",
            env,
            steps,
            to_id.map(|value| value.to_string()).unwrap_or_else(|| "null".to_string())
        ));
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap rollback").unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });
        rollback_steps_to_offset(steps).unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        let lock_key = hash_lock_key(env);
        run_sql_with_args(
            "SELECT pg_advisory_xact_lock($1)",
            &[lock_key.into()],
            "failed to acquire rollback lock",
        )
        .unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        let (live_schema, current_active) = load_environment_state(env).unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        let target_deployment_id = match to_id {
            Some(explicit_id) => {
                ensure_deployment_belongs_to_env(env, explicit_id).unwrap_or_else(|err| {
                    observability::record_rollback_error(
                        started_at,
                        observability::classify_operation_error(err.as_str()),
                    );
                    error!("{err}")
                });
                explicit_id
            }
            None => {
                find_rollback_target_by_steps(env, current_active, steps).unwrap_or_else(|err| {
                    observability::record_rollback_error(
                        started_at,
                        observability::classify_operation_error(err.as_str()),
                    );
                    error!("{err}")
                })
            }
        };

        if target_deployment_id == current_active {
            observability::record_rollback_error(started_at, "state");
            observability::log_warn(&format!(
                "stopgap.rollback failed env={} target_deployment_id={} reason=already-active",
                env, target_deployment_id
            ));
            error!(
                "stopgap rollback target {} is already active for env {}",
                target_deployment_id, env
            );
        }

        let target_status = load_deployment_status(target_deployment_id).unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });
        if target_status != DeploymentStatus::Active
            && target_status != DeploymentStatus::RolledBack
        {
            observability::record_rollback_error(started_at, "state");
            observability::log_warn(&format!(
                "stopgap.rollback failed env={} target_deployment_id={} reason=invalid-status status={}",
                env,
                target_deployment_id,
                target_status.as_str()
            ));
            error!(
                "stopgap rollback target {} has invalid status {}; expected active or rolled_back",
                target_deployment_id,
                target_status.as_str()
            );
        }

        reactivate_deployment(live_schema.as_str(), target_deployment_id).unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        transition_if_active(current_active, DeploymentStatus::RolledBack).unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });
        if target_status == DeploymentStatus::RolledBack {
            transition_deployment_status(target_deployment_id, DeploymentStatus::Active)
                .unwrap_or_else(|err| {
                    observability::record_rollback_error(
                        started_at,
                        observability::classify_operation_error(err.as_str()),
                    );
                    error!("{err}")
                });
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
        .unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        run_sql_with_args(
            "
            INSERT INTO stopgap.activation_log (env, from_deployment_id, to_deployment_id)
            VALUES ($1, $2, $3)
            ",
            &[env.into(), current_active.into(), target_deployment_id.into()],
            "failed to write rollback activation log",
        )
        .unwrap_or_else(|err| {
            observability::record_rollback_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });

        observability::log_info(&format!(
            "stopgap.rollback success env={} from_deployment_id={} to_deployment_id={}",
            env, current_active, target_deployment_id
        ));
        observability::record_rollback_success(started_at);

        target_deployment_id
    }

    #[pg_extern(security_definer)]
    fn diff(env: &str, from_schema: &str) -> JsonB {
        let started_at = observability::record_diff_start();
        observability::log_info(&format!(
            "stopgap.diff start env={} source_schema={}",
            env, from_schema
        ));
        ensure_role_membership(STOPGAP_DEPLOYER_ROLE, "stopgap diff").unwrap_or_else(|err| {
            observability::record_diff_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            error!("{err}")
        });
        let diff = load_diff(env, from_schema).unwrap_or_else(|err| {
            observability::record_diff_error(
                started_at,
                observability::classify_operation_error(err.as_str()),
            );
            observability::log_warn(&format!(
                "stopgap.diff failed env={} source_schema={} err={}",
                env, from_schema, err
            ));
            error!("{err}")
        });
        observability::record_diff_success(started_at);
        JsonB(diff)
    }
}
