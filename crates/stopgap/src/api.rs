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

fn validate_call_path(path: &str) -> Result<(), String> {
    let mut segments = path.split('.');
    let valid_prefix = matches!(segments.next(), Some("api"));
    let rest = segments.collect::<Vec<_>>();
    let has_min_segments = rest.len() >= 2;
    let has_valid_chars = rest.iter().all(|segment| {
        !segment.is_empty() && segment.chars().all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    });

    if !valid_prefix || !has_min_segments || !has_valid_chars {
        return Err(format!(
            "stopgap.call_fn invalid path '{}'; expected format api.<module_path>.<export_name>",
            path
        ));
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum RouteResolutionSource {
    Exact,
    Legacy,
}

struct RouteResolution {
    live_schema: String,
    live_fn_name: String,
    source: RouteResolutionSource,
}

fn resolve_route(
    deployment_id: i64,
    path: &str,
    export_name: &str,
) -> Result<Option<RouteResolution>, String> {
    Spi::connect(|client| -> Result<Result<Option<RouteResolution>, String>, pgrx::spi::Error> {
        let exact_rows = client
            .select(
                "
                SELECT live_fn_schema::text AS live_fn_schema,
                       live_fn_name::text AS live_fn_name
                FROM stopgap.fn_version
                WHERE deployment_id = $1
                  AND function_path = $2
                LIMIT 2
                ",
                None,
                &[deployment_id.into(), path.into()],
            )?
            .into_iter()
            .collect::<Vec<_>>();

        if exact_rows.len() > 1 {
            return Ok(Err(format!(
                "ambiguous route metadata for deployment {} path '{}'",
                deployment_id, path
            )));
        }

        if let Some(row) = exact_rows.into_iter().next() {
            let live_schema = row
                .get_by_name::<String, _>("live_fn_schema")?
                .expect("live_fn_schema must not be null");
            let live_fn_name = row
                .get_by_name::<String, _>("live_fn_name")?
                .expect("live_fn_name must not be null");
            return Ok(Ok(Some(RouteResolution {
                live_schema,
                live_fn_name,
                source: RouteResolutionSource::Exact,
            })));
        }

        let legacy_rows = client
            .select(
                "
                SELECT live_fn_schema::text AS live_fn_schema,
                       COALESCE(live_fn_name::text, fn_name::text) AS live_fn_name
                FROM stopgap.fn_version
                WHERE deployment_id = $1
                  AND function_path IS NULL
                  AND fn_name = $2
                LIMIT 2
                ",
                None,
                &[deployment_id.into(), export_name.into()],
            )?
            .into_iter()
            .collect::<Vec<_>>();

        if legacy_rows.len() > 1 {
            return Ok(Err(format!(
                "ambiguous legacy route metadata for deployment {} export '{}'",
                deployment_id, export_name
            )));
        }

        let resolved = legacy_rows.into_iter().next().map(|row| {
            let live_schema = row
                .get_by_name::<String, _>("live_fn_schema")?
                .expect("live_fn_schema must not be null");
            let live_fn_name = row
                .get_by_name::<String, _>("live_fn_name")?
                .expect("live_fn_name must not be null");
            Ok::<RouteResolution, pgrx::spi::Error>(RouteResolution {
                live_schema,
                live_fn_name,
                source: RouteResolutionSource::Legacy,
            })
        });

        Ok(resolved.transpose().map_err(|e| {
            format!(
                "failed to decode route metadata for deployment {} path '{}': {e}",
                deployment_id, path
            )
        }))
    })
    .map_err(|e| {
        format!("failed to resolve route for deployment {} path '{}': {e}", deployment_id, path)
    })?
}

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
        let started_at = observability::record_call_fn_start();

        let fail = |message: String| -> ! {
            observability::record_call_fn_error(
                started_at,
                observability::classify_call_fn_error(message.as_str()),
            );
            error!("{message}")
        };

        validate_call_path(path).unwrap_or_else(|message| fail(message));

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
        .unwrap_or_else(|e| {
            fail(format!("stopgap.call_fn failed to load environment '{}': {e}", env))
        });

        let (live_schema, active_deployment_id) = env_row.unwrap_or_else(|| {
            fail(format!(
                "stopgap.call_fn missing deployment environment '{}'; run stopgap.deploy first",
                env
            ))
        });

        let deployment_id = active_deployment_id.unwrap_or_else(|| {
            fail(format!(
                "stopgap.call_fn environment '{}' has no active deployment; run stopgap.deploy first",
                env
            ))
        });

        let route = resolve_route(deployment_id, path, export_name)
            .unwrap_or_else(|e| fail(format!("stopgap.call_fn {e}")))
            .unwrap_or_else(|| {
                fail(format!(
                    "stopgap.call_fn unknown path '{}' for env '{}' deployment {}",
                    path, env, deployment_id
                ))
            });

        match route.source {
            RouteResolutionSource::Exact => observability::record_call_fn_route_exact(),
            RouteResolutionSource::Legacy => observability::record_call_fn_route_legacy(),
        }

        let target_live_schema =
            if route.live_schema.is_empty() { live_schema } else { route.live_schema.clone() };

        let invoke_sql = format!(
            "SELECT {}.{}($1::jsonb)",
            crate::quote_ident(&target_live_schema),
            crate::quote_ident(route.live_fn_name.as_str())
        );

        let result = Spi::get_one_with_args::<JsonB>(invoke_sql.as_str(), &[args.into()])
            .unwrap_or_else(|e| {
                fail(format!("stopgap.call_fn execution failed for '{}': {e}", path))
            });
        observability::record_call_fn_success(started_at);
        result
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
