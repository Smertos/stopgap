use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;

mod api_ops;
mod deployment_state;
mod deployment_utils;
mod domain;
mod runtime_config;
mod security;

use api_ops::{load_deployments, load_diff, load_status, run_deploy_flow};

pub(crate) use deployment_state::{
    ensure_deployment_belongs_to_env, fetch_fn_versions, find_rollback_target_by_steps,
    load_deployment_status, load_environment_state, reactivate_deployment,
    transition_deployment_status, transition_if_active, update_deployment_manifest,
    update_failed_manifest,
};
pub(crate) use deployment_utils::{
    ensure_no_overloaded_plts_functions, fetch_deployable_functions,
    fetch_live_deployable_functions, harden_live_schema, live_function_has_dependents,
    materialize_live_pointer,
};
pub(crate) use domain::{
    compute_diff_rows, fn_manifest_item, hash_lock_key, prune_manifest_item,
    rollback_steps_to_offset, CandidateFn, DeploymentStatus, PruneReport,
};
#[cfg(test)]
pub(crate) use domain::{is_allowed_transition, FnVersionRow};
pub(crate) use runtime_config::{
    quote_ident, resolve_live_schema, resolve_prune_enabled, run_sql, run_sql_with_args,
};
pub(crate) use security::{
    ensure_deploy_permissions, ensure_diff_permissions, ensure_role_membership,
};

::pgrx::pg_module_magic!(name, version);

const STOPGAP_OWNER_ROLE: &str = "stopgap_owner";
const STOPGAP_DEPLOYER_ROLE: &str = "stopgap_deployer";
const APP_RUNTIME_ROLE: &str = "app_user";

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

extension_sql!(
    r#"
    DO $$
    BEGIN
        IF COALESCE(
            (SELECT r.rolsuper OR r.rolcreaterole FROM pg_roles r WHERE r.rolname = current_user),
            false
        ) THEN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stopgap_owner') THEN
                CREATE ROLE stopgap_owner NOLOGIN;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stopgap_deployer') THEN
                CREATE ROLE stopgap_deployer NOLOGIN;
            END IF;

            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
                CREATE ROLE app_user NOLOGIN;
            END IF;

            IF NOT pg_has_role(current_user, 'stopgap_owner', 'MEMBER') THEN
                EXECUTE format('GRANT %I TO %I', 'stopgap_owner', current_user);
            END IF;
        END IF;
    END;
    $$;

    REVOKE CREATE ON SCHEMA stopgap FROM PUBLIC;
    GRANT USAGE ON SCHEMA stopgap TO stopgap_deployer;
    "#,
    name = "stopgap_security_roles",
    requires = ["stopgap_sql_bootstrap"]
);

extension_sql!(
    r#"
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stopgap_owner') THEN
            EXECUTE format('ALTER SCHEMA stopgap OWNER TO %I', 'stopgap_owner');
        END IF;
    END;
    $$;

    ALTER FUNCTION stopgap.deploy(text, text, text) SECURITY DEFINER;
    ALTER FUNCTION stopgap.rollback(text, integer, bigint) SECURITY DEFINER;
    ALTER FUNCTION stopgap.diff(text, text) SECURITY DEFINER;

    ALTER FUNCTION stopgap.deploy(text, text, text) SET search_path TO pg_catalog, pg_temp;
    ALTER FUNCTION stopgap.rollback(text, integer, bigint) SET search_path TO pg_catalog, pg_temp;
    ALTER FUNCTION stopgap.diff(text, text) SET search_path TO pg_catalog, pg_temp;

    REVOKE ALL ON FUNCTION stopgap.deploy(text, text, text) FROM PUBLIC;
    REVOKE ALL ON FUNCTION stopgap.rollback(text, integer, bigint) FROM PUBLIC;
    REVOKE ALL ON FUNCTION stopgap.diff(text, text) FROM PUBLIC;

    GRANT EXECUTE ON FUNCTION stopgap.deploy(text, text, text) TO stopgap_deployer;
    GRANT EXECUTE ON FUNCTION stopgap.rollback(text, integer, bigint) TO stopgap_deployer;
    GRANT EXECUTE ON FUNCTION stopgap.diff(text, text) TO stopgap_deployer;
    "#,
    name = "stopgap_security_finalize",
    finalize
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

#[cfg(test)]
mod unit_tests {
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
        assert_eq!(
            summary,
            crate::domain::DiffSummary { added: 1, changed: 1, removed: 1, unchanged: 1 }
        );

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
        assert_eq!(crate::runtime_config::parse_bool_setting("true"), Some(true));
        assert_eq!(crate::runtime_config::parse_bool_setting("on"), Some(true));
        assert_eq!(crate::runtime_config::parse_bool_setting("1"), Some(true));
        assert_eq!(crate::runtime_config::parse_bool_setting("false"), Some(false));
        assert_eq!(crate::runtime_config::parse_bool_setting("off"), Some(false));
        assert_eq!(crate::runtime_config::parse_bool_setting("0"), Some(false));
    }

    #[test]
    fn test_parse_bool_setting_rejects_unknown_values() {
        assert_eq!(crate::runtime_config::parse_bool_setting("maybe"), None);
    }

    #[test]
    fn test_role_constants_are_stable() {
        assert_eq!(crate::STOPGAP_OWNER_ROLE, "stopgap_owner");
        assert_eq!(crate::STOPGAP_DEPLOYER_ROLE, "stopgap_deployer");
        assert_eq!(crate::APP_RUNTIME_ROLE, "app_user");
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

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    include!("../tests/pg/mod.rs");
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
