mod api;
mod api_ops;
mod deployment_state;
mod deployment_utils;
mod domain;
mod observability;
mod runtime_config;
mod security;
mod sql_bootstrap;

#[cfg(feature = "pg_test")]
use pgrx::prelude::*;

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
    CandidateFn, DeploymentStatus, PruneReport, compute_diff_rows, deployment_import_map,
    fn_manifest_item, hash_lock_key, prune_manifest_item, rollback_steps_to_offset,
};
#[cfg(test)]
pub(crate) use domain::{FnVersionRow, is_allowed_transition};
pub(crate) use runtime_config::{
    quote_ident, resolve_live_schema, resolve_prune_enabled, run_sql, run_sql_with_args,
};
pub(crate) use security::{
    ensure_deploy_permissions, ensure_diff_permissions, ensure_role_membership,
};

::pgrx::pg_module_magic!(name, version);

pub(crate) const STOPGAP_OWNER_ROLE: &str = "stopgap_owner";
pub(crate) const STOPGAP_DEPLOYER_ROLE: &str = "stopgap_deployer";
pub(crate) const APP_RUNTIME_ROLE: &str = "app_user";

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
        let item = crate::fn_manifest_item(
            "app",
            "live_deployment",
            "do_work",
            "mutation",
            "sha256:abc",
            &serde_json::Map::new(),
        );
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
    fn test_deployment_import_map_uses_stopgap_namespace_specifiers() {
        let functions = vec![
            crate::CandidateFn {
                fn_name: "alpha".to_string(),
                artifact_hash: "sha256:a".to_string(),
            },
            crate::CandidateFn {
                fn_name: "beta".to_string(),
                artifact_hash: "sha256:b".to_string(),
            },
        ];

        let import_map = crate::deployment_import_map("app", &functions);

        assert_eq!(
            import_map.get("@stopgap/app/alpha").and_then(|v| v.as_str()),
            Some("plts+artifact:sha256:a")
        );
        assert_eq!(
            import_map.get("@stopgap/app/beta").and_then(|v| v.as_str()),
            Some("plts+artifact:sha256:b")
        );
    }

    #[test]
    fn test_fn_manifest_item_includes_pointer_import_map_when_present() {
        let mut import_map = serde_json::Map::new();
        import_map.insert(
            "@stopgap/app/util".to_string(),
            serde_json::Value::String("plts+artifact:sha256:util".to_string()),
        );

        let item = crate::fn_manifest_item(
            "app",
            "live_deployment",
            "do_work",
            "mutation",
            "sha256:abc",
            &import_map,
        );

        assert_eq!(
            item.get("pointer")
                .and_then(|v| v.get("import_map"))
                .and_then(|v| v.get("@stopgap/app/util"))
                .and_then(|v| v.as_str()),
            Some("plts+artifact:sha256:util")
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
