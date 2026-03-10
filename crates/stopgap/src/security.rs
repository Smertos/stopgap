use pgrx::prelude::*;

pub(crate) fn ensure_deploy_permissions(
    from_schema: &str,
    live_schema: &str,
) -> Result<(), String> {
    ensure_required_role_exists(crate::STOPGAP_OWNER_ROLE)?;
    ensure_required_role_exists(crate::STOPGAP_DEPLOYER_ROLE)?;
    ensure_required_role_exists(crate::APP_RUNTIME_ROLE)?;

    ensure_supported_deploy_workflow_permissions(from_schema)?;
    ensure_compatibility_bridge_guards(live_schema)?;

    Ok(())
}

fn ensure_supported_deploy_workflow_permissions(from_schema: &str) -> Result<(), String> {
    ensure_schema_exists(from_schema, "source")?;

    let can_use_source = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege(session_user, $1, 'USAGE')",
        &[from_schema.into()],
    )
    .map_err(|e| format!("failed to check source schema privileges: {e}"))?
    .unwrap_or(false);

    if !can_use_source {
        return Err(format!(
            "permission denied for stopgap deploy: TS-first deploy requires USAGE on source schema {} to read deployable application functions",
            from_schema
        ));
    }

    let can_execute_compile = Spi::get_one::<bool>(
        "SELECT has_function_privilege(session_user, 'plts.compile_and_store(text, jsonb)', 'EXECUTE')",
    )
    .map_err(|e| format!("failed to check plts.compile_and_store execute privilege: {e}"))?
    .unwrap_or(false);

    if !can_execute_compile {
        return Err(
            "permission denied for stopgap deploy: TS-first deploy requires EXECUTE on plts.compile_and_store(text, jsonb)"
                .to_string(),
        );
    }

    let can_execute_typecheck = Spi::get_one::<bool>(
        "SELECT has_function_privilege(session_user, 'plts.typecheck_ts(text, jsonb)', 'EXECUTE')",
    )
    .map_err(|e| format!("failed to check plts.typecheck_ts execute privilege: {e}"))?
    .unwrap_or(false);

    if !can_execute_typecheck {
        return Err(
            "permission denied for stopgap deploy: TS-first deploy requires EXECUTE on plts.typecheck_ts(text, jsonb)"
                .to_string(),
        );
    }

    Ok(())
}

fn ensure_compatibility_bridge_guards(live_schema: &str) -> Result<(), String> {
    ensure_live_schema_is_stopgap_managed(live_schema)
}

pub(crate) fn ensure_diff_permissions(from_schema: &str) -> Result<(), String> {
    ensure_required_role_exists(crate::STOPGAP_DEPLOYER_ROLE)?;

    let can_use_source = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege(session_user, $1, 'USAGE')",
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

    Ok(())
}

fn ensure_required_role_exists(role_name: &str) -> Result<(), String> {
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
        &[role_name.into()],
    )
    .map_err(|e| format!("failed to check role {} existence: {e}", role_name))?
    .unwrap_or(false);

    if exists {
        Ok(())
    } else {
        Err(format!(
            "stopgap security model requires role {} to exist; install/update extension as a role that can create required roles",
            role_name
        ))
    }
}

fn ensure_schema_exists(schema_name: &str, schema_kind: &str) -> Result<(), String> {
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
        &[schema_name.into()],
    )
    .map_err(|e| format!("failed to check {} schema existence: {e}", schema_kind))?
    .unwrap_or(false);

    if exists {
        Ok(())
    } else {
        Err(format!(
            "permission denied for stopgap deploy: TS-first deploy {} schema {} does not exist",
            schema_kind, schema_name
        ))
    }
}

fn ensure_live_schema_is_stopgap_managed(live_schema: &str) -> Result<(), String> {
    let owner = Spi::get_one_with_args::<String>(
        "
        SELECT (
            SELECT r.rolname::text
            FROM pg_namespace n
            JOIN pg_roles r ON r.oid = n.nspowner
            WHERE n.nspname = $1
            LIMIT 1
        )
        ",
        &[live_schema.into()],
    )
    .map_err(|e| format!("failed to inspect live schema ownership: {e}"))?;

    if let Some(owner_role) = owner {
        if owner_role != crate::STOPGAP_OWNER_ROLE {
            return Err(format!(
                "permission denied for stopgap deploy: compatibility live schema {} is owned by {} (expected {}); extension-managed wrappers must remain stopgap-owned",
                live_schema,
                owner_role,
                crate::STOPGAP_OWNER_ROLE
            ));
        }
    }

    Ok(())
}

pub(crate) fn ensure_role_membership(required_role: &str, operation: &str) -> Result<(), String> {
    ensure_required_role_exists(required_role)?;

    let member = Spi::get_one_with_args::<bool>(
        "SELECT pg_has_role(session_user, oid, 'MEMBER') FROM pg_roles WHERE rolname = $1",
        &[required_role.into()],
    )
    .map_err(|e| format!("failed to check {} role membership: {e}", required_role))?
    .unwrap_or(false);

    if member {
        Ok(())
    } else {
        Err(format!(
            "permission denied for {}: session_user must be a member of role {}",
            operation, required_role
        ))
    }
}
