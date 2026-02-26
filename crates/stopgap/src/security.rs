use pgrx::prelude::*;

pub(crate) fn ensure_deploy_permissions(
    from_schema: &str,
    live_schema: &str,
) -> Result<(), String> {
    ensure_required_role_exists(crate::STOPGAP_OWNER_ROLE)?;
    ensure_required_role_exists(crate::STOPGAP_DEPLOYER_ROLE)?;
    ensure_required_role_exists(crate::APP_RUNTIME_ROLE)?;

    let can_use_source = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege(session_user, $1, 'USAGE')",
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

    let _ = live_schema;

    Ok(())
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
