use pgrx::prelude::*;

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
