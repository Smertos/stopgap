CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_sec_src') THEN
        EXECUTE 'DROP SCHEMA sg_reg_sec_src CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_sec_live') THEN
        EXECUTE 'DROP SCHEMA sg_reg_sec_live CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_sec_unmanaged_live') THEN
        EXECUTE 'DROP SCHEMA sg_reg_sec_unmanaged_live CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rg_sec_actor') THEN
        EXECUTE 'DROP OWNED BY rg_sec_actor';
        EXECUTE 'DROP ROLE rg_sec_actor';
    END IF;
END;
$$;

TRUNCATE stopgap.activation_log, stopgap.fn_version, stopgap.deployment RESTART IDENTITY;
DELETE FROM stopgap.environment WHERE env = 'rg_sec';

SELECT set_config('stopgap.live_schema', 'sg_reg_sec_live', false);

CREATE SCHEMA sg_reg_sec_src;

CREATE OR REPLACE FUNCTION sg_reg_sec_src.secure_fn(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({ok: true});
$$;

CREATE ROLE rg_sec_actor;
GRANT stopgap_deployer TO rg_sec_actor;

DO $$
BEGIN
    BEGIN
        EXECUTE 'SET SESSION AUTHORIZATION rg_sec_actor';
        PERFORM stopgap.deploy('rg_sec', 'sg_reg_sec_src', 'deny-source-usage');
        RAISE EXCEPTION 'expected source schema usage permission failure';
    EXCEPTION
        WHEN OTHERS THEN
            EXECUTE 'RESET SESSION AUTHORIZATION';
            IF POSITION('lacks USAGE on source schema' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;
END;
$$;

GRANT USAGE ON SCHEMA sg_reg_sec_src TO rg_sec_actor;

CREATE SCHEMA sg_reg_sec_unmanaged_live;
SELECT set_config('stopgap.live_schema', 'sg_reg_sec_unmanaged_live', false);

DO $$
BEGIN
    BEGIN
        EXECUTE 'SET SESSION AUTHORIZATION rg_sec_actor';
        PERFORM stopgap.deploy('rg_sec', 'sg_reg_sec_src', 'deny-unmanaged-live');
        RAISE EXCEPTION 'expected unmanaged live schema ownership failure';
    EXCEPTION
        WHEN OTHERS THEN
            EXECUTE 'RESET SESSION AUTHORIZATION';
            IF POSITION('live schema sg_reg_sec_unmanaged_live is owned by' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;
END;
$$;

DROP SCHEMA sg_reg_sec_unmanaged_live;
SELECT set_config('stopgap.live_schema', 'sg_reg_sec_live', false);

SELECT stopgap.deploy('rg_sec', 'sg_reg_sec_src', 'sec') > 0 AS deployed_security;

SELECT p.prosecdef
FROM pg_proc p
WHERE p.oid = 'stopgap.deploy(text, text, text)'::regprocedure;

SELECT p.prosecdef
FROM pg_proc p
WHERE p.oid = 'stopgap.rollback(text, integer, bigint)'::regprocedure;

SELECT p.prosecdef
FROM pg_proc p
WHERE p.oid = 'stopgap.diff(text, text)'::regprocedure;

SELECT pg_get_userbyid(p.proowner)::text AS owner
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'sg_reg_sec_live'
  AND p.proname = 'secure_fn'
  AND p.prorettype = 'jsonb'::regtype::oid
  AND array_length(p.proargtypes::oid[], 1) = 1
  AND p.proargtypes[0] = 'jsonb'::regtype::oid;

SELECT has_function_privilege('app_user', 'sg_reg_sec_live.secure_fn(jsonb)', 'EXECUTE')
    AS app_user_can_execute;

DROP OWNED BY rg_sec_actor;
DROP ROLE rg_sec_actor;
