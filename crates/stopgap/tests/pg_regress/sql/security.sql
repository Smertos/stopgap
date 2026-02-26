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
