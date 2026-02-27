CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_rb_src') THEN
        EXECUTE 'DROP SCHEMA sg_reg_rb_src CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_rb_live') THEN
        EXECUTE 'DROP SCHEMA sg_reg_rb_live CASCADE';
    END IF;
END;
$$;

TRUNCATE stopgap.activation_log, stopgap.fn_version, stopgap.deployment RESTART IDENTITY;
DELETE FROM stopgap.environment WHERE env = 'rg_rb';

SELECT set_config('stopgap.live_schema', 'sg_reg_rb_live', false);

CREATE SCHEMA sg_reg_rb_src;

CREATE OR REPLACE FUNCTION sg_reg_rb_src.stepper(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: "one"});
$$;

SELECT stopgap.deploy('rg_rb', 'sg_reg_rb_src', 'one') > 0 AS deployed_one;

CREATE OR REPLACE FUNCTION sg_reg_rb_src.stepper(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: "two"});
$$;

SELECT stopgap.deploy('rg_rb', 'sg_reg_rb_src', 'two') > 0 AS deployed_two;

SELECT sg_reg_rb_live.stepper('{"marker":"before"}'::jsonb) IS NOT NULL
AS live_exec_before_rollback;

SELECT stopgap.rollback('rg_rb', 1, NULL) = (
    SELECT d.id
    FROM stopgap.deployment d
    WHERE d.env = 'rg_rb'
      AND d.label = 'one'
    ORDER BY d.id DESC
    LIMIT 1
) AS rollback_target_is_first;

SELECT stopgap.status('rg_rb')->'active_deployment'->>'label' = 'one' AS active_label_is_first;

SELECT sg_reg_rb_live.stepper('{"marker":"after"}'::jsonb) IS NOT NULL
AS live_exec_after_rollback;

SELECT (
    SELECT p.prosrc::jsonb->>'artifact_hash'
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'sg_reg_rb_live'
      AND p.proname = 'stepper'
      AND p.prorettype = 'jsonb'::regtype::oid
      AND array_length(p.proargtypes::oid[], 1) = 1
      AND p.proargtypes[0] = 'jsonb'::regtype::oid
) = (
    SELECT fv.artifact_hash
    FROM stopgap.fn_version fv
    JOIN stopgap.environment e ON e.active_deployment_id = fv.deployment_id
    WHERE e.env = 'rg_rb'
      AND fv.fn_name = 'stepper'
    LIMIT 1
) AS pointer_matches_active_version;

SELECT 'rollback_done' AS rollback_done;

\q
