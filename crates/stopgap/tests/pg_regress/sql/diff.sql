CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_diff_src') THEN
        EXECUTE 'DROP SCHEMA sg_reg_diff_src CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_diff_live') THEN
        EXECUTE 'DROP SCHEMA sg_reg_diff_live CASCADE';
    END IF;
END;
$$;

TRUNCATE stopgap.activation_log, stopgap.fn_version, stopgap.deployment RESTART IDENTITY;
DELETE FROM stopgap.environment WHERE env = 'rg_diff';

SELECT set_config('stopgap.live_schema', 'sg_reg_diff_live', false);

CREATE SCHEMA sg_reg_diff_src;

CREATE OR REPLACE FUNCTION sg_reg_diff_src.alpha(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: 1});
$$;

CREATE OR REPLACE FUNCTION sg_reg_diff_src.beta(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: 1});
$$;

SELECT stopgap.deploy('rg_diff', 'sg_reg_diff_src', 'baseline') > 0 AS deployed_baseline;

CREATE OR REPLACE FUNCTION sg_reg_diff_src.alpha(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: 2});
$$;

DROP FUNCTION sg_reg_diff_src.beta(jsonb);

CREATE OR REPLACE FUNCTION sg_reg_diff_src.gamma(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({version: 1});
$$;

SELECT (stopgap.diff('rg_diff', 'sg_reg_diff_src')->'summary'->>'added')::int = 1 AS added_is_one;
SELECT (stopgap.diff('rg_diff', 'sg_reg_diff_src')->'summary'->>'changed')::int = 1 AS changed_is_one;
SELECT (stopgap.diff('rg_diff', 'sg_reg_diff_src')->'summary'->>'removed')::int = 1 AS removed_is_one;
SELECT (stopgap.diff('rg_diff', 'sg_reg_diff_src')->'summary'->>'unchanged')::int = 0 AS unchanged_is_zero;

SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(stopgap.diff('rg_diff', 'sg_reg_diff_src')->'functions') row
    WHERE row->>'fn_name' = 'alpha'
      AND row->>'change' = 'changed'
) AS alpha_is_changed;

SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(stopgap.diff('rg_diff', 'sg_reg_diff_src')->'functions') row
    WHERE row->>'fn_name' = 'gamma'
      AND row->>'change' = 'added'
) AS gamma_is_added;

SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(stopgap.diff('rg_diff', 'sg_reg_diff_src')->'functions') row
    WHERE row->>'fn_name' = 'beta'
      AND row->>'change' = 'removed'
) AS beta_is_removed;
