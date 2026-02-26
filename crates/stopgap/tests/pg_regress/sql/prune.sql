CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_prune_src') THEN
        EXECUTE 'DROP SCHEMA sg_reg_prune_src CASCADE';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sg_reg_prune_live') THEN
        EXECUTE 'DROP SCHEMA sg_reg_prune_live CASCADE';
    END IF;
END;
$$;

TRUNCATE stopgap.activation_log, stopgap.fn_version, stopgap.deployment RESTART IDENTITY;
DELETE FROM stopgap.environment WHERE env = 'rg_prune';

SELECT set_config('stopgap.live_schema', 'sg_reg_prune_live', false);
SELECT set_config('stopgap.prune', 'on', false);

CREATE SCHEMA sg_reg_prune_src;

CREATE OR REPLACE FUNCTION sg_reg_prune_src.keep(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({fn: "keep"});
$$;

CREATE OR REPLACE FUNCTION sg_reg_prune_src.dropme(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (_ctx) => ({fn: "dropme"});
$$;

SELECT stopgap.deploy('rg_prune', 'sg_reg_prune_src', 'initial') > 0 AS deployed_initial;

DROP FUNCTION sg_reg_prune_src.dropme(jsonb);

SELECT stopgap.deploy('rg_prune', 'sg_reg_prune_src', 'pruned') > 0 AS deployed_pruned;

SELECT array_agg(p.proname::text ORDER BY p.proname::text)
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'sg_reg_prune_live'
  AND p.prorettype = 'jsonb'::regtype::oid
  AND array_length(p.proargtypes::oid[], 1) = 1
  AND p.proargtypes[0] = 'jsonb'::regtype::oid;

SELECT (d.manifest->'prune'->>'enabled')::boolean AS prune_enabled
FROM stopgap.deployment d
WHERE d.env = 'rg_prune'
ORDER BY d.id DESC
LIMIT 1;

SELECT d.manifest->'prune'->'dropped' AS dropped
FROM stopgap.deployment d
WHERE d.env = 'rg_prune'
ORDER BY d.id DESC
LIMIT 1;
