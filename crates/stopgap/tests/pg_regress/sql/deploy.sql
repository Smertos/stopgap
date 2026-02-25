CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;

CREATE SCHEMA workspace;

CREATE OR REPLACE FUNCTION workspace.echo(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
export default (ctx) => ctx.args;
$$;

SELECT stopgap.deploy('prod', 'workspace', 'demo') > 0 AS deployed;

SELECT (stopgap.status('prod')->>'active_deployment_id')::bigint > 0 AS has_active;

SELECT jsonb_array_length(stopgap.deployments('prod')) = 1 AS one_deployment;

SELECT (stopgap.deployments('prod')->0->>'is_active')::boolean AS first_is_active;

SELECT p.proname
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'live_deployment'
ORDER BY p.proname;

SELECT live_deployment.echo('{"ok":true}'::jsonb);
