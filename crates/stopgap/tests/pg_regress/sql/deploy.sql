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

SELECT p.proname
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'live_deployment'
ORDER BY p.proname;

SELECT live_deployment.echo('{"ok":true}'::jsonb);
