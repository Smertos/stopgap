# Deployment Lifecycle Runbook

This runbook describes the expected operational flow for Stopgap deployments.

## Deploy lifecycle

`stopgap.deploy(env, from_schema, label)` runs in a single transaction and:

1. Acquires an advisory lock scoped by environment.
2. Ensures environment metadata exists.
3. Scans source schema for deployable functions:
   - `language plts`
   - `(args jsonb) returns jsonb`
   - no overloads
4. Compiles/stores artifacts through `plts.compile_and_store`.
5. Persists `stopgap.fn_version` rows.
6. Seals deployment metadata and materializes live pointer functions.
7. Updates active deployment and appends activation log.

## Rollback lifecycle

`stopgap.rollback(env, steps, to_id)`:

1. Acquires environment advisory lock.
2. Resolves rollback target (`steps` or explicit deployment id).
3. Re-materializes live pointer functions from target deployment.
4. Updates deployment statuses and environment active pointer.
5. Writes activation audit entry.

## Status and introspection

- `stopgap.status(env)` for active deployment snapshot
- `stopgap.deployments(env)` for history
- `stopgap.diff(env, from_schema)` to compare active deployment and workspace source schema
- `stopgap.activation_audit` and `stopgap.environment_overview` views for operational visibility

## CLI commands

The CLI mirrors DB APIs:

- `stopgap deploy --db <dsn> --env <env> --from-schema <schema> [--label <label>] [--prune]`
- `stopgap rollback --db <dsn> --env <env> [--steps <n>] [--to <deployment_id>]`
- `stopgap status --db <dsn> --env <env>`
- `stopgap deployments --db <dsn> --env <env>`
- `stopgap diff --db <dsn> --env <env> --from-schema <schema>`

Use `--output json` for machine-readable CI/CD integration.
