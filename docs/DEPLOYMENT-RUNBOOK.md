# Deployment Lifecycle Runbook

This runbook describes the expected operational flow for Stopgap deployments.

Status note (Mar 2026): primary workflow is now Convex-style and TypeScript-first.

## Source-of-truth project layout

- App functions live under project-local `./stopgap`.
- Each `stopgap/**/*.ts` file is a function module.
- Each module may export multiple named handlers via `query(...)` / `mutation(...)`.
- If `./stopgap` is missing, CLI should fail fast with a "Stopgap not initialized" style error.

## Deploy lifecycle

`stopgap deploy` runs as a TS-module deployment flow and, in DB terms, should execute atomically:

1. Acquires an advisory lock scoped by environment.
2. Ensures environment metadata exists.
3. Enumerates `stopgap/**/*.ts` modules from the CLI working directory.
4. Discovers named wrapper exports (`query` / `mutation`) and maps them to canonical function paths (`api.<module>.<export>`).
5. Compiles/stores artifacts through `plts.compile_and_store` (or equivalent pipeline stage).
6. Persists versioned function metadata keyed by function path.
7. Seals deployment metadata and updates active deployment pointer.
8. Makes functions invocable through `stopgap.call_fn(path, args)` routing.
9. Appends activation log.

Users should not author PostgreSQL `CREATE FUNCTION ... LANGUAGE plts` wrappers manually.

Roadmap note: compile/typecheck internals are planned to migrate to an in-process TSGo WASM backend; deploy lifecycle semantics stay the same (`plts` API boundary remains the integration point).

## Rollback lifecycle

`stopgap.rollback(env, steps, to_id)`:

1. Acquires environment advisory lock.
2. Resolves rollback target (`steps` or explicit deployment id).
3. Restores function-path manifest from target deployment.
4. Updates deployment statuses and environment active pointer.
5. Writes activation audit entry.

## Status and introspection

- `stopgap.status(env)` for active deployment snapshot
- `stopgap.deployments(env)` for history
- `stopgap.diff(...)` to compare active deployment and local module set (shape may evolve during pivot)
- `stopgap.activation_audit` and `stopgap.environment_overview` views for operational visibility
- `stopgap.call_fn(path, args)` for path-based runtime invocation

## CLI commands

The CLI mirrors DB APIs:

- `stopgap deploy --db <dsn> --env <env> [--label <label>] [--prune]`
- `stopgap rollback --db <dsn> --env <env> [--steps <n>] [--to <deployment_id>]`
- `stopgap status --db <dsn> --env <env>`
- `stopgap deployments --db <dsn> --env <env>`
- `stopgap diff --db <dsn> --env <env>`

Use `--output json` for machine-readable CI/CD integration.

## Metrics interpretation

Both extensions expose backend-process metrics snapshots:

- `SELECT plts.metrics()`
- `SELECT stopgap.metrics()`

Each operation group now reports:

- `calls`: total observed invocations
- `errors`: total failed invocations
- `latency_ms.total`: cumulative time spent in the operation
- `latency_ms.last`: most recent operation duration
- `latency_ms.max`: slowest observed operation duration
- `error_classes`: category counters for fast triage

Recommended operator workflow:

1. Compare `calls` and `errors` deltas over a short window to estimate failure rate.
2. Check `latency_ms.max` for outliers and `latency_ms.last` for current behavior.
3. Use dominant `error_classes` buckets to route investigation quickly:
   - `plts.compile.error_classes.diagnostics`: TS compile problems in source modules
   - `plts.execute.error_classes.timeout|memory|cancel|js_exception|sql`: runtime resource or handler failures
   - `stopgap.*.error_classes.permission|validation|state|sql`: deploy/rollback/diff input, state, or privilege failures
