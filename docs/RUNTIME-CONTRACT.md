# Runtime Contract Reference

This document captures the `plts` runtime call contract and result semantics.

Status note (Mar 2026): primary product UX is now Convex-style and TypeScript-first. Stopgap-managed app functions are addressed by logical path (`api.<module>.<export>`) and invoked through `stopgap.call_fn(path, args)`.

## Entrypoint shape

`LANGUAGE plts` handlers execute a module default export:

```ts
type PltsEntrypoint = (ctx: PltsContext) => any | Promise<any>;
```

## `ctx` shape

```ts
type PltsContext = {
  db: {
    mode: "ro" | "rw";
    query(input: string | SqlObjectLike, params?: unknown[]): Promise<unknown[]>;
    exec(input: string | SqlObjectLike, params?: unknown[]): Promise<{ ok: true }>;
  };
  args: unknown;
  fn: {
    oid: number;
    schema: string;
    name: string;
  };
  now: string;
};

type SqlObjectLike =
  | { sql: string; params?: unknown[] }
  | { toSQL(): { sql: string; params?: unknown[] } };
```

## Argument model

- Regular `plts` functions expose both positional and named/object argument forms.
- Stopgap-managed app functions are exported named handlers from `stopgap/**/*.ts` modules.
- Canonical function path format is `api.<module_path_without_ext>.<named_export>`.
- Runtime invocation surface is `stopgap.call_fn(path text, args jsonb)`.
- Wrapper validation is driven by `@stopgap/runtime` (`query`/`mutation`) and `v` schemas.
- Legacy JSON Schema-subset wrapper inputs remain supported as a compatibility path.
- Runtime wrapper validation now uses direct `zod/mini` `safeParse` issue surfacing for schema-like inputs while preserving clear path/issue context in thrown errors.

### Function path examples

- `stopgap/coolApi.ts` exporting `myFn` => `api.coolApi.myFn`
- `stopgap/admin/users.ts` exporting `list` => `api.admin.users.list`

### `stopgap.call_fn` contract

```sql
SELECT stopgap.call_fn('api.coolApi.myFn', '{"id":1}'::jsonb);
```

- `path` resolves against the active deployment for the selected environment.
- `args` is delivered as runtime `ctx.args` and validated by wrapper schema.
- Unknown path, missing deployment, and validation failures must return stable, explicit errors with path context.

Current implementation status:
- `stopgap.call_fn(path, args)` is implemented and routes via the active deployment for `stopgap.default_env` (fallback `prod`).
- Invalid path format, missing environment/active deployment, and unknown routed path return explicit `stopgap.call_fn` errors.
- While legacy SQL-name catalogs remain, route lookup currently resolves using the terminal export segment from `api.<module_path>.<export_name>`.

## DB API mode behavior

- `stopgap.query(...)` handlers execute with read-only DB mode (`ctx.db.mode = "ro"`):
  - `db.exec(...)` is denied.
  - `db.query(...)` enforces read-only-safe statements.
- `stopgap.mutation(...)` and regular `plts` handlers execute with read-write mode.

## Return normalization

- JS `undefined` -> SQL `NULL`
- JS `null` -> SQL `NULL`
- Other JS primitives, arrays, and objects -> `jsonb`

## Runtime limits and safety

- No filesystem or network globals are exposed.
- Execution timeout uses the stricter of `statement_timeout` and `plts.max_runtime_ms`.
- Optional heap cap enforced by `plts.max_heap_mb`.
- Runtime DB calls enforce:
  - `plts.max_sql_bytes`
  - `plts.max_params`
  - `plts.max_query_rows`

## Static vs dynamic runtime bootstrap

- Static bootstrap (startup snapshot path, one-time per backend process):
  - runtime-surface lockdown (remove `Deno`/network globals)
  - install immutable internal DB op bridge (`__plts_internal_ops`)
- Dynamic wiring (per invocation):
  - context payload attach (`ctx.args`, `ctx.fn`, `ctx.now`)
  - wrapper-aware DB mode (`ctx.db.mode`, read-only vs read-write behavior)
- Boundary requirement: invocation-local state must never be embedded into static bootstrap scripts.

## Isolate lifecycle and pool management

The runtime supports backend-local isolate reuse through an isolate pool with explicit lifecycle states:

- **States**:
  - `fresh`: newly created isolate, never used
  - `warm`: healthy isolate eligible for reuse
  - `tainted`: isolate observed failure (timeout, cancel, heap limit, or internal error)
  - `retired`: removed from active pool
- **Reuse eligibility**: checked on checkout and check-in
  - Tainted isolates are never reused
  - Recycle triggers: max age, max invocations, termination history, heap pressure events
- **Metrics**: pool hit/miss, active isolates, retired count, recycle reasons, cold/warm invocation split

## Contract verification

- DB-backed contract tests live in `crates/plts/tests/pg/runtime_contract.rs`.
- Existing behavior suites in `crates/plts/tests/pg/runtime_nulls.rs`, `crates/plts/tests/pg/runtime_db_input_forms.rs`, and `crates/plts/tests/pg/runtime_stopgap_wrappers.rs` also guard this document's guarantees.
- Static/dynamic boundary unit checks live in `crates/plts/src/runtime.rs`, and invocation-isolation coverage is in `crates/plts/tests/pg/runtime_contract.rs`.
- Pivot-specific path-routing coverage now includes DB-backed tests at `crates/stopgap/tests/pg/call_fn.rs`; expand these alongside full function-path catalog migration.
