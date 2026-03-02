# Runtime Contract Reference

This document captures the `plts` runtime call contract and result semantics.

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
- Stopgap-managed deployables are `(args jsonb) returns jsonb`; wrappers validate args via `v` schemas from `@stopgap/runtime`.
- Legacy JSON Schema-subset wrapper inputs remain supported as a compatibility path.
- Runtime wrapper validation now uses direct `zod/mini` `safeParse` issue surfacing for schema-like inputs while preserving clear path/issue context in thrown errors.

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

## Contract verification

- DB-backed contract tests live in `crates/plts/tests/pg/runtime_contract.rs`.
- Existing behavior suites in `crates/plts/tests/pg/runtime_nulls.rs`, `crates/plts/tests/pg/runtime_db_input_forms.rs`, and `crates/plts/tests/pg/runtime_stopgap_wrappers.rs` also guard this document's guarantees.
- Static/dynamic boundary unit checks live in `crates/plts/src/runtime.rs`, and invocation-isolation coverage is in `crates/plts/tests/pg/runtime_contract.rs`.
