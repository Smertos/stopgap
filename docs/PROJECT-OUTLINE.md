## Stopgap + PLTS (split extensions) — thorough project outline

Status note (Feb 2026): this document is the target architecture, and some sections include decisions that are now locked for current implementation.

You’ll have **two Postgres extensions** plus **shared TS/JS runtime conventions**:

- **`plts`**: the core *language + runtime* (“run TS/JS in-process”).
- **`stopgap`**: *deployments + live schema materialization* (versioned releases, rollback, envs), but it **does not implement its own language**; it uses `LANGUAGE plts`.

Both extensions share the same **function execution model**, and `stopgap` stores/references artifacts that `plts` can execute.

---

# 1) Core principles (your adjusted constraints)

1) **`LANGUAGE plts` is the only PL**.  
   - Defined by the `plts` extension.
   - Used by “regular” functions and `stopgap`-managed deployed functions.

2) **Regular `plts` functions**
   - SQL args are **normal typed args** (Datum conversion happens).
   - Return is always **nullable `jsonb`**.

3) **Stopgap “deployment functions”**
   - SQL signature is **`(args jsonb) returns jsonb`** (single jsonb input).
   - Type safety is done via **schema declared in TS** (runtime validation).
   - Return is always **nullable `jsonb`**.

4) **Stopgap adds JS/TS helper wrappers**
   - `query(argsSchema, handler)`
   - `mutation(argsSchema, handler)`
   - These produce the exported callable that `plts` executes.
   - Distinction is enforced via the DB API surface (read-only vs read-write), not by parsing SQL.

---

# 2) Repo / project layout (current workspace)

```
stopgap/
  crates/
    # planned:
    # common/               # shared Rust helpers (no extension-specific semantics)
    plts/                 # Rust extension: language handler, runtime, artifact store
    #   tests/pg/          # planned: pgrx integration tests outside extension source
    stopgap/              # Rust extension: deployments, live schema management
    #   tests/pg/          # planned: pgrx integration tests outside extension source
  packages/
    runtime/              # NPM package: TS types + wrappers (`@stopgap/runtime`)
  docs/
  # future (planned):
  # cli/stopgap-cli       # Deploy/rollback tooling (Node/TS or Rust)
```

Near-term structure direction:
- add `crates/common` for shared utility code used by both extensions
- keep split-extension ownership strict (`plts` runtime/language concerns stay in `plts`; deploy/materialization concerns stay in `stopgap`)
- move PG integration tests out of single large source files and keep test files granular by behavior

---

# 3) `plts` extension (language + runtime)

## 3.1 Responsibilities
- Provide `LANGUAGE plts` with:
  - call handler
  - optional validator
- In-process JS engine embedding (deno_core).
- Argument mapping: PG Datums → JS values (for regular plts funcs).
- Standard return mapping: JS value → `jsonb` (nullable).
- SPI bridge: provide `db` API to JS (with configurable read-only / read-write behavior).
- Artifact storage APIs and table(s) for compiled code reuse (used by stopgap too).

## 3.2 SQL surface shipped by `plts`
### Language
- `CREATE LANGUAGE plts ...` done by extension install script.

### Runtime/config
- `plts.version() -> text`
- GUCs (examples):
  - `plts.max_runtime_ms`
  - `plts.max_heap_mb`
  - `plts.engine` (`quickjs`|`v8`)
  - `plts.log_level`

### Artifact APIs (important for stopgap integration)
You want a **stable SQL API** stopgap can call:

- `plts.compile_ts(source_ts text, compiler_opts jsonb default '{}'::jsonb)`
  - returns: `(compiled_js text, diagnostics jsonb, compiler_fingerprint text)`
- `plts.upsert_artifact(source_ts text, compiled_js text, compiler_opts jsonb)`
  - returns: `artifact_hash text`
- `plts.get_artifact(artifact_hash text)`
  - returns: `(source_ts text, compiled_js text, compiler_opts jsonb)`

You can also combine compile + upsert:

- `plts.compile_and_store(source_ts text, compiler_opts jsonb) -> artifact_hash`

(Under the hood you’ll hash `(compiler_fingerprint + compiler_opts + source_ts)`.)

## 3.3 `plts` internal catalog
Create schema `plts`:

### `plts.artifact`
Stores both source + compiled output.

- `artifact_hash text primary key`
- `source_ts text not null`
- `compiled_js text not null`
- `compiler_opts jsonb not null`
- `compiler_fingerprint text not null` (engine/swc version etc.)
- `created_at timestamptz not null default now()`
- optional:
  - `source_map bytea`
  - `diagnostics jsonb`

This is the shared substrate: stopgap deployments reference these hashes.

## 3.4 Function body formats handled by `plts`
`plts` should support **two prosrc formats**:

### A) Workspace TS (regular authoring)
`prosrc` = TypeScript module text.

- Regular plts: SQL args map directly into JS `args[]` (and/or named args).
- Stopgap “deployment functions”: also authored as TS, but signature is `(args jsonb)`.

### B) Pointer stub (used by stopgap live schema)
`prosrc` = JSON metadata; example:

```json
{
  "plts": 1,
  "kind": "artifact_ptr",
  "artifact_hash": "sha256:...",
  "export": "default",
  "mode": "stopgap_deployed"
}
```

When `plts` sees `kind=artifact_ptr`, it loads `compiled_js` from `plts.artifact` and executes that.

## 3.5 Runtime calling convention (what JS sees)
Provide a consistent internal call shape:

```ts
type PltsContext = {
  db: DbApi;              // SPI wrapper (read-only or read-write)
  args: any;              // either array (regular) or object (stopgap)
  fn: { oid: number; name: string; schema: string };
  now: string;            // optional metadata
};

type PltsEntrypoint = (ctx: PltsContext) => any | Promise<any>;
```

- Regular plts function:
  - `ctx.args` could be `{0: ..., 1: ...}` or an array; pick one and stick to it.
- Stopgap deployed function:
  - `ctx.args` is the decoded `jsonb` object.

**Return normalization (global rule):**
- `undefined` → SQL `NULL`
- `null` → SQL `NULL` (or JSON null; choose one—recommended: SQL NULL)
- object/array/primitive → `jsonb`

(Recommend: `null` maps to SQL NULL too, and if someone wants JSON null they return `{ value: null }`—keeps DB semantics clearer.)

## 3.6 SPI DB API and read-only enforcement
Implement two DB API variants:

- `db_ro.query(sql, params)`:
  - executes via SPI
  - **rejects** any result not equivalent to SELECT (e.g. status codes for INSERT/UPDATE/DELETE)
- `db_rw.query(...)` + `db_rw.exec(...)`:
  - allows writes

Stopgap `query()` wrapper gets `db_ro`, mutation gets `db_rw`.

This gives you real enforcement without brittle SQL parsing.

Current implementation status:
- P0 runtime context now exposes RW `ctx.db.query(sql, params)` and `ctx.db.exec(sql, params)`.
- JS params are bound into SPI calls as typed values (`bool`, `int`, `float`, `text`, `jsonb`, null).
- Runtime DB calls execute inside the same PostgreSQL transaction as the invoking SQL function call; no independent transaction is started by the runtime.
- Runtime now reads `@stopgap/runtime` wrapper metadata (`__stopgap_kind`) and switches DB mode accordingly: `query` handlers get `ctx.db.mode='ro'` with `db.exec` denied and read-only-only `db.query` filtering, while `mutation`/regular handlers stay `rw`.

Current state: P0 baseline remains RW; P1 wrapper-aware read-only gating is now implemented for `stopgap.query` handlers.

## 3.7 Safety and ops (must-have)
- Tie JS interrupts to Postgres cancellation (`statement_timeout`, user cancel).
- Set memory/time limits per call.
- No filesystem / network APIs.

Current implementation status:
- Runtime global lockdown strips `Deno`, `fetch`, `Request`, `Response`, `Headers`, and `WebSocket` from module scope before user code executes; runtime DB access remains available only through `ctx.db.query/exec` wrappers backed by internal ops.
- Runtime now reads the active `statement_timeout` and applies a per-call V8 watchdog that terminates JS execution when the call exceeds that timeout.
- Runtime now also routes pending Postgres cancel/die interrupt flags into the same V8 termination path used by timeout enforcement.

---

# 4) Stopgap extension (deployments + environments + live schema)

## 4.1 Responsibilities
- Maintain **deployments**: immutable snapshots mapping “function identity” → `plts.artifact_hash`.
- Provide **active deployment pointer** per environment.
- Materialize “live deployment” schema by creating/replacing functions that are **`LANGUAGE plts` pointer stubs**.
- Provide a SQL API for the CLI: deploy, activate, rollback, status, list deployments.
- Enforce that humans cannot modify live schema (via roles/privileges + SECURITY DEFINER deploy ops).

## 4.2 Stopgap catalogs
Create schema `stopgap`:

### `stopgap.environment`
- `env text primary key` (prod/dev/staging)
- `live_schema name not null` (default from GUC, or per env)
- `active_deployment_id bigint null`
- `updated_at timestamptz not null default now()`

### `stopgap.deployment`
- `id bigserial primary key`
- `env text not null references stopgap.environment(env)`
- `label text null` (git sha / date tag)
- `created_at timestamptz not null default now()`
- `created_by name not null default current_user`
- `source_schema name not null` (workspace schema used)
- `status text not null` (`open`, `sealed`, `active`, `rolled_back`, `failed`)
- `manifest jsonb not null` (functions + metadata + artifact hashes)

### `stopgap.fn_version`
Key point: only includes **stopgap-deployable functions** (the `(args jsonb) returns jsonb` ones).

- `deployment_id bigint not null references stopgap.deployment(id)`
- `fn_name name not null` (function name)
- `fn_schema name not null` (workspace schema name)
- `live_fn_schema name not null` (usually live schema)
- `kind text not null` (`query`|`mutation`) (optional but useful)
- `artifact_hash text not null` references `plts.artifact(artifact_hash)`
- primary key `(deployment_id, fn_schema, fn_name)`

### `stopgap.activation_log` (recommended)
- `id bigserial primary key`
- `env text not null`
- `from_deployment_id bigint null`
- `to_deployment_id bigint not null`
- `activated_at timestamptz not null default now()`
- `activated_by name not null default current_user`

## 4.3 Stopgap configuration
GUCs:
- `stopgap.live_schema` default `live_deployment`
- `stopgap.default_env` default `prod`
- `stopgap.prune` default false
- `stopgap.deploy_lock_key` (if you want override/advisory lock namespace)

## 4.4 “Live schema” materialization (how deploy works)
### Deployable function signature (strict)
Stopgap-managed functions in workspace must be:

```sql
(args jsonb) returns jsonb language plts
```

Stopgap deploy scans the workspace schema for:
- `prolang = plts`
- `prorettype = jsonb`
- one arg of type `jsonb`

Everything else is ignored (still valid “regular plts”, just not deployed).

### Live function body is a pointer stub
Stopgap creates/replaces in `live_schema`:

```sql
create or replace function live_deployment.some_fn(args jsonb)
returns jsonb
language plts
as $$
{ "plts": 1, "kind": "artifact_ptr", "artifact_hash": "...", "export": "default" }
$$;
```

No TS in live schema, ever.

### Drop/prune policy
- Default: **no drop** (safer for dependencies).
- Optional `--prune` or `stopgap.prune=true`:
  - drop live functions not present in new deployment.
  - prune skips live functions that still have dependencies.

---

# 5) Shared TS/JS authoring model (query/mutation wrappers)

You want stopgap authors to write TS like:

```ts
export default stopgap.query(argsSchema, async (args, ctx) => {
  const rows = await ctx.db.query("select ... where id = $1", [args.id]);
  return rows[0] ?? null;
});
```

and similarly for mutations.

## 5.1 Where the wrappers live
Ship an NPM package `@stopgap/runtime` containing:
- TypeScript types (nice DX)
- `argsSchema` helpers (either a small DSL or JSON Schema builder)
- The wrapper implementation for local testing

In Postgres, `plts` exposes wrapper support through a built-in `@stopgap/runtime` module.

So in DB you can use:
- `import { query, mutation } from "@stopgap/runtime"`

Current implementation resolves this bare specifier through the runtime module loader.

## 5.2 Schema format
Pick one schema strategy:

- **JSON Schema** (portable, serializable, CLI-friendly)
- or a minimal DSL (Zod-like) that you control

Given you need to store schema for inputs, JSON Schema is pragmatic:
- In TS you can still get types via helper builders.
- At runtime you validate jsonb args with a Rust JSON schema validator or a lightweight JS validator.

Current implementation uses a JSON Schema subset validator inside the runtime wrappers (object/array/scalar `type`, `required`, `properties`, `items`, `enum`, `anyOf`, `additionalProperties=false`) and mirrors the same behavior in `packages/runtime` for local testing.

## 5.3 Wrapper semantics
### `stopgap.query(schema, handler)`
- Validates `args jsonb` against schema
- Provides `ctx.db` as **read-only** DB API
- Normalizes return to nullable jsonb

### `stopgap.mutation(schema, handler)`
- Validates args
- Provides `ctx.db` as **read-write** DB API
- Normalizes return to nullable jsonb

Optional: wrappers attach metadata for stopgap deploy to read (if you choose to evaluate during deploy):
- `export.default.__stopgap_kind = "query"`

But you don’t *need* this if you just treat kind as “convention” and enforce by which wrapper is used at runtime.

## 5.4 Drizzle query-builder interop direction (locked for next phase)

Goal: support Stopgap handlers that build queries with Drizzle-style APIs, including references to app schema declarations (`pgTable`, etc.).

For the next phase, the interoperability contract is:
- runtime DB APIs accept SQL string + params
- runtime DB APIs accept `{ sql, params }` objects
- runtime DB APIs accept objects that expose `toSQL(): { sql, params }`
- execution remains SQL text + bound params over SPI

This keeps integration deterministic and extension-friendly without requiring immediate full runtime module-graph support for arbitrary package imports.

Follow-up work can expand import/bundling coverage for richer in-DB Drizzle compatibility.

---

# 6) CLI tool (deployments UX)

Even if DB-side deploy exists, the CLI is the real product surface.

## 6.1 Commands
- `stopgap deploy --db <dsn> --env prod --from-schema <schema> --label <sha> [--prune]`
- `stopgap rollback --db <dsn> --env prod [--steps 1 | --to <id>]`
- `stopgap deployments --db <dsn> --env prod`
- `stopgap status --db <dsn> --env prod`
- (optional) `stopgap diff --db ...` compare workspace vs active

## 6.2 Deploy algorithm (single transaction, atomic)
1) `pg_advisory_xact_lock(...)` per env
2) Insert `stopgap.deployment(status='open')`
3) Scan workspace schema for deployable functions
4) For each:
   - read `prosrc` (TS)
   - compile TS→JS using `plts.compile_ts(...)` (DB-side) or CLI-side compiler
   - `plts.compile_and_store(...) -> artifact_hash`
   - insert `stopgap.fn_version` row
5) Seal deployment
6) Materialize live schema functions as plts pointer stubs
7) Update `environment.active_deployment_id`
8) Insert activation log
9) Commit

Rollback is the same “materialize live schema from older deployment id”.

---

# 7) Security / permissions model

## 7.1 Roles
- `stopgap_owner` (NOLOGIN): owns `stopgap` schema + live schema + SECURITY DEFINER functions
- `stopgap_deployer` (role): allowed to run deploy/rollback
- `app_user`: allowed to execute live functions

## 7.2 Live schema write protection
- `REVOKE CREATE ON SCHEMA live_deployment FROM PUBLIC;`
- `ALTER SCHEMA live_deployment OWNER TO stopgap_owner;`
- All functions created in live schema are owned by `stopgap_owner`.
- Stopgap deploy SQL functions run as `SECURITY DEFINER` and check caller role membership.

This gives you “deny updates to live schema” without fancy event triggers.

---

# 8) Implementation milestones (sequenced)

## P0 (your stated P0: transpile + store + execute)
**In `plts`:**
- Implement `LANGUAGE plts` handler executing TS (transpile-only) and returning jsonb
- Implement arg conversion for common PG types (at least: text, int, bool, jsonb)
- Implement artifact table + `plts.compile_and_store`
- Implement `db.query` via SPI (start read-write; add read-only gate right after)

Current progress snapshot:
- artifact table + compile/store APIs are in place
- handler now has a feature-gated V8 execution path for sync + async `export default` handlers
- handler resolves artifact-pointer stubs by loading compiled JS from `plts.artifact`
- runtime context now includes initial `ctx` shape (`db`, `args`, `fn`, `now`)
- runtime now wires `ctx.db.query/exec` to SPI with structured JS parameter binding
- deno_core dependency and feature-gated isolate bootstrap scaffolding are in place
- async default-export handler execution is now supported in the V8 runtime path
- runtime now evaluates ES modules via the module loader (including `data:` imports and a built-in bare `@stopgap/runtime` module); broader arbitrary import-resolution strategies are still pending
- `plts.compile_ts` now performs real TS->JS transpilation via `deno_ast` and returns structured diagnostics
- `plts` compiler fingerprinting now derives from real dependency versions (`deno_ast`/`deno_core`) from workspace lock metadata
- optional source-map persistence is now supported in `plts.artifact` when `compiler_opts.source_map=true`
- basic arg conversion work has started
- stopgap deploy now validates deployment status transitions (`open -> sealed -> active`, with failure paths)
- stopgap deploy records function-level manifest metadata including artifact hashes and live pointer payloads
- stopgap deploy now checks caller privileges for source/live schema access and compile API execution
- stopgap deploy/status/deployments SQL paths now bind runtime values with argumentized SPI calls
- stopgap now exposes `stopgap.status(env)`, `stopgap.deployments(env)`, `stopgap.diff(env, from_schema)`, and `stopgap.rollback(env, steps, to_id)` APIs
- stopgap now exposes `stopgap.activation_audit` and `stopgap.environment_overview` introspection views
- stopgap deploy now supports optional dependency-aware prune via `stopgap.prune=true`, dropping stale live pointer functions that have no dependents
- stopgap security model now provisions/enforces baseline roles (`stopgap_owner`, `stopgap_deployer`, `app_user`), runs deploy/rollback/diff as SECURITY DEFINER, and hardens live-schema/live-function ownership + execute grants
- plts runtime errors now include stage metadata, JS stack details (when present), and SQL function identity context
- DB-backed `plts` integration tests now cover `compile_and_store` / `get_artifact` round-trips, regular arg conversion (`text`, `int4`, `bool`, `jsonb`), runtime null normalization (`null`/`undefined` -> SQL `NULL`), and artifact-pointer execution (under `v8_runtime`)
- DB-backed `stopgap` integration tests now cover deploy pointer updates, live pointer payload correctness, `fn_version` integrity, overloaded-function rejection, and rollback rematerialization/status transitions
- CI workflow now runs workspace `cargo check`, `cargo test`, matrixed `cargo pgrx test` jobs per extension crate, and `cargo pgrx regress -p stopgap` (with `plts` installed first)
- repository toolchain and lint/format configs are pinned (`rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`)

**In `stopgap`:**
- Create catalog tables + minimal `stopgap.deploy` that:
  - scans a schema for `(args jsonb) returns jsonb language plts` functions
  - compiles and stores artifacts
  - creates live pointer stubs in `live_schema`
  - sets active deployment id

## P1 (DX + correctness)
- `stopgap.rollback` (implemented SQL API with `steps`/`to_id` targeting)
- read-only enforcement for queries (implemented for `stopgap.query`; SQL classifier hardening can continue iteratively)
- `stopgap.query/mutation` wrappers available in runtime + TS types package, with JSON Schema arg validation + inferred TS helper types (`InferJsonSchema`)
- better error messages + stack traces
- caching compiled artifacts per backend (artifact-pointer source cache now implemented in `plts`)

## P1.5 (structure + interop)
- introduce `crates/common` for shared helper logic across extensions
- split large single-file crate implementations into cohesive modules
- move PG integration tests out of extension source and keep suites granular
- add Drizzle-style SQL object / `toSQL()` interop while keeping SPI SQL+params execution model

## P2 (hardening)
- cancellation/timeouts wired to Postgres interrupts
- memory limits
- prune mode + dependency-safe behavior
- audit + status introspection

---

# 9) Key design choices to lock now (so you don’t repaint later)
1) **Engine**: **V8 via `deno_core`**.
2) **Return null semantics**: JS `undefined` and `null` normalize to SQL `NULL`.
3) **Schema format**: defer final choice to wrapper/runtime phase (JSON Schema currently preferred).
4) **Deploy compilation location**: **DB compile path** (`plts.compile_ts` / `plts.compile_and_store`).
5) **Function identity**: **forbid overloading** for stopgap-managed functions.
6) **Regular `plts` args view**: expose **both positional and named/object forms**.
7) **Entrypoint convention**: **default export**.
8) **P0 DB API mode**: **RW-only**, defer RO enforcement to P1.
9) **Drizzle interop v1**: normalize query-builder output to **SQL text + params** for SPI execution.
10) **Shared code strategy**: use a **`crates/common`** helper crate while preserving split-extension semantic boundaries.
