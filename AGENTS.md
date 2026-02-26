# AGENTS.md

This file captures how to work effectively in this repository.

## Repository shape

- Workspace root: `Cargo.toml`
- Crates:
  - `crates/common`: shared pure-Rust helpers used by both extensions
  - `crates/plts`: language/runtime extension (`LANGUAGE plts`, artifact APIs)
  - `crates/stopgap`: deployment/environment extension
- Packages:
  - `packages/runtime`: `@stopgap/runtime` wrappers + TS schema/type helpers
- Docs:
  - `docs/PROJECT-OUTLINE.md`: product/architecture source of truth

## Tooling baseline

- Rust toolchain is pinned in `rust-toolchain.toml`.
- Formatting/lint config is tracked in `rustfmt.toml` and `clippy.toml`.
- CI workflow lives at `.github/workflows/ci.yml` and runs workspace check/test plus per-crate `cargo pgrx test` matrix jobs; the stopgap matrix job also runs `cargo pgrx regress -p stopgap`.

## Current architecture assumptions (locked)

- Engine target for P0+: **V8 via `deno_core`**.
- JS `null` and `undefined` should normalize to SQL `NULL` (long-term behavior target).
- Stopgap deploy compile path is DB-side (`plts.compile_ts` / `plts.compile_and_store`).
- Stopgap-managed deployables are `(args jsonb) returns jsonb language plts`.
- Stopgap-managed overloading is forbidden.
- Regular `plts` calling convention should expose both positional and named/object argument forms.
- Default export is the entrypoint convention.
- P0 baseline DB API mode is RW; P1 adds wrapper-aware read-only gating for `stopgap.query` handlers.

## Implementation guardrails

- Keep the split-extension model:
  - `plts` owns language/runtime and artifact substrate.
  - `stopgap` owns deployments, activation, live materialization.
- Avoid introducing Stopgap-specific runtime semantics into `plts` beyond pointer execution compatibility.
- Keep SQL behavior deterministic and explicit; avoid hidden mutable global state.
- Prefer additive migrations and backward-compatible SQL API changes.

## Working conventions for contributors/agents

- Prefer small, verifiable increments over broad rewrites.
- When editing Rust + SQL entity graph code:
  - Use `#[pg_schema]` modules for schema-scoped SQL functions.
  - Keep `extension_sql!` focused on bootstrap/catalog DDL.
- For SPI use:
  - Prefer safe argumentized calls where practical.
  - If interpolating SQL strings, quote literals/idents robustly and keep inputs constrained.
- Keep runtime safety defaults conservative (timeouts, memory, no FS/network once runtime lands).

## Validation checklist for each meaningful change

- `cargo check`
- `cargo test`
- `cargo pgrx test -p plts`
- `cargo pgrx test -p stopgap`
- `cargo pgrx regress -p stopgap`

If SQL outputs or extension entities change, also run/update pg_regress artifacts where relevant.

## Near-term technical debt to remember

- `plts` runtime handler executes sync + async default-export JS when built with `v8_runtime`, now via ES module loading (including `data:` imports and bare `@stopgap/runtime`); broader arbitrary import-resolution coverage is still pending, but runtime errors surface stage/message/stack with SQL function identity context.
- `plts` runtime `ctx.db.query/exec` now accepts SQL string + params, `{ sql, params }` objects, and Drizzle-style `toSQL(): { sql, params }` inputs while keeping execution on SPI SQL text + bound params.
- `plts` runtime now exposes `ctx.db.query/exec` SPI bindings with structured JSON parameter binding and wrapper-aware DB mode (`stopgap.query` => read-only, `stopgap.mutation`/regular => read-write), with JSON-Schema-based arg validation in runtime wrappers.
- `plts` runtime now locks down module globals before execution (removing `Deno`, `fetch`, and related web APIs) so handlers only use the explicit `ctx.db` bridge and do not gain filesystem/network runtime surface.
- `plts` runtime now applies a `statement_timeout`-driven V8 watchdog per call and routes pending Postgres cancel/die interrupt flags into the same V8 termination path.
- `plts.compile_ts` now transpiles TS->JS via `deno_ast`, reports structured diagnostics, records compiler fingerprint metadata from lockfile-resolved dependency versions, and can persist source-map payloads when `compiler_opts.source_map=true`.
- `plts` now caches artifact-pointer compiled JS per backend process to avoid repeat `plts.artifact` lookups during live pointer execution.
- DB-backed `plts` integration tests cover `compile_and_store` / `get_artifact` round-trips, regular arg conversion (`text`, `int4`, `bool`, `jsonb`), runtime null normalization, artifact-pointer execution, and async default-export execution under `v8_runtime`.
- Stopgap deploy still records function kind as a default convention (`mutation`), while runtime enforcement relies on wrapper metadata.
- Stopgap deploy now enforces deployment status transitions, writes richer manifest metadata, checks deploy caller privileges, and ships rollback/status/deployments/diff APIs plus activation/environment introspection views.
- DB-backed `stopgap` integration tests now cover deploy pointer updates, live pointer payload correctness, `fn_version` integrity, overloaded-function rejection, and rollback status/pointer rematerialization.
- Stopgap deploy now supports optional dependency-aware prune via `stopgap.prune=true`; ownership/role hardening baseline is now in place (`stopgap_owner`, `stopgap_deployer`, `app_user`, SECURITY DEFINER deploy/rollback/diff, and live-schema execute grants).
- Most deploy SQL value binding uses argumentized SPI; remaining interpolation is primarily constrained identifier/DDL construction.
- Shared helper migration has started via `crates/common` (SQL quoting + bool-setting parsing); stopgap pure domain/state-transition logic now lives in `crates/stopgap/src/domain.rs`, runtime config/SPI helpers now live in `crates/stopgap/src/runtime_config.rs`, and broader module split plus focused `pg_regress` scenario expansion remain pending.

## Do not do without explicit direction

- Do not add forceful/destructive git operations.
- Do not change locked P0 decisions above unless requested.
- Do not introduce network/FS access into the runtime surface.
