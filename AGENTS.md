# AGENTS.md

This file captures how to work effectively in this repository.

## Repository shape

- Workspace root: `Cargo.toml`
- Crates:
  - `crates/common`: shared pure-Rust helpers used by both extensions
  - `crates/plts`: language/runtime extension (`LANGUAGE plts`, artifact APIs)
  - `crates/stopgap`: deployment/environment extension
  - `crates/stopgap-cli`: Rust CLI for deploy/rollback/status/deployments/diff flows
- Packages:
  - `packages/runtime`: `@stopgap/runtime` wrappers + TS schema/type helpers
- Docs:
  - `docs/PROJECT-OUTLINE.md`: product/architecture source of truth
  - `docs/DEVELOPER-QUICKSTART.md`: local setup + validation command reference
  - `docs/RUNTIME-CONTRACT.md`: `plts` runtime context/return contract
  - `docs/DEPLOYMENT-RUNBOOK.md`: deployment/rollback operational lifecycle
  - `docs/TROUBLESHOOTING.md`: common setup/test/runtime issue guide

## Tooling baseline

- Rust toolchain is pinned in `rust-toolchain.toml`.
- Formatting/lint config is tracked in `rustfmt.toml` and `clippy.toml`.
- CI workflow lives at `.github/workflows/ci.yml` and runs a fast baseline (`cargo check` + `cargo test`) plus per-crate `cargo pgrx test` matrix jobs; the stopgap matrix job also runs `cargo pgrx regress -p stopgap`, a dedicated `plts runtime v8 (pg16)` lane runs runtime-heavy `cargo pgrx test -p plts --features "pg16,v8_runtime"` coverage, and pgrx/runtime jobs upload failure-only diagnostics artifacts (`PGRX_HOME` + `target/debug` bundles).

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
- Statement/plan caching has been evaluated for the current runtime DB interop baseline; no explicit runtime statement cache is enabled yet, and SPI SQL+bound-params remains the stable execution model pending profiling-driven need.
- `plts` runtime now applies deterministic DB API guardrails per call (bounded SQL text size, bound parameter count, and query row count via `plts.max_sql_bytes`, `plts.max_params`, and `plts.max_query_rows`).
- Observability now includes backend-process metrics counters (`plts.metrics()`, `stopgap.metrics()`) and log-level gated compile/execute/deploy flow logging (`plts.log_level`, `stopgap.log_level`).
- `plts` runtime now exposes `ctx.db.query/exec` SPI bindings with structured JSON parameter binding and wrapper-aware DB mode (`stopgap.query` => read-only, `stopgap.mutation`/regular => read-write), with JSON-Schema-based arg validation in runtime wrappers.
- Runtime/package wrapper parity is maintained via shared module source at `packages/runtime/src/embedded.ts`, which is what `plts` loads for the built-in `@stopgap/runtime` module.
- `plts` runtime now locks down module globals before execution (removing `Deno`, `fetch`, and related web APIs) so handlers only use the explicit `ctx.db` bridge and do not gain filesystem/network runtime surface.
- `plts` runtime now applies a V8 watchdog per call using the stricter of `statement_timeout` and optional `plts.max_runtime_ms`, and routes pending Postgres cancel/die interrupt flags into the same V8 termination path.
- `plts` runtime now optionally enforces `plts.max_heap_mb` per call by setting V8 heap limits and terminating execution on near-heap-limit callbacks.
- `plts.compile_ts` now transpiles TS->JS via `deno_ast`, reports structured diagnostics, records compiler fingerprint metadata from lockfile-resolved dependency versions, and can persist source-map payloads when `compiler_opts.source_map=true`.
- `plts` now caches artifact-pointer compiled JS per backend process to avoid repeat `plts.artifact` lookups during live pointer execution.
- DB-backed `plts` integration tests cover `compile_and_store` / `get_artifact` round-trips, regular arg conversion (`text`, `int4`, `bool`, `jsonb`), runtime null normalization, artifact-pointer execution, and async default-export execution under `v8_runtime`.
- Stopgap deploy still records function kind as a default convention (`mutation`), while runtime enforcement relies on wrapper metadata.
- Stopgap deploy now enforces deployment status transitions, writes richer manifest metadata, checks deploy caller privileges, and ships rollback/status/deployments/diff APIs plus activation/environment introspection views.
- DB-backed `stopgap` integration tests now cover deploy pointer updates, live pointer payload correctness, `fn_version` integrity, overloaded-function rejection, and rollback status/pointer rematerialization.
- stopgap `pg_regress` rollback scenario now covers a real cross-extension path (`deploy -> live execute -> rollback`) and asserts both live execution continuity and pointer rematerialization after rollback.
- Stopgap deploy now supports optional dependency-aware prune via `stopgap.prune=true`; ownership/role hardening baseline is now in place (`stopgap_owner`, `stopgap_deployer`, `app_user`, SECURITY DEFINER deploy/rollback/diff, and live-schema execute grants).
- Most deploy SQL value binding uses argumentized SPI; remaining interpolation is primarily constrained identifier/DDL construction.
- Shared helper migration has started via `crates/common` (SQL quoting + bool-setting parsing); plts logic is now split across `crates/plts/src/api.rs`, `crates/plts/src/handler.rs`, `crates/plts/src/runtime.rs`, `crates/plts/src/compiler.rs`, `crates/plts/src/runtime_spi.rs`, `crates/plts/src/function_program.rs`, and `crates/plts/src/arg_mapping.rs` with a thin `crates/plts/src/lib.rs`, while stopgap pure domain/state-transition logic lives in `crates/stopgap/src/domain.rs`, runtime config/SPI helpers live in `crates/stopgap/src/runtime_config.rs`, deploy scan/materialization helpers live in `crates/stopgap/src/deployment_utils.rs`, stopgap deployment-state/rollback helpers live in `crates/stopgap/src/deployment_state.rs`, stopgap deploy/status/diff orchestration helpers live in `crates/stopgap/src/api_ops.rs`, stopgap role/permission helpers live in `crates/stopgap/src/security.rs`, and stopgap SQL API/bootstrap wiring lives in `crates/stopgap/src/api.rs` + `crates/stopgap/src/sql_bootstrap.rs` with a thin `crates/stopgap/src/lib.rs`.

## Do not do without explicit direction

- Do not add forceful/destructive git operations.
- Do not change locked P0 decisions above unless requested.
- Do not introduce network/FS access into the runtime surface.
