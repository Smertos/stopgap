# AGENTS.md

This file captures how to work effectively in this repository.

## Repository shape

- Root docs:
  - `README.md`: quickstart-first repository onboarding
- Workspace root: `Cargo.toml`
- Crates:
  - `crates/common`: shared pure-Rust helpers used by both extensions
  - `crates/plts`: language/runtime extension (`LANGUAGE plts`, artifact APIs)
  - `crates/stopgap`: deployment/environment extension
  - `crates/stopgap-cli`: Rust CLI for deploy/rollback/status/deployments/diff flows
- Packages:
  - `packages/runtime`: `@stopgap/runtime` wrappers + `v` schema/type helpers
- Third-party:
  - `third_party/typescript-go`: pinned `typescript-go` submodule for upcoming in-process TSGo WASM compiler backend migration
  - `third_party/stopgap-tsgo-api`: Go API shim scaffold for narrow TSGo typecheck/transpile bridge, including built WASI artifact at `third_party/stopgap-tsgo-api/dist/stopgap-tsgo-api.wasm`
- Docs:
  - `docs/ROADMAP.md`: tracked implementation plan and current backlog
  - `docs/PROJECT-OUTLINE.md`: product/architecture source of truth
  - `docs/DEVELOPER-QUICKSTART.md`: local setup + validation command reference
  - `docs/RUNTIME-CONTRACT.md`: `plts` runtime context/return contract
  - `docs/DEPLOYMENT-RUNBOOK.md`: deployment/rollback operational lifecycle
  - `docs/PERFORMANCE-BASELINE.md`: profiling baseline, bottlenecks, and next optimization targets
  - `docs/TROUBLESHOOTING.md`: common setup/test/runtime issue guide
  - `docs/TECH-DEBT.md`: current technical debt and follow-up notes

## Tooling baseline

- Rust toolchain is pinned in `rust-toolchain.toml`.
- Formatting/lint config is tracked in `rustfmt.toml` and `clippy.toml`.
- CI workflow lives at `.github/workflows/ci.yml` and runs a fast baseline (`packages/runtime` check/test + `cargo check` + `cargo test` + explicit `stopgap-cli` command integration tests) plus per-crate `cargo pgrx test` matrix jobs; Rust builds also refresh the compiled runtime artifact (`packages/runtime/dist/embedded_runtime.js`) via `crates/plts/build.rs`, the stopgap matrix job also runs `cargo pgrx regress -p stopgap` and a focused call-fn path-routing suite (`test_call_fn_` filter), a dedicated `plts runtime v8 (pg17)` lane runs runtime-heavy `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"` coverage, and pgrx/runtime jobs upload failure-only diagnostics artifacts (`PGRX_HOME` + `target/debug` bundles).

## Current architecture assumptions (locked)

- Engine target for P0+: **V8 via `deno_core`**.
- JS `null` and `undefined` should normalize to SQL `NULL` (long-term behavior target).
- Stopgap deploy compile path is DB-side (`plts.compile_ts` / `plts.compile_and_store`).
- Primary stopgap app authoring model is TS-first from project-local `stopgap/**/*.ts` modules.
- Stopgap app function identity is path-based: `api.<module_path_without_ext>.<named_export>`.
- Stopgap public invocation surface is `stopgap.call_fn(path text, args jsonb)`.
- `stopgap.call_fn(path, args)` is the primary invocation surface for TS-first apps; live-schema wrappers remain extension-generated compatibility bridges (not user-authored workflow).
- `stopgap.call_fn` should emit path-aware errors for invalid args and wrapper-mode violations (`wrong wrapper mode`) while preserving runtime detail text.
- Stopgap observability includes `stopgap.metrics().call_fn` counters with path-route source splits (`exact`/`legacy`) and call-fn error classes.
- CLI deploy preflight must fail fast when `./stopgap` is missing, discover named `query(...)`/`mutation(...)` exports from `stopgap/**/*.ts`, reject non-wrapper named exports, and normalize function identity to deterministic `api.<module_path_without_ext>.<export_name>` paths.
- CLI deploy currently forwards discovered export metadata to DB deploy through transaction-local `stopgap.deploy_exports`; stopgap deploy consumes this metadata to persist `function_path`/`module_path`/`export_name`/`kind` and pointer-export routing data, while non-metadata fallback paths remain legacy/default-export compatible.
- When `stopgap.deploy_exports` metadata is provided, stopgap deploy must fail fast on drift by rejecting missing, unknown, or duplicate export entries relative to deployable functions, including duplicate `function_path` routes.
- Deployment manifests should include both ordered `functions` and canonical `functions_by_path` entries keyed by `function_path` for deterministic path-addressable introspection.
- Current migration bridge: `stopgap.call_fn` routes through active deployment metadata, prefers exact `function_path` matches in `stopgap.fn_version`, and falls back to terminal export-segment resolution only for legacy rows; malformed paths and ambiguous fallback matches must fail with explicit route errors.
- Runtime pointer metadata supports explicit entrypoint selection (`{"export":"<named_export>"}`) with `default` fallback, so stopgap path routing can target named exports once deployment metadata emits non-default export pointers.
- TSGo migration first pass currently defers `@app/*` semantic-typecheck support; unresolved `@app/*` imports should emit explicit unsupported-import diagnostics.
- TSGo migration checkpoint: semantic typecheck now invokes embedded `stopgap-tsgo-api.wasm` in-process for TSGo diagnostics with no legacy `tsc` fallback in DB validator/compile/typecheck paths.
- Semantic typecheck workspace stubs for `@stopgap/runtime` should remain strict and typed (avoid permissive `any` fallbacks so `strict`/`noImplicitAny` catches wrapper-arg misuse).
- Stopgap-managed overloading is forbidden.
- Regular `plts` calling convention should expose both positional and named/object argument forms.
- Entrypoint conventions: regular `plts` modules use default export; stopgap app modules use named exports discovered at deploy time.
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
- Keep backend-local hot caches explicitly bounded/invalidation-aware (for example `plts` function-program cache uses `fn_oid` LRU keying with TTL + source-byte limits).
- Keep runtime bootstrap boundaries explicit: snapshot/static bootstrap must remain invocation-agnostic, while `ctx` payload wiring and DB-mode selection stay per-invocation.
- Keep runtime safety defaults conservative (timeouts, memory, no FS/network once runtime lands).
- If a change alters runtime contract behavior (`ctx` shape, DB API behavior, or return normalization), update `docs/RUNTIME-CONTRACT.md` and add/adjust contract-focused tests in the same change set.

## Quality policy: no shortcuts

- Do not use evasive fixes to make checks pass.
- Specifically forbidden without explicit maintainer direction:
  - disabling tests or CI lanes
  - marking tests as ignored to hide failures
  - weakening assertions to avoid root-cause fixes
  - temporary bypass flags that reduce coverage or runtime safety
- Treat this as the same quality bar as avoiding excessive `any` in TypeScript and unnecessary `unsafe` in Rust.
- Required approach: find the root cause, fix it directly, and add/adjust coverage so regressions are caught.

## Evergreen V8 expectation (P0)

- The `plts` V8 runtime lane is a release-blocking quality gate.
- V8 tests must stay green after every change; failing V8 coverage is a bug to fix, not a lane to bypass.
- Current CI parity command: `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"`.

## Runtime rollout gates (P4 lifecycle work)

- Use phased rollout for runtime lifecycle/pooling changes:
  - Phase 1: boundary + instrumentation
  - Phase 2: conservative isolate reuse defaults
  - Phase 3: tuning + SLO enforcement
- Acceptance gates per phase:
  - runtime contract gate: invocation isolation + runtime contract suites must pass
  - runtime safety gate: timeout/cancel/heap-limit suites must pass and tainted isolates must remain non-reusable
  - release verification gate: full command set in the checklist below must pass before phase promotion
- Rollback condition: if any acceptance gate regresses, revert to the prior stable phase defaults and re-validate all required lanes before retrying rollout.

Current checkpoint status (iteration 19):
- Phase 1 complete.
- Phase 2 complete (conservative isolate reuse defaults + recycle-policy unit validation in `crates/plts/src/isolate_pool.rs`).
- Phase 3 complete (runtime performance baseline now enforces compile/cold/warm SLO thresholds plus warm-vs-cold regression delta checks in `crates/plts/tests/pg/runtime_performance_baseline.rs`).
- Runtime performance baseline timing uses nanosecond totals (converted to per-call milliseconds), a 1,000-call execute loop, and bounded measurement retries to avoid millisecond-quantization flakes and coarse-clock zero-elapsed assertions in CI.

## Validation checklist for each meaningful change

- `cargo check`
- `cargo test`
- `cargo pgrx test -p plts`
- `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"`
- `cargo pgrx test -p stopgap`
- `cargo pgrx regress -p stopgap`

If SQL outputs or extension entities change, also run/update pg_regress artifacts where relevant.

## Do not do without explicit direction

- Do not add forceful/destructive git operations.
- Do not change locked P0 decisions above unless requested.
- Do not introduce network/FS access into the runtime surface.
- Do not disable, ignore, or bypass V8 runtime tests to unblock merges.
