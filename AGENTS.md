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
- CI workflow lives at `.github/workflows/ci.yml` and runs a fast baseline (`packages/runtime` check/test + `cargo check` + `cargo test`) plus per-crate `cargo pgrx test` matrix jobs; Rust builds also refresh the compiled runtime artifact (`packages/runtime/dist/embedded_runtime.js`) via `crates/plts/build.rs`, the stopgap matrix job also runs `cargo pgrx regress -p stopgap`, a dedicated `plts runtime v8 (pg17)` lane runs runtime-heavy `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"` coverage, and pgrx/runtime jobs upload failure-only diagnostics artifacts (`PGRX_HOME` + `target/debug` bundles).

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
