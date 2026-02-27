# Stopgap + PLTS Roadmap

This roadmap tracks implementation of the split-extension architecture described in `docs/PROJECT-OUTLINE.md`.

Legend:
- `[x]` done
- `[ ]` planned / in progress

---

## 0) Locked Decisions

- [x] Engine target: **V8 via `deno_core`**
- [x] Return null semantics target: JS `undefined` and `null` normalize to SQL `NULL`
- [x] Stopgap deploy compilation path target: DB-side (`plts.compile_ts` / `plts.compile_and_store`)
- [x] Stopgap deployable signature: `(args jsonb) returns jsonb language plts`
- [x] Stopgap-managed overloading: forbidden
- [x] Regular `plts` args target: expose both positional and named/object forms
- [x] Entrypoint convention: default export
- [x] P0 DB API enforcement mode: RW-only (defer read-only gates)

---

## 1) Workspace + Baseline Infrastructure

- [x] Set up Rust workspace members for both extension crates
- [x] Create `crates/plts` extension scaffold
- [x] Create `crates/stopgap` extension scaffold
- [x] Ensure `cargo check` succeeds for entire workspace
- [x] Ensure `cargo test` succeeds for entire workspace
- [x] Ensure `cargo pgrx test -p plts` succeeds
- [x] Ensure `cargo pgrx test -p stopgap` succeeds

### Follow-ups

- [x] Add CI workflow (check + test + pgrx test matrix)
- [x] Add pinned toolchain/rustfmt/clippy configs

### Next: structure + shared crate

- [x] Add `crates/common` workspace crate for shared pure-Rust helpers used by `plts` and `stopgap`
- [x] Keep split-extension ownership clear: shared helpers in `common`, extension semantics stay in owning crate
- [x] Extract Stopgap pure domain/state-transition helpers into a dedicated module as a first step toward a thin `lib.rs`
- [x] Extract Stopgap runtime config/SPI helper functions into a dedicated module as an additional `lib.rs` split increment
- [x] Extract `plts` compiler/transpile + fingerprint + source-map helpers into `crates/plts/src/compiler.rs`
- [x] Extract `plts` runtime SPI/query-binding/read-only SQL helpers into `crates/plts/src/runtime_spi.rs`
- [x] Extract `plts` function source loading + artifact-pointer cache helpers into `crates/plts/src/function_program.rs`
- [x] Extract `plts` regular-arg payload conversion helpers into `crates/plts/src/arg_mapping.rs`
- [x] Extract Stopgap deployable-function scan + live materialization helpers into `crates/stopgap/src/deployment_utils.rs`
- [x] Extract Stopgap deployment-state / rollback helpers into `crates/stopgap/src/deployment_state.rs`
- [x] Extract Stopgap role/permission checks into `crates/stopgap/src/security.rs`
- [x] Extract Stopgap deploy/status/diff orchestration helpers into `crates/stopgap/src/api_ops.rs`
- [x] Refactor `crates/plts/src/lib.rs` into focused modules with a thin entrypoint `lib.rs`
- [x] Refactor `crates/stopgap/src/lib.rs` into focused modules with a thin entrypoint `lib.rs`
- [x] Preserve existing SQL API and extension entity compatibility during refactor

---

## 2) `plts` Extension: Language + Runtime Substrate

### 2.1 Bootstrap / Catalog

- [x] Bootstrap `plts` schema and `plts.artifact` table
- [x] Install `plts_call_handler` and `plts_validator` SQL hooks
- [x] Create `LANGUAGE plts` if missing at extension install time

### 2.2 Artifact APIs

- [x] `plts.version()`
- [x] `plts.compile_ts(source_ts, compiler_opts)` (placeholder implementation)
- [x] `plts.upsert_artifact(source_ts, compiled_js, compiler_opts)`
- [x] `plts.compile_and_store(source_ts, compiler_opts)`
- [x] `plts.get_artifact(artifact_hash)`
- [x] Deterministic artifact hash strategy with compiler fingerprint + source + output + options

### 2.3 Call Handler Behavior (P0)

- [x] Handler safely returns SQL `NULL` when no valid result is available
- [x] `(args jsonb)` functions return input jsonb directly (P0 stopgap compatibility)
- [x] Basic regular-arg conversion supports common types (`text`, `int4`, `bool`, `jsonb`)
- [x] Regular call result currently emits a JSONB object with `positional` and `named` forms

### 2.4 Runtime Engine Work (core unfinished)

- [x] Add `deno_core` dependency and isolate bootstrap
- [x] Add feature-gated V8 script execution path for default-export handlers (source or artifact pointer)
- [x] Implement ES module execution inside V8 isolate with module loader support (`import`/`export`, including `data:` modules)
- [x] Expand module loader coverage beyond `data:` imports (for example controlled relative/bare-specifier strategies)
- [x] Resolve and invoke default export entrypoint (sync handlers)
- [x] Resolve and invoke async default export entrypoints (Promise-returning handlers)
- [x] Build initial runtime call contract: `ctx.db`, `ctx.args`, `ctx.fn`, `ctx.now`
- [x] Normalize JS return values to SQL/jsonb semantics:
  - [x] `undefined` -> SQL `NULL`
  - [x] `null` -> SQL `NULL`
  - [x] primitives/object/array -> `jsonb`
- [x] Add robust runtime error propagation (message + stack + SQL context)
- [x] Include SQL function identity (`schema.name`, oid) in runtime error messages
- [x] Cache artifact-pointer compiled source per backend to reduce repeat catalog lookups

### 2.5 Compiler Work (unfinished)

- [x] Replace placeholder `compile_ts` with real TS->JS transpilation
- [x] Return diagnostics payload with line/column and severity
- [x] Persist compiler metadata/fingerprint from real toolchain versions
- [x] Optionally persist source maps in `plts.artifact`

### 2.6 DB API Surface (unfinished)

- [x] Expose RW db API (`query`/`exec`) from runtime context
- [x] Add structured parameter binding from JS to SPI
- [x] Add transaction semantics docs for runtime calls
- [x] Add wrapper-aware RO gate for `stopgap.query` handlers (`db.exec` denied + read-only `db.query` filter)

---

## 3) `stopgap` Extension: Deployments + Environments

### 3.1 Catalogs

- [x] `stopgap.environment`
- [x] `stopgap.deployment`
- [x] `stopgap.fn_version`
- [x] `stopgap.activation_log`

### 3.2 Deploy API

- [x] `stopgap.version()`
- [x] `stopgap.deploy(env, from_schema, label)` scaffold
- [x] Advisory lock per env during deploy transaction scope
- [x] Ensure environment row exists/updated
- [x] Detect and reject overloaded `plts` function names in source schema
- [x] Scan source schema for deployables with strict signature filter
- [x] Compile and store artifacts via `plts.compile_and_store`
- [x] Insert `stopgap.fn_version` rows for deployed functions
- [x] Materialize live schema pointer stubs (`kind = artifact_ptr`)
- [x] Update active deployment pointer
- [x] Write activation log entry

### 3.3 P0 Gaps

- [x] Add explicit deployment state machine validation and transitions
- [x] Wrap deploy logic in hardened error handling with `failed` state writes
- [x] Add detailed manifest generation (full function + artifact metadata)
- [x] Add strict role/permission checks for deploy caller
- [x] Move dynamic SQL-heavy paths to stronger argumentized SPI patterns

### 3.4 Rollback / Status / Introspection (P1+)

- [x] `stopgap.rollback(env, steps|to_id)`
- [x] `stopgap.status(env)`
- [x] `stopgap.deployments(env)`
- [x] `stopgap.diff` supporting API (optional)
- [x] Activation/audit-focused introspection views

---

## 4) Live Schema Management + Safety

- [x] Create live schema on demand if missing
- [x] Create/replace pointer functions in live schema
- [x] Add prune mode (`stopgap.prune`) to drop stale functions safely
- [x] Add dependency-aware prune strategy
- [x] Enforce ownership/privilege model for live schema writes:
  - [x] owner role (`stopgap_owner`)
  - [x] deployer role (`stopgap_deployer`)
  - [x] app runtime role (`app_user`)
  - [x] security definer deploy functions
  - [x] explicit revoke/create grants

---

## 5) Wrapper Model + Runtime UX (`@stopgap/runtime`) (P1)

- [x] Create package scaffold for `@stopgap/runtime`
- [x] Implement `stopgap.query(schema, handler)` wrapper
- [x] Implement `stopgap.mutation(schema, handler)` wrapper
- [x] Expose runtime metadata for handler kind
- [x] Choose/implement schema strategy (JSON Schema target)
- [x] Runtime arg validation against schema
- [x] TS type inference helpers for args/results

### 5.1 Drizzle query-builder interoperability (next)

- [x] Support `ctx.db.query/exec` input as SQL string + params
- [x] Support `ctx.db.query/exec` input as `{ sql, params }` object
- [x] Support `ctx.db.query/exec` input as object exposing `toSQL(): { sql, params }` (Drizzle-style interop)
- [x] Keep v1 runtime execution contract as SQL text + bound params over SPI
- [x] Evaluate statement/plan caching after interop baseline is stable (conclusion: keep SPI SQL+params execution path unchanged for now; no explicit cache added without profiling evidence)
- [x] Defer full module-graph/bundling compatibility for arbitrary runtime imports to follow-up work (tracked in post-roadmap follow-up backlog)

---

## 6) Operational Hardening (P2)

- [x] Integrate Postgres cancellation and statement timeout with JS interrupts
  - [x] Wire `statement_timeout` to a V8 watchdog that terminates execution when the current call exceeds the active timeout
  - [x] Wire explicit cancel/interrupt-pending signals into the same runtime interrupt path
- [x] Add per-call memory limits
- [x] Add deterministic runtime resource constraints
- [x] Ensure no filesystem/network runtime surface
- [x] Add metrics and logs for compile/execute/deploy flows
- [x] Add tunable GUCs for runtime caps and log level

---

## 7) Testing Roadmap

### 7.1 Current

- [x] Unit tests for deterministic hash behavior (`plts`)
- [x] Unit tests for stable deploy lock hash (`stopgap`)

### 7.2 Next

- [x] Re-introduce DB-backed pgrx integration tests for `plts` SQL APIs
- [x] Add tests for regular args conversion (`text`, `int4`, `bool`, `jsonb`)
- [x] Add tests for null normalization behavior in runtime
- [x] Add tests for artifact pointer execution path
- [x] Add tests for async default-export execution path (`v8_runtime`)
- [x] Add stopgap deploy integration test that validates:
  - [x] active deployment pointer changes
  - [x] live schema pointer body payload
  - [x] `fn_version` integrity
  - [x] rejection of overloaded functions
- [x] Add rollback integration tests

### 7.3 Regressions / SQL snapshots

- [x] Base pg_regress setup file exists
- [x] Added deploy regression SQL/output scaffolding
- [x] Wire pg_regress execution into automated test flow
- [x] Keep expected files updated on SQL output/entity changes

### 7.4 Test structure + granularity (next)

- [x] Move pgrx `#[pg_test]` suites out of extension `src/lib.rs` files
- [x] Keep PG tests in dedicated crate-level test modules separate from extension source
- [x] Enforce granular test scope (one behavior/theme per test file)
- [x] Expand `pg_regress` SQL suites into focused scenario files (deploy, rollback, prune, diff, security)

---

## 8) CLI Roadmap (`stopgap-cli`)

- [x] Scaffold CLI project
- [x] Implement `deploy`
- [x] Implement `rollback`
- [x] Implement `status`
- [x] Implement `deployments`
- [x] Optional: implement `diff`
- [x] Add human-readable + JSON output modes
- [x] Add CI/CD-oriented exit codes and failure diagnostics

---

## 9) Documentation Roadmap

- [x] Keep project outline as source-of-truth architecture doc
- [x] Add repo-level agent guidance (`AGENTS.md`)
- [x] Add this roadmap with progress tracking
- [x] Add developer quickstart (local pgrx setup + commands)
- [x] Add runtime contract reference (`ctx` shape and return semantics)
- [x] Add deployment lifecycle and operational runbook
- [x] Add profiling baseline doc (hotspots, targets, next optimizations)
- [x] Add troubleshooting guide (common pgrx/test/install issues)

---

## 10) Suggested Execution Order from Here

1. [x] Continue module split of large extension `lib.rs` files, building on shared helpers now in `crates/common` (including `crates/plts/src/lib.rs`)
2. [x] Expand focused scenario coverage in `pg_regress` suites (deploy, rollback, prune, diff, security)
3. [x] Implement Drizzle-compatible SQL object / `toSQL()` interop in runtime DB APIs
4. [x] Reduce runtime-wrapper duplication between embedded module and `@stopgap/runtime`
5. [x] Finish remaining operational hardening (deterministic constraints, metrics/GUC tuning)
6. [x] Implement CLI surface (`deploy`, `rollback`, `status`, `deployments`, optional `diff`)

---

## 11) Current Snapshot

- **P0 status:** Complete.
- **P1 status:** Complete.
- **What works now:** workspace + extension scaffolds, shared `crates/common` helpers used by both extensions (currently SQL quoting + boolean setting parsing), artifact catalog/APIs, minimal deploy flow, rollback/status/deployments/diff APIs, activation/environment introspection views, live pointer materialization, overload rejection, dependency-aware live prune mode (`stopgap.prune`), baseline tests, DB-backed `plts` integration tests for compile/store and regular arg conversion, feature-gated runtime integration tests for null normalization + artifact pointer execution, stopgap deploy/rollback integration tests (active pointer + pointer payload + fn_version integrity + overload rejection), behavior-focused pgrx integration test files under `crates/*/tests/pg/`, focused `pg_regress` scenario files for deploy/rollback/prune/diff/security, and feature-gated sync + async default-export JS execution in `plts`, including module imports via `data:` URLs and bare `@stopgap/runtime` resolution with wrapper-aware DB mode (`query` => read-only, `mutation`/regular => read-write) plus JSON-Schema-based wrapper arg validation. Runtime DB APIs now support SQL string + params, `{ sql, params }` inputs, and Drizzle-style `toSQL()` objects while preserving SPI SQL + bound params execution. Runtime global lockdown now strips `Deno`/`fetch` and related web globals from user modules so filesystem/network APIs are not exposed, and runtime interrupts now terminate V8 execution using the stricter of `statement_timeout` and optional `plts.max_runtime_ms` plus pending Postgres cancel/die signals. Ongoing module splitting now includes `crates/plts/src/compiler.rs` for compile/fingerprint/source-map logic, `crates/plts/src/runtime_spi.rs` for SPI/query binding and read-only SQL helpers, `crates/plts/src/function_program.rs` for function source resolution/artifact-pointer cache loading, `crates/stopgap/src/deployment_utils.rs` for deploy scan/materialization helpers, `crates/stopgap/src/security.rs` for role/permission checks, and `crates/stopgap/src/runtime_config.rs` + `crates/stopgap/src/domain.rs`.
- **Module split note:** both extension entrypoints are now thin (`crates/plts/src/lib.rs` and `crates/stopgap/src/lib.rs`), with `plts` split across `api.rs`, `handler.rs`, `runtime.rs`, `compiler.rs`, `runtime_spi.rs`, `function_program.rs`, and `arg_mapping.rs`.
- **Wrapper parity note:** the in-DB `@stopgap/runtime` module source is now loaded from `packages/runtime/src/embedded.ts`, so wrapper validation/metadata behavior stays aligned between package and runtime.
- **Runtime constraints note:** runtime DB bridge calls now enforce deterministic per-call limits for SQL size (`plts.max_sql_bytes`), bound params (`plts.max_params`), and row volume (`plts.max_query_rows`) in addition to timeout and heap caps.
- **Performance note:** iteration 10 benchmark-backed optimizations are now in place for hot execute paths via backend-local non-pointer function program caching and argument-type caching for regular invocation payload mapping.
- **Runtime contract note:** `docs/RUNTIME-CONTRACT.md` is now aligned to current runtime behavior and is guarded by dedicated DB-backed tests in `crates/plts/tests/pg/runtime_contract.rs` plus existing runtime contract suites.
- **CLI note:** `crates/stopgap-cli` now provides `deploy`, `rollback`, `status`, `deployments`, and `diff` commands with `human`/`json` output and explicit CI-friendly non-zero exit codes.
- **Runtime package note:** `packages/runtime` now has a self-test harness (`selftest.mjs`) covering wrapper metadata, validation behavior, and default export API parity, and CI baseline runs package `check` + `test`.
- **CI note:** CI now includes a dedicated `plts runtime v8 (pg16)` job for runtime-heavy `cargo pgrx test -p plts --features "pg16,v8_runtime"` coverage in addition to the baseline pgrx matrix.
- **Cross-extension e2e note:** stopgap rollback pg_regress now covers `deploy -> live execute -> rollback` and verifies both execution continuity and pointer rematerialization after rollback.
- **Security hardening note:** deploy permission checks now explicitly enforce source-schema existence/USAGE, `plts.compile_and_store` EXECUTE access, and stopgap-owned live schema usage; security pg_regress now includes deny/allow scenarios for source-schema and unmanaged-live-schema paths.
- **Docs note:** quickstart, runtime contract, deployment runbook, performance baseline, and troubleshooting guides now live under `docs/`.
- **Biggest missing pieces:** broader runtime module-graph/bundling import compatibility.

---

## 12) Post-Roadmap Follow-up Backlog

- [ ] Expand runtime module-graph/bundling compatibility for arbitrary in-DB imports beyond `data:` URLs and built-in `@stopgap/runtime`.

---

## 13) Next Work Plan (small increments)

This is the active continuation plan for incremental execution. Progress should be recorded by checking off concrete items below.

### 13.1 Execution rules

- [ ] Each change set must complete at least one concrete unchecked item from section 13.2.
- [ ] Keep change scope small (1 primary item + optional 1 follow-up item).
- [ ] If work is partial, explicitly leave sub-bullets unchecked.

Required verification per meaningful item:
- `cargo check`
- `cargo test`
- `cargo pgrx test -p plts`
- `cargo pgrx test -p stopgap`
- `cargo pgrx regress -p stopgap`

### 13.2 Ordered backlog (execute top-down)

#### A. CI runtime lane foundation
- [x] Add explicit CI lane for `plts` runtime-heavy tests with `--features "pg16,v8_runtime"`.
- [x] Ensure lane is visible as a separate job (not hidden in broad matrix noise).
- [x] Record expected runtime of the new lane in CI notes.

Minimum implementation evidence:
- `.github/workflows/ci.yml` changed
- [ ] at least one CI run exercising the new lane

#### B. CI structure and diagnostics hardening
- [x] Split/clarify fast baseline vs heavy pgrx/runtime jobs.
- [x] Add artifact/log upload on failure for runtime/pgrx jobs.
- [x] Confirm failed jobs surface actionable logs for debugging.

Minimum implementation evidence:
- `.github/workflows/ci.yml` changed
- [x] failure-artifact behavior verified via workflow dry-run evidence (failure-only tar bundles from `PGRX_HOME` and `target/debug`, uploaded with `actions/upload-artifact`)

#### C. First true cross-extension e2e test
- [x] Add DB-backed test: `deploy -> live pointer active -> execute -> rollback`.
- [x] Avoid mock-only path for this test.
- [x] Assert deployment pointer and live behavior after rollback.

Minimum implementation evidence:
- [x] updated regression scenario at `crates/stopgap/tests/pg_regress/sql/rollback.sql`
- [x] updated expected output at `crates/stopgap/tests/pg_regress/expected/rollback.out`
- [x] `cargo pgrx regress -p stopgap --resetdb pg17 rollback`

#### D. Wrapper mode e2e enforcement test
- [x] Add e2e test proving `stopgap.query` is read-only (`db.exec` denied).
- [x] Add e2e test proving `stopgap.mutation` remains read-write.
- [x] Assert clear runtime error message for denied write path.

Minimum implementation evidence:
- [x] new tests in `crates/plts/tests/pg/runtime_stopgap_wrappers.rs`
- [x] passing pgrx evidence for both allow/deny paths

#### E. stopgap-cli integration coverage
- [x] Add integration tests for `deploy`, `status`, `rollback`, `deployments`, optional `diff`.
- [x] Validate non-zero exit code behavior on expected failure modes.
- [x] Validate JSON output schema for machine-readable mode.

Minimum implementation evidence:
- [x] new integration suite at `crates/stopgap-cli/tests/command_integration.rs`
- [x] command execution refactor in `crates/stopgap-cli/src/lib.rs` with injectable API boundary

#### F. `packages/runtime` test coverage
- [x] Add tests for wrapper metadata (`query`/`mutation`) and validation behavior.
- [x] Add tests for exported API behavior in `packages/runtime/src/index.ts`.
- [x] Wire package test/check execution into CI.

#### G. Contract drift closure
- [x] Reconcile `docs/RUNTIME-CONTRACT.md` with current runtime output/API shapes.
- [x] Add contract-focused tests guarding documented behavior.
- [x] Add review rule: contract-affecting code changes require doc updates in same PR.

#### H. Security hardening (read-only + privilege checks)
- [x] Strengthen `stopgap.query` read-only enforcement edge-case coverage.
- [x] Add explicit privilege checks for source + live schema handling during deploy.
- [x] Extend pg_regress security cases for deny/allow paths.

#### I. Observability depth
- [x] Add latency/error-class metrics across compile/execute/deploy/rollback/diff.
- [x] Add tests/assertions for metrics shape and increment behavior.
- [x] Document interpretation guidance in ops docs.

#### J. Performance profiling baseline
- [x] Capture baseline profiling for compile and runtime execution hotspots.
- [x] Document measurable bottlenecks and threshold targets.
- [x] Choose 1-2 optimizations with expected impact before implementation.

Minimum implementation evidence:
- [x] baseline harness test at `crates/plts/tests/pg/runtime_performance_baseline.rs`
- [x] baseline notes at `docs/PERFORMANCE-BASELINE.md`
- [x] profiling command executed: `cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot`

#### K. Targeted performance changes
- [x] Implement only benchmark-backed optimizations from iteration 10.
- [x] Validate no regression in runtime safety or contract behavior.
- [ ] Publish before/after benchmark evidence.

#### L. Runtime import/module-graph expansion
- [ ] Expand import compatibility beyond current limited support.
- [ ] Add compatibility matrix tests + negative cases.
- [ ] Update docs with supported/unsupported import rules.
