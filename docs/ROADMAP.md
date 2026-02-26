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
- [ ] Implement full TS/JS module execution inside V8 isolate (module loader/imports)
- [x] Resolve and invoke default export entrypoint (sync handlers)
- [x] Build initial runtime call contract: `ctx.db`, `ctx.args`, `ctx.fn`, `ctx.now`
- [x] Normalize JS return values to SQL/jsonb semantics:
  - [x] `undefined` -> SQL `NULL`
  - [x] `null` -> SQL `NULL`
  - [x] primitives/object/array -> `jsonb`
- [x] Add robust runtime error propagation (message + stack + SQL context)
- [x] Include SQL function identity (`schema.name`, oid) in runtime error messages

### 2.5 Compiler Work (unfinished)

- [x] Replace placeholder `compile_ts` with real TS->JS transpilation
- [x] Return diagnostics payload with line/column and severity
- [x] Persist compiler metadata/fingerprint from real toolchain versions
- [x] Optionally persist source maps in `plts.artifact`

### 2.6 DB API Surface (unfinished)

- [x] Expose RW db API (`query`/`exec`) from runtime context
- [x] Add structured parameter binding from JS to SPI
- [x] Add transaction semantics docs for runtime calls
- [ ] Defer RO gate to P1 (tracked below)

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
- [ ] Add prune mode (`stopgap.prune`) to drop stale functions safely
- [ ] Add dependency-aware prune strategy
- [ ] Enforce ownership/privilege model for live schema writes:
  - [ ] owner role (`stopgap_owner`)
  - [ ] deployer role (`stopgap_deployer`)
  - [ ] app runtime role (`app_user`)
  - [ ] security definer deploy functions
  - [ ] explicit revoke/create grants

---

## 5) Wrapper Model + Runtime UX (`@stopgap/runtime`) (P1)

- [ ] Create package scaffold for `@stopgap/runtime`
- [ ] Implement `stopgap.query(schema, handler)` wrapper
- [ ] Implement `stopgap.mutation(schema, handler)` wrapper
- [ ] Expose runtime metadata for handler kind
- [ ] Choose/implement schema strategy (JSON Schema target)
- [ ] Runtime arg validation against schema
- [ ] TS type inference helpers for args/results

---

## 6) Operational Hardening (P2)

- [ ] Integrate Postgres cancellation and statement timeout with JS interrupts
- [ ] Add per-call memory limits
- [ ] Add deterministic runtime resource constraints
- [ ] Ensure no filesystem/network runtime surface
- [ ] Add metrics and logs for compile/execute/deploy flows
- [ ] Add tunable GUCs for runtime caps and log level

---

## 7) Testing Roadmap

### 7.1 Current

- [x] Unit tests for deterministic hash behavior (`plts`)
- [x] Unit tests for stable deploy lock hash (`stopgap`)

### 7.2 Next

- [ ] Re-introduce DB-backed pgrx integration tests for `plts` SQL APIs
- [ ] Add tests for regular args conversion (`text`, `int4`, `bool`, `jsonb`)
- [ ] Add tests for null normalization behavior in runtime
- [ ] Add tests for artifact pointer execution path
- [ ] Add stopgap deploy integration test that validates:
  - [ ] active deployment pointer changes
  - [ ] live schema pointer body payload
  - [ ] `fn_version` integrity
  - [ ] rejection of overloaded functions
- [ ] Add rollback integration tests

### 7.3 Regressions / SQL snapshots

- [x] Base pg_regress setup file exists
- [x] Added deploy regression SQL/output scaffolding
- [ ] Wire pg_regress execution into automated test flow
- [ ] Keep expected files updated on SQL output/entity changes

---

## 8) CLI Roadmap (`stopgap-cli`)

- [ ] Scaffold CLI project
- [ ] Implement `deploy`
- [ ] Implement `rollback`
- [ ] Implement `status`
- [ ] Implement `deployments`
- [ ] Optional: implement `diff`
- [ ] Add human-readable + JSON output modes
- [ ] Add CI/CD-oriented exit codes and failure diagnostics

---

## 9) Documentation Roadmap

- [x] Keep project outline as source-of-truth architecture doc
- [x] Add repo-level agent guidance (`AGENTS.md`)
- [x] Add this roadmap with progress tracking
- [ ] Add developer quickstart (local pgrx setup + commands)
- [ ] Add runtime contract reference (`ctx` shape and return semantics)
- [ ] Add deployment lifecycle and operational runbook
- [ ] Add troubleshooting guide (common pgrx/test/install issues)

---

## 10) Suggested Execution Order from Here

1. [ ] Implement real `deno_core` runtime execution in `plts_call_handler`
2. [x] Implement real TS transpilation for `plts.compile_ts`
3. [ ] Add integration tests for runtime execution + null normalization
4. [ ] Harden `stopgap.deploy` error handling and state transitions
5. [x] Implement rollback/status APIs
6. [ ] Add permissions model for live schema and deploy APIs
7. [ ] Introduce wrapper package (`@stopgap/runtime`) and schema validation

---

## 11) Current Snapshot

- **P0 status:** Partially complete.
- **What works now:** workspace + extension scaffolds, artifact catalog/APIs, minimal deploy flow, rollback/status/deployments/diff APIs, activation/environment introspection views, live pointer materialization, overload rejection, baseline tests, and feature-gated sync default-export JS execution in `plts`.
- **Biggest missing piece:** full module runtime support (imports/async) in `plts` plus P1 read-only/wrapper features.
