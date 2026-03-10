# Performance Baseline (compile + execute + readiness)

This note captures the current profiling baseline for the top unfinished roadmap item (13.2.J).

## Baseline harness

- Profiling test: `crates/plts/tests/pg/runtime_performance_baseline.rs`
- Readiness test: `crates/plts/tests/pg/runtime_readiness_baseline.rs`
- Command:

```bash
cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot
```

Readiness-focused command:

```bash
cargo pgrx test -p plts pg17 test_runtime_readiness_baseline_snapshot --no-default-features --features "pg17,v8_runtime"
```

The performance baseline captures wall-clock timings around two hotspot loops:

1. compile loop: 25 calls to `plts.compile_and_store(...)`
2. execute loop: 1,000 calls to a stopgap-signature `LANGUAGE plts` function invocation path

The readiness baseline captures warm-shell behavior inside one backend by measuring:

1. first cold invocation after shell creation
2. same-function warm calls
3. different-function warm calls on the same pooled shell
4. `runtime.readiness.setup_realm_last_us` after each warm call
5. phase-attributed warm-path timings from `runtime.readiness.phases.*`:
   - `context_setup_*`
   - `module_load_*`
   - `module_evaluate_*`
   - `cleanup_*`
6. import-heavy warm reuse through both `data:` and `plts+artifact:` module graphs

## Bottlenecks and threshold targets

Based on current behavior and code-path review, the highest-cost paths are:

- compile path (current): embedded TSGo WASM transpile/init + artifact hashing + artifact row write
- execute path: call handler argument mapping + function source loading/dispatch
- readiness path: warm-shell checkout/setup and per-invocation module load/evaluate work
- readiness attribution path: separate context setup, module load/evaluate, and cleanup timings on the warm path

Tracking targets for this baseline:

- compile throughput target: keep average compile latency under `60ms/call`
- execute cold-path target: keep first execute-loop average under `5ms/call`
- execute warm-path target: keep second execute-loop average under `4ms/call`
- warm-regression target: keep warm average under `3.0x` of cold average while deeper warm-path module-load reuse remains pending
- readiness target: keep warm setup median under `5ms`

These thresholds are now enforced directly in:

- `test_runtime_performance_baseline_snapshot`
- `test_runtime_readiness_baseline_snapshot`
- `test_runtime_readiness_import_paths_are_observable`

## Optimization candidates selected for the next measured step

1. Reuse pre-bootstrapped V8 runtime shells per backend so warm calls avoid fresh `JsRuntime` construction.
2. Keep invocation isolation through per-call context wiring, per-invocation module identity versioning, and shell reset/retire policy.
3. Re-evaluate deeper module-graph or bytecode caching only after the new phase-attribution data justifies it.

These candidates are intentionally selected before any optimization implementation so changes can stay benchmark-backed.

## Iteration 10 implementation status

- Implemented in `crates/plts/src/function_program.rs`:
  - backend-local cache for non-pointer `FunctionProgram` metadata (`oid`, schema/name, source)
  - existing artifact-pointer source cache remains unchanged and is still used for pointer stubs
- Implemented in `crates/plts/src/arg_mapping.rs`:
  - backend-local cache for function argument type OIDs to avoid repeated `pg_proc` SPI lookups
  - lower-allocation payload/value construction for regular invocation args (`positional` + `named`)
- Validation commands executed after optimization:
  - `cargo check`
  - `cargo test`
  - `cargo pgrx test -p plts`
  - `cargo pgrx test -p stopgap`
  - `cargo pgrx regress -p stopgap`
  - `cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot`

## Iteration 11 before/after benchmark evidence

Command used for both runs (warm build cache in each workspace):

```bash
TIMEFMT='BENCHMARK_WALL_SECONDS=%E'; time cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot
```

Benchmark snapshots:

- before (pre-optimization commit `4f5f3f4`): `BENCHMARK_WALL_SECONDS=5.40s`
- after (optimized commit `158139d`): `BENCHMARK_WALL_SECONDS=5.78s`
- observed delta: `+0.38s` (`+7.0%` in this run)

Interpretation notes:

- This command-level wall clock includes extension rebuild/install and test harness startup overhead in addition to runtime execute-loop work.
- The before/after publication requirement is now satisfied with measured evidence, but signal quality is still noisy at this granularity.
- Follow-up profiling should capture loop-level timings directly (compile loop and execute loop separately) so optimization impact is easier to isolate from harness overhead.

## Iteration 12 cache policy hardening + benchmark snapshot

- Implemented in `crates/plts/src/function_program.rs`:
  - function-program cache now uses explicit keying by `fn_oid` with LRU eviction
  - cache entries now expire via TTL invalidation (`30s`) to avoid indefinite staleness
  - cache now enforces source-size memory bounds (`4 MiB`) in addition to entry-count bounds (`256`)
  - added unit coverage for byte-budget eviction and TTL expiration behavior
- Validation commands executed after this change:
  - `cargo check`
  - `cargo test`
  - `cargo pgrx test -p plts`
  - `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"`
  - `cargo pgrx test -p stopgap`
  - `cargo pgrx regress -p stopgap`

Benchmark snapshot command:

```bash
TIMEFMT='BENCHMARK_WALL_SECONDS=%E'; time cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot
```

- after (iteration 12 cache policy hardening): `BENCHMARK_WALL_SECONDS=6.18s`
- note: this is a command-level wall-clock measurement and includes extension build/install overhead in addition to execute-loop runtime work.

## Iteration 17 phase-3 SLO enforcement update

- Updated `crates/plts/tests/pg/runtime_performance_baseline.rs` to:
  - measure compile loop plus both cold and warm execute loops separately;
  - enforce SLO thresholds for compile/cold/warm per-call latencies;
  - enforce an explicit warm-vs-cold regression delta (`warm <= 1.2x cold`).
- This converts phase-3 performance guardrails from documentation-only targets into executable test assertions.

## Iteration 23 warm-readiness implementation update

- `plts` now executes V8 calls through backend-local pooled runtime shells instead of constructing a fresh `JsRuntime` per invocation.
- Pool defaults are configurable through:
  - `plts.isolate_reuse`
  - `plts.isolate_pool_size`
  - `plts.isolate_max_age_s`
  - `plts.isolate_max_invocations`
- `plts.metrics()` now exposes `runtime.readiness.*` counters/timers for checkout behavior, warm reuse, shell creation, and retire reasons.
- Warm-call isolation is maintained by:
  - rebuilding invocation-local `ctx` and DB mode state every call
  - versioning direct `data:` and `plts+artifact:` imports per invocation
  - resetting non-baseline globals before returning a shell to the pool
  - retiring shells on timeout, cancel, heap pressure, cleanup failure, setup failure, or config drift
- Dedicated readiness coverage now lives in `crates/plts/tests/pg/runtime_readiness_baseline.rs`, which asserts sub-`5ms` warm setup medians for both same-function and different-function reuse paths.

## Iteration 24 warm-path attribution and validation hardening update

- `plts.metrics()` now also exposes `runtime.readiness.phases.*` for:
  - context setup
  - module load
  - module evaluate
  - cleanup/reset
- `crates/plts/src/runtime.rs` records those phase timings directly in the live pooled-shell execution path.
- `crates/plts/tests/pg/runtime_readiness_baseline.rs` now prints phase medians for:
  - same-function warm reuse
  - cross-function warm reuse
  - import-heavy warm reuse through `data:` and `plts+artifact:` imports
- `crates/plts/tests/pg/runtime_performance_baseline.rs` now also captures a cross-function warm execute loop and prints the latest phase timings alongside the compile/cold/warm totals.
- Local runtime-heavy verification was re-run successfully with:
  - `RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"`

Interpretation:

- The next runtime optimization should be chosen from measured phase data, not guessed.
- If further work is justified, the first candidate remains per-shell module-graph reuse for stable `data:` / `plts+artifact:` imports, followed by same-function compiled-module reuse.

## Iteration 25 runtime decision gate

Decision-gate commands:

```bash
RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime" test_runtime_readiness_baseline_snapshot
RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime" test_runtime_readiness_import_paths_are_observable
RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime" test_runtime_performance_baseline_snapshot
```

Captured warm-path values (`us`):

- `same_fn`: invoke avg `871`, context setup median `50`, module load median `135`, module evaluate median `64`, cleanup median `93`
- `cross_fn`: invoke avg `6377`, context setup median `54`, module load median `153`, module evaluate median `79`, cleanup median `109`
- `same_import`: invoke avg `1298`, context setup median `78`, module load median `389`, module evaluate median `90`, cleanup median `123`
- `cross_import`: invoke avg `7188`, context setup median `73`, module load median `444`, module evaluate median `108`, cleanup median `142`

Cold-path reference:

- cold invoke `14632us`

Gate rule outcome:

- No warm scenario met the required `module_load + module_evaluate > 60% of warm invoke average` threshold.
- Only `cross_import` exceeded the `500us` combined-load/evaluate floor, but it still fell far short of the 60% threshold.
- Result: deeper runtime reuse is **not** justified yet.

Branch decision:

- Leave the pooled-shell runtime unchanged for now.
- Shift the next milestone to stopgap deploy/security reconciliation and compatibility-wrapper messaging instead of adding shell-local module-graph reuse.

## TSGo embedded Wasmtime cold-start cache layers

- `plts` now keeps the embedded `stopgap-tsgo-api.wasm` Wasmtime module behind three init layers in `crates/plts/src/compiler.rs`:
  - backend-local `OnceLock` for repeated calls inside one backend;
  - Wasmtime filesystem cache rooted under a `plts`-owned cache directory when `plts.tsgo_wasm_cache_mode=auto`;
  - manual serialized-module reuse under `<cache_root>/manual/<fingerprint>.cwasm` when built-in cache setup is skipped or fails.
- Default cache root resolution:
  - `directories_next::ProjectDirs("", "Stopgap", "plts").cache_dir()/tsgo-wasm`
  - fallback: `std::env::temp_dir()/stopgap/plts/tsgo-wasm`
- Manual cache artifacts are content-addressed across:
  - embedded wasm SHA-256
  - Wasmtime crate version
  - `OptLevel::None`
  - `RegallocAlgorithm::SinglePass`
  - `parallel_compilation=true`
  - compile-target identity (`arch`, `os`, `env`)
- Invalid manual artifacts are quarantined under `<cache_root>/quarantine/` and runtime init falls back to direct compile instead of surfacing cache corruption to `plts.typecheck_ts` / TSGo transpile callers.
- `plts.metrics()` now includes `tsgo_wasm.init` latency/call counters plus `tsgo_wasm.cache` counters so cold-start behavior can be inspected from SQL without extra profiling tooling.
