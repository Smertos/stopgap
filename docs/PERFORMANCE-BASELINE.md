# Performance Baseline (compile + execute)

This note captures the current profiling baseline for the top unfinished roadmap item (13.2.J).

## Baseline harness

- Profiling test: `crates/plts/tests/pg/runtime_performance_baseline.rs`
- Command:

```bash
cargo pgrx test -p plts pg17 test_runtime_performance_baseline_snapshot
```

The test captures wall-clock timings around two hotspot loops:

1. compile loop: 25 calls to `plts.compile_and_store(...)`
2. execute loop: 100 calls to a stopgap-signature `LANGUAGE plts` function invocation path

## Bottlenecks and threshold targets

Based on current behavior and code-path review, the highest-cost paths are:

- compile path: TS parse/transpile (`deno_ast`) + artifact hashing + artifact row write
- execute path: call handler argument mapping + function source loading/dispatch

Tracking targets for this baseline:

- compile throughput target: keep average compile latency under `10ms/call` in local dev baseline runs
- execute throughput target: keep average execute latency under `2ms/call` for passthrough stopgap-signature handlers

These are pragmatic guardrails for trend tracking, not hard CI gating thresholds.

## Optimization candidates selected for next step

1. Cache non-pointer function source metadata per backend process to reduce repeated catalog reads/parsing on hot execute paths.
2. Reduce allocation pressure in argument mapping and payload construction for regular invocations.

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
