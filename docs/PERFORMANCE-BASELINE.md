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
