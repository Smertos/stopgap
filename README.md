# Stopgap + PLTS

Stopgap is a split-extension Postgres deployment workflow for TypeScript/JavaScript functions.

- `plts` provides the `LANGUAGE plts` runtime and artifact APIs.
- `stopgap` provides deployment, activation, rollback, and live-schema materialization.

## Quickstart

Prerequisites:
- Rust toolchain from `rust-toolchain.toml`
- `cargo-pgrx` and a local PostgreSQL installation compatible with pgrx

One-time setup:

```bash
cargo pgrx init
```

Core validation commands:

```bash
cargo check
cargo test
cargo pgrx test -p plts
cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"
cargo pgrx test -p stopgap
cargo pgrx regress -p stopgap
```

## Repository layout

- `crates/plts`: language/runtime extension
- `crates/stopgap`: deployment/environment extension
- `crates/stopgap-cli`: deploy/status/rollback CLI
- `crates/common`: shared Rust helpers
- `packages/runtime`: `@stopgap/runtime` wrapper package
- `docs/`: architecture, runtime contract, runbook, quickstart, and roadmap

## Architecture docs

- `docs/PROJECT-OUTLINE.md` (full architecture and design decisions)
- `docs/DEVELOPER-QUICKSTART.md` (local setup and CI-parity commands)
- `docs/RUNTIME-CONTRACT.md` (runtime `ctx` and return semantics)
- `docs/ROADMAP.md` (active priorities and execution order)
