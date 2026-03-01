# Developer Quickstart

This quickstart gets a local Stopgap + PLTS workspace running for day-to-day development.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- Node.js 22+ with npm
- PostgreSQL dev headers and a local server compatible with `cargo pgrx`
- `cargo-pgrx` installed (`cargo install cargo-pgrx`)

## One-time setup

1. Initialize pgrx:

```bash
cargo pgrx init
```

2. Build workspace dependencies:

```bash
npm install --prefix packages/runtime --no-package-lock
cargo check
```

`plts` now refreshes the embedded runtime artifact during Rust builds via `crates/plts/build.rs`, so installing runtime package dependencies is required before running Cargo commands.

## Common development commands

Run these from the repository root:

```bash
cargo check
cargo test
cargo pgrx test -p plts
cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"
cargo pgrx test -p stopgap
cargo pgrx regress -p stopgap
```

## CLI development

`stopgap-cli` is a Rust binary crate at `crates/stopgap-cli`.

```bash
cargo run -p stopgap-cli -- --help
```

The CLI requires a database connection string via `--db` or `STOPGAP_DB`.

## CI lanes

- Baseline CI lane runs workspace `cargo check`/`cargo test` plus matrixed `cargo pgrx test` jobs for `plts` and `stopgap` (and `cargo pgrx regress -p stopgap` on the stopgap matrix leg).
- Runtime-heavy CI lane runs `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"` as a dedicated `plts runtime v8 (pg17)` job.
- This V8 lane is an evergreen quality gate: failures are fixed at the root cause and not bypassed.
- Expected runtime for the dedicated runtime lane is typically about 10-15 minutes on GitHub-hosted runners.
