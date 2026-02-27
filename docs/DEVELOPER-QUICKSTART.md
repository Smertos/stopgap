# Developer Quickstart

This quickstart gets a local Stopgap + PLTS workspace running for day-to-day development.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- PostgreSQL dev headers and a local server compatible with `cargo pgrx`
- `cargo-pgrx` installed (`cargo install cargo-pgrx`)

## One-time setup

1. Initialize pgrx:

```bash
cargo pgrx init
```

2. Build workspace dependencies:

```bash
cargo check
```

## Common development commands

Run these from the repository root:

```bash
cargo check
cargo test
cargo pgrx test -p plts
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
- Runtime-heavy CI lane runs `cargo pgrx test -p plts --features "pg16,v8_runtime"` as a dedicated `plts runtime v8 (pg16)` job.
- Expected runtime for the dedicated runtime lane is typically about 10-15 minutes on GitHub-hosted runners.
