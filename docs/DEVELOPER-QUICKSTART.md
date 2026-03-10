# Developer Quickstart

This quickstart gets a local Stopgap + PLTS workspace running for day-to-day development.

Status note (Mar 2026): user-facing product workflow is being course-corrected to a Convex-style TypeScript-first model (`./stopgap` directory with path-based invocation), while this document remains focused on extension/CLI development in this repository.

Compiler backend note: `plts` typecheck and default transpile now run through the embedded TSGo WASM backend. Rust builds still depend on runtime package build tooling because `crates/plts/build.rs` refreshes the embedded `@stopgap/runtime` artifact from `packages/runtime`.
PATH note: the build script invokes `pnpm` non-interactively, so `pnpm` must be available on `PATH` for non-interactive/build-tool shells as well as your normal interactive terminal.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- Node.js 22+
- pnpm 10+
- PostgreSQL dev headers and a local server compatible with `cargo pgrx`
- `cargo-pgrx` installed (`cargo install cargo-pgrx`)

## One-time setup

1. Initialize pinned third-party sources:

```bash
git submodule update --init --recursive
```

2. Initialize pgrx:

```bash
cargo pgrx init
```

3. Build workspace dependencies:

```bash
pnpm --dir packages/runtime install --frozen-lockfile
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

For local troubleshooting of the runtime-heavy lane on a busy machine, prefer serialized Rust test execution:

```bash
RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"
```

## CLI development

`stopgap-cli` is a Rust binary crate at `crates/stopgap-cli`.

```bash
cargo run -p stopgap-cli --bin stopgap -- --help
```

The CLI requires a database connection string via `--db` or `STOPGAP_DB`.

## App workflow target (Convex-style)

For stopgap-cli users (application repos):

1. Initialize project-local scaffolding:

```bash
stopgap init
```

2. Add `*.ts` function modules with named wrapper exports:
   - `export const myFn = query(argsSchema, handler)`
   - `export const myMutation = mutation(argsSchema, handler)`
3. Deploy from project root:

```bash
stopgap --db "$STOPGAP_DB" deploy --env prod --from-schema app --label <release>
```

4. Invoke from SQL by function path:

```sql
SELECT stopgap.call_fn('api.coolApi.myFn', '{"id":1}'::jsonb);
```

If `./stopgap` is missing, deploy should fail with a clear not-initialized message.

## CI lanes

- Baseline CI lane runs workspace `cargo check`/`cargo test` plus matrixed `cargo pgrx test` jobs for `plts` and `stopgap` (and `cargo pgrx regress -p stopgap` on the stopgap matrix leg).
- Runtime-heavy CI lane runs `cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"` as a dedicated `plts runtime v8 (pg17)` job.
- A final `release gates (runtime + regress)` job depends on baseline, pgrx matrix, and runtime-heavy lanes so branch protection can require a single aggregated gate.
- This V8 lane is an evergreen quality gate: failures are fixed at the root cause and not bypassed.
- Expected runtime for the dedicated runtime lane is typically about 10-15 minutes on GitHub-hosted runners.
