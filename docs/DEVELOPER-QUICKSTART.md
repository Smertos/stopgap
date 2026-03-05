# Developer Quickstart

This quickstart gets a local Stopgap + PLTS workspace running for day-to-day development.

Status note (Mar 2026): user-facing product workflow is being course-corrected to a Convex-style TypeScript-first model (`./stopgap` directory with path-based invocation), while this document remains focused on extension/CLI development in this repository.

Compiler backend migration note: roadmap work is planned to move `plts` typecheck/transpile to an in-process TSGo WASM backend. Until that lands, the current checker pipeline still depends on existing runtime package build tooling.

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
