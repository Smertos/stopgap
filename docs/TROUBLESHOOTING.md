# Troubleshooting

## `cargo pgrx init` fails

- Verify PostgreSQL dev packages are installed for your target server version.
- Re-run `cargo pgrx init` after installing missing packages.
- If a prior init is stale, remove broken config under `~/.pgrx/` and initialize again.

## `cargo pgrx test` cannot connect/start server

- Confirm local Postgres binaries are available to pgrx.
- Check for occupied ports from previous test runs.
- Re-run with verbose output to inspect startup errors:

```bash
cargo pgrx test -p stopgap -v
```

## Extension install/load errors in tests

- Ensure both extensions compile before running integration tests:

```bash
cargo check
```

- If SQL entities changed, rerun relevant `cargo pgrx test` and `cargo pgrx regress -p stopgap` suites.

## Runtime execution terminated unexpectedly

- Check configured caps:
  - `statement_timeout`
  - `plts.max_runtime_ms`
  - `plts.max_heap_mb`
  - `plts.max_sql_bytes`
  - `plts.max_params`
  - `plts.max_query_rows`
- Inspect runtime/deploy metrics:
  - `select plts.metrics();`
  - `select stopgap.metrics();`

## `LANGUAGE plts` create/replace fails with TypeScript diagnostics

- The validator enforces semantic TypeScript checks; fix reported diagnostics first.
- If errors mention checker/transpiler execution, confirm runtime package dependencies are installed (`pnpm --dir packages/runtime install --frozen-lockfile`) and retry.
- Use `SELECT plts.typecheck_ts($$...$$);` to inspect diagnostics directly before deploy.
- Typecheck/transpile internals already run through embedded TSGo WASM; failures here usually mean bad source input, TSGo runtime/cache issues, or local build-tooling setup problems.

## `pnpm` is installed, but Cargo/build.rs still says it is missing

- `crates/plts/build.rs` invokes `pnpm` non-interactively while refreshing the embedded `@stopgap/runtime` artifact.
- Make sure `pnpm` is on `PATH` for non-interactive shells and build tools, not only in interactive shell startup files.
- A reliable local check is:

```bash
zsh -lc 'command -v pnpm'
```

- If that prints nothing, export your Node toolchain path from a startup file read by non-interactive/login shells as well, or prefix commands explicitly:

```bash
PATH="$HOME/.n/bin:$PATH" cargo check
```

## Runtime-heavy V8 lane is flaky or slow locally

- The release/CI gate remains:

```bash
cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"
```

- For local troubleshooting on shared machines, serialize Rust test execution to reduce `pgrx` cluster contention:

```bash
RUST_TEST_THREADS=1 cargo pgrx test pg17 -p plts --no-default-features --features "pg17,v8_runtime"
```

- Use the serialized form when multiple cold-start pg test backends or repeated install/rebuild cycles are saturating the local `pgrx` test cluster.

## TSGo Wasmtime cache bootstrap warnings or permission errors

- Inspect `SELECT plts.metrics();` and check `tsgo_wasm.cache.config_errors` / `tsgo_wasm.cache.deserialize_errors`.
- If you need to override the cache root, set:

```sql
SELECT set_config('plts.tsgo_wasm_cache_dir', '/absolute/path/for/plts-tsgo-cache', false);
```

- To bypass persistent cache while debugging, set:

```sql
SELECT set_config('plts.tsgo_wasm_cache_mode', 'off', false);
```

- Available modes:
  - `auto`: built-in Wasmtime cache, then manual serialized cache, then direct compile
  - `manual-only`: skip built-in cache and use manual serialized cache first
  - `off`: disable persistent cache
- Ensure the PostgreSQL server user can create and rename files under the selected cache root. When auto resolution is used, `plts` first tries the user cache dir from `directories_next`, then falls back to `std::env::temp_dir()/stopgap/plts/tsgo-wasm`.

## Clear the TSGo Wasmtime cache

- Remove the resolved cache root on disk and rerun the TSGo path you want to profile or validate.
- Layout under the cache root:
  - `wasmtime-config.toml`
  - `wasmtime-cache/`
  - `manual/`
  - `quarantine/`
- Clearing those directories only affects TSGo Wasmtime cold-start reuse; it does not remove `plts.artifact` rows.

## CLI failures

- Ensure `--db` or `STOPGAP_DB` is provided.
- Ensure project-local `./stopgap` directory exists when running `stopgap deploy` from an app repo.
- If deploy reports "Stopgap not initialized" or "stopgap dir not found", create `./stopgap` and add `*.ts` modules.
- If `stopgap.call_fn(path, args)` fails with unknown function path, verify path format `api.<module_path>.<export_name>` and confirm the target export exists in deployed `stopgap/**/*.ts`.
- Use `--output json` in CI to capture structured failure context.
- Exit codes:
  - `10`: database connection error
  - `11`: SQL command execution error
  - `12`: invalid/undecodable database response
  - `13`: output serialization error
