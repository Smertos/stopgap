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

## CLI failures

- Ensure `--db` or `STOPGAP_DB` is provided.
- Use `--output json` in CI to capture structured failure context.
- Exit codes:
  - `10`: database connection error
  - `11`: SQL command execution error
  - `12`: invalid/undecodable database response
  - `13`: output serialization error
