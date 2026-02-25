# AGENTS.md

This file captures how to work effectively in this repository.

## Repository shape

- Workspace root: `Cargo.toml`
- Crates:
  - `crates/plts`: language/runtime extension (`LANGUAGE plts`, artifact APIs)
  - `crates/stopgap`: deployment/environment extension
- Docs:
  - `docs/PROJECT-OUTLINE.md`: product/architecture source of truth

## Tooling baseline

- Rust toolchain is pinned in `rust-toolchain.toml`.
- Formatting/lint config is tracked in `rustfmt.toml` and `clippy.toml`.
- CI workflow lives at `.github/workflows/ci.yml` and runs workspace check/test plus per-crate `cargo pgrx test` matrix jobs.

## Current architecture assumptions (locked)

- Engine target for P0+: **V8 via `deno_core`**.
- JS `null` and `undefined` should normalize to SQL `NULL` (long-term behavior target).
- Stopgap deploy compile path is DB-side (`plts.compile_ts` / `plts.compile_and_store`).
- Stopgap-managed deployables are `(args jsonb) returns jsonb language plts`.
- Stopgap-managed overloading is forbidden.
- Regular `plts` calling convention should expose both positional and named/object argument forms.
- Default export is the entrypoint convention.
- Read-only DB API enforcement is deferred (RW-only allowed in current P0 state).

## Implementation guardrails

- Keep the split-extension model:
  - `plts` owns language/runtime and artifact substrate.
  - `stopgap` owns deployments, activation, live materialization.
- Avoid introducing Stopgap-specific runtime semantics into `plts` beyond pointer execution compatibility.
- Keep SQL behavior deterministic and explicit; avoid hidden mutable global state.
- Prefer additive migrations and backward-compatible SQL API changes.

## Working conventions for contributors/agents

- Prefer small, verifiable increments over broad rewrites.
- When editing Rust + SQL entity graph code:
  - Use `#[pg_schema]` modules for schema-scoped SQL functions.
  - Keep `extension_sql!` focused on bootstrap/catalog DDL.
- For SPI use:
  - Prefer safe argumentized calls where practical.
  - If interpolating SQL strings, quote literals/idents robustly and keep inputs constrained.
- Keep runtime safety defaults conservative (timeouts, memory, no FS/network once runtime lands).

## Validation checklist for each meaningful change

- `cargo check`
- `cargo test`
- `cargo pgrx test -p plts`
- `cargo pgrx test -p stopgap`

If SQL outputs or extension entities change, also run/update pg_regress artifacts where relevant.

## Near-term technical debt to remember

- `plts` runtime handler executes sync default-export JS only when built with `v8_runtime`; full module/import support and async execution are still pending.
- `plts.compile_ts` is still a placeholder.
- Stopgap function kind (`query` vs `mutation`) is currently convention-based, not wrapper-enforced.
- Stopgap deploy now enforces deployment status transitions and writes richer manifest metadata, but rollback/status APIs are still pending.
- Some deploy SQL paths use interpolated SQL and should migrate to stricter SPI argumentization over time.

## Do not do without explicit direction

- Do not add forceful/destructive git operations.
- Do not change locked P0 decisions above unless requested.
- Do not introduce network/FS access into the runtime surface.
