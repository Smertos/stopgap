# stopgap-tsgo-api (scaffold)

This package is the first increment of the TSGo migration bridge tracked in `docs/ROADMAP.md` section 14.7.

Current scope:

- defines a narrow JSON API shape for `typecheck` and `transpile` operations
- provides a small CLI (`cmd/stopgap-tsgo-api`) that reads a JSON request from stdin and writes a JSON response to stdout
- preserves the explicit unsupported-import diagnostic behavior for `@app/*` imports used by `plts` semantic checks

Not yet implemented:

- direct integration with `typescript-go`
- WASM build/embed wiring into `crates/plts`
- replacement of subprocess-based checks in `plts`

## Commands

```bash
go run ./cmd/stopgap-tsgo-api typecheck <<'JSON'
{"source_ts":"import { x } from '@app/math'"}
JSON
```

```bash
go run ./cmd/stopgap-tsgo-api transpile <<'JSON'
{"source_ts":"export const value: number = 1"}
JSON
```
