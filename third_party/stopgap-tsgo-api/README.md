# stopgap-tsgo-api

This package is the Stopgap-owned TSGo WASI adapter tracked in `docs/ROADMAP.md` section 14.7.

Current scope:

- defines a narrow JSON API shape for `typecheck` and `transpile` operations
- provides a small CLI (`cmd/stopgap-tsgo-api`) that reads a JSON request from stdin and writes a JSON response to stdout
- routes `transpile` through real `typescript-go` emit for single-file TS->JS output
- preserves the current explicit unsupported-import diagnostic behavior for `@app/*` imports used by `plts` semantic checks
- ships a built WASI artifact at `dist/stopgap-tsgo-api.wasm` for embedding in `plts`

Still pending:

- replacement of scaffold-style semantic checker logic with a real TSGo semantic program/host pipeline
- full `@stopgap/runtime` virtual declaration environment for compiler-native semantic parity

Module-path note:

- this adapter intentionally uses the module path `github.com/microsoft/typescript-go/stopgap-tsgo-api`
- `go.mod` points `github.com/microsoft/typescript-go` at the local `../typescript-go` checkout so the adapter can legally import TSGo `internal/*` packages without modifying the vendored submodule

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
