# @stopgap/runtime

Runtime wrappers for Stopgap deployment functions.

Exports:

- `v` schema helpers (`v.object`, `v.int`, `v.string`, `v.enum`, `v.union`, ...)
- `query(argsSchema, handler)`
- `mutation(argsSchema, handler)`
- `validateArgs(schema, value)`
- `InferArgsSchema<TSchema>`

The wrapper attaches metadata (`__stopgap_kind`, `__stopgap_args_schema`) and validates `ctx.args` against `v` schemas at runtime. Legacy JSON Schema subset inputs still work for compatibility.

Current behavior uses `v` schema helpers (zod/mini-style API) and keeps legacy JSON Schema subset validation behavior available for compatibility.
