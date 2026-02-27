# @stopgap/runtime

Runtime wrappers for Stopgap deployment functions.

Exports:

- `query(argsSchema, handler)`
- `mutation(argsSchema, handler)`
- `validateArgs(schema, value)`
- `InferJsonSchema<TSchema>`

The wrapper attaches metadata (`__stopgap_kind`, `__stopgap_args_schema`) and validates `ctx.args` against a JSON Schema subset at runtime.

Current behavior is JSON Schema-subset validation.
Roadmap direction is migrating wrapper validation to zod/mini and re-exporting it as single-letter `v` from `@stopgap/runtime`, while keeping validation/error-shape behavior aligned with current expectations.
