import runtime, { mutation, query, validateArgs } from "./dist/index.js";

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const assertEqual = (actual, expected, message) => {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`);
  }
};

const assertDeepEqual = (actual, expected, message) => {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) {
    throw new Error(`${message}: expected ${right}, got ${left}`);
  }
};

const expectThrows = async (fn, includes) => {
  try {
    await fn();
  } catch (error) {
    const text = error instanceof Error ? error.message : String(error);
    if (!text.includes(includes)) {
      throw new Error(`error message mismatch: expected to include "${includes}", got "${text}"`);
    }
    return;
  }
  throw new Error(`expected throw containing "${includes}"`);
};

const makeCtx = (args, mode) => ({
  args,
  db: {
    mode,
    query: async () => [],
    exec: async () => ({ ok: true }),
  },
  fn: { oid: 1, schema: "public", name: "wrapped" },
  now: new Date().toISOString(),
});

const run = async () => {
  const argsSchema = {
    type: "object",
    required: ["id"],
    additionalProperties: false,
    properties: {
      id: { type: "integer" },
    },
  };

  const wrappedQuery = query(argsSchema, async (args, ctx) => ({
    kind: "query",
    id: args.id,
    dbMode: ctx.db.mode,
  }));

  assertEqual(wrappedQuery.__stopgap_kind, "query", "query wrapper metadata kind");
  assertDeepEqual(wrappedQuery.__stopgap_args_schema, argsSchema, "query wrapper metadata schema");

  const queryResult = await wrappedQuery(makeCtx({ id: 42 }, "ro"));
  assertDeepEqual(
    queryResult,
    { kind: "query", id: 42, dbMode: "ro" },
    "query wrapper executes handler with validated args"
  );

  await expectThrows(async () => {
    await wrappedQuery(makeCtx({}, "ro"));
  }, "missing required property");

  const wrappedMutation = mutation(argsSchema, async (args, ctx) => ({
    kind: "mutation",
    id: args.id,
    dbMode: ctx.db.mode,
  }));

  assertEqual(wrappedMutation.__stopgap_kind, "mutation", "mutation wrapper metadata kind");
  const mutationResult = await wrappedMutation(makeCtx({ id: 7 }, "rw"));
  assertDeepEqual(
    mutationResult,
    { kind: "mutation", id: 7, dbMode: "rw" },
    "mutation wrapper executes handler in rw mode"
  );

  const schemaLessQuery = query(async (args) => args);
  assertEqual(schemaLessQuery.__stopgap_args_schema, null, "schema-less query defaults schema to null");
  const passthrough = await schemaLessQuery(makeCtx({ ok: true }, "ro"));
  assertDeepEqual(passthrough, { ok: true }, "schema-less query passes args through");

  validateArgs({ enum: ["a", "b"] }, "a");
  validateArgs({ anyOf: [{ type: "integer" }, { type: "string" }] }, 10);

  await expectThrows(async () => {
    validateArgs({ enum: ["a", "b"] }, "c");
  }, "value is not in enum");

  assert(runtime.query === query, "default export exposes query");
  assert(runtime.mutation === mutation, "default export exposes mutation");
  assert(runtime.validateArgs === validateArgs, "default export exposes validateArgs");
};

run().catch((error) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  console.error(message);
  throw error;
});
