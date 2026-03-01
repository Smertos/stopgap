import runtime, { mutation, query, v, validateArgs } from "../src/index.js";
import { describe, expect, it } from "vitest";

const makeCtx = (args: unknown, mode: "ro" | "rw") => ({
  args,
  db: {
    mode,
    query: async () => [],
    exec: async () => ({ ok: true as const }),
  },
  fn: { oid: 1, schema: "public", name: "wrapped" },
  now: new Date().toISOString(),
});

describe("@stopgap/runtime wrappers", () => {
  it("attaches metadata and validates query args", async () => {
    const argsSchema = v.object({
      id: v.int(),
    });

    const wrappedQuery = query(argsSchema, async (args, ctx) => ({
      kind: "query",
      id: args.id,
      dbMode: ctx.db.mode,
    }));

    expect(wrappedQuery.__stopgap_kind).toBe("query");
    expect(wrappedQuery.__stopgap_args_schema).toBe(argsSchema);

    await expect(wrappedQuery(makeCtx({ id: 42 }, "ro"))).resolves.toEqual({
      kind: "query",
      id: 42,
      dbMode: "ro",
    });

    await expect(wrappedQuery(makeCtx({}, "ro"))).rejects.toThrow("missing required property");

    await expect(wrappedQuery(makeCtx({ id: 1, extra: true }, "ro"))).rejects.toThrow(
      "additional properties are not allowed"
    );
  });

  it("executes mutation wrapper in rw mode", async () => {
    const argsSchema = v.object({
      id: v.int(),
    });

    const wrappedMutation = mutation(argsSchema, async (args, ctx) => ({
      kind: "mutation",
      id: args.id,
      dbMode: ctx.db.mode,
    }));

    expect(wrappedMutation.__stopgap_kind).toBe("mutation");

    await expect(wrappedMutation(makeCtx({ id: 7 }, "rw"))).resolves.toEqual({
      kind: "mutation",
      id: 7,
      dbMode: "rw",
    });
  });

  it("supports schema-less wrappers and exports parity", async () => {
    const schemaLessQuery = query(async (args) => args);
    expect(schemaLessQuery.__stopgap_args_schema).toBeNull();

    await expect(schemaLessQuery(makeCtx({ ok: true }, "ro"))).resolves.toEqual({ ok: true });

    expect(runtime.query).toBe(query);
    expect(runtime.mutation).toBe(mutation);
    expect(runtime.v).toBe(v);
    expect(runtime.validateArgs).toBe(validateArgs);
  });

  it("validates args for v and legacy JSON schema formats", () => {
    expect(() => validateArgs(v.enum(["a", "b"]), "a")).not.toThrow();
    expect(() => validateArgs(v.union([v.int(), v.string()]), 10)).not.toThrow();
    expect(() => validateArgs({ enum: ["x", "y"] }, "x")).not.toThrow();

    expect(() => validateArgs(v.enum(["a", "b"]), "c")).toThrow("Invalid input");
    expect(() => validateArgs({ enum: ["x", "y"] }, "z")).toThrow("value is not in enum");
  });
});
