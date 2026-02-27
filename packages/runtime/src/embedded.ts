const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const formatPath = (base: string, segment: string | number): string =>
  typeof segment === "number" ? `${base}[${segment}]` : `${base}.${segment}`;

type ValidationIssue = {
  code: string;
  path: string;
  message: string;
  [k: string]: unknown;
};

type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: { issues: ValidationIssue[] } };

type SchemaLike<T = unknown> = {
  safeParse?: (value: unknown, path?: string, root?: boolean) => ValidationResult<T>;
  parse?: (value: unknown) => T;
};

const ok = <T>(data: T): ValidationResult<T> => ({ success: true, data });

const fail = (
  code: string,
  path: string,
  message: string,
  details: Record<string, unknown> = {}
): ValidationResult<never> => ({
  success: false,
  error: {
    issues: [{ code, path, message, ...details }],
  },
});

const describeValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
};

const sameJson = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);

const schema = <T>(safeParse: (value: unknown, path?: string, root?: boolean) => ValidationResult<T>) => ({
  safeParse,
  parse(value: unknown): T {
    const result = safeParse(value, "$", true);
    if (result.success) {
      return result.data;
    }
    const issue = result.error.issues[0];
    throw new TypeError(issue?.message ?? "stopgap args validation failed");
  },
});

const isSchemaLike = (candidate: unknown): candidate is SchemaLike =>
  isPlainObject(candidate) &&
  (typeof candidate.safeParse === "function" || typeof candidate.parse === "function");

const runSchemaValidation = (
  schemaValue: unknown,
  value: unknown,
  path = "$",
  root = true
): ValidationResult<unknown> => {
  if (!isSchemaLike(schemaValue)) {
    return fail(
      "invalid_schema",
      path,
      `stopgap args validation failed at ${path}: schema must provide parse/safeParse`,
      { root }
    );
  }

  if (typeof schemaValue.safeParse === "function") {
    try {
      const parsed = schemaValue.safeParse(value, path, root);
      if (parsed.success) {
        return ok(parsed.data);
      }
      const issue = parsed.error.issues[0];
      return fail(issue.code ?? "invalid", issue.path ?? path, issue.message, issue);
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      return fail("invalid", path, text, { root });
    }
  }

  if (typeof schemaValue.parse === "function") {
    try {
      return ok(schemaValue.parse(value));
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      return fail("invalid", path, text, { root });
    }
  }

  return fail("invalid_schema", path, `stopgap args validation failed at ${path}: unsupported schema`, { root });
};

export const v = {
  unknown: () => schema((value) => ok(value)),
  any: () => schema((value) => ok(value)),
  string: () =>
    schema((value, path = "$", root = false) =>
      typeof value === "string"
        ? ok(value)
        : fail(
            "invalid_type",
            path,
            `stopgap args validation failed at ${path}: expected string, got ${describeValue(value)}`,
            { expected: "string", received: describeValue(value), root }
          )
    ),
  number: () =>
    schema((value, path = "$", root = false) =>
      typeof value === "number" && Number.isFinite(value)
        ? ok(value)
        : fail(
            "invalid_type",
            path,
            `stopgap args validation failed at ${path}: expected number, got ${describeValue(value)}`,
            { expected: "number", received: describeValue(value), root }
          )
    ),
  int: () =>
    schema((value, path = "$", root = false) =>
      typeof value === "number" && Number.isInteger(value)
        ? ok(value)
        : fail(
            "invalid_type",
            path,
            `stopgap args validation failed at ${path}: expected integer, got ${describeValue(value)}`,
            { expected: "integer", received: describeValue(value), root }
          )
    ),
  boolean: () =>
    schema((value, path = "$", root = false) =>
      typeof value === "boolean"
        ? ok(value)
        : fail(
            "invalid_type",
            path,
            `stopgap args validation failed at ${path}: expected boolean, got ${describeValue(value)}`,
            { expected: "boolean", received: describeValue(value), root }
          )
    ),
  null: () =>
    schema((value, path = "$", root = false) =>
      value === null
        ? ok(value)
        : fail(
            "invalid_type",
            path,
            `stopgap args validation failed at ${path}: expected null, got ${describeValue(value)}`,
            { expected: "null", received: describeValue(value), root }
          )
    ),
  literal: (expected: unknown) =>
    schema((value, path = "$", root = false) =>
      sameJson(value, expected)
        ? ok(value)
        : fail(
            "invalid_literal",
            path,
            `stopgap args validation failed at ${path}: expected literal ${JSON.stringify(expected)}`,
            { expected, received: value, root }
          )
    ),
  enum: <T>(values: readonly T[]) =>
    schema((value, path = "$", root = false) =>
      values.some((entry) => sameJson(entry, value))
        ? ok(value)
        : fail(
            "invalid_enum",
            path,
            `stopgap args validation failed at ${path}: value is not in enum`,
            { options: values, received: value, root }
          )
    ),
  array: (itemSchema: unknown) =>
    schema((value, path = "$", root = false) => {
      if (!Array.isArray(value)) {
        return fail(
          "invalid_type",
          path,
          `stopgap args validation failed at ${path}: expected array, got ${describeValue(value)}`,
          { expected: "array", received: describeValue(value), root }
        );
      }

      const parsed: unknown[] = [];
      for (let i = 0; i < value.length; i += 1) {
        const result = runSchemaValidation(itemSchema, value[i], formatPath(path, i), false);
        if (!result.success) {
          return result;
        }
        parsed.push(result.data);
      }

      return ok(parsed);
    }),
  object: (shape: Record<string, unknown>) =>
    schema((value, path = "$", root = false) => {
      if (!isPlainObject(value)) {
        return fail(
          "invalid_type",
          path,
          `stopgap args validation failed at ${path}: expected object, got ${describeValue(value)}`,
          { expected: "object", received: describeValue(value), root }
        );
      }

      const parsed: Record<string, unknown> = {};
      for (const [key, fieldSchema] of Object.entries(shape ?? {})) {
        if (!Object.prototype.hasOwnProperty.call(value, key)) {
          return fail(
            "missing_required",
            formatPath(path, key),
            `stopgap args validation failed at ${formatPath(path, key)}: missing required property`,
            { key, root }
          );
        }

        const result = runSchemaValidation(fieldSchema, value[key], formatPath(path, key), false);
        if (!result.success) {
          return result;
        }
        parsed[key] = result.data;
      }

      for (const key of Object.keys(value)) {
        if (!Object.prototype.hasOwnProperty.call(shape ?? {}, key)) {
          return fail(
            "unrecognized_key",
            formatPath(path, key),
            `stopgap args validation failed at ${formatPath(path, key)}: additional properties are not allowed`,
            { key, root }
          );
        }
      }

      return ok(parsed);
    }),
  union: (schemas: readonly unknown[]) =>
    schema((value, path = "$", root = false) => {
      if (!Array.isArray(schemas) || schemas.length === 0) {
        return fail(
          "invalid_union",
          path,
          `stopgap args validation failed at ${path}: value does not match anyOf branches`,
          { root }
        );
      }

      for (const branch of schemas) {
        const result = runSchemaValidation(branch, value, path, false);
        if (result.success) {
          return result;
        }
      }

      return fail(
        "invalid_union",
        path,
        `stopgap args validation failed at ${path}: value does not match anyOf branches`,
        { root }
      );
    }),
};

const typeMatches = (expectedType: string, value: unknown): boolean => {
  switch (expectedType) {
    case "object":
      return isPlainObject(value);
    case "array":
      return Array.isArray(value);
    case "string":
      return typeof value === "string";
    case "boolean":
      return typeof value === "boolean";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "null":
      return value === null;
    default:
      return true;
  }
};

export const validateArgs = (schemaValue: unknown, value: unknown, path = "$"): void => {
  if (isSchemaLike(schemaValue)) {
    const result = runSchemaValidation(schemaValue, value, path, true);
    if (!result.success) {
      throw new TypeError(result.error.issues[0]?.message ?? "stopgap args validation failed");
    }
    return;
  }

  if (schemaValue == null || schemaValue === true) {
    return;
  }

  if (schemaValue === false) {
    throw new TypeError(`stopgap args validation failed at ${path}: schema forbids all values`);
  }

  if (!isPlainObject(schemaValue)) {
    throw new TypeError(`stopgap args validation failed at ${path}: schema must be an object`);
  }

  if (Array.isArray(schemaValue.enum)) {
    const matched = schemaValue.enum.some((entry) => sameJson(entry, value));
    if (!matched) {
      throw new TypeError(`stopgap args validation failed at ${path}: value is not in enum`);
    }
  }

  if (Array.isArray(schemaValue.anyOf) && schemaValue.anyOf.length > 0) {
    let matched = false;
    for (const branch of schemaValue.anyOf) {
      try {
        validateArgs(branch, value, path);
        matched = true;
        break;
      } catch {
        // check next branch
      }
    }
    if (!matched) {
      throw new TypeError(`stopgap args validation failed at ${path}: value does not match anyOf branches`);
    }
  }

  if (schemaValue.type !== undefined) {
    const expected = Array.isArray(schemaValue.type) ? schemaValue.type : [schemaValue.type];
    const matches = expected.some((entry) => typeMatches(String(entry), value));
    if (!matches) {
      throw new TypeError(
        `stopgap args validation failed at ${path}: expected ${expected.join("|")}, got ${describeValue(value)}`
      );
    }
  }

  if (isPlainObject(value)) {
    const properties = isPlainObject(schemaValue.properties) ? schemaValue.properties : {};
    const required = Array.isArray(schemaValue.required) ? schemaValue.required : [];

    for (const key of required) {
      if (!Object.prototype.hasOwnProperty.call(value, key)) {
        throw new TypeError(`stopgap args validation failed at ${path}.${key}: missing required property`);
      }
    }

    for (const [key, propertySchema] of Object.entries(properties)) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        validateArgs(propertySchema, value[key], `${path}.${key}`);
      }
    }

    if (schemaValue.additionalProperties === false) {
      for (const key of Object.keys(value)) {
        if (!Object.prototype.hasOwnProperty.call(properties, key)) {
          throw new TypeError(`stopgap args validation failed at ${path}.${key}: additional properties are not allowed`);
        }
      }
    }
  }

  if (Array.isArray(value) && schemaValue.items !== undefined) {
    for (let i = 0; i < value.length; i += 1) {
      validateArgs(schemaValue.items, value[i], `${path}[${i}]`);
    }
  }
};

const normalizeWrapperArgs = (
  kind: "query" | "mutation",
  argsSchema: unknown,
  handler: unknown
) => {
  if (typeof argsSchema === "function" && handler === undefined) {
    return { argsSchema: null, handler: argsSchema };
  }

  if (typeof handler !== "function") {
    throw new TypeError(`stopgap.${kind} expects a function handler`);
  }

  return { argsSchema: argsSchema ?? null, handler };
};

const wrap = (
  kind: "query" | "mutation",
  argsSchema: unknown,
  handler: unknown
) => {
  const normalized = normalizeWrapperArgs(kind, argsSchema, handler);

  const wrapped = async (ctx: unknown) => {
    const runtimeCtx = (ctx ?? {}) as { args?: unknown };
    const args = runtimeCtx.args ?? null;
    validateArgs(normalized.argsSchema, args);
    return await (normalized.handler as (args: unknown, ctx: unknown) => unknown)(args, runtimeCtx);
  };

  Object.assign(wrapped, {
    __stopgap_kind: kind,
    __stopgap_args_schema: normalized.argsSchema,
  });
  return wrapped;
};

export const query = (argsSchema: unknown, handler?: unknown) => wrap("query", argsSchema, handler);

export const mutation = (argsSchema: unknown, handler?: unknown) =>
  wrap("mutation", argsSchema, handler);

export default {
  v,
  query,
  mutation,
  validateArgs,
};
