const isPlainObject = (value) =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const formatPath = (base, segment) =>
  typeof segment === "number" ? `${base}[${segment}]` : `${base}.${segment}`;

const ok = (data) => ({ success: true, data });

const fail = (code, path, message, details = {}) => ({
  success: false,
  error: {
    issues: [{ code, path, message, ...details }],
  },
});

const schema = (safeParse) => ({
  safeParse,
  parse(value) {
    const result = safeParse(value, "$", true);
    if (result.success) {
      return result.data;
    }
    const issue = result.error.issues[0];
    throw new TypeError(issue?.message ?? "stopgap args validation failed");
  },
});

const v = {
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
  literal: (expected) =>
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
  enum: (values) =>
    schema((value, path = "$", root = false) =>
      Array.isArray(values) && values.some((entry) => sameJson(entry, value))
        ? ok(value)
        : fail(
            "invalid_enum",
            path,
            `stopgap args validation failed at ${path}: value is not in enum`,
            { options: Array.isArray(values) ? values : [], received: value, root }
          )
    ),
  array: (itemSchema) =>
    schema((value, path = "$", root = false) => {
      if (!Array.isArray(value)) {
        return fail(
          "invalid_type",
          path,
          `stopgap args validation failed at ${path}: expected array, got ${describeValue(value)}`,
          { expected: "array", received: describeValue(value), root }
        );
      }

      const parsed = [];
      for (let i = 0; i < value.length; i += 1) {
        const result = runSchemaValidation(itemSchema, value[i], formatPath(path, i), false);
        if (!result.success) {
          return result;
        }
        parsed.push(result.data);
      }
      return ok(parsed);
    }),
  object: (shape) =>
    schema((value, path = "$", root = false) => {
      if (!isPlainObject(value)) {
        return fail(
          "invalid_type",
          path,
          `stopgap args validation failed at ${path}: expected object, got ${describeValue(value)}`,
          { expected: "object", received: describeValue(value), root }
        );
      }

      const parsed = {};
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
  union: (schemas) =>
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

const isSchemaLike = (candidate) =>
  isPlainObject(candidate) &&
  (typeof candidate.safeParse === "function" || typeof candidate.parse === "function");

const runSchemaValidation = (schemaValue, value, path = "$", root = true) => {
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
      if (parsed && typeof parsed === "object" && parsed.success === true) {
        return ok(parsed.data);
      }

      if (parsed && typeof parsed === "object" && parsed.success === false) {
        const issue = parsed.error?.issues?.[0];
        if (issue) {
          return fail(issue.code ?? "invalid", issue.path ?? path, issue.message, issue);
        }
      }

      return fail(
        "invalid",
        path,
        `stopgap args validation failed at ${path}: schema rejected value`,
        { root }
      );
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      return fail("invalid", path, text, { root });
    }
  }

  try {
    return ok(schemaValue.parse(value));
  } catch (error) {
    const text = error instanceof Error ? error.message : String(error);
    return fail("invalid", path, text, { root });
  }
};

const describeValue = (value) => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
};

const sameJson = (left, right) => JSON.stringify(left) === JSON.stringify(right);

const typeMatches = (expectedType, value) => {
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

export const validateArgs = (schema, value, path = "$") => {
  if (isSchemaLike(schema)) {
    const result = runSchemaValidation(schema, value, path, true);
    if (!result.success) {
      throw new TypeError(result.error.issues[0]?.message ?? "stopgap args validation failed");
    }
    return;
  }

  if (schema == null || schema === true) {
    return;
  }

  if (schema === false) {
    throw new TypeError(`stopgap args validation failed at ${path}: schema forbids all values`);
  }

  if (!isPlainObject(schema)) {
    throw new TypeError(`stopgap args validation failed at ${path}: schema must be an object`);
  }

  if (Array.isArray(schema.enum)) {
    const matched = schema.enum.some((entry) => sameJson(entry, value));
    if (!matched) {
      throw new TypeError(`stopgap args validation failed at ${path}: value is not in enum`);
    }
  }

  if (Array.isArray(schema.anyOf) && schema.anyOf.length > 0) {
    let matched = false;
    for (const branch of schema.anyOf) {
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

  if (schema.type !== undefined) {
    const expected = Array.isArray(schema.type) ? schema.type : [schema.type];
    const matches = expected.some((entry) => typeMatches(String(entry), value));
    if (!matches) {
      throw new TypeError(
        `stopgap args validation failed at ${path}: expected ${expected.join("|")}, got ${describeValue(value)}`
      );
    }
  }

  if (isPlainObject(value)) {
    const properties = isPlainObject(schema.properties) ? schema.properties : {};
    const required = Array.isArray(schema.required) ? schema.required : [];

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

    if (schema.additionalProperties === false) {
      for (const key of Object.keys(value)) {
        if (!Object.prototype.hasOwnProperty.call(properties, key)) {
          throw new TypeError(`stopgap args validation failed at ${path}.${key}: additional properties are not allowed`);
        }
      }
    }
  }

  if (Array.isArray(value) && schema.items !== undefined) {
    for (let i = 0; i < value.length; i += 1) {
      validateArgs(schema.items, value[i], `${path}[${i}]`);
    }
  }
};

const normalizeWrapperArgs = (kind, argsSchema, handler) => {
  if (typeof argsSchema === "function" && handler === undefined) {
    return { argsSchema: null, handler: argsSchema };
  }

  if (typeof handler !== "function") {
    throw new TypeError(`stopgap.${kind} expects a function handler`);
  }

  return { argsSchema: argsSchema ?? null, handler };
};

const wrap = (kind, argsSchema, handler) => {
  const normalized = normalizeWrapperArgs(kind, argsSchema, handler);

  const wrapped = async (ctx) => {
    const runtimeCtx = ctx ?? {};
    const args = runtimeCtx.args ?? null;
    validateArgs(normalized.argsSchema, args);
    return await normalized.handler(args, runtimeCtx);
  };

  Object.assign(wrapped, {
    __stopgap_kind: kind,
    __stopgap_args_schema: normalized.argsSchema,
  });
  return wrapped;
};

export const query = (argsSchema, handler) => wrap("query", argsSchema, handler);

export const mutation = (argsSchema, handler) =>
  wrap("mutation", argsSchema, handler);

export default {
  v,
  query,
  mutation,
  validateArgs,
};

export { v };
