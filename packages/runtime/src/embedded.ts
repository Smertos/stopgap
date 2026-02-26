const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const describeValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
};

const sameJson = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);

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

export const validateArgs = (
  schema: unknown,
  value: unknown,
  path = "$"
): void => {
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
  query,
  mutation,
  validateArgs,
};
