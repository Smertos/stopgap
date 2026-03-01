import * as zodMini from "zod/mini";

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const formatPath = (base: string, segment: string | number): string =>
  typeof segment === "number" ? `${base}[${segment}]` : `${base}.${segment}`;

type ValidationIssue = {
  code?: string;
  path?: Array<string | number>;
  message?: string;
  input?: unknown;
  keys?: string[];
};

type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: { issues?: ValidationIssue[] } };

type SchemaLike<T = unknown> = {
  safeParse?: (value: unknown) => ValidationResult<T>;
  parse?: (value: unknown) => T;
};

const sameJson = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);

const toIssuePath = (path: Array<string | number> | undefined, fallback: string): string => {
  if (!Array.isArray(path) || path.length === 0) {
    return fallback;
  }

  let text = "$";
  for (const segment of path) {
    text = formatPath(text, segment);
  }
  return text;
};

const getPathParent = (value: unknown, path: Array<string | number>): unknown => {
  let current = value;
  for (let i = 0; i < path.length - 1; i += 1) {
    const segment = path[i];
    if (typeof segment === "number") {
      if (!Array.isArray(current) || segment < 0 || segment >= current.length) {
        return undefined;
      }
      current = current[segment];
      continue;
    }

    if (!isPlainObject(current) || !Object.prototype.hasOwnProperty.call(current, segment)) {
      return undefined;
    }
    current = current[segment];
  }
  return current;
};

const isMissingRequiredIssue = (issue: ValidationIssue, value: unknown): boolean => {
  if (issue.code !== "invalid_type" || !Array.isArray(issue.path) || issue.path.length === 0) {
    return false;
  }

  const lastSegment = issue.path[issue.path.length - 1];
  if (typeof lastSegment !== "string") {
    return false;
  }

  const parent = getPathParent(value, issue.path);
  return isPlainObject(parent) && !Object.prototype.hasOwnProperty.call(parent, lastSegment);
};

const formatSafeParseIssue = (
  issue: ValidationIssue | undefined,
  fallbackPath: string,
  value: unknown
): string => {
  if (!issue) {
    return `stopgap args validation failed at ${fallbackPath}: schema rejected value`;
  }

  const issuePath =
    issue.code === "unrecognized_keys" && Array.isArray(issue.keys) && issue.keys.length > 0
      ? formatPath(fallbackPath, issue.keys[0])
      : toIssuePath(issue.path, fallbackPath);

  if (issue.code === "unrecognized_keys") {
    return `stopgap args validation failed at ${issuePath}: additional properties are not allowed`;
  }

  if (isMissingRequiredIssue(issue, value)) {
    return `stopgap args validation failed at ${issuePath}: missing required property`;
  }

  const message = issue.message?.trim();
  if (message && message.length > 0) {
    return `stopgap args validation failed at ${issuePath}: ${message}`;
  }

  return `stopgap args validation failed at ${issuePath}: schema rejected value`;
};

const isSchemaLike = (candidate: unknown): candidate is SchemaLike =>
  isPlainObject(candidate) &&
  (typeof candidate.safeParse === "function" || typeof candidate.parse === "function");

const validateSchemaLikeArgs = (schemaValue: SchemaLike, value: unknown, path = "$", root = true): void => {
  if (typeof schemaValue.safeParse === "function") {
    try {
      const parsed = schemaValue.safeParse(value);
      if (parsed.success) {
        return;
      }

      throw new TypeError(formatSafeParseIssue(parsed.error?.issues?.[0], path, value));
    } catch (error) {
      if (error instanceof TypeError) {
        throw error;
      }
      const text = error instanceof Error ? error.message : String(error);
      throw new TypeError(`stopgap args validation failed at ${path}: ${text}`);
    }
  }

  if (typeof schemaValue.parse === "function") {
    try {
      schemaValue.parse(value);
      return;
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      throw new TypeError(`stopgap args validation failed at ${path}: ${text}`);
    }
  }

  throw new TypeError(
    `stopgap args validation failed at ${path}: schema must provide parse/safeParse (root=${root})`
  );
};

export const v = {
  ...zodMini,
  object: zodMini.strictObject,
};

const describeValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
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
    validateSchemaLikeArgs(schemaValue, value, path, true);
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
