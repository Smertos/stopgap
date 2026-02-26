export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [k: string]: JsonValue };

export type JsonSchema = {
  type?: "object" | "array" | "string" | "number" | "integer" | "boolean" | "null";
  properties?: Record<string, JsonSchema>;
  required?: readonly string[];
  additionalProperties?: boolean;
  items?: JsonSchema;
  enum?: readonly JsonValue[];
  anyOf?: readonly JsonSchema[];
};

type SchemaTypeName<S extends JsonSchema> = S["type"] extends string ? S["type"] : never;

type RequiredKeys<S extends JsonSchema> = S["required"] extends readonly (infer K)[]
  ? K & string
  : never;

type InferObject<S extends JsonSchema> = S["properties"] extends Record<string, JsonSchema>
  ? {
      [K in keyof S["properties"]]: K extends RequiredKeys<S>
        ? InferJsonSchema<S["properties"][K]>
        : InferJsonSchema<S["properties"][K]> | undefined;
    }
  : Record<string, JsonValue>;

type InferArray<S extends JsonSchema> = S["items"] extends JsonSchema
  ? InferJsonSchema<S["items"]>[]
  : JsonValue[];

type InferByType<S extends JsonSchema> = SchemaTypeName<S> extends "object"
  ? InferObject<S>
  : SchemaTypeName<S> extends "array"
    ? InferArray<S>
    : SchemaTypeName<S> extends "string"
      ? string
      : SchemaTypeName<S> extends "number"
        ? number
        : SchemaTypeName<S> extends "integer"
          ? number
          : SchemaTypeName<S> extends "boolean"
            ? boolean
            : SchemaTypeName<S> extends "null"
              ? null
              : JsonValue;

export type InferJsonSchema<S extends JsonSchema> = S["enum"] extends readonly JsonValue[]
  ? S["enum"][number]
  : S["anyOf"] extends readonly JsonSchema[]
    ? InferJsonSchema<S["anyOf"][number]>
    : InferByType<S>;

export type DbMode = "ro" | "rw";

export type DbApi = {
  mode: DbMode;
  query: (sql: string, params?: JsonValue[]) => Promise<JsonValue[]>;
  exec: (sql: string, params?: JsonValue[]) => Promise<{ ok: true }>;
};

export type StopgapContext<TArgs> = {
  args: TArgs;
  db: DbApi;
  fn: { oid: number; schema: string; name: string };
  now: string;
};

type StopgapWrapped = ((ctx: unknown) => Promise<unknown>) & {
  __stopgap_kind: "query" | "mutation";
  __stopgap_args_schema: JsonSchema | null;
};

type StopgapHandler<TArgs, TResult> = (args: TArgs, ctx: StopgapContext<TArgs>) => TResult | Promise<TResult>;

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const describeValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
};

const sameJson = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);

const typeMatches = (expected: string, value: unknown): boolean => {
  switch (expected) {
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

export const validateArgs = (schema: JsonSchema | null | undefined, value: unknown, path = "$"): void => {
  if (schema == null) return;

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

  if (schema.type) {
    if (!typeMatches(schema.type, value)) {
      throw new TypeError(`stopgap args validation failed at ${path}: expected ${schema.type}, got ${describeValue(value)}`);
    }
  }

  if (isPlainObject(value)) {
    const properties = isPlainObject(schema.properties) ? schema.properties : {};
    const required = Array.isArray(schema.required) ? schema.required : [];

    for (const key of required) {
      if (!(key in value)) {
        throw new TypeError(`stopgap args validation failed at ${path}.${key}: missing required property`);
      }
    }

    for (const [key, propertySchema] of Object.entries(properties)) {
      if (key in value) {
        validateArgs(propertySchema, (value as Record<string, unknown>)[key], `${path}.${key}`);
      }
    }

    if (schema.additionalProperties === false) {
      for (const key of Object.keys(value)) {
        if (!(key in properties)) {
          throw new TypeError(`stopgap args validation failed at ${path}.${key}: additional properties are not allowed`);
        }
      }
    }
  }

  if (Array.isArray(value) && schema.items) {
    for (let i = 0; i < value.length; i += 1) {
      validateArgs(schema.items, value[i], `${path}[${i}]`);
    }
  }
};

function wrap<TArgs extends JsonValue, TResult>(
  kind: "query" | "mutation",
  argsSchema: JsonSchema | null,
  handler: StopgapHandler<TArgs, TResult>
): StopgapWrapped {
  const wrapped = (async (ctx: unknown) => {
    const runtimeCtx = (ctx ?? {}) as StopgapContext<TArgs>;
    const args = runtimeCtx.args;
    validateArgs(argsSchema, args);
    return handler(args, runtimeCtx);
  }) as StopgapWrapped;

  wrapped.__stopgap_kind = kind;
  wrapped.__stopgap_args_schema = argsSchema;
  return wrapped;
}

export function query<S extends JsonSchema, TResult>(
  argsSchema: S,
  handler: StopgapHandler<InferJsonSchema<S>, TResult>
): StopgapWrapped;
export function query<TResult>(
  handler: StopgapHandler<JsonValue, TResult>
): StopgapWrapped;
export function query<S extends JsonSchema, TResult>(
  argsSchemaOrHandler: S | StopgapHandler<JsonValue, TResult>,
  maybeHandler?: StopgapHandler<InferJsonSchema<S>, TResult>
): StopgapWrapped {
  if (typeof argsSchemaOrHandler === "function") {
    return wrap("query", null, argsSchemaOrHandler as StopgapHandler<JsonValue, TResult>);
  }
  if (!maybeHandler) {
    throw new TypeError("stopgap.query expects a function handler");
  }
  return wrap("query", argsSchemaOrHandler, maybeHandler as StopgapHandler<JsonValue, TResult>);
}

export function mutation<S extends JsonSchema, TResult>(
  argsSchema: S,
  handler: StopgapHandler<InferJsonSchema<S>, TResult>
): StopgapWrapped;
export function mutation<TResult>(
  handler: StopgapHandler<JsonValue, TResult>
): StopgapWrapped;
export function mutation<S extends JsonSchema, TResult>(
  argsSchemaOrHandler: S | StopgapHandler<JsonValue, TResult>,
  maybeHandler?: StopgapHandler<InferJsonSchema<S>, TResult>
): StopgapWrapped {
  if (typeof argsSchemaOrHandler === "function") {
    return wrap("mutation", null, argsSchemaOrHandler as StopgapHandler<JsonValue, TResult>);
  }
  if (!maybeHandler) {
    throw new TypeError("stopgap.mutation expects a function handler");
  }
  return wrap("mutation", argsSchemaOrHandler, maybeHandler as StopgapHandler<JsonValue, TResult>);
}

export default {
  query,
  mutation,
  validateArgs
};
