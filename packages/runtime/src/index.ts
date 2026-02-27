import {
  mutation as mutationCore,
  query as queryCore,
  validateArgs as validateArgsCore,
} from "./embedded.js";

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

export const validateArgs = (schema: JsonSchema | null | undefined, value: unknown, path = "$"): void =>
  validateArgsCore(schema, value, path);

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
  return queryCore(argsSchemaOrHandler, maybeHandler) as StopgapWrapped;
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
  return mutationCore(argsSchemaOrHandler, maybeHandler) as StopgapWrapped;
}

export default {
  query,
  mutation,
  validateArgs
};
