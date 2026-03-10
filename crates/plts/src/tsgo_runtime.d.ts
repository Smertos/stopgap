declare const Deno: unknown;
declare const fetch: unknown;
declare const Request: unknown;
declare const WebSocket: unknown;

declare module "@stopgap/runtime" {
  export type JsonPrimitive = string | number | boolean | null;
  export type JsonValue = unknown;

  export type JsonSchema = Record<string, unknown>;

  export interface StopgapSchema<T> {
    readonly __stopgap_output: T;
  }

  export type InferArgsSchema<S> = S extends StopgapSchema<infer T> ? T : JsonValue;

  export type DbMode = "ro" | "rw";

  export type DbApi = {
    mode: DbMode;
    query: (sql: string, params?: unknown[]) => Promise<JsonValue[]>;
    exec: (sql: string, params?: unknown[]) => Promise<{ ok: true }>;
  };

  export type StopgapContext<TArgs> = {
    args: TArgs;
    db: DbApi;
    fn: { oid: number; schema: string; name: string };
    now: string;
  };

  export type StopgapWrapped = ((ctx: unknown) => Promise<unknown>) & {
    __stopgap_kind: "query" | "mutation";
    __stopgap_args_schema: unknown;
  };

  export type StopgapHandler<TArgs, TResult> = (
    args: TArgs,
    ctx: StopgapContext<TArgs>
  ) => TResult | Promise<TResult>;

  export const validateArgs: (schema: JsonSchema | null | undefined, value: unknown, path?: string) => void;

  export const v: {
    object<T extends Record<string, StopgapSchema<unknown>>>(
      shape: T
    ): StopgapSchema<{ [K in keyof T]: InferArgsSchema<T[K]> }>;
    string(): StopgapSchema<string>;
    int(): StopgapSchema<number>;
    number(): StopgapSchema<number>;
    boolean(): StopgapSchema<boolean>;
    null(): StopgapSchema<null>;
    array<T>(value: StopgapSchema<T>): StopgapSchema<T[]>;
    optional<T>(value: StopgapSchema<T>): StopgapSchema<T | undefined>;
    union<T extends readonly StopgapSchema<unknown>[]>(
      options: T
    ): StopgapSchema<InferArgsSchema<T[number]>>;
    enum<T extends readonly JsonPrimitive[]>(values: T): StopgapSchema<T[number]>;
  };

  export function query<S extends StopgapSchema<unknown> | JsonSchema, TResult>(
    argsSchema: S,
    handler: StopgapHandler<InferArgsSchema<S>, TResult>
  ): StopgapWrapped;
  export function query<TResult>(
    handler: StopgapHandler<JsonValue, TResult>
  ): StopgapWrapped;

  export function mutation<S extends StopgapSchema<unknown> | JsonSchema, TResult>(
    argsSchema: S,
    handler: StopgapHandler<InferArgsSchema<S>, TResult>
  ): StopgapWrapped;
  export function mutation<TResult>(
    handler: StopgapHandler<JsonValue, TResult>
  ): StopgapWrapped;

  const runtimeApi: {
    v: typeof v;
    query: typeof query;
    mutation: typeof mutation;
    validateArgs: typeof validateArgs;
  };

  export default runtimeApi;
}
