package api

import (
	"strings"
	"testing"
)

const testRuntimeDeclarations = `declare const Deno: unknown;
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
  export type StopgapContext<TArgs> = {
    args: TArgs;
    db: {
      mode: "ro" | "rw";
      query: (sql: string, params?: unknown[]) => Promise<JsonValue[]>;
      exec: (sql: string, params?: unknown[]) => Promise<{ ok: true }>;
    };
    fn: { oid: number; schema: string; name: string };
    now: string;
  };
  export type StopgapHandler<TArgs, TResult> =
    (args: TArgs, ctx: StopgapContext<TArgs>) => TResult | Promise<TResult>;
  export type StopgapWrapped = ((ctx: unknown) => Promise<unknown>) & {
    __stopgap_kind: "query" | "mutation";
    __stopgap_args_schema: unknown;
  };
  export const v: {
    object<T extends Record<string, StopgapSchema<unknown>>>(
      shape: T
    ): StopgapSchema<{ [K in keyof T]: InferArgsSchema<T[K]> }>;
    int(): StopgapSchema<number>;
    string(): StopgapSchema<string>;
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
  export function mutation<S, TResult>(
    argsSchema: S,
    handler: StopgapHandler<InferArgsSchema<S>, TResult>
  ): StopgapWrapped;
  export function mutation<TResult>(
    handler: StopgapHandler<JsonValue, TResult>
  ): StopgapWrapped;
  export const validateArgs: (schema: JsonSchema | null | undefined, value: unknown, path?: string) => void;
}`

func runtimeDeclarations() []VirtualDeclaration {
	return []VirtualDeclaration{{
		FileName: "/stopgap/runtime/index.d.ts",
		Content:  testRuntimeDeclarations,
	}}
}

func TestTypecheckReportsUnsupportedAppImport(t *testing.T) {
	source := "import { add } from '@app/math'\nexport default add(1, 2);\n"
	result := Typecheck(TypecheckRequest{SourceTS: source})

	if len(result.Diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d", len(result.Diagnostics))
	}

	diagnostic := result.Diagnostics[0]
	if diagnostic.Severity != "error" {
		t.Fatalf("unexpected severity: %s", diagnostic.Severity)
	}
	if diagnostic.Phase != "semantic" {
		t.Fatalf("unexpected phase: %s", diagnostic.Phase)
	}
	if diagnostic.Message != "unsupported bare module import `@app/math`: `@app/*` imports are not supported yet during plts typecheck" {
		t.Fatalf("unexpected message: %s", diagnostic.Message)
	}
	if diagnostic.Line == nil || *diagnostic.Line != 1 {
		t.Fatalf("unexpected line: %v", diagnostic.Line)
	}
	if diagnostic.Column == nil || *diagnostic.Column < 1 {
		t.Fatalf("unexpected column: %v", diagnostic.Column)
	}
}

func TestTypecheckIgnoresNonImportAppTokens(t *testing.T) {
	source := "const value = '@app/not-an-import';\nexport default value;\n"
	result := Typecheck(TypecheckRequest{SourceTS: source})
	if len(result.Diagnostics) != 0 {
		t.Fatalf("expected no diagnostics, got %d", len(result.Diagnostics))
	}
}

func TestTypecheckReportsWrapperArgMethodMismatch(t *testing.T) {
	source := "import { query, v } from '@stopgap/runtime';\n" +
		"export default query(v.object({ id: v.int() }), async (args, _ctx) => {\n" +
		"  return { bad: args.id.toUpperCase() };\n" +
		"});\n"
	result := Typecheck(TypecheckRequest{
		SourceTS:     source,
		Declarations: runtimeDeclarations(),
	})

	if len(result.Diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d", len(result.Diagnostics))
	}

	diagnostic := result.Diagnostics[0]
	if diagnostic.Severity != "error" {
		t.Fatalf("unexpected severity: %s", diagnostic.Severity)
	}
	if diagnostic.Phase != "semantic" {
		t.Fatalf("unexpected phase: %s", diagnostic.Phase)
	}
	if diagnostic.Message != "Property 'toUpperCase' does not exist on type 'number'." {
		t.Fatalf("unexpected message: %s", diagnostic.Message)
	}
}

func TestTypecheckReportsBracketAccessMethodMismatch(t *testing.T) {
	source := "import { query, v } from '@stopgap/runtime';\n" +
		"export default query(v.object({ id: v.int() }), async (args, _ctx) => {\n" +
		"  return { bad: args['id'].toUpperCase() };\n" +
		"});\n"
	result := Typecheck(TypecheckRequest{
		SourceTS:     source,
		Declarations: runtimeDeclarations(),
	})

	if len(result.Diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d", len(result.Diagnostics))
	}

	diagnostic := result.Diagnostics[0]
	if diagnostic.Message != "Property 'toUpperCase' does not exist on type 'number'." {
		t.Fatalf("unexpected message: %s", diagnostic.Message)
	}
}

func TestTranspileEmitsJavaScript(t *testing.T) {
	result := Transpile(TranspileRequest{SourceTS: "export const value: number = 1;"})
	if result.Backend != "typescript-go" {
		t.Fatalf("unexpected backend: %s", result.Backend)
	}
	if len(result.Diagnostics) != 0 {
		t.Fatalf("expected 0 diagnostics, got %d", len(result.Diagnostics))
	}
	if result.CompiledJS == "" {
		t.Fatalf("expected emitted JavaScript")
	}
	if result.CompiledJS == "export const value: number = 1;" {
		t.Fatalf("expected TypeScript syntax to be stripped from output: %s", result.CompiledJS)
	}
	if result.CompiledJS != "export const value = 1;\n" {
		t.Fatalf("unexpected JS output: %q", result.CompiledJS)
	}
}

func TestTranspileSupportsInlineSourceMaps(t *testing.T) {
	result := Transpile(TranspileRequest{
		SourceTS:  "export const value: number = 1;",
		SourceMap: true,
	})
	if len(result.Diagnostics) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", result.Diagnostics)
	}
	if !strings.Contains(result.CompiledJS, "//# sourceMappingURL=data:application/json;base64,") {
		t.Fatalf("expected inline source map, got %q", result.CompiledJS)
	}
}

func TestTranspilePreservesBareImportsWithoutResolutionErrors(t *testing.T) {
	result := Transpile(TranspileRequest{
		SourceTS:     "import { query } from '@stopgap/runtime';\nexport default query(async () => null);\n",
		Declarations: runtimeDeclarations(),
	})
	if len(result.Diagnostics) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", result.Diagnostics)
	}
	if !strings.Contains(result.CompiledJS, "from '@stopgap/runtime'") {
		t.Fatalf("expected bare import to be preserved, got %q", result.CompiledJS)
	}
}

func TestTypecheckIgnoresRuntimeResolvedModuleSpecifiers(t *testing.T) {
	source := "import { imported } from 'data:text/javascript;base64,ZXhwb3J0IGNvbnN0IGltcG9ydGVkID0gMTs=';\n" +
		"import { other } from 'plts+artifact:sha256:abc';\n" +
		"export default () => ({ imported, other });\n"
	result := Typecheck(TypecheckRequest{
		SourceTS:     source,
		Declarations: runtimeDeclarations(),
	})

	if len(result.Diagnostics) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", result.Diagnostics)
	}
}

func TestTypecheckAllowsLockedDownGlobalTypeQueries(t *testing.T) {
	source := "export default () => ({ denoType: typeof Deno, fetchType: typeof fetch, requestType: typeof Request, websocketType: typeof WebSocket });\n"
	result := Typecheck(TypecheckRequest{
		SourceTS:     source,
		Declarations: runtimeDeclarations(),
	})

	if len(result.Diagnostics) != 0 {
		t.Fatalf("unexpected diagnostics: %+v", result.Diagnostics)
	}
}
