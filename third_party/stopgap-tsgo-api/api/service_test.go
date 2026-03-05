package api

import "testing"

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
	if diagnostic.Message != "Property 'toUpperCase' does not exist on type 'number'" {
		t.Fatalf("unexpected message: %s", diagnostic.Message)
	}
}

func TestTranspileReturnsScaffoldDiagnostic(t *testing.T) {
	result := Transpile(TranspileRequest{SourceTS: "export const value: number = 1;"})
	if result.Backend != "tsgo-api-scaffold" {
		t.Fatalf("unexpected backend: %s", result.Backend)
	}
	if len(result.Diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d", len(result.Diagnostics))
	}
	if result.Diagnostics[0].Phase != "transpile" {
		t.Fatalf("unexpected phase: %s", result.Diagnostics[0].Phase)
	}
}
