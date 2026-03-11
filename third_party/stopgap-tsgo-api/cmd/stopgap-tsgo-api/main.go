package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/microsoft/typescript-go/stopgap-tsgo-api/api"
)

func main() {
	if len(os.Args) != 2 {
		fail("usage: stopgap-tsgo-api <typecheck|transpile|compile_checked>")
	}

	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail("failed reading stdin: %v", err)
	}

	switch os.Args[1] {
	case "typecheck":
		var req api.TypecheckRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			fail("failed to decode typecheck request: %v", err)
		}
		writeJSON(api.Typecheck(req))
	case "transpile":
		var req api.TranspileRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			fail("failed to decode transpile request: %v", err)
		}
		writeJSON(api.Transpile(req))
	case "compile_checked":
		var req api.TranspileRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			fail("failed to decode compile_checked request: %v", err)
		}
		writeJSON(api.CompileChecked(req))
	default:
		fail("unknown command %q; expected typecheck, transpile, or compile_checked", os.Args[1])
	}
}

func writeJSON(v any) {
	encoded, err := json.Marshal(v)
	if err != nil {
		fail("failed to encode response: %v", err)
	}
	if _, err := os.Stdout.Write(encoded); err != nil {
		fail("failed writing response: %v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
