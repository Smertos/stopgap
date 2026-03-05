package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/Smertos/stopgap/third_party/stopgap-tsgo-api/api"
)

func main() {
	if len(os.Args) != 2 {
		fail("usage: stopgap-tsgo-api <typecheck|transpile>")
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
	default:
		fail("unknown command %q; expected typecheck or transpile", os.Args[1])
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
