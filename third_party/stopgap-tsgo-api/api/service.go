package api

import (
	"fmt"
	"regexp"
	"strings"
)

var appImportPattern = regexp.MustCompile(`(?m)^\s*import(?:\s+type)?(?:[\s\S]*?)from\s+['"](@app/[^'"]+)['"]|^\s*import\s+['"](@app/[^'"]+)['"]`)

func Typecheck(req TypecheckRequest) TypecheckResponse {
	if strings.TrimSpace(req.SourceTS) == "" {
		return TypecheckResponse{Diagnostics: []Diagnostic{}}
	}

	diagnostics := []Diagnostic{}
	matches := appImportPattern.FindAllStringSubmatchIndex(req.SourceTS, -1)
	for _, match := range matches {
		specifier := ""
		specifierStart := -1
		if len(match) >= 4 && match[2] >= 0 && match[3] >= 0 {
			specifier = req.SourceTS[match[2]:match[3]]
			specifierStart = match[2]
		} else if len(match) >= 6 && match[4] >= 0 && match[5] >= 0 {
			specifier = req.SourceTS[match[4]:match[5]]
			specifierStart = match[4]
		}

		line, column := lineColumnForOffset(req.SourceTS, specifierStart)
		lineCopy := line
		columnCopy := column
		diagnostics = append(diagnostics, Diagnostic{
			Severity: "error",
			Phase:    "semantic",
			Message: fmt.Sprintf(
				"unsupported bare module import `%s`: `@app/*` imports are not supported yet during plts typecheck",
				specifier,
			),
			Line:   &lineCopy,
			Column: &columnCopy,
		})
	}

	return TypecheckResponse{Diagnostics: diagnostics}
}

func Transpile(_ TranspileRequest) TranspileResponse {
	message := "tsgo transpile backend not wired yet: stopgap-tsgo-api currently exposes API shape only"
	return TranspileResponse{
		CompiledJS: "",
		Diagnostics: []Diagnostic{{
			Severity: "error",
			Phase:    "transpile",
			Message:  message,
		}},
		Backend: "tsgo-api-scaffold",
	}
}

func lineColumnForOffset(source string, offset int) (int, int) {
	if offset < 0 || offset > len(source) {
		return 1, 1
	}

	line := 1
	column := 1
	for idx, r := range source {
		if idx >= offset {
			break
		}
		if r == '\n' {
			line++
			column = 1
			continue
		}
		column++
	}

	return line, column
}
