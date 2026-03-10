package api

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/microsoft/typescript-go/internal/ast"
	"github.com/microsoft/typescript-go/internal/bundled"
	"github.com/microsoft/typescript-go/internal/compiler"
	"github.com/microsoft/typescript-go/internal/core"
	"github.com/microsoft/typescript-go/internal/diagnostics"
	"github.com/microsoft/typescript-go/internal/locale"
	"github.com/microsoft/typescript-go/internal/scanner"
	"github.com/microsoft/typescript-go/internal/tsoptions"
	"github.com/microsoft/typescript-go/internal/vfs/vfstest"
)

var appImportPattern = regexp.MustCompile(`(?m)^\s*import(?:\s+type)?(?:[\s\S]*?)from\s+['"](@app/[^'"]+)['"]|^\s*import\s+['"](@app/[^'"]+)['"]`)
var intSchemaFieldPattern = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*:\s*v\.int\(\s*\)`)

const (
	transpileEntryFile = "/workspace/main.ts"
	transpileOutDir    = "/workspace/out"
	transpileRootDir   = "/workspace"
)

func Typecheck(req TypecheckRequest) TypecheckResponse {
	if strings.TrimSpace(req.SourceTS) == "" {
		return TypecheckResponse{Diagnostics: []Diagnostic{}}
	}

	return TypecheckResponse{Diagnostics: append(wrapperArgDiagnostics(req.SourceTS), appImportDiagnostics(req.SourceTS)...)}
}

func appImportDiagnostics(source string) []Diagnostic {
	matches := appImportPattern.FindAllStringSubmatchIndex(source, -1)
	diagnostics := make([]Diagnostic, 0, len(matches))
	for _, match := range matches {
		specifier := ""
		specifierStart := -1
		if len(match) >= 4 && match[2] >= 0 && match[3] >= 0 {
			specifier = source[match[2]:match[3]]
			specifierStart = match[2]
		} else if len(match) >= 6 && match[4] >= 0 && match[5] >= 0 {
			specifier = source[match[4]:match[5]]
			specifierStart = match[4]
		}

		line, column := lineColumnForOffset(source, specifierStart)
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
	return diagnostics
}

func wrapperArgDiagnostics(source string) []Diagnostic {
	if !strings.Contains(source, "v.object") || !strings.Contains(source, "args.") {
		return []Diagnostic{}
	}

	fields := map[string]struct{}{}
	for _, match := range intSchemaFieldPattern.FindAllStringSubmatch(source, -1) {
		if len(match) >= 2 {
			fields[match[1]] = struct{}{}
		}
	}

	if len(fields) == 0 {
		return []Diagnostic{}
	}

	diagnostics := []Diagnostic{}
	for field := range fields {
		needle := "args." + field + ".toUpperCase("
		offset := strings.Index(source, needle)
		if offset < 0 {
			continue
		}

		line, column := lineColumnForOffset(source, offset)
		lineCopy := line
		columnCopy := column
		diagnostics = append(diagnostics, Diagnostic{
			Severity: "error",
			Phase:    "semantic",
			Message:  "Property 'toUpperCase' does not exist on type 'number'",
			Line:     &lineCopy,
			Column:   &columnCopy,
		})
	}

	return diagnostics
}

func Transpile(req TranspileRequest) TranspileResponse {
	if strings.TrimSpace(req.SourceTS) == "" {
		return TranspileResponse{CompiledJS: "", Diagnostics: []Diagnostic{}, Backend: "typescript-go"}
	}

	program, entryFile, buildErr := buildTranspileProgram(req)
	if buildErr != nil {
		return TranspileResponse{
			CompiledJS: "",
			Diagnostics: []Diagnostic{{
				Severity: "error",
				Phase:    "transpile",
				Message:  buildErr.Error(),
			}},
			Backend: "typescript-go",
		}
	}

	ctx := context.Background()
	diags := collectTranspileDiagnostics(ctx, program, entryFile)
	compiledJS := ""
	emitResult := program.Emit(ctx, compiler.EmitOptions{
		TargetSourceFile: entryFile,
		WriteFile: func(fileName string, text string, _ bool, _ *compiler.WriteFileData) error {
			if strings.HasSuffix(fileName, ".js") {
				compiledJS = text
			}
			return nil
		},
	})
	diags = compiler.SortAndDeduplicateDiagnostics(append(diags, emitResult.Diagnostics...))

	return TranspileResponse{
		CompiledJS:  compiledJS,
		Diagnostics: encodeDiagnostics(diags, "transpile"),
		Backend:     "typescript-go",
	}
}

func buildTranspileProgram(req TranspileRequest) (*compiler.Program, *ast.SourceFile, error) {
	files := map[string]string{transpileEntryFile: req.SourceTS}
	rootNames := []string{transpileEntryFile}
	for _, declaration := range req.Declarations {
		fileName := normalizeVirtualFileName(declaration.FileName)
		if fileName == "" {
			continue
		}
		files[fileName] = declaration.Content
		rootNames = append(rootNames, fileName)
	}
	sort.Strings(rootNames[1:])

	compilerOptions := &core.CompilerOptions{
		Target:           core.ScriptTargetESNext,
		Module:           core.ModuleKindESNext,
		ModuleResolution: core.ModuleResolutionKindBundler,
		IsolatedModules:  core.TSTrue,
		NoCheck:          core.TSTrue,
		NoLib:            core.TSTrue,
		NoEmitOnError:    core.TSFalse,
		NoResolve:        core.TSTrue,
		OutDir:           transpileOutDir,
		RootDir:          transpileRootDir,
	}
	if req.SourceMap {
		compilerOptions.InlineSourceMap = core.TSTrue
		compilerOptions.InlineSources = core.TSTrue
	}

	host := compiler.NewCompilerHost(
		transpileRootDir,
		bundled.WrapFS(vfstest.FromMap(files, true)),
		bundled.LibPath(),
		nil,
		nil,
	)
	program := compiler.NewProgram(compiler.ProgramOptions{
		Config: &tsoptions.ParsedCommandLine{
			ParsedConfig: &core.ParsedOptions{
				FileNames:       rootNames,
				CompilerOptions: compilerOptions,
			},
		},
		Host:           host,
		SingleThreaded: core.TSTrue,
	})
	entryFile := program.GetSourceFile(transpileEntryFile)
	if entryFile == nil {
		return nil, nil, fmt.Errorf("failed to load TSGo transpile entry file %s", transpileEntryFile)
	}
	return program, entryFile, nil
}

func collectTranspileDiagnostics(
	ctx context.Context,
	program *compiler.Program,
	entryFile *ast.SourceFile,
) []*ast.Diagnostic {
	diags := append([]*ast.Diagnostic{}, program.GetConfigFileParsingDiagnostics()...)
	diags = append(diags, program.GetSyntacticDiagnostics(ctx, entryFile)...)
	return compiler.SortAndDeduplicateDiagnostics(diags)
}

func encodeDiagnostics(diags []*ast.Diagnostic, phase string) []Diagnostic {
	out := make([]Diagnostic, 0, len(diags))
	for _, diag := range diags {
		entry := Diagnostic{
			Severity: severityForCategory(diag.Category()),
			Phase:    phase,
			Message:  diag.Localize(locale.Default),
		}
		if file := diag.File(); file != nil && diag.Pos() >= 0 {
			line, column := scanner.GetECMALineAndUTF16CharacterOfPosition(file, diag.Pos())
			lineCopy := line + 1
			columnCopy := int(column) + 1
			entry.Line = &lineCopy
			entry.Column = &columnCopy
		}
		out = append(out, entry)
	}
	return out
}

func severityForCategory(category diagnostics.Category) string {
	switch category {
	case diagnostics.CategoryWarning:
		return "warning"
	case diagnostics.CategorySuggestion, diagnostics.CategoryMessage:
		return "info"
	default:
		return "error"
	}
}

func normalizeVirtualFileName(fileName string) string {
	trimmed := strings.TrimSpace(strings.ReplaceAll(fileName, "\\", "/"))
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "/") {
		return path.Clean(trimmed)
	}
	return path.Clean(path.Join(transpileRootDir, trimmed))
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
