package api

import (
	"context"
	"fmt"
	"path"
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

const (
	transpileEntryFile = "/workspace/main.ts"
	transpileOutDir    = "/workspace/out"
	transpileRootDir   = "/workspace"
)

type unsupportedAppImport struct {
	specifier string
	line      int
	column    int
}

func Typecheck(req TypecheckRequest) TypecheckResponse {
	if strings.TrimSpace(req.SourceTS) == "" {
		return TypecheckResponse{Diagnostics: []Diagnostic{}}
	}

	program, entryFile, buildErr := buildTypecheckProgram(req)
	if buildErr != nil {
		return TypecheckResponse{
			Diagnostics: []Diagnostic{{
				Severity: "error",
				Phase:    "semantic",
				Message:  buildErr.Error(),
			}},
		}
	}

	ctx := context.Background()
	return TypecheckResponse{Diagnostics: collectTypecheckDiagnostics(ctx, program, entryFile)}
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

func buildTypecheckProgram(req TypecheckRequest) (*compiler.Program, *ast.SourceFile, error) {
	return buildProgram(req.SourceTS, req.Declarations, typecheckCompilerOptions())
}

func buildTranspileProgram(req TranspileRequest) (*compiler.Program, *ast.SourceFile, error) {
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

	return buildProgram(req.SourceTS, req.Declarations, compilerOptions)
}

func buildProgram(
	sourceTS string,
	declarations []VirtualDeclaration,
	compilerOptions *core.CompilerOptions,
) (*compiler.Program, *ast.SourceFile, error) {
	files := map[string]string{transpileEntryFile: sourceTS}
	rootNames := []string{transpileEntryFile}
	for _, declaration := range declarations {
		fileName := normalizeVirtualFileName(declaration.FileName)
		if fileName == "" {
			continue
		}
		files[fileName] = declaration.Content
		rootNames = append(rootNames, fileName)
	}
	sort.Strings(rootNames[1:])

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
		return nil, nil, fmt.Errorf("failed to load TSGo entry file %s", transpileEntryFile)
	}
	return program, entryFile, nil
}

func typecheckCompilerOptions() *core.CompilerOptions {
	return &core.CompilerOptions{
		Target:                           core.ScriptTargetESNext,
		Module:                           core.ModuleKindESNext,
		ModuleResolution:                 core.ModuleResolutionKindBundler,
		Strict:                           core.TSTrue,
		NoImplicitAny:                    core.TSTrue,
		ForceConsistentCasingInFileNames: core.TSTrue,
		NoEmit:                           core.TSTrue,
		NoLib:                            core.TSFalse,
		SkipLibCheck:                     core.TSTrue,
		RootDir:                          transpileRootDir,
	}
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

func collectTypecheckDiagnostics(
	ctx context.Context,
	program *compiler.Program,
	entryFile *ast.SourceFile,
) []Diagnostic {
	configDiagnostics := compiler.SortAndDeduplicateDiagnostics(
		append([]*ast.Diagnostic{}, program.GetConfigFileParsingDiagnostics()...),
	)
	syntacticDiagnostics :=
		compiler.SortAndDeduplicateDiagnostics(program.GetSyntacticDiagnostics(ctx, entryFile))
	semanticDiagnostics :=
		compiler.SortAndDeduplicateDiagnostics(program.GetSemanticDiagnostics(ctx, entryFile))

	importSpecifiers := collectRuntimeResolvedImportSpecifiers(entryFile)
	semanticDiagnostics = filterRuntimeImportResolutionDiagnostics(semanticDiagnostics, importSpecifiers)

	appImports := collectUnsupportedAppImports(entryFile)

	diagnostics := make([]Diagnostic, 0, len(configDiagnostics)+len(syntacticDiagnostics)+len(semanticDiagnostics)+len(appImports))
	diagnostics = append(diagnostics, encodeDiagnostics(configDiagnostics, "config")...)
	diagnostics = append(diagnostics, encodeDiagnostics(syntacticDiagnostics, "syntactic")...)
	diagnostics = append(diagnostics, encodeDiagnostics(semanticDiagnostics, "semantic")...)
	diagnostics = append(diagnostics, explicitUnsupportedAppImportDiagnostics(appImports)...)
	return dedupeDiagnostics(diagnostics)
}

func collectUnsupportedAppImports(entryFile *ast.SourceFile) []unsupportedAppImport {
	imports := entryFile.Imports()
	if len(imports) == 0 {
		return nil
	}

	out := make([]unsupportedAppImport, 0, len(imports))
	for _, importNode := range imports {
		specifier := importNode.Text()
		if !strings.HasPrefix(specifier, "@app/") {
			continue
		}

		line, column := scanner.GetECMALineAndUTF16CharacterOfPosition(entryFile, importNode.Pos())
		out = append(out, unsupportedAppImport{
			specifier: specifier,
			line:      line + 1,
			column:    int(column) + 1,
		})
	}

	return out
}

func collectRuntimeResolvedImportSpecifiers(entryFile *ast.SourceFile) []string {
	imports := entryFile.Imports()
	if len(imports) == 0 {
		return nil
	}

	out := make([]string, 0, len(imports))
	for _, importNode := range imports {
		specifier := importNode.Text()
		if !isRuntimeResolvedImportSpecifier(specifier) {
			continue
		}
		out = append(out, specifier)
	}

	return out
}

func isRuntimeResolvedImportSpecifier(specifier string) bool {
	if strings.HasPrefix(specifier, "data:") || strings.HasPrefix(specifier, "plts+artifact:") {
		return true
	}

	if strings.HasPrefix(specifier, "@app/") {
		return true
	}

	return !strings.HasPrefix(specifier, "./") &&
		!strings.HasPrefix(specifier, "../") &&
		!strings.HasPrefix(specifier, "/") &&
		!strings.Contains(specifier, "://")
}

func filterRuntimeImportResolutionDiagnostics(
	diagnostics []*ast.Diagnostic,
	importSpecifiers []string,
) []*ast.Diagnostic {
	if len(importSpecifiers) == 0 {
		return diagnostics
	}

	filtered := make([]*ast.Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		message := strings.ToLower(diagnostic.Localize(locale.Default))
		if strings.Contains(message, "cannot find module") ||
			strings.Contains(message, "corresponding type declarations") ||
			strings.Contains(message, "declaration file for module") {
			skip := false
			for _, specifier := range importSpecifiers {
				if strings.Contains(message, strings.ToLower(specifier)) {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
		}
		filtered = append(filtered, diagnostic)
	}

	return filtered
}

func explicitUnsupportedAppImportDiagnostics(appImports []unsupportedAppImport) []Diagnostic {
	diagnostics := make([]Diagnostic, 0, len(appImports))
	for _, appImport := range appImports {
		line := appImport.line
		column := appImport.column
		diagnostics = append(diagnostics, Diagnostic{
			Severity: "error",
			Phase:    "semantic",
			Message: fmt.Sprintf(
				"unsupported bare module import `%s`: `@app/*` imports are not supported yet during plts typecheck",
				appImport.specifier,
			),
			Line:   &line,
			Column: &column,
		})
	}
	return diagnostics
}

func dedupeDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	seen := map[string]struct{}{}
	deduped := make([]Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		line := 0
		column := 0
		if diagnostic.Line != nil {
			line = *diagnostic.Line
		}
		if diagnostic.Column != nil {
			column = *diagnostic.Column
		}
		key := fmt.Sprintf(
			"%s|%s|%s|%d|%d",
			diagnostic.Severity,
			diagnostic.Phase,
			diagnostic.Message,
			line,
			column,
		)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, diagnostic)
	}
	return deduped
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
