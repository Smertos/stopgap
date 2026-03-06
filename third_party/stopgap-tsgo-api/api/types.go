package api

type Diagnostic struct {
	Severity string `json:"severity"`
	Phase    string `json:"phase,omitempty"`
	Message  string `json:"message"`
	Line     *int   `json:"line,omitempty"`
	Column   *int   `json:"column,omitempty"`
}

type VirtualDeclaration struct {
	FileName string `json:"file_name"`
	Content  string `json:"content"`
}

type TypecheckRequest struct {
	SourceTS     string               `json:"source_ts"`
	Declarations []VirtualDeclaration `json:"declarations,omitempty"`
}

type TypecheckResponse struct {
	Diagnostics []Diagnostic `json:"diagnostics"`
}

type TranspileRequest struct {
	SourceTS     string               `json:"source_ts"`
	SourceMap    bool                 `json:"source_map,omitempty"`
	Declarations []VirtualDeclaration `json:"declarations,omitempty"`
}

type TranspileResponse struct {
	CompiledJS  string       `json:"compiled_js"`
	Diagnostics []Diagnostic `json:"diagnostics"`
	Backend     string       `json:"backend"`
}
