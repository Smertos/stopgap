package api

type Diagnostic struct {
	Severity string `json:"severity"`
	Phase    string `json:"phase,omitempty"`
	Message  string `json:"message"`
	Line     *int   `json:"line,omitempty"`
	Column   *int   `json:"column,omitempty"`
}

type TypecheckRequest struct {
	SourceTS string `json:"source_ts"`
}

type TypecheckResponse struct {
	Diagnostics []Diagnostic `json:"diagnostics"`
}

type TranspileRequest struct {
	SourceTS string `json:"source_ts"`
}

type TranspileResponse struct {
	CompiledJS  string       `json:"compiled_js"`
	Diagnostics []Diagnostic `json:"diagnostics"`
	Backend     string       `json:"backend"`
}
