#[cfg(feature = "v8_runtime")]
use pgrx::JsonB;
#[cfg(feature = "v8_runtime")]
use pgrx::datum::DatumWithOid;
#[cfg(feature = "v8_runtime")]
use pgrx::prelude::*;
#[cfg(feature = "v8_runtime")]
use serde_json::Value;
#[cfg(feature = "v8_runtime")]
use serde_json::json;

#[cfg(feature = "v8_runtime")]
const DEFAULT_MAX_SQL_BYTES: usize = 128 * 1024;
#[cfg(feature = "v8_runtime")]
const DEFAULT_MAX_PARAMS: usize = 256;
#[cfg(feature = "v8_runtime")]
const DEFAULT_MAX_QUERY_ROWS: usize = 1000;

#[cfg(feature = "v8_runtime")]
#[derive(Debug)]
pub(crate) enum BoundParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(Value),
    NullText,
}

#[cfg(feature = "v8_runtime")]
impl BoundParam {
    fn from_json(value: Value) -> Self {
        match value {
            Value::Bool(v) => Self::Bool(v),
            Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    Self::Int(v)
                } else if let Some(v) = n.as_f64() {
                    Self::Float(v)
                } else {
                    Self::Json(Value::Number(n))
                }
            }
            Value::String(v) => Self::Text(v),
            Value::Array(_) | Value::Object(_) => Self::Json(value),
            Value::Null => Self::NullText,
        }
    }

    fn as_datum_with_oid(&self) -> DatumWithOid<'_> {
        match self {
            Self::Bool(v) => (*v).into(),
            Self::Int(v) => (*v).into(),
            Self::Float(v) => (*v).into(),
            Self::Text(v) => v.as_str().into(),
            Self::Json(v) => JsonB(v.clone()).into(),
            Self::NullText => Option::<&str>::None.into(),
        }
    }
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn bind_json_params(params: Vec<Value>) -> Vec<BoundParam> {
    params.into_iter().map(BoundParam::from_json).collect()
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn query_json_rows_with_params(
    sql: &str,
    params: Vec<Value>,
    read_only: bool,
) -> Result<Value, String> {
    let limits = RuntimeDbLimits::from_settings();

    if read_only && !is_read_only_sql(sql) {
        return Err(
            "db.query is read-only for stopgap.query handlers; use a SELECT-only statement"
                .to_string(),
        );
    }

    validate_sql_and_params("db.query", sql, params.len(), &limits)?;

    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    let fetch_limit = limits.max_query_rows.saturating_add(1);
    let wrapped_sql = format!(
        "SELECT COALESCE(jsonb_agg(row_json), '[]'::jsonb) FROM (SELECT to_jsonb(q) AS row_json FROM ({}) q LIMIT {}) rows",
        sql, fetch_limit
    );

    let rows = Spi::get_one_with_args::<JsonB>(&wrapped_sql, &args)
        .map_err(|e| format!("db.query SPI error: {e}"))?
        .map(|v| v.0)
        .unwrap_or_else(|| json!([]));

    if rows.as_array().is_some_and(|entries| entries.len() > limits.max_query_rows) {
        return Err(format!(
            "db.query returned more than {} rows; increase plts.max_query_rows if this result set is expected",
            limits.max_query_rows
        ));
    }

    Ok(rows)
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn exec_sql_with_params(
    sql: &str,
    params: Vec<Value>,
    read_only: bool,
) -> Result<Value, String> {
    let limits = RuntimeDbLimits::from_settings();

    if read_only {
        return Err("db.exec is disabled for stopgap.query handlers; switch to stopgap.mutation"
            .to_string());
    }

    validate_sql_and_params("db.exec", sql, params.len(), &limits)?;

    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    Spi::run_with_args(sql, &args).map_err(|e| format!("db.exec SPI error: {e}"))?;
    Ok(json!({ "ok": true }))
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn is_read_only_sql(sql: &str) -> bool {
    let normalized = strip_leading_sql_comments(sql).to_ascii_lowercase();
    if !(normalized.starts_with("select") || normalized.starts_with("with")) {
        return false;
    }

    let forbidden = [
        "insert", "update", "delete", "merge", "create", "alter", "drop", "truncate", "grant",
        "revoke", "vacuum", "analyze", "reindex", "cluster", "call", "copy",
    ];

    let mut token = String::new();
    for ch in normalized.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            token.push(ch);
            continue;
        }

        if !token.is_empty() {
            if forbidden.contains(&token.as_str()) {
                return false;
            }
            token.clear();
        }
    }

    if !token.is_empty() && forbidden.contains(&token.as_str()) {
        return false;
    }

    true
}

#[cfg(feature = "v8_runtime")]
fn strip_leading_sql_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if let Some(line_comment) = rest.strip_prefix("--") {
            if let Some(newline_idx) = line_comment.find('\n') {
                rest = line_comment[(newline_idx + 1)..].trim_start();
                continue;
            }
            return "";
        }

        if let Some(block_comment) = rest.strip_prefix("/*") {
            if let Some(end_idx) = block_comment.find("*/") {
                rest = block_comment[(end_idx + 2)..].trim_start();
                continue;
            }
            return "";
        }

        return rest;
    }
}

#[cfg(feature = "v8_runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuntimeDbLimits {
    max_sql_bytes: usize,
    max_params: usize,
    max_query_rows: usize,
}

#[cfg(feature = "v8_runtime")]
impl RuntimeDbLimits {
    fn from_settings() -> Self {
        Self {
            max_sql_bytes: read_limit_setting("plts.max_sql_bytes", DEFAULT_MAX_SQL_BYTES),
            max_params: read_limit_setting("plts.max_params", DEFAULT_MAX_PARAMS),
            max_query_rows: read_limit_setting("plts.max_query_rows", DEFAULT_MAX_QUERY_ROWS),
        }
    }
}

#[cfg(feature = "v8_runtime")]
fn validate_sql_and_params(
    op_name: &str,
    sql: &str,
    params_len: usize,
    limits: &RuntimeDbLimits,
) -> Result<(), String> {
    if sql.len() > limits.max_sql_bytes {
        return Err(format!(
            "{op_name} SQL text exceeds {} bytes; increase plts.max_sql_bytes for larger statements",
            limits.max_sql_bytes
        ));
    }

    if params_len > limits.max_params {
        return Err(format!(
            "{op_name} parameter count ({params_len}) exceeds {}; increase plts.max_params to allow more bound parameters",
            limits.max_params
        ));
    }

    Ok(())
}

#[cfg(feature = "v8_runtime")]
fn read_limit_setting(name: &str, fallback: usize) -> usize {
    current_setting_text(name).as_deref().and_then(parse_positive_usize).unwrap_or(fallback)
}

#[cfg(feature = "v8_runtime")]
fn current_setting_text(name: &str) -> Option<String> {
    let sql = format!("SELECT current_setting('{}', true)", name);
    Spi::get_one::<String>(&sql).ok().flatten().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
    })
}

#[cfg_attr(not(feature = "v8_runtime"), allow(dead_code))]
pub(crate) fn parse_positive_usize(raw: &str) -> Option<usize> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    trimmed.parse::<usize>().ok().filter(|value| *value > 0)
}
