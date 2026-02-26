#[cfg(feature = "v8_runtime")]
use pgrx::datum::DatumWithOid;
#[cfg(feature = "v8_runtime")]
use pgrx::prelude::*;
#[cfg(feature = "v8_runtime")]
use pgrx::JsonB;
#[cfg(feature = "v8_runtime")]
use serde_json::json;
#[cfg(feature = "v8_runtime")]
use serde_json::Value;

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
    if read_only && !is_read_only_sql(sql) {
        return Err(
            "db.query is read-only for stopgap.query handlers; use a SELECT-only statement"
                .to_string(),
        );
    }

    let bound = bind_json_params(params);
    let args: Vec<DatumWithOid<'_>> = bound.iter().map(BoundParam::as_datum_with_oid).collect();
    let wrapped_sql =
        format!("SELECT COALESCE(jsonb_agg(to_jsonb(q)), '[]'::jsonb) FROM ({}) q", sql);

    let rows = Spi::get_one_with_args::<JsonB>(&wrapped_sql, &args)
        .map_err(|e| format!("db.query SPI error: {e}"))?
        .map(|v| v.0)
        .unwrap_or_else(|| json!([]));

    Ok(rows)
}

#[cfg(feature = "v8_runtime")]
pub(crate) fn exec_sql_with_params(
    sql: &str,
    params: Vec<Value>,
    read_only: bool,
) -> Result<Value, String> {
    if read_only {
        return Err("db.exec is disabled for stopgap.query handlers; switch to stopgap.mutation"
            .to_string());
    }

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
