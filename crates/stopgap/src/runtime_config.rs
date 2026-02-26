use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

pub(crate) fn run_sql(sql: &str, context: &str) -> Result<(), String> {
    Spi::run(sql).map_err(|e| format!("{context}: {e}"))
}

pub(crate) fn run_sql_with_args<'a>(
    sql: &str,
    args: &[DatumWithOid<'a>],
    context: &str,
) -> Result<(), String> {
    Spi::run_with_args(sql, args).map_err(|e| format!("{context}: {e}"))
}

pub(crate) fn quote_ident(ident: &str) -> String {
    common::sql::quote_ident(ident)
}

pub(crate) fn resolve_live_schema() -> String {
    let live = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('stopgap.live_schema', true), 'live_deployment')",
    )
    .ok()
    .flatten();
    live.unwrap_or_else(|| "live_deployment".to_string())
}

pub(crate) fn resolve_prune_enabled() -> bool {
    let raw = Spi::get_one::<String>(
        "SELECT COALESCE(current_setting('stopgap.prune', true), 'false')::text",
    )
    .ok()
    .flatten();

    raw.as_deref().and_then(parse_bool_setting).unwrap_or(false)
}

pub(crate) fn parse_bool_setting(value: &str) -> Option<bool> {
    common::settings::parse_bool_setting(value)
}
