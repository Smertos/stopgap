use pgrx::prelude::*;
use serde_json::json;

use crate::domain::LiveFnRow;
use crate::runtime_config::{quote_ident, run_sql};
use crate::{APP_RUNTIME_ROLE, STOPGAP_OWNER_ROLE};

#[derive(Debug)]
pub(crate) struct DeployableFn {
    pub(crate) fn_name: String,
    pub(crate) prosrc: String,
}

pub(crate) fn fetch_live_deployable_functions(live_schema: &str) -> Result<Vec<LiveFnRow>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "
            SELECT p.oid::bigint AS fn_oid,
                   p.proname::text AS fn_name
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = $1
              AND l.lanname = 'plts'
              AND p.prorettype = 'jsonb'::regtype::oid
              AND array_length(p.proargtypes::oid[], 1) = 1
              AND p.proargtypes[0] = 'jsonb'::regtype::oid
            ORDER BY p.proname
            ",
            None,
            &[live_schema.into()],
        )?;

        let mut out = Vec::new();
        for row in rows {
            let oid = row
                .get_by_name::<i64, _>("fn_oid")
                .expect("fn_oid must be bigint")
                .expect("fn_oid cannot be null");
            let fn_name = row
                .get_by_name::<String, _>("fn_name")
                .expect("fn_name must be text")
                .expect("fn_name cannot be null");
            out.push(LiveFnRow { oid, fn_name });
        }

        Ok::<Vec<LiveFnRow>, pgrx::spi::Error>(out)
    })
    .map_err(|e| format!("failed to load live deployable functions in schema {live_schema}: {e}"))
}

pub(crate) fn live_function_has_dependents(function_oid: i64) -> Result<bool, String> {
    Spi::get_one_with_args::<bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM pg_depend d
            WHERE d.refclassid = 'pg_proc'::regclass
              AND d.refobjid = $1
              AND d.deptype IN ('n', 'a', 'i')
              AND NOT (d.classid = 'pg_proc'::regclass AND d.objid = $1)
        )
        ",
        &[function_oid.into()],
    )
    .map_err(|e| {
        format!("failed to inspect dependencies for live function oid {}: {e}", function_oid)
    })
    .map(|value| value.unwrap_or(false))
}

pub(crate) fn fetch_deployable_functions(from_schema: &str) -> Result<Vec<DeployableFn>, String> {
    Spi::connect(|client| {
        let rows = client.select(
            "
                SELECT p.proname::text AS fn_name, p.prosrc
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                JOIN pg_language l ON l.oid = p.prolang
                WHERE n.nspname = $1
                  AND l.lanname = 'plts'
                  AND p.prorettype = 'jsonb'::regtype::oid
                  AND array_length(p.proargtypes::oid[], 1) = 1
                  AND p.proargtypes[0] = 'jsonb'::regtype::oid
                ORDER BY p.proname
                ",
            None,
            &[from_schema.into()],
        )?;

        let mut out = Vec::new();
        for row in rows {
            let fn_name = row
                .get_by_name::<String, _>("fn_name")
                .expect("fn_name must be text")
                .expect("fn_name cannot be null");
            let prosrc = row
                .get_by_name::<String, _>("prosrc")
                .expect("prosrc must be text")
                .expect("prosrc cannot be null");
            out.push(DeployableFn { fn_name, prosrc });
        }

        Ok::<Vec<DeployableFn>, pgrx::spi::Error>(out)
    })
    .map_err(|e| format!("failed to scan deployable functions in schema {from_schema}: {e}"))
}

pub(crate) fn ensure_no_overloaded_plts_functions(from_schema: &str) {
    let overloaded = Spi::get_one_with_args::<String>(
        "
        SELECT proname::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname = $1
          AND l.lanname = 'plts'
        GROUP BY proname
        HAVING count(*) > 1
        LIMIT 1
        ",
        &[from_schema.into()],
    )
    .ok()
    .flatten();

    if let Some(name) = overloaded {
        error!(
            "stopgap deploy forbids overloaded plts functions in schema {}; offending function: {}",
            from_schema, name
        );
    }
}

pub(crate) fn materialize_live_pointer(
    live_schema: &str,
    fn_name: &str,
    artifact_hash: &str,
    import_map: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let mut pointer = json!({
        "plts": 1,
        "kind": "artifact_ptr",
        "artifact_hash": artifact_hash,
        "export": "default",
        "mode": "stopgap_deployed"
    });
    if !import_map.is_empty() {
        pointer["import_map"] = serde_json::Value::Object(import_map.clone());
    }

    let body = pointer.to_string().replace('\'', "''");

    let sql = format!(
        "
        CREATE OR REPLACE FUNCTION {}.{}(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {} $$
        ",
        quote_ident(live_schema),
        quote_ident(fn_name),
        body
    );

    run_sql(&sql, "failed to materialize live pointer function")?;

    run_sql(
        &format!(
            "ALTER FUNCTION {}.{}(jsonb) OWNER TO {}",
            quote_ident(live_schema),
            quote_ident(fn_name),
            quote_ident(STOPGAP_OWNER_ROLE)
        ),
        "failed to set live pointer function owner",
    )?;

    run_sql(
        &format!(
            "REVOKE ALL ON FUNCTION {}.{}(jsonb) FROM PUBLIC",
            quote_ident(live_schema),
            quote_ident(fn_name)
        ),
        "failed to revoke public execute from live pointer function",
    )?;

    run_sql(
        &format!(
            "GRANT EXECUTE ON FUNCTION {}.{}(jsonb) TO {}",
            quote_ident(live_schema),
            quote_ident(fn_name),
            quote_ident(APP_RUNTIME_ROLE)
        ),
        "failed to grant app runtime execute on live pointer function",
    )
}

pub(crate) fn harden_live_schema(live_schema: &str) -> Result<(), String> {
    run_sql(
        &format!(
            "ALTER SCHEMA {} OWNER TO {}",
            quote_ident(live_schema),
            quote_ident(STOPGAP_OWNER_ROLE)
        ),
        "failed to set live schema owner",
    )?;

    run_sql(
        &format!("REVOKE ALL ON SCHEMA {} FROM PUBLIC", quote_ident(live_schema)),
        "failed to revoke public privileges from live schema",
    )?;

    run_sql(
        &format!(
            "GRANT USAGE ON SCHEMA {} TO {}",
            quote_ident(live_schema),
            quote_ident(APP_RUNTIME_ROLE)
        ),
        "failed to grant app runtime usage on live schema",
    )
}
