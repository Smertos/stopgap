use pgrx::prelude::*;

mod api;
mod arg_mapping;
mod compiler;
#[cfg(test)]
mod compiler_core;
mod function_program;
#[cfg(test)]
mod function_program_core;
mod handler;
mod isolate_pool;
mod observability;
mod runtime;
#[cfg(test)]
mod runtime_core;
mod runtime_spi;

::pgrx::pg_module_magic!(name, version);

#[cfg(not(test))]
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    runtime::bootstrap_v8_isolate();
}

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS plts;

    CREATE TABLE IF NOT EXISTS plts.artifact (
        artifact_hash text PRIMARY KEY,
        source_ts text NOT NULL,
        compiled_js text NOT NULL,
        compiler_opts jsonb NOT NULL,
        compiler_fingerprint text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        source_map text,
        diagnostics jsonb
    );

    ALTER TABLE plts.artifact
    ADD COLUMN IF NOT EXISTS source_map text;

    CREATE FUNCTION plts_call_handler()
    RETURNS language_handler
    AS 'MODULE_PATHNAME', 'plts_call_handler'
    LANGUAGE C STRICT;

    CREATE FUNCTION plts_validator(oid)
    RETURNS void
    AS 'MODULE_PATHNAME', 'plts_validator'
    LANGUAGE C STRICT;

    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_language WHERE lanname = 'plts') THEN
            CREATE LANGUAGE plts HANDLER plts_call_handler VALIDATOR plts_validator;
        END IF;
    END;
    $$;
    "#,
    name = "plts_sql_bootstrap"
);

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    include!("../tests/pg/mod.rs");
}

#[cfg(test)]
pub mod pg_test {
    use std::sync::Once;
    use tracing_subscriber::EnvFilter;

    static TRACING_INIT: Once = Once::new();

    pub fn setup(_options: Vec<&str>) {
        TRACING_INIT.call_once(|| {
            let _ = tracing_log::LogTracer::init();
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            let _ = tracing_subscriber::fmt().with_env_filter(filter).with_test_writer().try_init();
            tracing::info!("initialized tracing subscriber for plts pg tests");
        });
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
