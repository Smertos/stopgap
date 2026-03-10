use pgrx::GucSetting;
use pgrx::prelude::*;
#[cfg(not(test))]
use pgrx::{GucContext, GucFlags, GucRegistry};

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

pub(crate) static ISOLATE_REUSE_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static ISOLATE_POOL_SIZE_GUC: GucSetting<i32> = GucSetting::<i32>::new(2);
pub(crate) static ISOLATE_MAX_AGE_S_GUC: GucSetting<i32> = GucSetting::<i32>::new(120);
pub(crate) static ISOLATE_MAX_INVOCATIONS_GUC: GucSetting<i32> = GucSetting::<i32>::new(250);

#[cfg(not(test))]
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_bool_guc(
        c"plts.isolate_reuse",
        c"Enable backend-local pooled runtime reuse for the V8 runtime.",
        c"Controls whether plts keeps warm runtime shells ready for subsequent invocations.",
        &ISOLATE_REUSE_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"plts.isolate_pool_size",
        c"Maximum number of warm runtime shells kept ready per backend.",
        c"Controls how many pre-bootstrapped V8 runtime shells plts retains in a backend-local pool.",
        &ISOLATE_POOL_SIZE_GUC,
        0,
        32,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"plts.isolate_max_age_s",
        c"Maximum lifetime in seconds for a pooled runtime shell.",
        c"Older pooled runtime shells are retired before reuse to keep isolate state bounded.",
        &ISOLATE_MAX_AGE_S_GUC,
        0,
        86_400,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"plts.isolate_max_invocations",
        c"Maximum number of invocations allowed on a pooled runtime shell.",
        c"Pooled runtime shells are retired after this many invocations to cap retained module graph growth.",
        &ISOLATE_MAX_INVOCATIONS_GUC,
        1,
        100_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    runtime::bootstrap_v8_isolate();
}

pub(crate) fn isolate_reuse_enabled() -> bool {
    ISOLATE_REUSE_GUC.get()
}

pub(crate) fn isolate_pool_size() -> usize {
    ISOLATE_POOL_SIZE_GUC.get().max(0) as usize
}

pub(crate) fn isolate_max_age_seconds() -> u64 {
    ISOLATE_MAX_AGE_S_GUC.get().max(0) as u64
}

pub(crate) fn isolate_max_invocations() -> u64 {
    ISOLATE_MAX_INVOCATIONS_GUC.get().max(1) as u64
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
