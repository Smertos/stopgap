use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[pg_test]
fn test_metrics_compile_calls_increase_after_compile_and_store() {
    let before = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let before_calls = before
        .0
        .get("compile")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("compile.calls should be present");

    let source = "export default () => ({ ok: true })";
    let _ = Spi::get_one_with_args::<String>(
        "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
        &[source.into()],
    )
    .expect("compile_and_store query should succeed")
    .expect("compile_and_store should return artifact hash");

    let after = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_calls = after
        .0
        .get("compile")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("compile.calls should be present");
    let _compile_latency_last = after
        .0
        .get("compile")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("compile.latency_ms.last should be present");
    let _execute_latency_last = after
        .0
        .get("execute")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("execute.latency_ms.last should be present");
    let _execute_error_classes = after
        .0
        .get("execute")
        .and_then(|value| value.get("error_classes"))
        .and_then(Value::as_object)
        .expect("execute.error_classes should be an object");
    let readiness = after
        .0
        .get("runtime")
        .and_then(|value| value.get("readiness"))
        .and_then(Value::as_object)
        .expect("runtime.readiness should be an object");
    for field in [
        "checkout_hits",
        "checkout_misses",
        "checkout_last_us",
        "checkout_max_us",
        "setup_realm_last_us",
        "setup_realm_max_us",
        "cold_shell_creates",
        "warm_shell_reuses",
        "retired",
    ] {
        assert!(
            readiness.get(field).and_then(Value::as_u64).is_some(),
            "runtime.readiness.{field} should be numeric"
        );
    }
    let retire_reasons = readiness
        .get("retire_reasons")
        .and_then(Value::as_object)
        .expect("runtime.readiness.retire_reasons should be an object");
    for field in ["max_age", "max_invocations", "termination", "heap_pressure", "other"] {
        assert!(
            retire_reasons.get(field).and_then(Value::as_u64).is_some(),
            "runtime.readiness.retire_reasons.{field} should be numeric"
        );
    }

    assert!(after_calls > before_calls, "compile.calls should increase after compile_and_store");
}

#[pg_test]
fn test_metrics_include_tsgo_wasm_init_and_cache_fields() {
    let cache_dir = std::env::temp_dir().join(format!(
        "plts-tsgo-pg-metrics-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&cache_dir).expect("cache dir should be creatable");

    let cache_dir_sql = cache_dir.display().to_string().replace('\'', "''");
    Spi::run(&format!(
        "SELECT set_config('plts.tsgo_wasm_cache_mode', 'manual-only', true), set_config('plts.tsgo_wasm_cache_dir', '{}', true)",
        cache_dir_sql
    ))
    .expect("cache settings should be configurable");

    let before = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let before_init_calls = before
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("init"))
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("tsgo_wasm.init.calls should be present");

    let _ = Spi::get_one_with_args::<JsonB>(
        "SELECT plts.typecheck_ts($1::text, '{}'::jsonb)",
        &[String::from("export const value: number = 1;").into()],
    )
    .expect("typecheck_ts query should succeed")
    .expect("typecheck_ts should return diagnostics json");

    let after_first = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_first_init_calls = after_first
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("init"))
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("tsgo_wasm.init.calls should be present");
    assert_eq!(
        after_first_init_calls,
        before_init_calls + 1,
        "first typecheck should initialize the tsgo wasm runtime once"
    );

    let _ = Spi::get_one_with_args::<JsonB>(
        "SELECT plts.typecheck_ts($1::text, '{}'::jsonb)",
        &[String::from("export const again: number = 2;").into()],
    )
    .expect("second typecheck_ts query should succeed")
    .expect("second typecheck_ts should return diagnostics json");

    let after_second = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_second_init_calls = after_second
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("init"))
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("tsgo_wasm.init.calls should be present");
    assert_eq!(
        after_second_init_calls, after_first_init_calls,
        "second typecheck in the same backend should reuse the initialized runtime"
    );

    let cache = after_second
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("cache"))
        .and_then(Value::as_object)
        .expect("tsgo_wasm.cache should be an object");
    for field in [
        "built_in_configured",
        "manual_hits",
        "manual_misses",
        "fallback_compiles",
        "config_errors",
        "deserialize_errors",
    ] {
        assert!(
            cache.get(field).and_then(Value::as_u64).is_some(),
            "tsgo_wasm.cache.{field} should be numeric"
        );
    }

    let _ = fs::remove_dir_all(&cache_dir);
}

#[pg_test]
fn test_metrics_compile_uses_tsgo_wasm_by_default() {
    let before = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let before_init_calls = before
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("init"))
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("tsgo_wasm.init.calls should be present");

    let compiled = Spi::get_one_with_args::<String>(
        "SELECT compiled_js FROM plts.compile_ts($1::text, '{}'::jsonb)",
        &[String::from("export const value: number = 1;").into()],
    )
    .expect("compile_ts query should succeed")
    .expect("compile_ts should return compiled javascript");
    assert_eq!(compiled, "export const value = 1;\n");

    let after = Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_init_calls = after
        .0
        .get("tsgo_wasm")
        .and_then(|value| value.get("init"))
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("tsgo_wasm.init.calls should be present");

    assert_eq!(
        after_init_calls,
        before_init_calls + 1,
        "compile_ts should initialize the tsgo wasm runtime when TSGo transpile is the default backend"
    );
}
