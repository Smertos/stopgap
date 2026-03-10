const WARM_SETUP_SLO_US: u64 = 5_000;
const READINESS_SAMPLE_COUNT: usize = 7;

fn readiness_metrics() -> Value {
    Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist")
        .0
}

fn metric_u64(metrics: &Value, path: &[&str]) -> u64 {
    path.iter()
        .fold(Some(metrics), |current, segment| current.and_then(|value| value.get(*segment)))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("metrics field {} should be present", path.join(".")))
}

fn readiness_u64(metrics: &Value, field: &str) -> u64 {
    metric_u64(metrics, &["runtime", "readiness", field])
}

fn readiness_phase_u64(metrics: &Value, field: &str) -> u64 {
    metric_u64(metrics, &["runtime", "readiness", "phases", field])
}

fn median_u64(values: &mut [u64]) -> u64 {
    assert!(!values.is_empty(), "median requires at least one value");
    values.sort_unstable();
    values[values.len() / 2]
}

fn invoke_readiness_fn(sql: &str) -> u128 {
    let started_at = std::time::Instant::now();
    let _ = Spi::get_one::<JsonB>(sql)
        .expect("runtime readiness invocation should succeed")
        .expect("runtime readiness invocation should return jsonb");
    started_at.elapsed().as_micros()
}

#[pg_test]
fn test_runtime_readiness_baseline_snapshot() {
    Spi::run(
        r#"
        SET LOCAL plts.isolate_reuse = 'on';
        SET LOCAL plts.isolate_pool_size = '2';

        CREATE OR REPLACE FUNCTION tests_runtime_readiness_a(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => ({ fn: "a" });
        $$;

        CREATE OR REPLACE FUNCTION tests_runtime_readiness_b(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        export default () => ({ fn: "b" });
        $$;
        "#,
    )
    .expect("runtime readiness setup should succeed");

    let before = readiness_metrics();
    let before_hits = readiness_u64(&before, "checkout_hits");
    let before_misses = readiness_u64(&before, "checkout_misses");
    let before_cold_creates = readiness_u64(&before, "cold_shell_creates");
    let before_warm_reuses = readiness_u64(&before, "warm_shell_reuses");

    let cold_invoke_us = invoke_readiness_fn("SELECT tests_runtime_readiness_a('{}'::jsonb)");

    let mut same_function_setup_us = Vec::new();
    let mut same_function_context_setup_us = Vec::new();
    let mut same_function_module_load_us = Vec::new();
    let mut same_function_module_evaluate_us = Vec::new();
    let mut same_function_cleanup_us = Vec::new();
    let mut same_function_invoke_us = Vec::new();
    for _ in 0..READINESS_SAMPLE_COUNT {
        same_function_invoke_us
            .push(invoke_readiness_fn("SELECT tests_runtime_readiness_a('{}'::jsonb)"));
        let metrics = readiness_metrics();
        same_function_setup_us.push(readiness_u64(&metrics, "setup_realm_last_us"));
        same_function_context_setup_us
            .push(readiness_phase_u64(&metrics, "context_setup_last_us"));
        same_function_module_load_us.push(readiness_phase_u64(&metrics, "module_load_last_us"));
        same_function_module_evaluate_us
            .push(readiness_phase_u64(&metrics, "module_evaluate_last_us"));
        same_function_cleanup_us.push(readiness_phase_u64(&metrics, "cleanup_last_us"));
    }

    let mut cross_function_setup_us = Vec::new();
    let mut cross_function_context_setup_us = Vec::new();
    let mut cross_function_module_load_us = Vec::new();
    let mut cross_function_module_evaluate_us = Vec::new();
    let mut cross_function_cleanup_us = Vec::new();
    let mut cross_function_invoke_us = Vec::new();
    for sql in [
        "SELECT tests_runtime_readiness_b('{}'::jsonb)",
        "SELECT tests_runtime_readiness_a('{}'::jsonb)",
        "SELECT tests_runtime_readiness_b('{}'::jsonb)",
        "SELECT tests_runtime_readiness_a('{}'::jsonb)",
        "SELECT tests_runtime_readiness_b('{}'::jsonb)",
        "SELECT tests_runtime_readiness_a('{}'::jsonb)",
    ] {
        cross_function_invoke_us.push(invoke_readiness_fn(sql));
        let metrics = readiness_metrics();
        cross_function_setup_us.push(readiness_u64(&metrics, "setup_realm_last_us"));
        cross_function_context_setup_us
            .push(readiness_phase_u64(&metrics, "context_setup_last_us"));
        cross_function_module_load_us.push(readiness_phase_u64(&metrics, "module_load_last_us"));
        cross_function_module_evaluate_us
            .push(readiness_phase_u64(&metrics, "module_evaluate_last_us"));
        cross_function_cleanup_us.push(readiness_phase_u64(&metrics, "cleanup_last_us"));
    }

    let after = readiness_metrics();
    let after_hits = readiness_u64(&after, "checkout_hits");
    let after_misses = readiness_u64(&after, "checkout_misses");
    let after_cold_creates = readiness_u64(&after, "cold_shell_creates");
    let after_warm_reuses = readiness_u64(&after, "warm_shell_reuses");

    let same_function_setup_median_us = median_u64(&mut same_function_setup_us);
    let same_function_context_setup_median_us = median_u64(&mut same_function_context_setup_us);
    let same_function_module_load_median_us = median_u64(&mut same_function_module_load_us);
    let same_function_module_evaluate_median_us =
        median_u64(&mut same_function_module_evaluate_us);
    let same_function_cleanup_median_us = median_u64(&mut same_function_cleanup_us);
    let cross_function_setup_median_us = median_u64(&mut cross_function_setup_us);
    let cross_function_context_setup_median_us = median_u64(&mut cross_function_context_setup_us);
    let cross_function_module_load_median_us = median_u64(&mut cross_function_module_load_us);
    let cross_function_module_evaluate_median_us =
        median_u64(&mut cross_function_module_evaluate_us);
    let cross_function_cleanup_median_us = median_u64(&mut cross_function_cleanup_us);
    let same_function_invoke_avg_us =
        same_function_invoke_us.iter().copied().sum::<u128>() / same_function_invoke_us.len() as u128;
    let cross_function_invoke_avg_us =
        cross_function_invoke_us.iter().copied().sum::<u128>() / cross_function_invoke_us.len() as u128;

    eprintln!(
        "READINESS_BASELINE cold_invoke_us={} same_fn_setup_median_us={} same_fn_context_setup_median_us={} same_fn_module_load_median_us={} same_fn_module_evaluate_median_us={} same_fn_cleanup_median_us={} same_fn_invoke_avg_us={} cross_fn_setup_median_us={} cross_fn_context_setup_median_us={} cross_fn_module_load_median_us={} cross_fn_module_evaluate_median_us={} cross_fn_cleanup_median_us={} cross_fn_invoke_avg_us={}",
        cold_invoke_us,
        same_function_setup_median_us,
        same_function_context_setup_median_us,
        same_function_module_load_median_us,
        same_function_module_evaluate_median_us,
        same_function_cleanup_median_us,
        same_function_invoke_avg_us,
        cross_function_setup_median_us,
        cross_function_context_setup_median_us,
        cross_function_module_load_median_us,
        cross_function_module_evaluate_median_us,
        cross_function_cleanup_median_us,
        cross_function_invoke_avg_us
    );

    assert!(
        after_misses > before_misses,
        "runtime.readiness.checkout_misses should increase on the cold shell create path"
    );
    assert!(
        after_cold_creates > before_cold_creates,
        "runtime.readiness.cold_shell_creates should increase on the cold shell create path"
    );
    assert!(
        after_hits > before_hits,
        "runtime.readiness.checkout_hits should increase on reused-shell calls"
    );
    assert!(
        after_warm_reuses > before_warm_reuses,
        "runtime.readiness.warm_shell_reuses should increase on reused-shell calls"
    );
    assert!(
        same_function_setup_median_us < WARM_SETUP_SLO_US,
        "same-function warm setup median should stay under {}us, got {}us",
        WARM_SETUP_SLO_US,
        same_function_setup_median_us
    );
    assert!(
        cross_function_setup_median_us < WARM_SETUP_SLO_US,
        "cross-function warm setup median should stay under {}us, got {}us",
        WARM_SETUP_SLO_US,
        cross_function_setup_median_us
    );
}

#[pg_test]
fn test_runtime_readiness_import_paths_are_observable() {
    let artifact_hash = Spi::get_one::<String>(
        r#"
        SELECT plts.compile_and_store(
            $$export const imported = 23;$$,
            '{}'::jsonb
        )
        "#,
    )
    .expect("artifact compile should succeed")
    .expect("artifact hash should be present");

    let setup_sql = format!(
        r#"
        SET LOCAL plts.isolate_reuse = 'on';
        SET LOCAL plts.isolate_pool_size = '2';

        CREATE OR REPLACE FUNCTION tests_runtime_readiness_import_data(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import {{ imported }} from "data:text/javascript;base64,ZXhwb3J0IGNvbnN0IGltcG9ydGVkID0gOTs=";
        export default () => ({{ imported }});
        $$;

        CREATE OR REPLACE FUNCTION tests_runtime_readiness_import_artifact(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        import {{ imported }} from "plts+artifact:{artifact_hash}";
        export default () => ({{ imported }});
        $$;
        "#,
    );
    Spi::run(&setup_sql).expect("runtime import readiness setup should succeed");

    let before = readiness_metrics();
    let before_hits = readiness_u64(&before, "checkout_hits");
    let before_warm_reuses = readiness_u64(&before, "warm_shell_reuses");

    let cold_data_invoke_us =
        invoke_readiness_fn("SELECT tests_runtime_readiness_import_data('{}'::jsonb)");

    let mut same_import_setup_us = Vec::new();
    let mut same_import_module_load_us = Vec::new();
    let mut same_import_module_evaluate_us = Vec::new();
    let mut same_import_cleanup_us = Vec::new();
    let mut same_import_invoke_us = Vec::new();
    for _ in 0..READINESS_SAMPLE_COUNT {
        same_import_invoke_us
            .push(invoke_readiness_fn("SELECT tests_runtime_readiness_import_data('{}'::jsonb)"));
        let metrics = readiness_metrics();
        same_import_setup_us.push(readiness_u64(&metrics, "setup_realm_last_us"));
        same_import_module_load_us.push(readiness_phase_u64(&metrics, "module_load_last_us"));
        same_import_module_evaluate_us
            .push(readiness_phase_u64(&metrics, "module_evaluate_last_us"));
        same_import_cleanup_us.push(readiness_phase_u64(&metrics, "cleanup_last_us"));
    }

    let mut cross_import_setup_us = Vec::new();
    let mut cross_import_module_load_us = Vec::new();
    let mut cross_import_module_evaluate_us = Vec::new();
    let mut cross_import_cleanup_us = Vec::new();
    let mut cross_import_invoke_us = Vec::new();
    for sql in [
        "SELECT tests_runtime_readiness_import_artifact('{}'::jsonb)",
        "SELECT tests_runtime_readiness_import_data('{}'::jsonb)",
        "SELECT tests_runtime_readiness_import_artifact('{}'::jsonb)",
        "SELECT tests_runtime_readiness_import_data('{}'::jsonb)",
        "SELECT tests_runtime_readiness_import_artifact('{}'::jsonb)",
        "SELECT tests_runtime_readiness_import_data('{}'::jsonb)",
    ] {
        cross_import_invoke_us.push(invoke_readiness_fn(sql));
        let metrics = readiness_metrics();
        cross_import_setup_us.push(readiness_u64(&metrics, "setup_realm_last_us"));
        cross_import_module_load_us.push(readiness_phase_u64(&metrics, "module_load_last_us"));
        cross_import_module_evaluate_us
            .push(readiness_phase_u64(&metrics, "module_evaluate_last_us"));
        cross_import_cleanup_us.push(readiness_phase_u64(&metrics, "cleanup_last_us"));
    }

    let after = readiness_metrics();
    let same_import_setup_median_us = median_u64(&mut same_import_setup_us);
    let same_import_module_load_median_us = median_u64(&mut same_import_module_load_us);
    let same_import_module_evaluate_median_us =
        median_u64(&mut same_import_module_evaluate_us);
    let same_import_cleanup_median_us = median_u64(&mut same_import_cleanup_us);
    let cross_import_setup_median_us = median_u64(&mut cross_import_setup_us);
    let cross_import_module_load_median_us = median_u64(&mut cross_import_module_load_us);
    let cross_import_module_evaluate_median_us =
        median_u64(&mut cross_import_module_evaluate_us);
    let cross_import_cleanup_median_us = median_u64(&mut cross_import_cleanup_us);
    let same_import_invoke_avg_us =
        same_import_invoke_us.iter().copied().sum::<u128>() / same_import_invoke_us.len() as u128;
    let cross_import_invoke_avg_us = cross_import_invoke_us.iter().copied().sum::<u128>()
        / cross_import_invoke_us.len() as u128;

    eprintln!(
        "READINESS_IMPORT_BASELINE cold_data_invoke_us={} same_import_setup_median_us={} same_import_module_load_median_us={} same_import_module_evaluate_median_us={} same_import_cleanup_median_us={} same_import_invoke_avg_us={} cross_import_setup_median_us={} cross_import_module_load_median_us={} cross_import_module_evaluate_median_us={} cross_import_cleanup_median_us={} cross_import_invoke_avg_us={}",
        cold_data_invoke_us,
        same_import_setup_median_us,
        same_import_module_load_median_us,
        same_import_module_evaluate_median_us,
        same_import_cleanup_median_us,
        same_import_invoke_avg_us,
        cross_import_setup_median_us,
        cross_import_module_load_median_us,
        cross_import_module_evaluate_median_us,
        cross_import_cleanup_median_us,
        cross_import_invoke_avg_us
    );

    assert!(
        readiness_u64(&after, "checkout_hits") > before_hits,
        "runtime.readiness.checkout_hits should increase on warm import-path calls"
    );
    assert!(
        readiness_u64(&after, "warm_shell_reuses") > before_warm_reuses,
        "runtime.readiness.warm_shell_reuses should increase on warm import-path calls"
    );
    assert!(
        same_import_invoke_avg_us > 0 && cross_import_invoke_avg_us > 0,
        "import-path warm invocation measurements should be captured"
    );
}
