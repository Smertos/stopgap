const WARM_SETUP_SLO_US: u64 = 5_000;

fn readiness_metrics() -> Value {
    Spi::get_one::<JsonB>("SELECT plts.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist")
        .0
}

fn readiness_u64(metrics: &Value, field: &str) -> u64 {
    metrics
        .get("runtime")
        .and_then(|value| value.get("readiness"))
        .and_then(|value| value.get(field))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("runtime.readiness.{field} should be present"))
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
    let mut same_function_invoke_us = Vec::new();
    for _ in 0..7 {
        same_function_invoke_us
            .push(invoke_readiness_fn("SELECT tests_runtime_readiness_a('{}'::jsonb)"));
        let metrics = readiness_metrics();
        same_function_setup_us.push(readiness_u64(&metrics, "setup_realm_last_us"));
    }

    let mut cross_function_setup_us = Vec::new();
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
    }

    let after = readiness_metrics();
    let after_hits = readiness_u64(&after, "checkout_hits");
    let after_misses = readiness_u64(&after, "checkout_misses");
    let after_cold_creates = readiness_u64(&after, "cold_shell_creates");
    let after_warm_reuses = readiness_u64(&after, "warm_shell_reuses");

    let same_function_setup_median_us = median_u64(&mut same_function_setup_us);
    let cross_function_setup_median_us = median_u64(&mut cross_function_setup_us);
    let same_function_invoke_avg_us =
        same_function_invoke_us.iter().copied().sum::<u128>() / same_function_invoke_us.len() as u128;
    let cross_function_invoke_avg_us =
        cross_function_invoke_us.iter().copied().sum::<u128>() / cross_function_invoke_us.len() as u128;

    eprintln!(
        "READINESS_BASELINE cold_invoke_us={} same_fn_setup_median_us={} same_fn_invoke_avg_us={} cross_fn_setup_median_us={} cross_fn_invoke_avg_us={}",
        cold_invoke_us,
        same_function_setup_median_us,
        same_function_invoke_avg_us,
        cross_function_setup_median_us,
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
