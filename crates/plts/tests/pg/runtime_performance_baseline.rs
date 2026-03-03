use std::time::Instant;

const COMPILE_SLO_MS_PER_CALL: f64 = 15.0;
const EXECUTE_COLD_SLO_MS_PER_CALL: f64 = 5.0;
const EXECUTE_WARM_SLO_MS_PER_CALL: f64 = 4.0;
const EXECUTE_WARM_REGRESSION_FACTOR: f64 = 1.20;

fn execute_loop_total_ns() -> u128 {
    let started_at = Instant::now();
    Spi::run(
        "SELECT tests_runtime_perf(jsonb_build_object('n', i)) FROM generate_series(1, 100) AS i",
    )
    .expect("runtime baseline execution loop should succeed");
    started_at.elapsed().as_nanos()
}

#[pg_test]
fn test_runtime_performance_baseline_snapshot() {
    let compile_iterations = 25_u128;
    let execute_iterations = 100_u128;

    let compile_started_at = Instant::now();
    Spi::run(
        "SELECT plts.compile_and_store(
            format('export default ({ args }) => ({ value: %s, arg: args })', i),
            '{}'::jsonb
        )
        FROM generate_series(1, 25) AS i",
    )
    .expect("compile baseline loop should succeed");
    let compile_total_ns = compile_started_at.elapsed().as_nanos();

    Spi::run(
        "CREATE OR REPLACE FUNCTION tests_runtime_perf(args jsonb)
         RETURNS jsonb
         LANGUAGE plts
         AS $$
         export default () => null;
         $$",
    )
    .expect("runtime baseline function creation should succeed");

    let execute_cold_total_ns = execute_loop_total_ns();
    let execute_warm_total_ns = execute_loop_total_ns();

    let ns_per_ms = 1_000_000.0;
    let compile_per_call_ms = (compile_total_ns as f64 / compile_iterations as f64) / ns_per_ms;
    let execute_cold_per_call_ms =
        (execute_cold_total_ns as f64 / execute_iterations as f64) / ns_per_ms;
    let execute_warm_per_call_ms =
        (execute_warm_total_ns as f64 / execute_iterations as f64) / ns_per_ms;

    eprintln!(
        "PERF_BASELINE compile_total_ns={} compile_per_call_ms={:.3} execute_cold_total_ns={} execute_cold_per_call_ms={:.3} execute_warm_total_ns={} execute_warm_per_call_ms={:.3}",
        compile_total_ns,
        compile_per_call_ms,
        execute_cold_total_ns,
        execute_cold_per_call_ms,
        execute_warm_total_ns,
        execute_warm_per_call_ms
    );

    assert!(compile_total_ns > 0, "compile loop should take measurable time");
    assert!(execute_cold_total_ns > 0, "cold execute loop should take measurable time");
    assert!(execute_warm_total_ns > 0, "warm execute loop should take measurable time");

    assert!(
        compile_per_call_ms <= COMPILE_SLO_MS_PER_CALL,
        "compile latency SLO exceeded: {:.2}ms > {:.2}ms",
        compile_per_call_ms,
        COMPILE_SLO_MS_PER_CALL
    );
    assert!(
        execute_cold_per_call_ms <= EXECUTE_COLD_SLO_MS_PER_CALL,
        "cold execute latency SLO exceeded: {:.2}ms > {:.2}ms",
        execute_cold_per_call_ms,
        EXECUTE_COLD_SLO_MS_PER_CALL
    );
    assert!(
        execute_warm_per_call_ms <= EXECUTE_WARM_SLO_MS_PER_CALL,
        "warm execute latency SLO exceeded: {:.2}ms > {:.2}ms",
        execute_warm_per_call_ms,
        EXECUTE_WARM_SLO_MS_PER_CALL
    );
    assert!(
        execute_warm_per_call_ms <= execute_cold_per_call_ms * EXECUTE_WARM_REGRESSION_FACTOR,
        "warm execute regression exceeded allowed factor: warm {:.2}ms, cold {:.2}ms, factor {:.2}",
        execute_warm_per_call_ms,
        execute_cold_per_call_ms,
        EXECUTE_WARM_REGRESSION_FACTOR
    );
}
