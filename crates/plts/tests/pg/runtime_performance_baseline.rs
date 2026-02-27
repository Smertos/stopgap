use std::time::Instant;

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
    let compile_total_ms = compile_started_at.elapsed().as_millis();

    Spi::run(
        "CREATE OR REPLACE FUNCTION tests_runtime_perf(args jsonb)
         RETURNS jsonb
         LANGUAGE plts
         AS $$
         export default () => null;
         $$",
    )
    .expect("runtime baseline function creation should succeed");

    let execute_started_at = Instant::now();
    Spi::run(
        "SELECT tests_runtime_perf(jsonb_build_object('n', i)) FROM generate_series(1, 100) AS i",
    )
    .expect("runtime baseline execution loop should succeed");
    let execute_total_ms = execute_started_at.elapsed().as_millis();

    let compile_per_call_ms = compile_total_ms as f64 / compile_iterations as f64;
    let execute_per_call_ms = execute_total_ms as f64 / execute_iterations as f64;

    eprintln!(
        "PERF_BASELINE compile_total_ms={} compile_per_call_ms={:.2} execute_total_ms={} execute_per_call_ms={:.2}",
        compile_total_ms, compile_per_call_ms, execute_total_ms, execute_per_call_ms
    );

    assert!(compile_total_ms > 0, "compile loop should take measurable time");
    assert!(execute_total_ms > 0, "execute loop should take measurable time");
}
