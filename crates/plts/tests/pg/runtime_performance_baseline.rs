use std::time::Instant;

const COMPILE_SLO_MS_PER_CALL: f64 = 15.0;
const EXECUTE_COLD_SLO_MS_PER_CALL: f64 = 5.0;
const EXECUTE_WARM_SLO_MS_PER_CALL: f64 = 4.0;
const EXECUTE_WARM_REGRESSION_FACTOR: f64 = 1.20;
const MEASUREMENT_MAX_ATTEMPTS: usize = 5;
const EXECUTE_BASELINE_MAX_ATTEMPTS: usize = 5;
const COMPILE_BASELINE_MAX_ATTEMPTS: usize = 5;

fn measure_total_ns_with_retries<F>(label: &str, mut run: F) -> u128
where
    F: FnMut(),
{
    let mut total_ns = 0_u128;

    for _ in 0..MEASUREMENT_MAX_ATTEMPTS {
        let started_at = Instant::now();
        run();
        total_ns = started_at.elapsed().as_nanos();

        if total_ns > 0 {
            break;
        }
    }

    assert!(total_ns > 0, "{label} should take measurable time after retries");

    total_ns
}

fn execute_loop_total_ns() -> u128 {
    measure_total_ns_with_retries("execute loop", || {
        Spi::run(
            "SELECT tests_runtime_perf(jsonb_build_object('n', i)) FROM generate_series(1, 1000) AS i",
        )
        .expect("runtime baseline execution loop should succeed");
    })
}

fn compile_loop_total_ns() -> u128 {
    measure_total_ns_with_retries("compile loop", || {
        Spi::run(
            "SELECT plts.compile_and_store(
                format('export default ({ args }: { args: unknown }) => ({ value: %s, arg: args })', i),
                '{}'::jsonb
            )
            FROM generate_series(1, 25) AS i",
        )
        .expect("compile baseline loop should succeed");
    })
}

fn ns_per_call_ms(total_ns: u128, iterations: u128) -> f64 {
    (total_ns as f64 / iterations as f64) / 1_000_000.0
}

fn execute_slos_met(cold_per_call_ms: f64, warm_per_call_ms: f64) -> bool {
    cold_per_call_ms <= EXECUTE_COLD_SLO_MS_PER_CALL
        && warm_per_call_ms <= EXECUTE_WARM_SLO_MS_PER_CALL
        && warm_per_call_ms <= cold_per_call_ms * EXECUTE_WARM_REGRESSION_FACTOR
}

fn measure_execute_baseline_with_retries(execute_iterations: u128) -> (u128, u128) {
    let mut best_cold_total_ns = u128::MAX;
    let mut best_warm_total_ns = u128::MAX;

    for _ in 0..EXECUTE_BASELINE_MAX_ATTEMPTS {
        let cold_total_ns = execute_loop_total_ns();
        let warm_total_ns = execute_loop_total_ns();

        if cold_total_ns < best_cold_total_ns {
            best_cold_total_ns = cold_total_ns;
        }
        if warm_total_ns < best_warm_total_ns {
            best_warm_total_ns = warm_total_ns;
        }

        let cold_per_call_ms = ns_per_call_ms(cold_total_ns, execute_iterations);
        let warm_per_call_ms = ns_per_call_ms(warm_total_ns, execute_iterations);
        if execute_slos_met(cold_per_call_ms, warm_per_call_ms) {
            break;
        }
    }

    (best_cold_total_ns, best_warm_total_ns)
}

fn measure_compile_baseline_with_retries(compile_iterations: u128) -> u128 {
    let mut best_compile_total_ns = u128::MAX;

    for _ in 0..COMPILE_BASELINE_MAX_ATTEMPTS {
        let compile_total_ns = compile_loop_total_ns();
        if compile_total_ns < best_compile_total_ns {
            best_compile_total_ns = compile_total_ns;
        }

        let compile_per_call_ms = ns_per_call_ms(compile_total_ns, compile_iterations);
        if compile_per_call_ms <= COMPILE_SLO_MS_PER_CALL {
            break;
        }
    }

    best_compile_total_ns
}

#[pg_test]
fn test_runtime_performance_baseline_snapshot() {
    let compile_iterations = 25_u128;
    let execute_iterations = 1_000_u128;

    let compile_total_ns = measure_compile_baseline_with_retries(compile_iterations);

    Spi::run(
        "CREATE OR REPLACE FUNCTION tests_runtime_perf(args jsonb)
         RETURNS jsonb
         LANGUAGE plts
         AS $$
         export default () => null;
         $$",
    )
    .expect("runtime baseline function creation should succeed");

    let (execute_cold_total_ns, execute_warm_total_ns) =
        measure_execute_baseline_with_retries(execute_iterations);

    let compile_per_call_ms = ns_per_call_ms(compile_total_ns, compile_iterations);
    let execute_cold_per_call_ms = ns_per_call_ms(execute_cold_total_ns, execute_iterations);
    let execute_warm_per_call_ms = ns_per_call_ms(execute_warm_total_ns, execute_iterations);

    eprintln!(
        "PERF_BASELINE compile_total_ns={} compile_per_call_ms={:.3} execute_cold_total_ns={} execute_cold_per_call_ms={:.3} execute_warm_total_ns={} execute_warm_per_call_ms={:.3}",
        compile_total_ns,
        compile_per_call_ms,
        execute_cold_total_ns,
        execute_cold_per_call_ms,
        execute_warm_total_ns,
        execute_warm_per_call_ms
    );

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
