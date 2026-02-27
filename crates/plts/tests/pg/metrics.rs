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

    assert!(after_calls > before_calls, "compile.calls should increase after compile_and_store");
}
