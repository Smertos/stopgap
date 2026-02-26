#[pg_test]
fn test_metrics_deploy_calls_increase_after_deploy() {
    ensure_mock_plts_runtime();

    let before = Spi::get_one::<JsonB>("SELECT stopgap.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let before_calls = before
        .0
        .get("deploy")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("deploy.calls should be present");

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_metrics_src CASCADE;
        DROP SCHEMA IF EXISTS sg_metrics_live CASCADE;
        CREATE SCHEMA sg_metrics_src;
        SELECT set_config('stopgap.live_schema', 'sg_metrics_live', true);
        ",
    )
    .expect("integration setup should succeed");

    create_deployable_function(
        "sg_metrics_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('version', 'v1'); END",
    );

    let _ = Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_metrics', 'sg_metrics_src', 'v1')")
        .expect("deploy should succeed")
        .expect("deploy should return deployment id");

    let after = Spi::get_one::<JsonB>("SELECT stopgap.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_calls = after
        .0
        .get("deploy")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("deploy.calls should be present");

    assert!(after_calls > before_calls, "deploy.calls should increase after deploy");
}
