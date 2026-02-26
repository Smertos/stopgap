#[pg_test]
fn test_deploy_updates_active_pointer_and_live_pointer() {
    ensure_mock_plts_runtime();

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_it_src CASCADE;
        DROP SCHEMA IF EXISTS sg_it_live CASCADE;
        CREATE SCHEMA sg_it_src;
        SELECT set_config('stopgap.live_schema', 'sg_it_live', true);
        ",
    )
    .expect("integration setup should succeed");

    create_deployable_function(
        "sg_it_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('version', 'v1'); END",
    );

    let first_deployment =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_deploy', 'sg_it_src', 'v1')")
            .expect("first deploy should succeed")
            .expect("first deploy should return deployment id");

    create_deployable_function(
        "sg_it_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('version', 'v2'); END",
    );

    let second_deployment =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_deploy', 'sg_it_src', 'v2')")
            .expect("second deploy should succeed")
            .expect("second deploy should return deployment id");

    assert!(
        second_deployment > first_deployment,
        "second deployment id should be greater than first deployment id"
    );

    let active_deployment = Spi::get_one::<i64>(
        "SELECT active_deployment_id FROM stopgap.environment WHERE env = 'it_env_deploy'",
    )
    .expect("active deployment lookup should succeed")
    .expect("environment row should have active deployment");
    assert_eq!(
        active_deployment, second_deployment,
        "active deployment pointer should move to the latest deploy"
    );

    let live_pointer_hash = pointer_artifact_hash("sg_it_live", "hello");
    let fn_version_hash = fn_version_artifact_hash(second_deployment, "hello");
    assert_eq!(
        live_pointer_hash, fn_version_hash,
        "live pointer should reference artifact_hash recorded in fn_version"
    );

    let artifact_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM plts.artifact WHERE artifact_hash = $1)",
        &[live_pointer_hash.as_str().into()],
    )
    .expect("artifact existence check should succeed")
    .expect("artifact existence check should return a row");
    assert!(artifact_exists, "deployed artifact hash should exist in plts.artifact");
}
