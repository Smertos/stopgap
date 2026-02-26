#[pg_test]
fn test_rollback_reactivates_prior_deploy() {
    ensure_mock_plts_runtime();

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_it_rb_src CASCADE;
        DROP SCHEMA IF EXISTS sg_it_rb_live CASCADE;
        CREATE SCHEMA sg_it_rb_src;
        SELECT set_config('stopgap.live_schema', 'sg_it_rb_live', true);
        ",
    )
    .expect("rollback setup should succeed");

    create_deployable_function(
        "sg_it_rb_src",
        "stepper",
        "BEGIN RETURN jsonb_build_object('version', 'one'); END",
    );
    let deploy_one =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_rb', 'sg_it_rb_src', 'one')")
            .expect("deploy one should succeed")
            .expect("deploy one should return id");

    create_deployable_function(
        "sg_it_rb_src",
        "stepper",
        "BEGIN RETURN jsonb_build_object('version', 'two'); END",
    );
    let deploy_two =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_rb', 'sg_it_rb_src', 'two')")
            .expect("deploy two should succeed")
            .expect("deploy two should return id");

    create_deployable_function(
        "sg_it_rb_src",
        "stepper",
        "BEGIN RETURN jsonb_build_object('version', 'three'); END",
    );
    let deploy_three =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_rb', 'sg_it_rb_src', 'three')")
            .expect("deploy three should succeed")
            .expect("deploy three should return id");

    let rolled_back_to = Spi::get_one::<i64>("SELECT stopgap.rollback('it_env_rb', 1, NULL)")
        .expect("rollback should succeed")
        .expect("rollback should return target deployment id");
    assert_eq!(
        rolled_back_to, deploy_two,
        "rollback by one step should target the immediate previous deployment"
    );

    let active_deployment = Spi::get_one::<i64>(
        "SELECT active_deployment_id FROM stopgap.environment WHERE env = 'it_env_rb'",
    )
    .expect("active deployment lookup should succeed")
    .expect("active deployment should be present after rollback");
    assert_eq!(active_deployment, deploy_two, "rollback should change active deployment");

    let rolled_back_status = Spi::get_one_with_args::<String>(
        "SELECT status FROM stopgap.deployment WHERE id = $1",
        &[deploy_three.into()],
    )
    .expect("status lookup for rolled back deployment should succeed")
    .expect("rolled back deployment should exist");
    assert_eq!(
        rolled_back_status, "rolled_back",
        "previously active deployment should be marked rolled_back"
    );

    let reactivated_status = Spi::get_one_with_args::<String>(
        "SELECT status FROM stopgap.deployment WHERE id = $1",
        &[deploy_two.into()],
    )
    .expect("status lookup for reactivated deployment should succeed")
    .expect("reactivated deployment should exist");
    assert_eq!(reactivated_status, "active", "rollback target should be active");

    let live_pointer_hash = pointer_artifact_hash("sg_it_rb_live", "stepper");
    let fn_version_hash = fn_version_artifact_hash(deploy_two, "stepper");
    assert_eq!(
        live_pointer_hash, fn_version_hash,
        "rollback should rematerialize live pointer to target deployment artifact"
    );

    assert!(deploy_one < deploy_two && deploy_two < deploy_three);
}
