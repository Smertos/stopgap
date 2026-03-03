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

#[pg_test]
fn test_rollback_rematerializes_multiple_exports_from_same_module() {
    ensure_mock_plts_runtime();

    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS sg_it_rb_multi_src CASCADE;
        DROP SCHEMA IF EXISTS sg_it_rb_multi_live CASCADE;
        CREATE SCHEMA sg_it_rb_multi_src;
        SELECT set_config('stopgap.live_schema', 'sg_it_rb_multi_live', true);
        SELECT set_config(
            'stopgap.deploy_exports',
            '[
                {
                    "module_path": "admin/users",
                    "export_name": "hello",
                    "function_path": "api.admin.users.hello",
                    "kind": "query"
                },
                {
                    "module_path": "admin/users",
                    "export_name": "update",
                    "function_path": "api.admin.users.update",
                    "kind": "mutation"
                }
            ]',
            true
        );
        "#,
    )
    .expect("multi-export rollback setup should succeed");

    create_deployable_function(
        "sg_it_rb_multi_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('version', 'v1', 'fn', 'hello'); END",
    );
    create_deployable_function(
        "sg_it_rb_multi_src",
        "update",
        "BEGIN RETURN jsonb_build_object('version', 'v1', 'fn', 'update'); END",
    );

    let deploy_one = Spi::get_one::<i64>(
        "SELECT stopgap.deploy('it_env_rb_multi', 'sg_it_rb_multi_src', 'multi-v1')",
    )
    .expect("multi-export deploy one should succeed")
    .expect("multi-export deploy one should return id");

    create_deployable_function(
        "sg_it_rb_multi_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('version', 'v2', 'fn', 'hello'); END",
    );
    create_deployable_function(
        "sg_it_rb_multi_src",
        "update",
        "BEGIN RETURN jsonb_build_object('version', 'v2', 'fn', 'update'); END",
    );

    let deploy_two = Spi::get_one::<i64>(
        "SELECT stopgap.deploy('it_env_rb_multi', 'sg_it_rb_multi_src', 'multi-v2')",
    )
    .expect("multi-export deploy two should succeed")
    .expect("multi-export deploy two should return id");

    let rolled_back_to = Spi::get_one::<i64>("SELECT stopgap.rollback('it_env_rb_multi', 1, NULL)")
        .expect("multi-export rollback should succeed")
        .expect("multi-export rollback should return target deployment id");
    assert_eq!(rolled_back_to, deploy_one, "rollback should target first deployment");

    let hello_live_pointer_hash = pointer_artifact_hash("sg_it_rb_multi_live", "hello");
    let hello_v1_hash = fn_version_artifact_hash(deploy_one, "hello");
    assert_eq!(
        hello_live_pointer_hash, hello_v1_hash,
        "rollback should rematerialize hello pointer to first deployment artifact"
    );

    let update_live_pointer_hash = pointer_artifact_hash("sg_it_rb_multi_live", "update");
    let update_v1_hash = fn_version_artifact_hash(deploy_one, "update");
    assert_eq!(
        update_live_pointer_hash, update_v1_hash,
        "rollback should rematerialize update pointer to first deployment artifact"
    );

    let exported_paths = Spi::get_one_with_args::<i64>(
        "
        SELECT COUNT(*)::bigint
        FROM stopgap.fn_version
        WHERE deployment_id = $1
          AND function_path IN ('api.admin.users.hello', 'api.admin.users.update')
        ",
        &[deploy_one.into()],
    )
    .expect("function-path metadata lookup should succeed")
    .expect("function-path metadata count should return a row");
    assert_eq!(exported_paths, 2, "deployment should retain both module export paths");

    assert!(deploy_one < deploy_two, "second deploy id should be newer");
}
