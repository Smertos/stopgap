#[pg_test]
fn test_deploy_security_model_sets_live_fn_acl() {
    ensure_mock_plts_runtime();

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_it_sec_src CASCADE;
        DROP SCHEMA IF EXISTS sg_it_sec_live CASCADE;
        CREATE SCHEMA sg_it_sec_src;
        SELECT set_config('stopgap.live_schema', 'sg_it_sec_live', true);
        ",
    )
    .expect("security setup should succeed");

    create_deployable_function(
        "sg_it_sec_src",
        "secure_fn",
        "BEGIN RETURN jsonb_build_object('ok', true); END",
    );

    let deployment_id =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_sec', 'sg_it_sec_src', 'sec')")
            .expect("security deploy should succeed")
            .expect("security deploy should return deployment id");
    assert!(deployment_id > 0);

    let owner = Spi::get_one::<String>(
        "
        SELECT pg_get_userbyid(p.proowner)::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sg_it_sec_live'
          AND p.proname = 'secure_fn'
          AND p.prorettype = 'jsonb'::regtype::oid
          AND array_length(p.proargtypes::oid[], 1) = 1
          AND p.proargtypes[0] = 'jsonb'::regtype::oid
        ",
    )
    .expect("live function owner lookup should succeed")
    .expect("live function should exist");
    assert_eq!(owner, crate::STOPGAP_OWNER_ROLE);

    let app_can_execute = Spi::get_one::<bool>(
        "
        SELECT has_function_privilege('app_user', 'sg_it_sec_live.secure_fn(jsonb)', 'EXECUTE')
        ",
    )
    .expect("live function execute privilege check should succeed")
    .expect("execute privilege check should return a row");
    assert!(app_can_execute, "app_user should have execute on live pointer function");
}
