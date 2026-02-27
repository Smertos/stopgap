#[pg_test]
fn test_deploy_materializes_pointer_import_map_for_live_functions() {
    ensure_mock_plts_runtime();

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_it_src_import_map CASCADE;
        DROP SCHEMA IF EXISTS sg_it_live_import_map CASCADE;
        CREATE SCHEMA sg_it_src_import_map;
        SELECT set_config('stopgap.live_schema', 'sg_it_live_import_map', true);
        ",
    )
    .expect("import-map integration setup should succeed");

    create_deployable_function(
        "sg_it_src_import_map",
        "alpha",
        "BEGIN RETURN jsonb_build_object('fn', 'alpha'); END",
    );
    create_deployable_function(
        "sg_it_src_import_map",
        "beta",
        "BEGIN RETURN jsonb_build_object('fn', 'beta'); END",
    );

    let deployment_id = Spi::get_one::<i64>(
        "SELECT stopgap.deploy('it_env_import_map', 'sg_it_src_import_map', 'v1')",
    )
    .expect("deploy should succeed")
    .expect("deploy should return deployment id");

    let alpha_pointer = pointer_body_json("sg_it_live_import_map", "alpha");
    let alpha_hash = fn_version_artifact_hash(deployment_id, "alpha");
    let beta_hash = fn_version_artifact_hash(deployment_id, "beta");
    let expected_alpha = format!("plts+artifact:{alpha_hash}");
    let expected_beta = format!("plts+artifact:{beta_hash}");

    assert_eq!(
        alpha_pointer
            .get("import_map")
            .and_then(|v| v.get("@stopgap/sg_it_src_import_map/alpha"))
            .and_then(Value::as_str),
        Some(expected_alpha.as_str())
    );
    assert_eq!(
        alpha_pointer
            .get("import_map")
            .and_then(|v| v.get("@stopgap/sg_it_src_import_map/beta"))
            .and_then(Value::as_str),
        Some(expected_beta.as_str())
    );
}

fn pointer_body_json(live_schema: &str, fn_name: &str) -> Value {
    let pointer = Spi::get_one_with_args::<String>(
        "
        SELECT p.prosrc::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1
          AND p.proname = $2
          AND p.prorettype = 'jsonb'::regtype::oid
          AND array_length(p.proargtypes::oid[], 1) = 1
          AND p.proargtypes[0] = 'jsonb'::regtype::oid
        ",
        &[live_schema.into(), fn_name.into()],
    )
    .expect("live pointer function lookup should succeed")
    .expect("live pointer function should exist");

    serde_json::from_str::<Value>(&pointer).expect("live pointer body should be valid json")
}
