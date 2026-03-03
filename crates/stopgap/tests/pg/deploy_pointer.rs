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

#[pg_test]
fn test_deploy_uses_cli_export_metadata_for_pointer() {
    ensure_mock_plts_runtime();

    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS sg_meta_src CASCADE;
        DROP SCHEMA IF EXISTS sg_meta_live CASCADE;
        CREATE SCHEMA sg_meta_src;
        SELECT set_config('stopgap.live_schema', 'sg_meta_live', true);
        SELECT set_config(
            'stopgap.deploy_exports',
            '[
                {
                    "module_path": "admin/users",
                    "export_name": "hello",
                    "function_path": "api.admin.users.hello",
                    "kind": "query"
                }
            ]',
            true
        );
        "#,
    )
    .expect("metadata deploy setup should succeed");

    create_deployable_function(
        "sg_meta_src",
        "hello",
        "BEGIN RETURN jsonb_build_object('ok', true); END",
    );

    let deployment_id =
        Spi::get_one::<i64>("SELECT stopgap.deploy('it_env_meta', 'sg_meta_src', 'meta-v1')")
            .expect("deploy should succeed")
            .expect("deploy should return deployment id");

    let row = Spi::get_one_with_args::<JsonB>(
        "
        SELECT jsonb_build_object(
            'function_path', function_path,
            'module_path', module_path,
            'export_name', export_name,
            'kind', kind
        )
        FROM stopgap.fn_version
        WHERE deployment_id = $1
          AND fn_name = 'hello'
        ",
        &[deployment_id.into()],
    )
    .expect("fn_version metadata lookup should succeed")
    .expect("fn_version metadata row should exist");

    assert_eq!(
        row.0.get("function_path").and_then(|value| value.as_str()),
        Some("api.admin.users.hello")
    );
    assert_eq!(row.0.get("module_path").and_then(|value| value.as_str()), Some("admin/users"));
    assert_eq!(row.0.get("export_name").and_then(|value| value.as_str()), Some("hello"));
    assert_eq!(row.0.get("kind").and_then(|value| value.as_str()), Some("query"));

    let pointer = Spi::get_one::<String>(
        "
        SELECT p.prosrc::text
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sg_meta_live'
          AND p.proname = 'hello'
        ",
    )
    .expect("live pointer lookup should succeed")
    .expect("live pointer should exist");

    let pointer_json: serde_json::Value =
        serde_json::from_str(pointer.as_str()).expect("live pointer should be valid json");
    assert_eq!(pointer_json.get("export").and_then(|value| value.as_str()), Some("hello"));
}
