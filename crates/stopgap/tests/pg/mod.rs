use pgrx::prelude::*;
use serde_json::Value;

fn ensure_mock_plts_runtime() {
    Spi::run(
        "
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_language WHERE lanname = 'plts') THEN
                CREATE LANGUAGE plts
                HANDLER plpgsql_call_handler;
            END IF;
        END;
        $$;

        CREATE SCHEMA IF NOT EXISTS plts;

        CREATE TABLE IF NOT EXISTS plts.artifact (
            artifact_hash text PRIMARY KEY,
            source_ts text NOT NULL,
            compiled_js text NOT NULL,
            compiler_opts jsonb NOT NULL
        );

        CREATE OR REPLACE FUNCTION plts.compile_and_store(source_ts text, compiler_opts jsonb)
        RETURNS text
        LANGUAGE plpgsql
        AS $$
        DECLARE
            hash text;
        BEGIN
            hash := 'sha256:' || md5(COALESCE(source_ts, '') || COALESCE(compiler_opts::text, ''));

            INSERT INTO plts.artifact(artifact_hash, source_ts, compiled_js, compiler_opts)
            VALUES (hash, source_ts, source_ts, compiler_opts)
            ON CONFLICT (artifact_hash) DO UPDATE
            SET source_ts = EXCLUDED.source_ts,
                compiled_js = EXCLUDED.compiled_js,
                compiler_opts = EXCLUDED.compiler_opts;

            RETURN hash;
        END;
        $$;
        ",
    )
    .expect("mock plts runtime setup should succeed");
}

fn create_deployable_function(schema: &str, fn_name: &str, source: &str) {
    let sql = format!(
        "
        CREATE OR REPLACE FUNCTION {}.{}(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$ {} $$;
        ",
        crate::quote_ident(schema),
        crate::quote_ident(fn_name),
        source
    );
    Spi::run(sql.as_str()).expect("deployable function should be created");
}

fn pointer_artifact_hash(live_schema: &str, fn_name: &str) -> String {
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

    serde_json::from_str::<Value>(&pointer)
        .expect("live pointer body should be valid json")
        .get("artifact_hash")
        .and_then(Value::as_str)
        .expect("live pointer body should include artifact_hash")
        .to_string()
}

fn fn_version_artifact_hash(deployment_id: i64, fn_name: &str) -> String {
    Spi::get_one_with_args::<String>(
        "
        SELECT artifact_hash
        FROM stopgap.fn_version
        WHERE deployment_id = $1
          AND fn_name = $2
        ",
        &[deployment_id.into(), fn_name.into()],
    )
    .expect("fn_version lookup should succeed")
    .expect("fn_version row should exist")
}

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
fn test_deploy_rejects_overloaded_plts_fns() {
    ensure_mock_plts_runtime();

    Spi::run(
        "
        DROP SCHEMA IF EXISTS sg_it_overload CASCADE;
        DROP SCHEMA IF EXISTS sg_it_overload_live CASCADE;
        CREATE SCHEMA sg_it_overload;
        SELECT set_config('stopgap.live_schema', 'sg_it_overload_live', true);
        CREATE OR REPLACE FUNCTION sg_it_overload.same_name(args jsonb)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        BEGIN RETURN args; END
        $$;
        CREATE OR REPLACE FUNCTION sg_it_overload.same_name(arg int4)
        RETURNS jsonb
        LANGUAGE plts
        AS $$
        BEGIN RETURN jsonb_build_object('arg', arg); END
        $$;
        ",
    )
    .expect("overload setup should succeed");

    Spi::run(
        "
        DO $$
        BEGIN
            PERFORM stopgap.deploy('it_env_overload', 'sg_it_overload', NULL);
            RAISE EXCEPTION 'expected overloaded-function deploy failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('overloaded plts functions' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END;
        $$;
        ",
    )
    .expect("deploy should fail with overloaded-function error");
}

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

#[pg_test]
fn test_deploy_function_is_security_definer() {
    let is_security_definer = Spi::get_one::<bool>(
        "
        SELECT p.prosecdef
        FROM pg_proc p
        WHERE p.oid = 'stopgap.deploy(text, text, text)'::regprocedure
        ",
    )
    .expect("deploy function lookup should succeed")
    .expect("deploy function should exist");

    assert!(is_security_definer, "stopgap.deploy should be SECURITY DEFINER");
}
