#[pg_test]
fn test_call_fn_invokes_active_route() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_live CASCADE;
        CREATE SCHEMA call_fn_live;

        CREATE OR REPLACE FUNCTION call_fn_live.hello(args jsonb)
        RETURNS jsonb
        LANGUAGE sql
        AS $$
            SELECT jsonb_build_object('ok', true, 'args', args)
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('prod', 'call_fn_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91001, 'prod', 'call-fn', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91001,
            updated_at = now()
        WHERE env = 'prod';

        INSERT INTO stopgap.fn_version (deployment_id, fn_name, fn_schema, live_fn_schema, kind, artifact_hash)
        VALUES (91001, 'hello', 'call_fn_src', 'call_fn_live', 'mutation', 'sha256:callfn-hello')
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO UPDATE
        SET artifact_hash = EXCLUDED.artifact_hash,
            live_fn_schema = EXCLUDED.live_fn_schema,
            kind = EXCLUDED.kind;
        "#,
    )
    .expect("call_fn route fixtures should be created");

    let out =
        Spi::get_one::<JsonB>(r#"SELECT stopgap.call_fn('api.users.hello', '{"id":1}'::jsonb)"#)
            .expect("call_fn execution should succeed")
            .expect("call_fn should return jsonb");

    assert_eq!(
        out.0.get("ok").and_then(|value| value.as_bool()),
        Some(true),
        "call_fn should execute target live function"
    );
    assert_eq!(
        out.0.get("args").and_then(|value| value.get("id")).and_then(|value| value.as_i64()),
        Some(1),
        "call_fn should pass args payload through to the routed function"
    );
}

#[pg_test]
fn test_call_fn_rejects_invalid_path() {
    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('hello', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn invalid-path failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('invalid path' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("invalid path should fail with clear error");
}

#[pg_test]
fn test_call_fn_reports_missing_environment() {
    Spi::run("SELECT set_config('stopgap.default_env', 'missing_env_for_call_fn', true)")
        .expect("test should set default env override");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.hello', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn missing-environment failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('missing deployment environment' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("missing environment should fail with clear error");
}

#[pg_test]
fn test_call_fn_reports_missing_active_deployment() {
    Spi::run(
        r#"
        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('no_active_call_fn', 'call_fn_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();
        SELECT set_config('stopgap.default_env', 'no_active_call_fn', true);
        "#,
    )
    .expect("test should create env without active deployment");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.hello', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn missing-active-deployment failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('has no active deployment' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("missing active deployment should fail with clear error");
}

#[pg_test]
fn test_call_fn_reports_unknown_path() {
    Spi::run(
        r#"
        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('unknown_path_env', 'call_fn_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91002, 'unknown_path_env', 'call-fn', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91002,
            updated_at = now()
        WHERE env = 'unknown_path_env';

        INSERT INTO stopgap.fn_version (deployment_id, fn_name, fn_schema, live_fn_schema, kind, artifact_hash)
        VALUES (91002, 'known', 'call_fn_src', 'call_fn_live', 'mutation', 'sha256:known')
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'unknown_path_env', true);
        "#,
    )
    .expect("test should prepare unknown path fixtures");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.missing', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn unknown-path failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('unknown path' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("unknown path should fail with clear error");
}
