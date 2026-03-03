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

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91001,
            'hello',
            'call_fn_src',
            'call_fn_live',
            'hello',
            'api.users.hello',
            'users',
            'hello',
            'mutation',
            'sha256:callfn-hello'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO UPDATE
        SET artifact_hash = EXCLUDED.artifact_hash,
            function_path = EXCLUDED.function_path,
            module_path = EXCLUDED.module_path,
            export_name = EXCLUDED.export_name,
            live_fn_name = EXCLUDED.live_fn_name,
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

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91002,
            'known',
            'call_fn_src',
            'call_fn_live',
            'known',
            'api.users.known',
            'users',
            'known',
            'mutation',
            'sha256:known'
        )
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

#[pg_test]
fn test_call_fn_routes_by_full_function_path() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_path_live CASCADE;
        CREATE SCHEMA call_fn_path_live;

        CREATE OR REPLACE FUNCTION call_fn_path_live.route_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE sql
        AS $$
            SELECT jsonb_build_object('route', 'impl', 'args', args)
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('call_fn_path_env', 'call_fn_path_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91003, 'call_fn_path_env', 'call-fn-path', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91003,
            updated_at = now()
        WHERE env = 'call_fn_path_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91003,
            'legacy_hello',
            'call_fn_src',
            'call_fn_path_live',
            'route_impl',
            'api.users.hello',
            'users',
            'hello',
            'mutation',
            'sha256:path-route'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'call_fn_path_env', true);
        "#,
    )
    .expect("test should prepare function-path routing fixtures");

    let out =
        Spi::get_one::<JsonB>(r#"SELECT stopgap.call_fn('api.users.hello', '{"id":7}'::jsonb)"#)
            .expect("path-routed call_fn execution should succeed")
            .expect("path-routed call_fn should return jsonb");

    assert_eq!(
        out.0.get("route").and_then(|value| value.as_str()),
        Some("impl"),
        "call_fn should route via function_path metadata"
    );
    assert_eq!(
        out.0.get("args").and_then(|value| value.get("id")).and_then(|value| value.as_i64()),
        Some(7),
        "call_fn should pass args to path-routed live function"
    );
}

#[pg_test]
fn test_call_fn_rejects_path_with_invalid_segment_chars() {
    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.hello-world', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn invalid-path failure for invalid chars';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('invalid path' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("invalid path segments should fail with clear error");
}

#[pg_test]
fn test_call_fn_reports_ambiguous_legacy_route() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_legacy_live CASCADE;
        CREATE SCHEMA call_fn_legacy_live;

        CREATE OR REPLACE FUNCTION call_fn_legacy_live.first_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE sql
        AS $$
            SELECT jsonb_build_object('impl', 'first', 'args', args)
        $$;

        CREATE OR REPLACE FUNCTION call_fn_legacy_live.second_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE sql
        AS $$
            SELECT jsonb_build_object('impl', 'second', 'args', args)
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('legacy_ambiguous_env', 'call_fn_legacy_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91004, 'legacy_ambiguous_env', 'call-fn-legacy', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91004,
            updated_at = now()
        WHERE env = 'legacy_ambiguous_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES
        (
            91004,
            'legacy_target',
            'legacy_src_a',
            'call_fn_legacy_live',
            'first_impl',
            NULL,
            NULL,
            NULL,
            'mutation',
            'sha256:legacy-a'
        ),
        (
            91004,
            'legacy_target',
            'legacy_src_b',
            'call_fn_legacy_live',
            'second_impl',
            NULL,
            NULL,
            NULL,
            'mutation',
            'sha256:legacy-b'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'legacy_ambiguous_env', true);
        "#,
    )
    .expect("test should prepare ambiguous legacy routing fixtures");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.legacy.legacy_target', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn ambiguous-route failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('ambiguous legacy route metadata' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("ambiguous legacy routes should fail with clear error");
}

#[pg_test]
fn test_call_fn_surfaces_invalid_args_semantics() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_invalid_args_live CASCADE;
        CREATE SCHEMA call_fn_invalid_args_live;

        CREATE OR REPLACE FUNCTION call_fn_invalid_args_live.validate_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'stopgap args validation failed at $.id: missing required property';
        END;
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('call_fn_invalid_args_env', 'call_fn_invalid_args_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91005, 'call_fn_invalid_args_env', 'call-fn-invalid-args', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91005,
            updated_at = now()
        WHERE env = 'call_fn_invalid_args_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91005,
            'validate_impl',
            'call_fn_src',
            'call_fn_invalid_args_live',
            'validate_impl',
            'api.users.validate',
            'users',
            'validate',
            'query',
            'sha256:invalid-args'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'call_fn_invalid_args_env', true);
        "#,
    )
    .expect("test should prepare invalid-args route fixtures");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.validate', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn invalid-args failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('invalid args for ''api.users.validate''' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
                IF POSITION('args validation failed at $.id' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("invalid args failures should surface path-aware call_fn semantics");
}

#[pg_test]
fn test_call_fn_surfaces_wrong_wrapper_mode_semantics() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_wrapper_mode_live CASCADE;
        CREATE SCHEMA call_fn_wrapper_mode_live;

        CREATE OR REPLACE FUNCTION call_fn_wrapper_mode_live.readonly_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'db.exec is disabled for stopgap.query handlers';
        END;
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('call_fn_wrapper_mode_env', 'call_fn_wrapper_mode_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91006, 'call_fn_wrapper_mode_env', 'call-fn-wrapper-mode', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91006,
            updated_at = now()
        WHERE env = 'call_fn_wrapper_mode_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91006,
            'readonly_impl',
            'call_fn_src',
            'call_fn_wrapper_mode_live',
            'readonly_impl',
            'api.users.readonly',
            'users',
            'readonly',
            'query',
            'sha256:wrapper-mode'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'call_fn_wrapper_mode_env', true);
        "#,
    )
    .expect("test should prepare wrapper-mode route fixtures");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.readonly', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn wrapper-mode failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('wrong wrapper mode for ''api.users.readonly''' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
                IF POSITION('db.exec is disabled for stopgap.query handlers' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("wrapper-mode failures should surface explicit call_fn semantics");
}

#[pg_test]
fn test_call_fn_preserves_routed_guardrail_failure_detail() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_guardrail_live CASCADE;
        CREATE SCHEMA call_fn_guardrail_live;

        CREATE OR REPLACE FUNCTION call_fn_guardrail_live.guardrail_impl(args jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'plts.max_sql_bytes exceeded: SQL input 8192 bytes > 4096 bytes';
        END;
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('call_fn_guardrail_env', 'call_fn_guardrail_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (91007, 'call_fn_guardrail_env', 'call-fn-guardrail', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO NOTHING;

        UPDATE stopgap.environment
        SET active_deployment_id = 91007,
            updated_at = now()
        WHERE env = 'call_fn_guardrail_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            module_path,
            export_name,
            kind,
            artifact_hash
        )
        VALUES (
            91007,
            'guardrail_impl',
            'call_fn_src',
            'call_fn_guardrail_live',
            'guardrail_impl',
            'api.users.guardrail',
            'users',
            'guardrail',
            'query',
            'sha256:guardrail'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO NOTHING;

        SELECT set_config('stopgap.default_env', 'call_fn_guardrail_env', true);
        "#,
    )
    .expect("test should prepare routed guardrail fixtures");

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('api.users.guardrail', '{}'::jsonb);
            RAISE EXCEPTION 'expected call_fn routed guardrail failure';
        EXCEPTION
            WHEN OTHERS THEN
                IF POSITION('execution failed for ''api.users.guardrail''' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
                IF POSITION('plts.max_sql_bytes exceeded' IN SQLERRM) = 0 THEN
                    RAISE;
                END IF;
        END
        $$;
        "#,
    )
    .expect("guardrail failures should preserve path-aware call_fn error context and detail");
}
