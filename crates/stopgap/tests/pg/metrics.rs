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
    let _deploy_latency_last = after
        .0
        .get("deploy")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("deploy.latency_ms.last should be present");
    let _rollback_latency_last = after
        .0
        .get("rollback")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("rollback.latency_ms.last should be present");
    let _diff_latency_last = after
        .0
        .get("diff")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("diff.latency_ms.last should be present");
    let _diff_error_classes = after
        .0
        .get("diff")
        .and_then(|value| value.get("error_classes"))
        .and_then(Value::as_object)
        .expect("diff.error_classes should be an object");

    assert!(after_calls > before_calls, "deploy.calls should increase after deploy");
}

#[pg_test]
fn test_metrics_call_fn_route_counts_and_error_classes() {
    Spi::run(
        r#"
        DROP SCHEMA IF EXISTS call_fn_metrics_live CASCADE;
        CREATE SCHEMA call_fn_metrics_live;

        CREATE OR REPLACE FUNCTION call_fn_metrics_live.hello(args jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN jsonb_build_object('ok', true, 'args', args);
        END;
        $$;

        INSERT INTO stopgap.environment (env, live_schema, active_deployment_id)
        VALUES ('call_fn_metrics_env', 'call_fn_metrics_live', NULL)
        ON CONFLICT (env) DO UPDATE
        SET live_schema = EXCLUDED.live_schema,
            active_deployment_id = NULL,
            updated_at = now();

        INSERT INTO stopgap.deployment (id, env, label, source_schema, status, manifest)
        VALUES (92001, 'call_fn_metrics_env', 'call-fn-metrics', 'call_fn_src', 'active', '{"functions":[]}'::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET env = EXCLUDED.env,
            label = EXCLUDED.label,
            source_schema = EXCLUDED.source_schema,
            status = EXCLUDED.status,
            manifest = EXCLUDED.manifest;

        UPDATE stopgap.environment
        SET active_deployment_id = 92001,
            updated_at = now()
        WHERE env = 'call_fn_metrics_env';

        INSERT INTO stopgap.fn_version (
            deployment_id,
            fn_name,
            fn_schema,
            live_fn_schema,
            live_fn_name,
            function_path,
            kind,
            module_path,
            export_name,
            artifact_hash
        )
        VALUES (
            92001,
            'hello',
            'call_fn_src',
            'call_fn_metrics_live',
            'hello',
            'api.users.hello',
            'query',
            'users.ts',
            'hello',
            'sha256:call-fn-metrics-route'
        )
        ON CONFLICT (deployment_id, fn_schema, fn_name) DO UPDATE
        SET function_path = EXCLUDED.function_path,
            live_fn_schema = EXCLUDED.live_fn_schema,
            live_fn_name = EXCLUDED.live_fn_name,
            kind = EXCLUDED.kind,
            module_path = EXCLUDED.module_path,
            export_name = EXCLUDED.export_name,
            artifact_hash = EXCLUDED.artifact_hash;

        SELECT set_config('stopgap.default_env', 'call_fn_metrics_env', true);
        "#,
    )
    .expect("call_fn metrics fixtures should be created");

    let before = Spi::get_one::<JsonB>("SELECT stopgap.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let before_calls = before
        .0
        .get("call_fn")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("call_fn.calls should be present");
    let before_exact_routes = before
        .0
        .get("call_fn")
        .and_then(|value| value.get("route_counts"))
        .and_then(|value| value.get("exact"))
        .and_then(Value::as_u64)
        .expect("call_fn.route_counts.exact should be present");
    let before_validation_errors = before
        .0
        .get("call_fn")
        .and_then(|value| value.get("error_classes"))
        .and_then(|value| value.get("validation"))
        .and_then(Value::as_u64)
        .expect("call_fn.error_classes.validation should be present");

    let ok_result =
        Spi::get_one::<JsonB>(r#"SELECT stopgap.call_fn('api.users.hello', '{"id":42}'::jsonb)"#)
            .expect("call_fn execution should succeed")
            .expect("call_fn should return jsonb");
    assert_eq!(ok_result.0.get("ok").and_then(Value::as_bool), Some(true));

    Spi::run(
        r#"
        DO $$
        BEGIN
            PERFORM stopgap.call_fn('bad.path', '{}'::jsonb);
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
    .expect("invalid call_fn path should fail with validation error");

    let after = Spi::get_one::<JsonB>("SELECT stopgap.metrics()")
        .expect("metrics query should succeed")
        .expect("metrics row should exist");
    let after_calls = after
        .0
        .get("call_fn")
        .and_then(|value| value.get("calls"))
        .and_then(Value::as_u64)
        .expect("call_fn.calls should be present");
    let after_exact_routes = after
        .0
        .get("call_fn")
        .and_then(|value| value.get("route_counts"))
        .and_then(|value| value.get("exact"))
        .and_then(Value::as_u64)
        .expect("call_fn.route_counts.exact should be present");
    let after_validation_errors = after
        .0
        .get("call_fn")
        .and_then(|value| value.get("error_classes"))
        .and_then(|value| value.get("validation"))
        .and_then(Value::as_u64)
        .expect("call_fn.error_classes.validation should be present");
    let _call_fn_latency_last = after
        .0
        .get("call_fn")
        .and_then(|value| value.get("latency_ms"))
        .and_then(|value| value.get("last"))
        .and_then(Value::as_u64)
        .expect("call_fn.latency_ms.last should be present");

    assert!(after_calls >= before_calls + 2, "call_fn.calls should increase for success + error");
    assert!(after_exact_routes > before_exact_routes, "exact route count should increase");
    assert!(
        after_validation_errors > before_validation_errors,
        "validation error class count should increase"
    );
}
