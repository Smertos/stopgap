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
