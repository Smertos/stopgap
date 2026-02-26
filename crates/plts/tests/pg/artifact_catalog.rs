#[pg_test]
fn test_compile_and_store_round_trip() {
    let source = "export default (ctx) => ({ ok: true, args: ctx.args })";
    let artifact_hash = Spi::get_one_with_args::<String>(
        "SELECT plts.compile_and_store($1::text, '{}'::jsonb)",
        &[source.into()],
    )
    .expect("compile_and_store query should succeed")
    .expect("compile_and_store should return an artifact hash");

    assert!(artifact_hash.starts_with("sha256:"));

    let artifact =
        Spi::get_one_with_args::<JsonB>("SELECT plts.get_artifact($1)", &[artifact_hash.into()])
            .expect("get_artifact query should succeed")
            .expect("artifact must exist after compile_and_store");

    assert_eq!(
        artifact.0.get("source_ts").and_then(Value::as_str),
        Some(source),
        "stored artifact should preserve source_ts"
    );
    assert!(
        artifact
            .0
            .get("compiled_js")
            .and_then(Value::as_str)
            .is_some_and(|compiled| !compiled.is_empty()),
        "stored artifact should include compiled_js"
    );
}
