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
