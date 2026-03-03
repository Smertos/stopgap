use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use stopgap_cli::{
    AppError, Command, EXIT_DB_QUERY, EXIT_PROJECT_LAYOUT, OutputMode, StopgapApi,
    discover_stopgap_exports, discover_stopgap_modules, execute_command_with_project_root,
};

struct MockApi {
    deploy_result: Result<i64>,
    rollback_result: Result<i64>,
    status_result: Result<Option<Value>>,
    deployments_result: Result<Value>,
    diff_result: Result<Value>,
    deploy_exports_json: Option<String>,
}

impl Default for MockApi {
    fn default() -> Self {
        Self {
            deploy_result: Ok(0),
            rollback_result: Ok(0),
            status_result: Ok(None),
            deployments_result: Ok(json!([])),
            diff_result: Ok(json!({})),
            deploy_exports_json: None,
        }
    }
}

impl StopgapApi for MockApi {
    fn deploy(
        &mut self,
        _env: &str,
        _from_schema: &str,
        _label: Option<&str>,
        _prune: bool,
        deploy_exports_json: Option<&str>,
    ) -> Result<i64> {
        self.deploy_exports_json = deploy_exports_json.map(str::to_string);
        self.deploy_result.as_ref().map(|value| *value).map_err(clone_error)
    }

    fn rollback(&mut self, _env: &str, _steps: i32, _to_id: Option<i64>) -> Result<i64> {
        self.rollback_result.as_ref().map(|value| *value).map_err(clone_error)
    }

    fn status(&mut self, _env: &str) -> Result<Option<Value>> {
        self.status_result.as_ref().map(|value| value.clone()).map_err(clone_error)
    }

    fn deployments(&mut self, _env: &str) -> Result<Value> {
        self.deployments_result.as_ref().map(|value| value.clone()).map_err(clone_error)
    }

    fn diff(&mut self, _env: &str, _from_schema: &str) -> Result<Value> {
        self.diff_result.as_ref().map(|value| value.clone()).map_err(clone_error)
    }
}

fn clone_error(error: &anyhow::Error) -> anyhow::Error {
    anyhow!(error.to_string())
}

fn parse_json_output(buffer: Vec<u8>) -> Value {
    serde_json::from_slice(&buffer).expect("valid json output")
}

#[test]
fn deploy_json_output_schema_is_stable() {
    let mut api = MockApi { deploy_result: Ok(42), ..Default::default() };
    let mut out = Vec::new();
    let project = create_project_root("deploy_json_output_schema_is_stable");
    write_file(
        project.join("stopgap/coolApi.ts"),
        "export const list = query(v.object({}), async () => []);",
    );
    write_file(
        project.join("stopgap/admin/users.ts"),
        "export const get = query(v.object({}), async () => []);\nexport const set = mutation(v.object({}), async () => ({}));",
    );
    execute_command_with_project_root(
        Command::Deploy {
            env: "prod".to_string(),
            from_schema: "app".to_string(),
            label: Some("v1".to_string()),
            prune: true,
        },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project,
    )
    .expect("deploy succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "deploy");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["from_schema"], "app");
    assert_eq!(payload["source_root"], "stopgap");
    assert_eq!(payload["module_count"], 2);
    assert_eq!(payload["module_paths"][0], "api.admin.users");
    assert_eq!(payload["module_paths"][1], "api.coolApi");
    assert_eq!(payload["function_count"], 3);
    assert_eq!(payload["function_paths"][0], "api.admin.users.get");
    assert_eq!(payload["function_paths"][1], "api.admin.users.set");
    assert_eq!(payload["function_paths"][2], "api.coolApi.list");
    assert_eq!(payload["deployment_id"], 42);
    assert_eq!(payload["prune"], true);

    let deploy_exports = api
        .deploy_exports_json
        .as_ref()
        .map(|value| {
            serde_json::from_str::<Value>(value).expect("deploy exports json should decode")
        })
        .expect("deploy exports json should be forwarded to api layer");
    assert_eq!(deploy_exports.as_array().map(|items| items.len()), Some(3));
    assert_eq!(deploy_exports[0]["function_path"], "api.admin.users.get");
    assert_eq!(deploy_exports[1]["kind"], "mutation");
}

#[test]
fn rollback_json_output_schema_is_stable() {
    let mut api = MockApi { rollback_result: Ok(40), ..Default::default() };
    let mut out = Vec::new();
    execute_command_with_project_root(
        Command::Rollback { env: "prod".to_string(), steps: 2, to_id: Some(40) },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project_root_for_non_deploy_tests(),
    )
    .expect("rollback succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "rollback");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["steps"], 2);
    assert_eq!(payload["to_id"], 40);
    assert_eq!(payload["deployment_id"], 40);
}

#[test]
fn status_json_output_schema_is_stable() {
    let mut api = MockApi {
        status_result: Ok(Some(json!({"active_deployment_id": 7, "env": "prod"}))),
        ..Default::default()
    };
    let mut out = Vec::new();
    execute_command_with_project_root(
        Command::Status { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project_root_for_non_deploy_tests(),
    )
    .expect("status succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "status");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["status"]["active_deployment_id"], 7);
}

#[test]
fn deployments_json_output_schema_is_stable() {
    let mut api = MockApi {
        deployments_result: Ok(json!([
            {"id": 5, "status": "active"},
            {"id": 4, "status": "rolled_back"}
        ])),
        ..Default::default()
    };
    let mut out = Vec::new();
    execute_command_with_project_root(
        Command::Deployments { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project_root_for_non_deploy_tests(),
    )
    .expect("deployments succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "deployments");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["count"], 2);
    assert!(payload["deployments"].is_array());
}

#[test]
fn diff_json_output_schema_is_stable() {
    let mut api = MockApi {
        diff_result: Ok(json!({"added": ["new_fn"], "removed": []})),
        ..Default::default()
    };
    let mut out = Vec::new();
    execute_command_with_project_root(
        Command::Diff { env: "prod".to_string(), from_schema: "app".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project_root_for_non_deploy_tests(),
    )
    .expect("diff succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "diff");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["from_schema"], "app");
    assert_eq!(payload["diff"]["added"][0], "new_fn");
}

#[test]
fn db_query_failures_use_non_zero_query_exit_code() {
    let mut api = MockApi { status_result: Err(anyhow!("query failed")), ..Default::default() };
    let mut out = Vec::new();

    let error = execute_command_with_project_root(
        Command::Status { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project_root_for_non_deploy_tests(),
    )
    .expect_err("status should fail");

    assert!(matches!(error, AppError::DbQuery(_)));
    assert_eq!(error.code(), EXIT_DB_QUERY);
}

#[test]
fn deploy_fails_fast_when_stopgap_source_root_missing() {
    let mut api = MockApi { deploy_result: Ok(42), ..Default::default() };
    let mut out = Vec::new();
    let project = create_project_root("deploy_fails_fast_when_stopgap_source_root_missing");

    let error = execute_command_with_project_root(
        Command::Deploy {
            env: "prod".to_string(),
            from_schema: "app".to_string(),
            label: None,
            prune: false,
        },
        OutputMode::Json,
        &mut api,
        &mut out,
        &project,
    )
    .expect_err("deploy should fail if stopgap/ is missing");

    assert!(matches!(error, AppError::ProjectLayout(_)));
    assert_eq!(error.code(), EXIT_PROJECT_LAYOUT);
    assert!(
        error.to_string().contains("project not initialized: expected `stopgap/` directory"),
        "error message should include init guidance"
    );
}

#[test]
fn discover_stopgap_modules_normalizes_paths_deterministically() {
    let project =
        create_project_root("discover_stopgap_modules_normalizes_paths_deterministically");
    write_file(
        project.join("stopgap/coolApi.ts"),
        "export const cool = query(v.object({}), async () => []);",
    );
    write_file(
        project.join("stopgap/admin/users.ts"),
        "export const list = mutation(v.object({}), async () => ({}));",
    );
    write_file(project.join("stopgap/admin/types.d.ts"), "export type T = string;");
    write_file(project.join("stopgap/README.md"), "not a module");

    let modules = discover_stopgap_modules(&project).expect("module discovery should succeed");
    assert_eq!(modules, vec!["api.admin.users", "api.coolApi"]);
}

#[test]
fn discover_stopgap_exports_finds_multi_export_handlers() {
    let project = create_project_root("discover_stopgap_exports_finds_multi_export_handlers");
    write_file(
        project.join("stopgap/admin/users.ts"),
        "export const list = query(v.object({}), async () => []);\nexport const create = mutation(v.object({}), async () => ({}));",
    );

    let exports = discover_stopgap_exports(&project).expect("export discovery should succeed");
    let paths = exports.into_iter().map(|item| item.function_path).collect::<Vec<_>>();
    assert_eq!(paths, vec!["api.admin.users.create", "api.admin.users.list"]);
}

#[test]
fn discover_stopgap_exports_rejects_non_wrapper_named_exports() {
    let project = create_project_root("discover_stopgap_exports_rejects_non_wrapper_named_exports");
    write_file(
        project.join("stopgap/users.ts"),
        "export const helper = 1;\nexport const list = query(v.object({}), async () => []);",
    );

    let error = discover_stopgap_exports(&project).expect_err("non-wrapper exports should fail");
    assert!(error.to_string().contains("exports non-wrapper symbols"));
    assert!(error.to_string().contains("helper"));
}

fn project_root_for_non_deploy_tests() -> PathBuf {
    PathBuf::from(".")
}

fn create_project_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic enough for test tempdirs")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("stopgap-cli-{test_name}-{nanos}"));
    fs::create_dir_all(&root).expect("temp project dir should be created");
    root
}

fn write_file(path: PathBuf, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent dir should exist");
    }
    fs::write(path, content).expect("file should be written");
}
