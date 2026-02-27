use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use stopgap_cli::{AppError, Command, EXIT_DB_QUERY, OutputMode, StopgapApi, execute_command};

struct MockApi {
    deploy_result: Result<i64>,
    rollback_result: Result<i64>,
    status_result: Result<Option<Value>>,
    deployments_result: Result<Value>,
    diff_result: Result<Value>,
}

impl Default for MockApi {
    fn default() -> Self {
        Self {
            deploy_result: Ok(0),
            rollback_result: Ok(0),
            status_result: Ok(None),
            deployments_result: Ok(json!([])),
            diff_result: Ok(json!({})),
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
    ) -> Result<i64> {
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
    execute_command(
        Command::Deploy {
            env: "prod".to_string(),
            from_schema: "app".to_string(),
            label: Some("v1".to_string()),
            prune: true,
        },
        OutputMode::Json,
        &mut api,
        &mut out,
    )
    .expect("deploy succeeds");

    let payload = parse_json_output(out);
    assert_eq!(payload["command"], "deploy");
    assert_eq!(payload["env"], "prod");
    assert_eq!(payload["from_schema"], "app");
    assert_eq!(payload["deployment_id"], 42);
    assert_eq!(payload["prune"], true);
}

#[test]
fn rollback_json_output_schema_is_stable() {
    let mut api = MockApi { rollback_result: Ok(40), ..Default::default() };
    let mut out = Vec::new();
    execute_command(
        Command::Rollback { env: "prod".to_string(), steps: 2, to_id: Some(40) },
        OutputMode::Json,
        &mut api,
        &mut out,
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
    execute_command(
        Command::Status { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
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
    execute_command(
        Command::Deployments { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
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
    execute_command(
        Command::Diff { env: "prod".to_string(), from_schema: "app".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
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

    let error = execute_command(
        Command::Status { env: "prod".to_string() },
        OutputMode::Json,
        &mut api,
        &mut out,
    )
    .expect_err("status should fail");

    assert!(matches!(error, AppError::DbQuery(_)));
    assert_eq!(error.code(), EXIT_DB_QUERY);
}
