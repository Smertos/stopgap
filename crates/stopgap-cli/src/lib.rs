use std::{fmt, io::Write};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use postgres::{Client, NoTls, Row};
use serde_json::{Value, json};

pub const EXIT_DB_CONNECT: u8 = 10;
pub const EXIT_DB_QUERY: u8 = 11;
pub const EXIT_RESPONSE_DECODE: u8 = 12;
pub const EXIT_OUTPUT_FORMAT: u8 = 13;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Human,
    Json,
}

impl fmt::Display for OutputMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Human => write!(f, "human"),
            Self::Json => write!(f, "json"),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "stopgap", version, about = "Stopgap deployment CLI")]
pub struct Cli {
    #[arg(long, env = "STOPGAP_DB")]
    pub db: String,

    #[arg(long, value_enum, default_value_t = OutputMode::Human)]
    pub output: OutputMode,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    Deploy {
        #[arg(long, default_value = "prod")]
        env: String,
        #[arg(long = "from-schema")]
        from_schema: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        prune: bool,
    },
    Rollback {
        #[arg(long, default_value = "prod")]
        env: String,
        #[arg(long, default_value_t = 1)]
        steps: i32,
        #[arg(long = "to")]
        to_id: Option<i64>,
    },
    Status {
        #[arg(long, default_value = "prod")]
        env: String,
    },
    Deployments {
        #[arg(long, default_value = "prod")]
        env: String,
    },
    Diff {
        #[arg(long, default_value = "prod")]
        env: String,
        #[arg(long = "from-schema")]
        from_schema: String,
    },
}

#[derive(Debug)]
pub enum AppError {
    DbConnect(anyhow::Error),
    DbQuery(anyhow::Error),
    Decode(anyhow::Error),
    Print(anyhow::Error),
}

impl AppError {
    pub fn code(&self) -> u8 {
        match self {
            Self::DbConnect(_) => EXIT_DB_CONNECT,
            Self::DbQuery(_) => EXIT_DB_QUERY,
            Self::Decode(_) => EXIT_RESPONSE_DECODE,
            Self::Print(_) => EXIT_OUTPUT_FORMAT,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DbConnect(err) => write!(f, "database connection failed: {err:#}"),
            Self::DbQuery(err) => write!(f, "database command failed: {err:#}"),
            Self::Decode(err) => write!(f, "invalid database response: {err:#}"),
            Self::Print(err) => write!(f, "failed to print output: {err:#}"),
        }
    }
}

pub trait StopgapApi {
    fn deploy(
        &mut self,
        env: &str,
        from_schema: &str,
        label: Option<&str>,
        prune: bool,
    ) -> Result<i64>;

    fn rollback(&mut self, env: &str, steps: i32, to_id: Option<i64>) -> Result<i64>;

    fn status(&mut self, env: &str) -> Result<Option<Value>>;

    fn deployments(&mut self, env: &str) -> Result<Value>;

    fn diff(&mut self, env: &str, from_schema: &str) -> Result<Value>;
}

pub struct PgStopgapApi {
    client: Client,
}

impl PgStopgapApi {
    pub fn connect(db: &str) -> std::result::Result<Self, AppError> {
        let client = Client::connect(db, NoTls).map_err(|err| AppError::DbConnect(err.into()))?;
        Ok(Self { client })
    }
}

impl StopgapApi for PgStopgapApi {
    fn deploy(
        &mut self,
        env: &str,
        from_schema: &str,
        label: Option<&str>,
        prune: bool,
    ) -> Result<i64> {
        let mut tx = self.client.build_transaction().start()?;
        let prune_setting = if prune { "on" } else { "off" };
        tx.batch_execute(&format!("SET LOCAL stopgap.prune = '{prune_setting}'"))?;
        let row = tx.query_one(
            "SELECT stopgap.deploy($1, $2, $3) AS deployment_id",
            &[&env, &from_schema, &label],
        )?;
        tx.commit()?;
        Ok(row.get("deployment_id"))
    }

    fn rollback(&mut self, env: &str, steps: i32, to_id: Option<i64>) -> Result<i64> {
        let row = self.client.query_one(
            "SELECT stopgap.rollback($1, $2, $3) AS deployment_id",
            &[&env, &steps, &to_id],
        )?;
        Ok(row.get("deployment_id"))
    }

    fn status(&mut self, env: &str) -> Result<Option<Value>> {
        let row = self.client.query_one("SELECT stopgap.status($1) AS status", &[&env])?;
        read_json_column(&row, "status")
    }

    fn deployments(&mut self, env: &str) -> Result<Value> {
        let row =
            self.client.query_one("SELECT stopgap.deployments($1) AS deployments", &[&env])?;
        read_required_json_column(&row, "deployments")
    }

    fn diff(&mut self, env: &str, from_schema: &str) -> Result<Value> {
        let row =
            self.client.query_one("SELECT stopgap.diff($1, $2) AS diff", &[&env, &from_schema])?;
        read_required_json_column(&row, "diff")
    }
}

pub fn run(cli: Cli, writer: &mut dyn Write) -> std::result::Result<(), AppError> {
    let mut api = PgStopgapApi::connect(&cli.db)?;
    execute_command(cli.command, cli.output, &mut api, writer)
}

pub fn execute_command(
    command: Command,
    output: OutputMode,
    api: &mut dyn StopgapApi,
    writer: &mut dyn Write,
) -> std::result::Result<(), AppError> {
    match command {
        Command::Deploy { env, from_schema, label, prune } => {
            let deployment_id = api
                .deploy(&env, &from_schema, label.as_deref(), prune)
                .map_err(AppError::DbQuery)?;
            let payload = json!({
                "command": "deploy",
                "env": env,
                "from_schema": from_schema,
                "deployment_id": deployment_id,
                "prune": prune,
            });
            print_payload(output, payload, writer, || {
                format!(
                    "deployed env={} from_schema={} deployment_id={} prune={}",
                    env, from_schema, deployment_id, prune
                )
            })
        }
        Command::Rollback { env, steps, to_id } => {
            let deployment_id = api.rollback(&env, steps, to_id).map_err(AppError::DbQuery)?;
            let payload = json!({
                "command": "rollback",
                "env": env,
                "steps": steps,
                "to_id": to_id,
                "deployment_id": deployment_id,
            });
            print_payload(output, payload, writer, || {
                format!(
                    "rolled back env={} target_deployment_id={} steps={}{}",
                    env,
                    deployment_id,
                    steps,
                    to_id.map(|value| format!(" to_id={value}")).unwrap_or_default()
                )
            })
        }
        Command::Status { env } => {
            let status = api.status(&env).map_err(AppError::DbQuery)?;
            let payload = json!({
                "command": "status",
                "env": env,
                "status": status,
            });
            print_payload(output, payload, writer, || {
                status
                    .as_ref()
                    .map(|value| format!("status env={} {}", env, compact_json(value)))
                    .unwrap_or_else(|| format!("status env={} none", env))
            })
        }
        Command::Deployments { env } => {
            let deployments = api.deployments(&env).map_err(AppError::DbQuery)?;
            let count = deployments.as_array().map(|entries| entries.len()).unwrap_or(0);
            let payload = json!({
                "command": "deployments",
                "env": env,
                "count": count,
                "deployments": deployments,
            });
            print_payload(output, payload, writer, || {
                format!("deployments env={} count={}", env, count)
            })
        }
        Command::Diff { env, from_schema } => {
            let diff = api.diff(&env, &from_schema).map_err(AppError::DbQuery)?;
            let payload = json!({
                "command": "diff",
                "env": env,
                "from_schema": from_schema,
                "diff": diff,
            });
            print_payload(output, payload, writer, || {
                format!("diff env={} from_schema={}", env, from_schema)
            })
        }
    }
}

fn print_payload<F>(
    output: OutputMode,
    payload: Value,
    writer: &mut dyn Write,
    human_builder: F,
) -> std::result::Result<(), AppError>
where
    F: FnOnce() -> String,
{
    let rendered = match output {
        OutputMode::Human => human_builder(),
        OutputMode::Json => {
            serde_json::to_string_pretty(&payload).map_err(|err| AppError::Print(err.into()))?
        }
    };
    writeln!(writer, "{rendered}").map_err(|err| AppError::Print(err.into()))
}

fn read_json_column(row: &Row, column: &str) -> Result<Option<Value>> {
    row.try_get(column).with_context(|| format!("column `{column}` is not valid jsonb"))
}

fn read_required_json_column(row: &Row, column: &str) -> Result<Value> {
    read_json_column(row, column)?.with_context(|| format!("column `{column}` unexpectedly null"))
}

pub fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{\"error\":\"json-encode-failed\"}".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_exposes_expected_subcommands() {
        let command = Cli::command();
        let names: Vec<_> =
            command.get_subcommands().map(|subcommand| subcommand.get_name().to_string()).collect();
        assert_eq!(names, vec!["deploy", "rollback", "status", "deployments", "diff"]);
    }

    #[test]
    fn compact_json_handles_objects() {
        let rendered = compact_json(&json!({"key": "value"}));
        assert_eq!(rendered, "{\"key\":\"value\"}");
    }

    #[test]
    fn exit_codes_are_stable() {
        assert_eq!(EXIT_DB_CONNECT, 10);
        assert_eq!(EXIT_DB_QUERY, 11);
        assert_eq!(EXIT_RESPONSE_DECODE, 12);
        assert_eq!(EXIT_OUTPUT_FORMAT, 13);
    }
}
