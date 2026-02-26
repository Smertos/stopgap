use std::{fmt, process::ExitCode};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use postgres::{Client, NoTls, Row};
use serde_json::{json, Value};

const EXIT_DB_CONNECT: u8 = 10;
const EXIT_DB_QUERY: u8 = 11;
const EXIT_RESPONSE_DECODE: u8 = 12;
const EXIT_OUTPUT_FORMAT: u8 = 13;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputMode {
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
struct Cli {
    #[arg(long, env = "STOPGAP_DB")]
    db: String,

    #[arg(long, value_enum, default_value_t = OutputMode::Human)]
    output: OutputMode,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
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
enum AppError {
    DbConnect(anyhow::Error),
    DbQuery(anyhow::Error),
    Decode(anyhow::Error),
    Print(anyhow::Error),
}

impl AppError {
    fn code(&self) -> u8 {
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("stopgap: {err}");
            ExitCode::from(err.code())
        }
    }
}

fn run(cli: Cli) -> std::result::Result<(), AppError> {
    let mut client =
        Client::connect(&cli.db, NoTls).map_err(|err| AppError::DbConnect(err.into()))?;

    match cli.command {
        Command::Deploy { env, from_schema, label, prune } => {
            deploy(&mut client, cli.output, &env, &from_schema, label.as_deref(), prune)
        }
        Command::Rollback { env, steps, to_id } => {
            rollback(&mut client, cli.output, &env, steps, to_id)
        }
        Command::Status { env } => status(&mut client, cli.output, &env),
        Command::Deployments { env } => deployments(&mut client, cli.output, &env),
        Command::Diff { env, from_schema } => diff(&mut client, cli.output, &env, &from_schema),
    }
}

fn deploy(
    client: &mut Client,
    output: OutputMode,
    env: &str,
    from_schema: &str,
    label: Option<&str>,
    prune: bool,
) -> std::result::Result<(), AppError> {
    let mut tx = client.build_transaction().start().map_err(|err| AppError::DbQuery(err.into()))?;
    let prune_setting = if prune { "on" } else { "off" };
    tx.batch_execute(&format!("SET LOCAL stopgap.prune = '{prune_setting}'"))
        .map_err(|err| AppError::DbQuery(err.into()))?;
    let row = tx
        .query_one(
            "SELECT stopgap.deploy($1, $2, $3) AS deployment_id",
            &[&env, &from_schema, &label],
        )
        .map_err(|err| AppError::DbQuery(err.into()))?;
    tx.commit().map_err(|err| AppError::DbQuery(err.into()))?;

    let deployment_id: i64 = row.get("deployment_id");
    let payload = json!({
        "command": "deploy",
        "env": env,
        "from_schema": from_schema,
        "deployment_id": deployment_id,
        "prune": prune,
    });
    print_payload(output, payload, || {
        format!(
            "deployed env={} from_schema={} deployment_id={} prune={}",
            env, from_schema, deployment_id, prune
        )
    })
}

fn rollback(
    client: &mut Client,
    output: OutputMode,
    env: &str,
    steps: i32,
    to_id: Option<i64>,
) -> std::result::Result<(), AppError> {
    let row = client
        .query_one("SELECT stopgap.rollback($1, $2, $3) AS deployment_id", &[&env, &steps, &to_id])
        .map_err(|err| AppError::DbQuery(err.into()))?;
    let deployment_id: i64 = row.get("deployment_id");
    let payload = json!({
        "command": "rollback",
        "env": env,
        "steps": steps,
        "to_id": to_id,
        "deployment_id": deployment_id,
    });
    print_payload(output, payload, || {
        format!(
            "rolled back env={} target_deployment_id={} steps={}{}",
            env,
            deployment_id,
            steps,
            to_id.map(|value| format!(" to_id={value}")).unwrap_or_default()
        )
    })
}

fn status(client: &mut Client, output: OutputMode, env: &str) -> std::result::Result<(), AppError> {
    let row = client
        .query_one("SELECT stopgap.status($1) AS status", &[&env])
        .map_err(|err| AppError::DbQuery(err.into()))?;
    let status = read_json_column(&row, "status").map_err(AppError::Decode)?;
    let payload = json!({
        "command": "status",
        "env": env,
        "status": status,
    });

    print_payload(output, payload, || {
        status
            .as_ref()
            .map(|value| format!("status env={} {}", env, compact_json(value)))
            .unwrap_or_else(|| format!("status env={} none", env))
    })
}

fn deployments(
    client: &mut Client,
    output: OutputMode,
    env: &str,
) -> std::result::Result<(), AppError> {
    let row = client
        .query_one("SELECT stopgap.deployments($1) AS deployments", &[&env])
        .map_err(|err| AppError::DbQuery(err.into()))?;
    let deployments = read_required_json_column(&row, "deployments").map_err(AppError::Decode)?;
    let count = deployments.as_array().map(|entries| entries.len()).unwrap_or(0);

    let payload = json!({
        "command": "deployments",
        "env": env,
        "count": count,
        "deployments": deployments,
    });
    print_payload(output, payload, || format!("deployments env={} count={}", env, count))
}

fn diff(
    client: &mut Client,
    output: OutputMode,
    env: &str,
    from_schema: &str,
) -> std::result::Result<(), AppError> {
    let row = client
        .query_one("SELECT stopgap.diff($1, $2) AS diff", &[&env, &from_schema])
        .map_err(|err| AppError::DbQuery(err.into()))?;
    let diff = read_required_json_column(&row, "diff").map_err(AppError::Decode)?;
    let payload = json!({
        "command": "diff",
        "env": env,
        "from_schema": from_schema,
        "diff": diff,
    });
    print_payload(output, payload, || format!("diff env={} from_schema={}", env, from_schema))
}

fn print_payload<F>(
    output: OutputMode,
    payload: Value,
    human_builder: F,
) -> std::result::Result<(), AppError>
where
    F: FnOnce() -> String,
{
    match output {
        OutputMode::Human => {
            println!("{}", human_builder());
            Ok(())
        }
        OutputMode::Json => {
            let serialized = serde_json::to_string_pretty(&payload)
                .map_err(|err| AppError::Print(err.into()))?;
            println!("{serialized}");
            Ok(())
        }
    }
}

fn read_json_column(row: &Row, column: &str) -> Result<Option<Value>> {
    row.try_get(column).with_context(|| format!("column `{column}` is not valid jsonb"))
}

fn read_required_json_column(row: &Row, column: &str) -> Result<Value> {
    read_json_column(row, column)?.with_context(|| format!("column `{column}` unexpectedly null"))
}

fn compact_json(value: &Value) -> String {
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
