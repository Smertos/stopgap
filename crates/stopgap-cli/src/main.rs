use std::process::ExitCode;

use clap::Parser;
use stopgap_cli::{Cli, run};

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut stdout = std::io::stdout();

    match run(cli, &mut stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("stopgap: {err}");
            ExitCode::from(err.code())
        }
    }
}
