use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo"));
    let runtime_dir = manifest_dir.join("../../packages/runtime");

    for relative in ["package.json", "tsconfig.json", "src/embedded.ts", "src/index.ts"] {
        println!("cargo:rerun-if-changed={}", runtime_dir.join(relative).display());
    }

    let status = Command::new("npm")
        .arg("run")
        .arg("build")
        .arg("--prefix")
        .arg(&runtime_dir)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke `npm run build --prefix {}` from plts build script: {err}",
                runtime_dir.display()
            )
        });

    if !status.success() {
        panic!(
            "runtime package build failed (exit status: {status}) while preparing {}",
            runtime_dir.display()
        );
    }
}
