//! The settings file the explorer reads at startup: `ENV_FILE`, or `.env` in
//! its working directory. Each test reads the first log line, which names the
//! database chosen, and stops the explorer there.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// The explorer in `dir`, with nothing it could pick up from this shell: no
/// node to index from, no third party to ask, and a port of the kernel's.
fn explorer(dir: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nvnmchain-explorer"));
    cmd.current_dir(dir)
        .env("HOST", "127.0.0.1")
        .env("PORT", "0")
        .env("NVNM_RPC", "http://127.0.0.1:9")
        .env("INDEX_WS", "0")
        .env("SIGNATURE_LOOKUP_URL", "")
        .env("RUST_LOG", "nvnmchain_explorer=info")
        .env_remove("ENV_FILE")
        .env_remove("DATABASE_URL")
        .env_remove("DB_PATH")
        .env_remove("ROLE")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    cmd
}

/// The startup line, which names the database the explorer chose.
fn startup_line(mut cmd: Command) -> String {
    let mut child = cmd.spawn().expect("spawn the explorer");
    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains("starting nvnmchain Explorer") {
                let _ = tx.send(line);
                return;
            }
        }
    });
    let line = rx.recv_timeout(Duration::from_secs(30));
    let _ = child.kill();
    let _ = child.wait();
    line.expect("the explorer logged no startup line")
}

fn write(path: &Path, text: &str) {
    std::fs::write(path, text).expect("write the settings file");
}

#[test]
fn the_explorer_reads_dot_env_in_its_working_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(&dir.path().join(".env"), "# local\nDB_PATH=from-file.db\n");
    let line = startup_line(explorer(dir.path()));
    assert!(line.contains("db=from-file.db"), "{line}");
}

#[test]
fn a_variable_already_set_wins_over_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(&dir.path().join(".env"), "DB_PATH=from-file.db\n");
    let mut cmd = explorer(dir.path());
    cmd.env("DB_PATH", "from-env.db");
    let line = startup_line(cmd);
    assert!(line.contains("db=from-env.db"), "{line}");

    // Set but empty still counts as set, which is how a shell turns the
    // file's Postgres off.
    write(
        &dir.path().join(".env"),
        "DATABASE_URL=postgres://127.0.0.1:9/nowhere\n",
    );
    let mut cmd = explorer(dir.path());
    cmd.env("DATABASE_URL", "");
    let line = startup_line(cmd);
    assert!(line.contains("db=explorer.db"), "{line}");
}

#[test]
fn env_file_names_another_file_and_empty_reads_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(&dir.path().join(".env"), "DB_PATH=from-file.db\n");
    write(&dir.path().join("named.env"), "DB_PATH=named.db\n");

    let mut cmd = explorer(dir.path());
    cmd.env("ENV_FILE", dir.path().join("named.env"));
    let line = startup_line(cmd);
    assert!(line.contains("db=named.db"), "{line}");

    let mut cmd = explorer(dir.path());
    cmd.env("ENV_FILE", "");
    let line = startup_line(cmd);
    assert!(line.contains("db=explorer.db"), "{line}");
}

/// `.env.example` parses: from a malformed line copied into `.env` on, the
/// file would be skipped.
#[test]
fn the_example_parses() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(".env.example");
    let vars = dotenvy::from_path_iter(&path)
        .expect("open .env.example")
        .collect::<Result<Vec<_>, _>>()
        .expect("parse .env.example");
    assert!(
        vars.iter().any(|(key, _)| key == "DATABASE_URL"),
        "{vars:?}"
    );
}
