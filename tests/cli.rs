#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command as ProcessCommand;

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn status_uses_isolated_state_database() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state.sqlite3");
    Command::cargo_bin("github-reviews")
        .unwrap()
        .env("GITHUB_REVIEWS_STATE_PATH", &state)
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("registered repositories: 0"));
}

#[test]
fn register_discovers_origin_and_persists_mapping() {
    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repo");
    fs::create_dir(&repository).unwrap();
    assert!(
        ProcessCommand::new("git")
            .args(["init", "--quiet"])
            .current_dir(&repository)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        ProcessCommand::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "https://github.com/acme/widgets.git",
            ])
            .current_dir(&repository)
            .status()
            .unwrap()
            .success()
    );

    let bin = directory.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let gh = bin.join("gh");
    fs::write(
        &gh,
        "#!/bin/sh\nprintf 'HTTP/1.1 200 OK\\r\\nContent-Type: application/json\\r\\n\\r\\n'\nprintf '%s' '{\"id\":7,\"full_name\":\"acme/widgets\",\"html_url\":\"https://github.com/acme/widgets\"}'\n",
    )
    .unwrap();
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
    let state = directory.path().join("state.sqlite3");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    Command::cargo_bin("github-reviews")
        .unwrap()
        .current_dir(&repository)
        .env("PATH", &path)
        .env("GITHUB_REVIEWS_STATE_PATH", &state)
        .arg("register")
        .assert()
        .success()
        .stdout(predicate::str::contains("registered acme/widgets"));

    Command::cargo_bin("github-reviews")
        .unwrap()
        .env("GITHUB_REVIEWS_STATE_PATH", &state)
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("acme/widgets"))
        .stdout(predicate::str::contains(
            repository.to_string_lossy().as_ref(),
        ));
}

#[test]
fn repository_discovery_errors_identify_working_directory() {
    let directory = tempfile::tempdir().unwrap();
    let working_directory = directory.path().canonicalize().unwrap();
    let state = directory.path().join("state.sqlite3");

    for command in ["register", "unregister"] {
        Command::cargo_bin("github-reviews")
            .unwrap()
            .current_dir(&working_directory)
            .env("GITHUB_REVIEWS_STATE_PATH", &state)
            .arg(command)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                working_directory.to_string_lossy().as_ref(),
            ));
    }
}
