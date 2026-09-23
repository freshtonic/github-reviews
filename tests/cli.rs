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

#[test]
fn first_ctrl_c_stops_pull_request_discovery_when_no_reviews_are_running() {
    use std::process::Stdio;
    use std::thread;
    use std::time::{Duration, Instant};

    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repo");
    fs::create_dir(&repository).unwrap();
    for args in [
        &["init", "--quiet"][..],
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/acme/widgets.git",
        ][..],
    ] {
        assert!(
            ProcessCommand::new("git")
                .args(args)
                .current_dir(&repository)
                .status()
                .unwrap()
                .success()
        );
    }

    // Pull request reads block, so the daemon is inside discovery when the
    // signal arrives. `discovery-started` tells the test when that happens.
    let bin = directory.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let started_marker = directory.path().join("discovery-started");
    let notifications: Vec<String> = (1..=5)
        .map(|number| {
            format!(
                r#"{{"id":"n{number}","reason":"review_requested","unread":true,"updated_at":"2030-01-01T00:00:00Z","repository":{{"id":7,"full_name":"acme/widgets"}},"subject":{{"title":"PR {number}","type":"PullRequest","url":"https://api.github.com/repos/acme/widgets/pulls/{number}"}}}}"#
            )
        })
        .collect();
    let gh = bin.join("gh");
    fs::write(
        &gh,
        format!(
            r#"#!/bin/sh
respond() {{ printf 'HTTP/2 %s\nX-Poll-Interval: 1\n\n%s' "$1" "$2"; }}
case "$*" in
  */user/teams*) respond '200 OK' '[]' ;;
  */user) respond '200 OK' '{{"id":1,"login":"me"}}' ;;
  */repos/acme/widgets/notifications*) respond '200 OK' '[{}]' ;;
  */notifications*) respond '304 Not Modified' ''; exit 1 ;;
  */repos/acme/widgets) respond '200 OK' '{{"id":7,"full_name":"acme/widgets","html_url":"https://github.com/acme/widgets"}}' ;;
  *) touch '{}'; exec sleep 30 ;;
esac
"#,
            notifications.join(","),
            started_marker.display()
        ),
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
        .success();

    let mut daemon = ProcessCommand::new(assert_cmd::cargo::cargo_bin("github-reviews"))
        .env("PATH", &path)
        .env("GITHUB_REVIEWS_STATE_PATH", &state)
        .args([
            "run",
            "--interval",
            "1s",
            "--review-command",
            "/usr/bin/true",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while !started_marker.exists() {
        assert!(Instant::now() < deadline, "discovery did not start");
        if let Some(status) = daemon.try_wait().unwrap() {
            panic!("daemon exited before discovery started: {status}");
        }
        thread::sleep(Duration::from_millis(50));
    }

    let interrupted = Instant::now();
    assert!(
        ProcessCommand::new("kill")
            .args(["-INT", &daemon.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = loop {
        if let Some(status) = daemon.try_wait().unwrap() {
            break status;
        }
        if interrupted.elapsed() > Duration::from_secs(5) {
            let _ = daemon.kill();
            panic!("daemon did not stop within 5 seconds of one CTRL-C");
        }
        thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(daemon.stderr.as_mut().unwrap(), &mut stderr).unwrap();
    assert!(status.success(), "daemon failed: {stderr}");
    assert!(
        stderr.contains("all reviews completed, quitting"),
        "{stderr}"
    );
    assert!(!stderr.contains("could not refresh"), "{stderr}");
    assert!(!stderr.contains("discovery deferred"), "{stderr}");
}
