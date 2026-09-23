#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use serde_json::{Value, json};

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

struct Fixture {
    _directory: tempfile::TempDir,
    repository: PathBuf,
    bin: PathBuf,
    arguments: PathBuf,
    stdin: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        let bin = directory.path().join("bin");
        fs::create_dir(&repository).unwrap();
        fs::create_dir(&bin).unwrap();

        write_executable(
            &bin.join("gh"),
            "#!/bin/sh\nif [ \"$1\" = auth ] && [ \"$2\" = status ]; then exit 0; fi\nprintf 'unexpected gh invocation\\n' >&2\nexit 99\n",
        );
        write_executable(
            &bin.join("claude"),
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >\"$CLAUDE_TEST_ARGUMENTS\"\ncat >\"$CLAUDE_TEST_STDIN\"\nprintf '%s\\n' \"$CLAUDE_TEST_RESULT\"\nexit \"${CLAUDE_TEST_STATUS:-0}\"\n",
        );

        let arguments = directory.path().join("arguments");
        let stdin = directory.path().join("stdin");
        Self {
            _directory: directory,
            repository,
            bin,
            arguments,
            stdin,
        }
    }

    fn envelope(&self) -> Value {
        json!({
            "schema_version": 1,
            "reason": "review_requested",
            "repository": {
                "id": 7,
                "full_name": "acme/widgets",
                "local_path": self.repository,
            },
            "pull_request": {
                "id": 11,
                "number": 42,
                "url": "https://github.com/acme/widgets/pull/42",
                "base_sha": "1111111111111111111111111111111111111111",
                "head_sha": "2222222222222222222222222222222222222222",
            },
        })
    }

    fn run(&self, result: Value) -> Output {
        let response = json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "result": "",
            "structured_output": result,
        });
        self.run_response(response)
    }

    fn run_response(&self, response: Value) -> Output {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/claude-review.py");
        let path = format!(
            "{}:{}",
            self.bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = Command::new(script)
            .arg("Pay particular attention to authorization boundaries.")
            .current_dir(&self.repository)
            .env("PATH", path)
            .env("CLAUDE_TEST_ARGUMENTS", &self.arguments)
            .env("CLAUDE_TEST_STDIN", &self.stdin)
            .env(
                "CLAUDE_TEST_RESULT",
                serde_json::to_string(&response).unwrap(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        serde_json::to_writer(child.stdin.as_mut().unwrap(), &self.envelope()).unwrap();
        child.stdin.as_mut().unwrap().write_all(b"\n").unwrap();
        drop(child.stdin.take());
        child.wait_with_output().unwrap()
    }
}

#[test]
fn claude_review_example_runs_with_guardrails_and_reports_operations() {
    let fixture = Fixture::new();
    let output = fixture.run(json!({
        "status": "success",
        "summary": "Reviewed the pull request and requested one correction.",
        "operations": [{
            "kind": "inline_comment",
            "target": "https://github.com/acme/widgets/pull/42#discussion_r1",
            "summary": "Flagged an authorization bypass",
        }],
        "errors": [],
    }));

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Reviewed the pull request"));
    assert!(stdout.contains("inline_comment: Flagged an authorization bypass"));

    let arguments = fs::read_to_string(&fixture.arguments).unwrap();
    for expected in [
        "--print",
        "--output-format",
        "--json-schema",
        "opus",
        "--effort",
        "high",
        "--max-budget-usd",
        "5.00",
        "--no-session-persistence",
        "--permission-mode",
        "dontAsk",
        "--permission-prompts",
        "none",
        "Skill",
        "Bash(git diff:*)",
        "Bash(gh api:*)",
        "Edit",
        "Task",
        "Additional review instructions supplied by the operator",
        "Pay particular attention to authorization boundaries.",
    ] {
        assert!(
            arguments.contains(expected),
            "missing {expected:?} in {arguments}"
        );
    }

    let delivered: Value = serde_json::from_slice(&fs::read(&fixture.stdin).unwrap()).unwrap();
    assert_eq!(delivered, fixture.envelope());
}

#[test]
fn claude_review_example_fails_when_the_agent_reports_partial_failure() {
    let fixture = Fixture::new();
    let output = fixture.run(json!({
        "status": "failure",
        "summary": "The review was only partially submitted.",
        "operations": [],
        "errors": ["GitHub rejected an inline comment"],
    }));

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("only partially submitted")
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("GitHub rejected an inline comment")
    );
}

#[test]
fn claude_review_example_fails_when_structured_output_is_missing() {
    let fixture = Fixture::new();
    let output = fixture.run_response(json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "result": "Review complete",
        "structured_output": null,
    }));

    assert_eq!(output.status.code(), Some(70));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("structured_output must be an object")
    );
}
