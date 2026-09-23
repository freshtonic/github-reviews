//! Local Git repository discovery and pull-request object preparation.
//!
//! Fetches deliberately write only `FETCH_HEAD`: they neither check anything
//! out nor create a local branch or remote-tracking ref.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use wait_timeout::ChildExt;

const FETCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Executables used for repository discovery and preparation.
///
/// Keeping these injectable makes command construction testable without
/// changing the user's Git or SSH configuration.
#[derive(Clone, Debug)]
pub struct GitTools {
    pub git: PathBuf,
    pub ssh: PathBuf,
}

impl Default for GitTools {
    fn default() -> Self {
        Self {
            git: PathBuf::from("git"),
            ssh: PathBuf::from("ssh"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitHubOrigin {
    pub hostname: String,
    pub owner: String,
    pub repository: String,
}

impl GitHubOrigin {
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.repository)
    }

    fn equivalent_to(&self, other: &Self) -> bool {
        self.hostname.eq_ignore_ascii_case(&other.hostname)
            && self.owner.eq_ignore_ascii_case(&other.owner)
            && self.repository.eq_ignore_ascii_case(&other.repository)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalRepository {
    pub worktree_root: PathBuf,
    pub origin_url: String,
    pub origin: GitHubOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchSpec {
    pub pull_number: u64,
    pub base_ref: String,
    pub expected_base_oid: String,
    pub expected_head_oid: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevisionKind {
    Base,
    Head,
}

#[derive(Debug)]
pub enum GitError {
    Io(std::io::Error),
    CommandFailed {
        operation: &'static str,
        status: ExitStatus,
        stderr: String,
    },
    CommandTimedOut {
        operation: &'static str,
        timeout: Duration,
    },
    Cancelled {
        operation: &'static str,
    },
    InvalidOutput {
        operation: &'static str,
        detail: String,
    },
    UnsupportedOrigin(String),
    OriginMismatch {
        expected: String,
        actual: String,
    },
    WorktreeMismatch {
        expected: PathBuf,
        actual: PathBuf,
    },
    /// GitHub's ref moved after the API response used to create the action.
    StaleRevision {
        kind: RevisionKind,
        expected: String,
        fetched: String,
    },
}

impl fmt::Display for GitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "Git operation failed: {error}"),
            Self::CommandFailed {
                operation,
                status,
                stderr,
            } => write!(f, "{operation} failed with {status}: {}", stderr.trim()),
            Self::CommandTimedOut { operation, timeout } => {
                write!(f, "{operation} timed out after {}s", timeout.as_secs())
            }
            Self::Cancelled { operation } => write!(f, "{operation} was cancelled"),
            Self::InvalidOutput { operation, detail } => {
                write!(f, "{operation} returned invalid output: {detail}")
            }
            Self::UnsupportedOrigin(url) => {
                write!(f, "origin is not a supported GitHub.com URL: {url}")
            }
            Self::OriginMismatch { expected, actual } => {
                write!(
                    f,
                    "registered origin is {expected}, but local origin is {actual}"
                )
            }
            Self::WorktreeMismatch { expected, actual } => write!(
                f,
                "registered path {} resolves to worktree {}",
                expected.display(),
                actual.display()
            ),
            Self::StaleRevision {
                kind,
                expected,
                fetched,
            } => write!(
                f,
                "GitHub {:?} revision moved while preparing review (expected {expected}, fetched {fetched})",
                kind
            ),
        }
    }
}

impl std::error::Error for GitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for GitError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl GitTools {
    /// Discover the containing worktree and its first `origin` fetch URL.
    pub fn discover(&self, start: impl AsRef<Path>) -> Result<LocalRepository, GitError> {
        let cancelled = AtomicBool::new(false);
        self.discover_with_cancel(start, &cancelled)
    }

    /// Discover a worktree while responding promptly to daemon lease loss.
    pub fn discover_with_cancel(
        &self,
        start: impl AsRef<Path>,
        cancelled: &AtomicBool,
    ) -> Result<LocalRepository, GitError> {
        let start = start.as_ref();
        let root = self.git_stdout_cancel(
            start,
            [OsStr::new("rev-parse"), OsStr::new("--show-toplevel")],
            "discover Git worktree",
            DISCOVERY_TIMEOUT,
            cancelled,
        )?;
        let root = PathBuf::from(root.trim());
        let urls = self.git_stdout_cancel(
            &root,
            [
                OsStr::new("remote"),
                OsStr::new("get-url"),
                OsStr::new("--all"),
                OsStr::new("origin"),
            ],
            "read origin fetch URL",
            DISCOVERY_TIMEOUT,
            cancelled,
        )?;
        let origin_url = urls
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .ok_or_else(|| GitError::InvalidOutput {
                operation: "read origin fetch URL",
                detail: "origin has no fetch URL".into(),
            })?
            .to_owned();
        let origin = self.parse_origin_with_cancel(&origin_url, Some(cancelled))?;
        Ok(LocalRepository {
            worktree_root: root,
            origin_url,
            origin,
        })
    }

    /// Ensure a registered directory still points at the expected GitHub repo.
    pub fn validate_registered_path(
        &self,
        registered_root: impl AsRef<Path>,
        expected_origin: &GitHubOrigin,
    ) -> Result<LocalRepository, GitError> {
        let registered_root = registered_root.as_ref();
        let discovered = self.discover(registered_root)?;
        let expected_path = fs::canonicalize(registered_root)?;
        let actual_path = fs::canonicalize(&discovered.worktree_root)?;
        if expected_path != actual_path {
            return Err(GitError::WorktreeMismatch {
                expected: expected_path,
                actual: actual_path,
            });
        }
        if !discovered.origin.equivalent_to(expected_origin) {
            return Err(GitError::OriginMismatch {
                expected: expected_origin.full_name(),
                actual: discovered.origin.full_name(),
            });
        }
        Ok(discovered)
    }

    /// Parse a GitHub URL, consulting `ssh -G` when an SSH host is an alias.
    pub fn parse_origin(&self, url: &str) -> Result<GitHubOrigin, GitError> {
        self.parse_origin_with_cancel(url, None)
    }

    fn parse_origin_with_cancel(
        &self,
        url: &str,
        cancelled: Option<&AtomicBool>,
    ) -> Result<GitHubOrigin, GitError> {
        let parsed =
            ParsedOrigin::parse(url).ok_or_else(|| GitError::UnsupportedOrigin(url.into()))?;
        let hostname = if parsed.ssh && !parsed.host.eq_ignore_ascii_case("github.com") {
            self.resolve_ssh_hostname(&parsed.host, cancelled)
                .unwrap_or_else(|_| parsed.host.clone())
        } else {
            parsed.host.clone()
        };
        if !hostname.eq_ignore_ascii_case("github.com") {
            return Err(GitError::UnsupportedOrigin(url.into()));
        }
        let (owner, repository) =
            split_repo_path(&parsed.path).ok_or_else(|| GitError::UnsupportedOrigin(url.into()))?;
        Ok(GitHubOrigin {
            hostname: "github.com".into(),
            owner,
            repository,
        })
    }

    /// Fetch and verify the exact base and synthetic pull-request head refs.
    ///
    /// One five-minute deadline covers both fetches and their verification.
    pub fn fetch_pull_request(
        &self,
        worktree_root: impl AsRef<Path>,
        spec: &FetchSpec,
    ) -> Result<(), GitError> {
        let cancelled = AtomicBool::new(false);
        self.fetch_pull_request_with_cancel(worktree_root, spec, &cancelled)
    }

    /// Fetch and verify a pull request while responding promptly to daemon
    /// lease loss.
    pub fn fetch_pull_request_with_cancel(
        &self,
        worktree_root: impl AsRef<Path>,
        spec: &FetchSpec,
        cancelled: &AtomicBool,
    ) -> Result<(), GitError> {
        validate_ref_component(&spec.base_ref)?;
        validate_oid(&spec.expected_base_oid)?;
        validate_oid(&spec.expected_head_oid)?;

        let deadline = Instant::now() + FETCH_TIMEOUT;
        let base_source = format!("refs/heads/{}", spec.base_ref);
        let fetched_base = self.fetch_one(
            worktree_root.as_ref(),
            &base_source,
            remaining(deadline, "fetch pull-request revisions")?,
            cancelled,
        )?;
        verify_revision(
            RevisionKind::Base,
            &spec.expected_base_oid,
            fetched_base.trim(),
        )?;

        let head_source = format!("refs/pull/{}/head", spec.pull_number);
        let fetched_head = self.fetch_one(
            worktree_root.as_ref(),
            &head_source,
            remaining(deadline, "fetch pull-request revisions")?,
            cancelled,
        )?;
        verify_revision(
            RevisionKind::Head,
            &spec.expected_head_oid,
            fetched_head.trim(),
        )
    }

    fn fetch_one(
        &self,
        root: &Path,
        source_ref: &str,
        timeout: Duration,
        cancelled: &AtomicBool,
    ) -> Result<String, GitError> {
        let deadline = Instant::now() + timeout;
        self.git_stdout_with_env_cancel(
            root,
            [
                OsStr::new("fetch"),
                OsStr::new("--no-tags"),
                // Do not let remote.origin.fetch update a tracking ref when
                // the base source happens to match its configured refmap.
                OsStr::new("--refmap="),
                OsStr::new("origin"),
                OsStr::new(source_ref),
            ],
            [(OsStr::new("GIT_TERMINAL_PROMPT"), OsStr::new("0"))],
            "fetch pull-request revision",
            timeout,
            Some(cancelled),
        )?;
        self.git_stdout(
            root,
            [
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new("FETCH_HEAD^{commit}"),
            ],
            "verify fetched revision",
            remaining(deadline, "fetch pull-request revision")?,
        )
    }

    fn resolve_ssh_hostname(
        &self,
        host: &str,
        cancelled: Option<&AtomicBool>,
    ) -> Result<String, GitError> {
        let output = run_output(
            &self.ssh,
            [OsStr::new("-G"), OsStr::new(host)],
            None,
            &[],
            "resolve SSH host",
            DISCOVERY_TIMEOUT,
            cancelled,
        )?;
        let text = String::from_utf8(output.stdout).map_err(|error| GitError::InvalidOutput {
            operation: "resolve SSH host",
            detail: error.to_string(),
        })?;
        text.lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next()?.eq_ignore_ascii_case("hostname"))
                    .then(|| fields.next().map(str::to_owned))
                    .flatten()
            })
            .ok_or_else(|| GitError::InvalidOutput {
                operation: "resolve SSH host",
                detail: "ssh -G did not print a hostname".into(),
            })
    }

    fn git_stdout<'a>(
        &self,
        root: &Path,
        args: impl IntoIterator<Item = &'a OsStr>,
        operation: &'static str,
        timeout: Duration,
    ) -> Result<String, GitError> {
        self.git_stdout_with_env(root, args, [], operation, timeout)
    }

    fn git_stdout_cancel<'a>(
        &self,
        root: &Path,
        args: impl IntoIterator<Item = &'a OsStr>,
        operation: &'static str,
        timeout: Duration,
        cancelled: &AtomicBool,
    ) -> Result<String, GitError> {
        self.git_stdout_with_env_cancel(root, args, [], operation, timeout, Some(cancelled))
    }

    fn git_stdout_with_env<'a, 'b>(
        &self,
        root: &Path,
        args: impl IntoIterator<Item = &'a OsStr>,
        envs: impl IntoIterator<Item = (&'b OsStr, &'b OsStr)>,
        operation: &'static str,
        timeout: Duration,
    ) -> Result<String, GitError> {
        self.git_stdout_with_env_cancel(root, args, envs, operation, timeout, None)
    }

    fn git_stdout_with_env_cancel<'a, 'b>(
        &self,
        root: &Path,
        args: impl IntoIterator<Item = &'a OsStr>,
        envs: impl IntoIterator<Item = (&'b OsStr, &'b OsStr)>,
        operation: &'static str,
        timeout: Duration,
        cancelled: Option<&AtomicBool>,
    ) -> Result<String, GitError> {
        let owned_args: Vec<OsString> = args.into_iter().map(OsStr::to_owned).collect();
        let mut command_args = vec![OsString::from("-C"), root.as_os_str().to_owned()];
        command_args.extend(owned_args);
        let owned_envs: Vec<(OsString, OsString)> = envs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let output = run_output(
            &self.git,
            command_args.iter().map(OsString::as_os_str),
            None,
            &owned_envs,
            operation,
            timeout,
            cancelled,
        )?;
        String::from_utf8(output.stdout).map_err(|error| GitError::InvalidOutput {
            operation,
            detail: error.to_string(),
        })
    }
}

struct ParsedOrigin {
    host: String,
    path: String,
    ssh: bool,
}

impl ParsedOrigin {
    fn parse(url: &str) -> Option<Self> {
        let url = url.trim();
        if let Some(rest) = url.strip_prefix("https://") {
            let (authority, path) = rest.split_once('/')?;
            let host = authority.rsplit('@').next()?.split(':').next()?.to_owned();
            return Some(Self {
                host,
                path: path.to_owned(),
                ssh: false,
            });
        }
        if let Some(rest) = url.strip_prefix("ssh://") {
            let (authority, path) = rest.split_once('/')?;
            let host = authority.rsplit('@').next()?.split(':').next()?.to_owned();
            return Some(Self {
                host,
                path: path.to_owned(),
                ssh: true,
            });
        }
        let (authority, path) = url.split_once(':')?;
        if authority.contains('/') || path.starts_with('/') {
            return None;
        }
        let host = authority.rsplit('@').next()?.to_owned();
        Some(Self {
            host,
            path: path.to_owned(),
            ssh: true,
        })
    }
}

fn split_repo_path(path: &str) -> Option<(String, String)> {
    let path = path
        .trim_matches('/')
        .strip_suffix(".git")
        .unwrap_or(path.trim_matches('/'));
    let mut fields = path.split('/');
    let owner = fields.next()?;
    let repository = fields.next()?;
    if owner.is_empty() || repository.is_empty() || fields.next().is_some() {
        return None;
    }
    Some((owner.to_owned(), repository.to_owned()))
}

fn validate_ref_component(value: &str) -> Result<(), GitError> {
    if value.is_empty()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("@{")
        || value
            .chars()
            .any(|character| character.is_control() || " ~^:?*[\\".contains(character))
    {
        return Err(GitError::InvalidOutput {
            operation: "construct base ref",
            detail: format!("invalid base ref {value:?}"),
        });
    }
    Ok(())
}

fn validate_oid(value: &str) -> Result<(), GitError> {
    if !(40..=64).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GitError::InvalidOutput {
            operation: "verify fetched revision",
            detail: format!("invalid object ID {value:?}"),
        });
    }
    Ok(())
}

fn verify_revision(kind: RevisionKind, expected: &str, fetched: &str) -> Result<(), GitError> {
    if expected.eq_ignore_ascii_case(fetched) {
        Ok(())
    } else {
        Err(GitError::StaleRevision {
            kind,
            expected: expected.to_owned(),
            fetched: fetched.to_owned(),
        })
    }
}

fn remaining(deadline: Instant, operation: &'static str) -> Result<Duration, GitError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(GitError::CommandTimedOut {
            operation,
            timeout: FETCH_TIMEOUT,
        })
}

#[derive(Debug)]
struct CapturedOutput {
    stdout: Vec<u8>,
}

fn run_output<'a>(
    program: &Path,
    args: impl IntoIterator<Item = &'a OsStr>,
    current_dir: Option<&Path>,
    envs: &[(OsString, OsString)],
    operation: &'static str,
    timeout: Duration,
    cancelled: Option<&AtomicBool>,
) -> Result<CapturedOutput, GitError> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn()?;
    let mut stdout = child.stdout.take().expect("stdout was piped");
    let mut stderr = child.stderr.take().expect("stderr was piped");
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let stderr_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).map(|_| bytes)
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if cancelled.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            let _ = child.kill();
            let _ = child.wait();
            drop(stdout_reader);
            drop(stderr_reader);
            return Err(GitError::Cancelled { operation });
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let _ = child.kill();
            let _ = child.wait();
            drop(stdout_reader);
            drop(stderr_reader);
            return Err(GitError::CommandTimedOut { operation, timeout });
        }
        if let Some(status) = child.wait_timeout(remaining.min(Duration::from_millis(50)))? {
            break status;
        }
    };
    let stdout = stdout_reader.join().map_err(|_| GitError::InvalidOutput {
        operation,
        detail: "stdout reader panicked".into(),
    })??;
    let stderr = stderr_reader.join().map_err(|_| GitError::InvalidOutput {
        operation,
        detail: "stderr reader panicked".into(),
    })??;
    if !status.success() {
        return Err(GitError::CommandFailed {
            operation,
            status,
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        });
    }
    Ok(CapturedOutput { stdout })
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;

    fn tools() -> GitTools {
        GitTools::default()
    }

    #[test]
    fn parses_supported_github_urls() {
        let cases = [
            "https://github.com/acme/widgets.git",
            "ssh://git@github.com/acme/widgets.git",
            "git@github.com:acme/widgets.git",
        ];
        for url in cases {
            assert_eq!(
                tools().parse_origin(url).unwrap(),
                GitHubOrigin {
                    hostname: "github.com".into(),
                    owner: "acme".into(),
                    repository: "widgets".into(),
                }
            );
        }
    }

    #[test]
    fn rejects_non_github_and_nested_paths() {
        assert!(matches!(
            tools().parse_origin("https://example.com/acme/widgets.git"),
            Err(GitError::UnsupportedOrigin(_))
        ));
        assert!(matches!(
            tools().parse_origin("https://github.com/acme/extra/widgets.git"),
            Err(GitError::UnsupportedOrigin(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn resolves_an_injected_ssh_host_alias() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = tempdir().unwrap();
        let ssh = fixture.path().join("ssh");
        fs::write(&ssh, "#!/bin/sh\nprintf 'hostname github.com\\n'\n").unwrap();
        let mut permissions = fs::metadata(&ssh).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&ssh, permissions).unwrap();
        let tools = GitTools {
            git: PathBuf::from("git"),
            ssh,
        };

        assert_eq!(
            tools.parse_origin("git@work-gh:acme/widgets.git").unwrap(),
            GitHubOrigin {
                hostname: "github.com".into(),
                owner: "acme".into(),
                repository: "widgets".into(),
            }
        );
    }

    #[test]
    fn detects_a_stale_revision_distinctly() {
        let expected = "a".repeat(40);
        let fetched = "b".repeat(40);
        assert!(matches!(
            verify_revision(RevisionKind::Head, &expected, &fetched),
            Err(GitError::StaleRevision {
                kind: RevisionKind::Head,
                ..
            })
        ));
    }

    #[test]
    fn command_cancellation_interrupts_preparation() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&cancelled);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            signal.store(true, Ordering::SeqCst);
        });
        let started = Instant::now();
        let error = run_output(
            Path::new("/bin/sleep"),
            [OsStr::new("30")],
            None,
            &[],
            "test preparation",
            Duration::from_secs(30),
            Some(&cancelled),
        )
        .unwrap_err();
        assert!(matches!(error, GitError::Cancelled { .. }));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn validates_ref_components_before_passing_them_to_git() {
        assert!(validate_ref_component("main").is_ok());
        assert!(validate_ref_component("release/next").is_ok());
        assert!(validate_ref_component("--upload-pack=evil").is_ok());
        assert!(validate_ref_component("bad ref").is_err());
        assert!(validate_ref_component("bad..ref").is_err());
    }

    #[test]
    fn targeted_fetch_verifies_both_oids_without_creating_refs() {
        let fixture = tempdir().unwrap();
        let origin = fixture.path().join("origin.git");
        let source = fixture.path().join("source");
        let local = fixture.path().join("local");

        git(fixture.path(), ["init", "--bare", origin.to_str().unwrap()]);
        git(fixture.path(), ["init", source.to_str().unwrap()]);
        git(
            &source,
            [
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-m",
                "base",
            ],
        );
        git(&source, ["branch", "-M", "main"]);
        let base_oid = git(&source, ["rev-parse", "HEAD"]);
        git(
            &source,
            ["remote", "add", "origin", origin.to_str().unwrap()],
        );
        git(&source, ["push", "origin", "HEAD:refs/heads/main"]);
        git(
            &source,
            [
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-m",
                "head",
            ],
        );
        let head_oid = git(&source, ["rev-parse", "HEAD"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/7/head"]);

        git(fixture.path(), ["init", local.to_str().unwrap()]);
        git(
            &local,
            ["remote", "add", "origin", origin.to_str().unwrap()],
        );
        tools()
            .fetch_pull_request(
                &local,
                &FetchSpec {
                    pull_number: 7,
                    base_ref: "main".into(),
                    expected_base_oid: base_oid,
                    expected_head_oid: head_oid,
                },
            )
            .unwrap();

        let refs = Command::new("git")
            .args(["-C", local.to_str().unwrap(), "show-ref"])
            .output()
            .unwrap();
        assert!(
            !refs.status.success(),
            "fetch unexpectedly created a ref: {}",
            String::from_utf8_lossy(&refs.stdout)
        );
        assert!(refs.stdout.is_empty());
    }

    fn git<const N: usize>(directory: &Path, args: [&str; N]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }
}
