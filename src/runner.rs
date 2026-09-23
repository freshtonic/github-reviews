//! Direct (non-shell) review-command execution.

use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use wait_timeout::ChildExt;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewCommand {
    argv: Vec<OsString>,
}

impl ReviewCommand {
    pub fn new<I, S>(argv: I) -> Result<Self, RunnerError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let argv: Vec<OsString> = argv.into_iter().map(Into::into).collect();
        if argv.is_empty() || argv[0].is_empty() {
            return Err(RunnerError::EmptyCommand);
        }
        Ok(Self { argv })
    }

    pub fn argv(&self) -> &[OsString] {
        &self.argv
    }

    /// Resolve the program against the daemon's working directory and confirm
    /// that it can be started.
    ///
    /// Commands run from each registered repository's worktree, and Unix
    /// resolves a relative program path such as `./review.py` from the
    /// child's new working directory. Anchoring it here keeps the path
    /// meaning what it meant where the operator typed it. A bare program name
    /// is left for `PATH` lookup, which is checked now so that a typo fails at
    /// startup rather than on every review attempt.
    pub fn resolve(mut self, working_directory: &Path) -> Result<Self, RunnerError> {
        let program = PathBuf::from(&self.argv[0]);
        if program.components().count() > 1 || program.is_absolute() {
            let program = if program.is_absolute() {
                program
            } else {
                working_directory.join(program)
            };
            if !is_executable_file(&program) {
                return Err(RunnerError::ProgramNotFound(program));
            }
            self.argv[0] = program.into_os_string();
        } else {
            let found = std::env::var_os("PATH").is_some_and(|path| {
                std::env::split_paths(&path)
                    .any(|directory| is_executable_file(&directory.join(&program)))
            });
            if !found {
                return Err(RunnerError::ProgramNotFound(program));
            }
        }
        Ok(self)
    }

    /// Execute one review command and block until it exits or times out.
    ///
    /// Async-mode workers can call this function on their own worker threads;
    /// every child is waited on and therefore reaped.
    pub fn execute<T: Serialize>(
        &self,
        worktree_root: impl AsRef<Path>,
        envelope: &T,
        timeout: Option<Duration>,
    ) -> Result<RunOutcome, RunnerError> {
        let cancelled = AtomicBool::new(false);
        self.execute_with_cancel(worktree_root, envelope, timeout, &cancelled)
    }

    /// Execute one review command, stopping its process tree when cooperative
    /// cancellation is requested (for example, after losing a daemon lease).
    pub fn execute_with_cancel<T: Serialize>(
        &self,
        worktree_root: impl AsRef<Path>,
        envelope: &T,
        timeout: Option<Duration>,
        cancelled: &AtomicBool,
    ) -> Result<RunOutcome, RunnerError> {
        let mut input = serde_json::to_vec(envelope)?;
        input.push(b'\n');
        if cancelled.load(Ordering::Acquire) {
            return Ok(RunOutcome::Cancelled);
        }

        let mut process = Command::new(&self.argv[0]);
        process
            .args(&self.argv[1..])
            .current_dir(worktree_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        configure_process_group(&mut process);
        let mut child = process.spawn().map_err(RunnerError::Spawn)?;
        let mut stdin = child.stdin.take().expect("stdin was piped");
        let input_writer = thread::spawn(move || {
            stdin.write_all(&input)?;
            // Dropping the pipe after this closure supplies EOF.
            stdin.flush()
        });

        let started = Instant::now();
        let status = loop {
            if cancelled.load(Ordering::Acquire) {
                terminate_and_reap(&mut child).map_err(RunnerError::Wait)?;
                drop(input_writer);
                return Ok(RunOutcome::Cancelled);
            }

            let wait_for = match timeout {
                Some(timeout) => match timeout.checked_sub(started.elapsed()) {
                    Some(remaining) if !remaining.is_zero() => remaining.min(CANCEL_POLL_INTERVAL),
                    _ => {
                        terminate_and_reap(&mut child).map_err(RunnerError::Wait)?;
                        // A command may have spawned descendants that inherited
                        // stdin. Detach the writer rather than allowing one of
                        // those descendants to extend the requested deadline.
                        drop(input_writer);
                        return Ok(RunOutcome::TimedOut);
                    }
                },
                None => CANCEL_POLL_INTERVAL,
            };

            match child.wait_timeout(wait_for).map_err(RunnerError::Wait)? {
                Some(status) => break status,
                None if timeout.is_some_and(|timeout| started.elapsed() >= timeout) => {
                    terminate_and_reap(&mut child).map_err(RunnerError::Wait)?;
                    // A command may have spawned descendants that inherited
                    // stdin. Detach the writer rather than allowing one of
                    // those descendants to extend the requested deadline.
                    drop(input_writer);
                    return Ok(RunOutcome::TimedOut);
                }
                None => {}
            }
        };

        let write_result = input_writer
            .join()
            .map_err(|_| RunnerError::InputWriterPanicked)?;
        if let Err(error) = write_result {
            // Broken pipe is expected if a command exits without consuming its
            // documented input. It is still a protocol failure on a successful
            // process, but a nonzero exit remains the more useful result.
            if status.success() {
                return Err(RunnerError::WriteInput(error));
            }
        }

        Ok(if status.success() {
            RunOutcome::Succeeded
        } else {
            RunOutcome::Failed { status }
        })
    }
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    // A fresh process group lets a timeout terminate descendants as well as
    // the directly spawned review command.
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_and_reap(child: &mut Child) -> std::io::Result<()> {
    let process_group = i32::try_from(child.id()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "review command process ID does not fit in a Unix pid_t",
        )
    })?;

    // A negative pid targets the process group whose ID is the absolute
    // value. The child became its group leader through `process_group(0)`.
    //
    // SAFETY: `kill` has no memory-safety preconditions. The converted PID is
    // positive, negation therefore cannot accidentally target every process.
    let killed = unsafe { kill(-process_group, SIGKILL) };
    if killed == -1 {
        let error = std::io::Error::last_os_error();
        // ESRCH means the group exited between wait_timeout and kill. The
        // direct child still needs to be waited on below.
        if error.raw_os_error() != Some(ESRCH) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    }
    child.wait().map(|_| ())
}

#[cfg(not(unix))]
fn terminate_and_reap(child: &mut Child) -> std::io::Result<()> {
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(error) => return Err(error),
    }
    child.wait().map(|_| ())
}

#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const ESRCH: i32 = 3;

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

#[derive(Debug)]
pub enum RunnerError {
    EmptyCommand,
    ProgramNotFound(PathBuf),
    Serialize(serde_json::Error),
    Spawn(std::io::Error),
    WriteInput(std::io::Error),
    Wait(std::io::Error),
    InputWriterPanicked,
}

impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommand => write!(f, "review command must include a program"),
            Self::ProgramNotFound(program) => write!(
                f,
                "review command {} is not an executable file",
                program.display()
            ),
            Self::Serialize(error) => {
                write!(f, "could not serialize review command input: {error}")
            }
            Self::Spawn(error) => write!(f, "could not start review command: {error}"),
            Self::WriteInput(error) => write!(f, "could not write review command input: {error}"),
            Self::Wait(error) => write!(f, "could not wait for review command: {error}"),
            Self::InputWriterPanicked => write!(f, "review command input writer panicked"),
        }
    }
}

impl std::error::Error for RunnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize(error) => Some(error),
            Self::Spawn(error) | Self::WriteInput(error) | Self::Wait(error) => Some(error),
            Self::EmptyCommand | Self::ProgramNotFound(_) | Self::InputWriterPanicked => None,
        }
    }
}

impl From<serde_json::Error> for RunnerError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialize(value)
    }
}

#[derive(Debug)]
pub enum RunOutcome {
    Succeeded,
    Failed { status: ExitStatus },
    TimedOut,
    Cancelled,
}

impl RunOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }

    pub fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Failed { status } => status.code(),
            Self::Succeeded => Some(0),
            Self::TimedOut | Self::Cancelled => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::time::Instant;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn writes_compact_json_newline_and_eof_in_requested_worktree() {
        let root = tempdir().unwrap();
        let command = ReviewCommand::new([
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new("cat > command-input.json"),
        ])
        .unwrap();

        let outcome = command
            .execute(root.path(), &json!({"hello": "world"}), None)
            .unwrap();

        assert!(outcome.is_success());
        assert_eq!(
            fs::read_to_string(root.path().join("command-input.json")).unwrap(),
            "{\"hello\":\"world\"}\n"
        );
    }

    #[test]
    fn reports_nonzero_exit_status() {
        let root = tempdir().unwrap();
        let command = ReviewCommand::new([
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new("cat >/dev/null; exit 23"),
        ])
        .unwrap();
        let outcome = command.execute(root.path(), &json!({}), None).unwrap();
        assert_eq!(outcome.exit_code(), Some(23));
    }

    #[test]
    fn kills_and_reaps_a_timed_out_command() {
        let root = tempdir().unwrap();
        let command = ReviewCommand::new([OsStr::new("/bin/sleep"), OsStr::new("5")]).unwrap();
        let started = Instant::now();
        let outcome = command
            .execute(root.path(), &json!({}), Some(Duration::from_millis(25)))
            .unwrap();
        assert!(matches!(outcome, RunOutcome::TimedOut));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_descendants_in_the_review_command_process_group() {
        let root = tempdir().unwrap();
        let command = ReviewCommand::new([
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new("(sleep 1; touch descendant-survived) & wait"),
        ])
        .unwrap();

        let outcome = command
            .execute(root.path(), &json!({}), Some(Duration::from_millis(100)))
            .unwrap();
        assert!(matches!(outcome, RunOutcome::TimedOut));

        thread::sleep(Duration::from_millis(1_100));
        assert!(
            !root.path().join("descendant-survived").exists(),
            "descendant process survived review timeout"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_promptly_kills_descendants_and_is_distinct_from_timeout() {
        use std::sync::Arc;

        let root = tempdir().unwrap();
        let command = ReviewCommand::new([
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new("(sleep 1; touch cancelled-descendant-survived) & wait"),
        ])
        .unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let request = Arc::clone(&cancelled);
        let request_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            request.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let outcome = command
            .execute_with_cancel(root.path(), &json!({}), None, &cancelled)
            .unwrap();
        request_thread.join().unwrap();
        assert!(matches!(outcome, RunOutcome::Cancelled));
        assert!(started.elapsed() < Duration::from_secs(1));

        thread::sleep(Duration::from_millis(1_100));
        assert!(
            !root.path().join("cancelled-descendant-survived").exists(),
            "descendant process survived review cancellation"
        );
    }

    #[cfg(unix)]
    #[test]
    fn relative_program_runs_from_the_daemon_directory_not_the_worktree() {
        use std::os::unix::fs::PermissionsExt;

        let daemon_directory = tempdir().unwrap();
        let worktree = tempdir().unwrap();
        let script = daemon_directory.path().join("review.sh");
        fs::write(&script, "#!/bin/sh\ncat > reviewed.json\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let command = ReviewCommand::new([OsStr::new("./review.sh")])
            .unwrap()
            .resolve(daemon_directory.path())
            .unwrap();
        let outcome = command.execute(worktree.path(), &json!({}), None).unwrap();

        assert!(outcome.is_success());
        assert!(worktree.path().join("reviewed.json").exists());
    }

    #[test]
    fn resolve_rejects_a_missing_program() {
        let daemon_directory = tempdir().unwrap();
        for program in ["./missing-review.sh", "github-reviews-missing-program"] {
            let error = ReviewCommand::new([OsStr::new(program)])
                .unwrap()
                .resolve(daemon_directory.path())
                .unwrap_err();
            assert!(
                matches!(error, RunnerError::ProgramNotFound(_)),
                "{program}"
            );
        }
    }

    #[test]
    fn rejects_an_empty_command() {
        assert!(matches!(
            ReviewCommand::new(Vec::<OsString>::new()),
            Err(RunnerError::EmptyCommand)
        ));
    }
}
