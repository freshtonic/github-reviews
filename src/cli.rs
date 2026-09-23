use std::{ffi::OsString, str::FromStr, time::Duration};

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "github-reviews", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Register the current GitHub repository and worktree.
    Register,
    /// Remove the current GitHub repository from the registry.
    Unregister,
    /// Poll GitHub and run the configured review command.
    Run(RunArgs),
    /// Show repository, daemon, and action state.
    Status,
    /// Retry the newest parked action for OWNER/REPO#NUMBER.
    Retry {
        /// Pull request selector, for example acme/widgets#42.
        selector: String,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum Mode {
    #[default]
    Sync,
    Async,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Base poll interval. GitHub's X-Poll-Interval can increase it.
    #[arg(long, default_value = "1m", value_parser = parse_duration)]
    pub interval: Duration,

    /// Whether review commands block polling.
    #[arg(long, value_enum, default_value_t = Mode::Sync)]
    pub mode: Mode,

    /// Maximum concurrent review commands in async mode.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    pub max_concurrency: usize,

    /// Optional limit for one review-command attempt.
    #[arg(long, value_parser = parse_duration)]
    pub review_timeout: Option<Duration>,

    /// Program and literal arguments. This option must be last.
    #[arg(
        long,
        required = true,
        num_args = 1..,
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    pub review_command: Vec<OsString>,
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".into());
    }
    Ok(duration)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    if value == 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PullRequestSelector {
    pub full_name: String,
    pub number: u64,
}

impl FromStr for PullRequestSelector {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (full_name, number) = value
            .rsplit_once('#')
            .ok_or_else(|| anyhow::anyhow!("expected OWNER/REPO#NUMBER"))?;
        if full_name.split('/').count() != 2 || full_name.contains(char::is_whitespace) {
            bail!("expected OWNER/REPO#NUMBER");
        }
        let number = number.parse::<u64>()?;
        if number == 0 {
            bail!("pull request number must be greater than zero");
        }
        Ok(Self {
            full_name: full_name.to_owned(),
            number,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_arguments_can_begin_with_hyphens() {
        let cli = Cli::try_parse_from([
            "github-reviews",
            "run",
            "--mode",
            "async",
            "--review-command",
            "reviewer",
            "--model",
            "fast",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run")
        };
        assert_eq!(args.mode, Mode::Async);
        assert_eq!(
            args.review_command,
            ["reviewer", "--model", "fast"].map(OsString::from)
        );
    }

    #[test]
    fn parses_retry_selector() {
        assert_eq!(
            "acme/widgets#42".parse::<PullRequestSelector>().unwrap(),
            PullRequestSelector {
                full_name: "acme/widgets".into(),
                number: 42
            }
        );
    }
}
