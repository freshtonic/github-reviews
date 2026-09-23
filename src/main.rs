use anyhow::Result;
use clap::Parser;
use github_reviews::cli::Cli;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();

    github_reviews::daemon::execute(Cli::parse())
}
