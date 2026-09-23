# github-reviews

`github-reviews` polls GitHub review-request notifications and runs a local review command from the matching repository clone. It is designed for review scripts that delegate to an LLM and benefit from local Git history.

## Requirements

- Rust 1.85 or newer
- Git
- [GitHub CLI](https://cli.github.com/) authenticated to GitHub.com with a classic token that can read notifications and the registered repositories

## Build

```sh
cargo build --release
```

## Register repositories

Run this once from each clone that should receive reviews:

```sh
github-reviews register
```

Registration maps the clone's canonical GitHub `origin` to its worktree root. Registering another clone of the same repository replaces the previous path.

```sh
github-reviews status
github-reviews unregister
```

## Run

```sh
github-reviews run --review-command ./scripts/review-pr
```

The default mode is synchronous with a one-minute poll interval. Async mode keeps polling while commands run and permits four concurrent commands globally, but never more than one per repository:

```sh
github-reviews run \
  --mode async \
  --max-concurrency 4 \
  --review-timeout 30m \
  --review-command ./scripts/review-pr --model strong
```

`--review-command` consumes the rest of the command line. Put every `github-reviews` option before it.

The command:

- runs from the registered worktree root;
- inherits the daemon's environment, stdout, and stderr;
- receives one versioned JSON object followed by a newline and EOF on stdin;
- runs only after the exact current base and pull-request head objects have been fetched and verified without checkout.

A successful command is not repeated for the same review action. Failed commands receive three attempts and are then parked:

```sh
github-reviews retry OWNER/REPO#123
```

## State and logging

State is shared in `~/.config/github-reviews/state.sqlite3`. The directory and database are restricted to the current user. Run the daemon in the foreground under launchd, systemd, or another supervisor; logs are written to stderr. Set `RUST_LOG` to change verbosity.

The daemon uses `gh` for API authentication and transport. It never marks notifications read, checks out branches, or modifies worktree files.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
