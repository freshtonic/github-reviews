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

## Example Codex reviewer

[`examples/codex-review.py`](examples/codex-review.py) runs one headless Codex review with `gpt-5.6-sol` at high reasoning effort. It accepts one argument containing additional review instructions and receives the normal review envelope on stdin.

Review commands run from the registered repository, so invoke the example by absolute path or install it somewhere on `PATH`:

```sh
github-reviews run \
  --mode async \
  --max-concurrency 1 \
  --review-timeout 15m \
  --review-command /absolute/path/to/github-reviews/examples/codex-review.py \
  'Concentrate on correctness, security, and missing tests.'
```

The example requires Codex CLI 0.156 or newer, `gh` authenticated with permission to write pull-request reviews, and Python 3. It deliberately does not require `jq`.

Codex receives read-only filesystem access and GitHub-only network access. It can inspect the checked-out repository, the verified pull-request head at `FETCH_HEAD`, and the verified base named by `pull_request.base_sha` in the envelope. User-configured skills and plugins remain loaded, while goals and multi-agent delegation are disabled to constrain expense.

This is an unattended, side-effecting example. Codex inspects the live pull-request conversation and uses `gh` directly to submit whichever review verdict, general comments, inline comments, replies, or thread-resolution changes it considers appropriate. The prompt prohibits other GitHub mutations and treats pull-request content as untrusted, but the GitHub credential remains the ultimate authorization boundary. Use a narrowly scoped credential and review the script before running it.

The script reports confirmed operations on stdout and exits unsuccessfully if Codex reports that any intended operation failed. A timed-out or failed action can leave partial GitHub changes; retries inspect existing activity and are instructed not to duplicate it. The 15-minute timeout above limits each attempt, but the daemon can make up to three attempts before parking an action.

## State and logging

State is shared in `~/.config/github-reviews/state.sqlite3`. The directory and database are restricted to the current user. Run the daemon in the foreground under launchd, systemd, or another supervisor; logs are written to stderr. Set `RUST_LOG` to change verbosity.

The daemon uses `gh` for API authentication and transport. It never marks notifications read, checks out branches, or modifies worktree files.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
