# github-reviews

`github-reviews` polls GitHub review-request notifications and runs a local review command from the matching repository clone. It is designed for review scripts that delegate to an LLM and benefit from local Git history.

## Demo

This deterministic demo uses synthetic GitHub data and an inert Automated Review Command. It performs no GitHub mutations. To regenerate it, run `demo/render.sh` (requires [VHS](https://github.com/charmbracelet/vhs) and FFmpeg).

![Terminal demo showing github-reviews installation, repository registration, review-command launch, and graceful shutdown](docs/assets/github-reviews-demo.gif)

<details>
<summary>Demo transcript</summary>

Log lines are shortened: the timestamp, level, and module prefix are removed. Two pull request discovery log lines are also removed.

```console
$ cargo install --locked --path .
  Installing github-reviews v0.1.0 (~/src/github-reviews)
    Finished `release` profile [optimized] target(s)
   Installed package `github-reviews v0.1.0 (~/src/github-reviews)` (executable `github-reviews`)
$ cd ~/src/acme/widgets
$ github-reviews register
registered acme/widgets at ~/src/acme/widgets
$ github-reviews status
state: /tmp/github-reviews-readme-demo/state.sqlite3
registered repositories: 1
  acme/widgets -> ~/src/acme/widgets
actions: 0 pending, 0 running, 0 parked, 0 succeeded, 0 cancelled
$ github-reviews run --interval 1s --review-command demo-review
daemon started viewer=demo-user
repository bootstrap complete repository=acme/widgets
review action queued repository=acme/widgets pull=42 action=1 reason="review_requested"
launching review command for (#42) Add retry support action=1 repository=acme/widgets
Reviewing acme/widgets#42: Add retry support
Demo review completed; no GitHub changes were made.
review command succeeded action=1 repository=acme/widgets pull=42
^C
CTRL-C received; immediately stopping acceptance of new review requests
press CTRL-C again to immediately abort and kill all in-progress reviews
waiting for 0 in-progress reviews to complete
all reviews completed, quitting
```

</details>

## Requirements

- Rust 1.85 or newer
- Git
- [GitHub CLI](https://cli.github.com/) authenticated to GitHub.com with a classic token that can read notifications and the registered repositories

## Install

```sh
cargo install --locked --path .
```

This installs `github-reviews` into Cargo's binary directory (`~/.cargo/bin` by default). Ensure that directory is on `PATH` before continuing.

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

## Example Claude reviewer

[`examples/claude-review.py`](examples/claude-review.py) provides the same review-command contract using Claude Code. It runs Claude Opus at high effort in non-interactive mode, disables session persistence and subagents, and applies a USD 5 maximum API budget per attempt:

```sh
github-reviews run \
  --mode async \
  --max-concurrency 1 \
  --review-timeout 15m \
  --review-command /absolute/path/to/github-reviews/examples/claude-review.py \
  'Concentrate on correctness, security, and missing tests.'
```

The example requires Claude Code 2.1.280 or newer, an authenticated Claude account or API configuration, `gh` authenticated with permission to write pull-request reviews, and Python 3.

Configured Claude skills, plugins, and repository instructions remain loaded. The invocation uses `dontAsk` mode with pre-approved read tools, read-only Git commands, and the `gh` commands needed to inspect and update the exact pull request. File-editing tools and subagents are explicitly denied. Unlike the Codex example's OS-level filesystem sandbox, this boundary is enforced by Claude Code's permission rules; trusted user and project settings or hooks can affect those rules, so audit them before unattended use.

Claude performs review operations directly through `gh` and returns a structured execution report. The wrapper fails closed when Claude exits unsuccessfully, omits structured output, reports a partial failure, or exceeds its budget. The daemon's timeout and retry behavior can still leave partial GitHub changes, so retries are instructed to inspect existing activity before acting.

## State and logging

State is shared in `~/.config/github-reviews/state.sqlite3`. The directory and database are restricted to the current user. Run the daemon in the foreground under launchd, systemd, or another supervisor; logs are written to stderr. Set `RUST_LOG` to change verbosity.

The daemon uses `gh` for API authentication and transport. It never marks notifications read, checks out branches, or modifies worktree files.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
