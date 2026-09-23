# Fix the README installation flow

Status: needs-triage

## Finding

The setup instructions run `cargo build --release` and then invoke bare `github-reviews`. A release build creates `target/release/github-reviews` but does not place it on `PATH`, so a clean user gets command-not-found or accidentally runs an older installed binary.

## Expected outcome

Document an installation command such as `cargo install --path .`, or consistently invoke the built binary by its path. Ensure the subsequent registration and daemon examples use the artifact produced by the documented setup.
