# Correct and enforce the minimum supported Rust version

Status: needs-triage

## Finding

`README.md` promises Rust 1.85 or newer, but the let-chain in `src/github.rs` requires Rust 1.88. A locked check with Rust 1.85 fails with E0658. `Cargo.toml` does not declare `package.rust-version`.

## Expected outcome

Either rewrite the incompatible expression to support Rust 1.85 or raise the documented minimum to the actual supported version. Declare the chosen version in `Cargo.toml` and verify it in CI.
