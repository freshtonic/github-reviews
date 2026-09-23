#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
demo_root=/tmp/github-reviews-readme-demo
demo_home=$demo_root/home
demo_repository=$demo_home/src/acme/widgets
demo_origin=$demo_root/widgets.git
demo_source=$demo_home/src/github-reviews
install_root=$demo_root/cargo-install
output=$repository_root/docs/assets/github-reviews-demo.gif
real_home=${HOME:?}
real_cargo_home=${CARGO_HOME:-$real_home/.cargo}
real_rustup_home=${RUSTUP_HOME:-$real_home/.rustup}

cleanup() {
    rm -rf -- "$demo_root"
}
trap cleanup EXIT INT TERM

rm -rf -- "$demo_root"
mkdir -p "$demo_repository" "$demo_source" "$install_root" "$(dirname -- "$output")"

# Install from a disposable copy so the recording does not show local paths.
git -C "$repository_root" ls-files -z --cached --others --exclude-standard |
    (cd "$repository_root" && tar -cf - --null -T -) |
    tar -xf - -C "$demo_source"

git init --quiet --bare "$demo_origin"
git -C "$demo_repository" init --quiet --initial-branch main
git -C "$demo_repository" config user.name "Demo Author"
git -C "$demo_repository" config user.email "demo@example.com"
git -C "$demo_repository" config commit.gpgsign false
printf '# Widgets\n' >"$demo_repository/README.md"
git -C "$demo_repository" add README.md
git -C "$demo_repository" commit --quiet -m "Initial commit"
base_sha=$(git -C "$demo_repository" rev-parse HEAD)

git -C "$demo_repository" remote add seed "$demo_origin"
git -C "$demo_repository" push --quiet seed HEAD:refs/heads/main
printf '\nRetry transient failures.\n' >>"$demo_repository/README.md"
git -C "$demo_repository" commit --quiet -am "Add retry support"
head_sha=$(git -C "$demo_repository" rev-parse HEAD)
git -C "$demo_repository" push --quiet seed HEAD:refs/pull/42/head
git -C "$demo_repository" reset --quiet --hard "$base_sha"
git -C "$demo_repository" remote remove seed
git -C "$demo_repository" remote add origin git@demo-gh:acme/widgets.git

# Prebuild so the recorded `cargo install` reuses these artifacts. The demo
# disables any rustc wrapper because wrappers can depend on the real HOME.
env -u RUSTUP_TOOLCHAIN \
    CARGO_BUILD_RUSTC_WRAPPER= \
    CARGO_TARGET_DIR="$repository_root/target" \
    CARGO_NET_OFFLINE=true \
    cargo build --manifest-path "$demo_source/Cargo.toml" --release --locked --quiet

cd "$demo_source"

env -u RUSTUP_TOOLCHAIN \
    HOME="$demo_home" \
    CARGO_HOME="$real_cargo_home" \
    RUSTUP_HOME="$real_rustup_home" \
    CARGO_INSTALL_ROOT="$install_root" \
    CARGO_BUILD_RUSTC_WRAPPER= \
    CARGO_TARGET_DIR="$repository_root/target" \
    CARGO_NET_OFFLINE=true \
    GITHUB_REVIEWS_STATE_PATH="$demo_root/state.sqlite3" \
    DEMO_GIT_ORIGIN="$demo_origin" \
    DEMO_BASE_SHA="$base_sha" \
    DEMO_HEAD_SHA="$head_sha" \
    PATH="$install_root/bin:$repository_root/demo/bin:$PATH" \
    PS1='$ ' \
    vhs "$repository_root/demo/github-reviews.tape" --output "$output" --quiet

if [ ! -x "$install_root/bin/github-reviews" ]; then
    printf 'cargo install did not install github-reviews during the recording\n' >&2
    exit 1
fi

size=$(wc -c <"$output" | tr -d ' ')
if [ "$size" -gt 2097152 ]; then
    printf 'demo GIF is %s bytes; expected at most 2097152\n' "$size" >&2
    exit 1
fi

printf 'wrote %s (%s bytes)\n' "$output" "$size"
