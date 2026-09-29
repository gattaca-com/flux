# Use nightly toolchain for fmt as our rustfmt.toml requires unstable features
# Clippy pulls default toolchain from the rust-toolchain.toml file.
TOOLCHAIN_FMT := "nightly-2025-10-01"

_default:
  @just --list

fmt:
  rustup toolchain install {{TOOLCHAIN_FMT}} --component rustfmt > /dev/null 2>&1 && \
  cargo +{{TOOLCHAIN_FMT}} fmt

fmt-check:
  rustup toolchain install {{TOOLCHAIN_FMT}} --component rustfmt > /dev/null 2>&1 && \
  cargo +{{TOOLCHAIN_FMT}} fmt --check

clippy:
  cargo clippy --locked --all-features --no-deps --all-targets -- -D warnings

clippy-fix:
  cargo clippy --fix --locked --all-features --no-deps --all-targets -- -D warnings

# Find unused deps: in crates (cargo machete) and in [workspace.dependencies].
machete:
  cargo install cargo-machete --locked && \
  cargo machete && \
  ./scripts/check_workspace_deps.sh

test:
  cargo test --workspace --all-features --locked

# Validate, create, and push the workspace version tag from local main.
release:
  python3 .github/scripts/release.py --push-tag

lint: fmt clippy machete test
