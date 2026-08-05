# Use nightly toolchain for fmt as our rustfmt.toml requires unstable features
# Clippy pulls default toolchain from the rust-toolchain.toml file.
TOOLCHAIN_FMT := "nightly-2025-10-01"

fmt:
  rustup toolchain install {{TOOLCHAIN_FMT}} > /dev/null 2>&1 && \
  cargo +{{TOOLCHAIN_FMT}} fmt

fmt-check:
  rustup toolchain install {{TOOLCHAIN_FMT}} > /dev/null 2>&1 && \
  cargo +{{TOOLCHAIN_FMT}} fmt --check

clippy:
	cargo clippy --locked --all-features --no-deps --all-targets -- -D warnings

clippy-fix:
	cargo clippy --fix --locked --all-features --no-deps --all-targets -- -D warnings

# cargo machete finds deps a crate declares but never uses; check_workspace_deps.sh
# finds the reverse, [workspace.dependencies] entries no crate consumes.
machete:
  cargo install cargo-machete --locked && \
  cargo machete && \
  ./scripts/check_workspace_deps.sh

test:
  cargo test --workspace --all-features --locked

lint: fmt clippy machete test
