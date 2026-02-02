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
	cargo clippy --all-features --no-deps -- -D warnings -A clippy::collapsible_if

clippy-fix:
	cargo clippy --fix --all-features --no-deps -- -D warnings -A clippy::collapsible_if

machete:
  cargo install cargo-machete && \
  cargo machete