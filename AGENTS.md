# Repository guidance

## Releases

Flux uses one version for every crate. The source of truth is
`workspace.package.version` in the root `Cargo.toml`. Every workspace member
must inherit `version`, `repository`, `license`, and `publish`; do not add
crate-specific copies of those fields. Releases are GitHub-only, so
`workspace.package.publish` remains `false`.

### Version policy

Before preparing a release, review every change since the latest
`vMAJOR.MINOR.PATCH` tag and decide whether any change breaks compatibility.
Breaking changes are not limited to Rust public API changes: consider runtime
behavior, wire and persistence formats, configuration, and CLI contracts too.

Choose the next version according to Cargo's semantic-compatibility boundaries:

| Current version | Compatible fix or feature | Breaking change |
| --- | --- | --- |
| `0.1.x` | bump patch (`0.1.1`) | bump minor (`0.2.0`) |
| `1.2.x` | fix: `1.2.4`; feature: `1.3.0` | bump major (`2.0.0`) |

Before `1.0.0`, Flux follows Cargo's left-most-non-zero compatibility rule. A
breaking change to `0.1.x` is therefore released as `0.2.0`, while compatible
changes remain in `0.1.x`.

`cargo-semver-checks` compares every version bump with the latest release tag.
It catches Rust public API breakage under a compatible bump. It cannot detect
behavioral, wire-format, persistence-format, configuration, or CLI breakage,
so those changes still require an intentionally incompatible version bump.

### Release process

1. Change only `workspace.package.version` in the root `Cargo.toml`.
2. Run `cargo check --workspace --all-features` to refresh `Cargo.lock`.
3. Run `cargo test --workspace --all-features --locked`.
4. Open a pull request containing the version bump and the updated `Cargo.lock`.

The release checks validate the version shape, confirm every crate inherits the
workspace package metadata, and check public API compatibility. Once the change
is merged to `main`, create the version tag from a machine whose public IP is in
the GitHub organization allow list:

1. Check out `main` with a clean worktree.
2. Run `just release`.

The command fetches `origin` and all tags, requires local `main` to exactly match
`origin/main`, repeats the workspace/version validation, creates an annotated
`vMAJOR.MINOR.PATCH` tag, and pushes only that tag. It never moves or recreates
an existing version tag. It does not create a GitHub Release or build release
artifacts.
