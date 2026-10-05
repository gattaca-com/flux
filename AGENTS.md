# Repository guidance

## Communication

- Be concise and direct. Use short sentences and the active voice.
- Use the same term for the same item.
- State what a result means and how you verified it. Say when you did not verify it.
- If you find that an earlier statement was wrong, say so directly and correct it.

## Engineering priorities

Success means the smallest complete change that solves the requested problem and preserves existing behavior.

- Change only the requested behavior and necessary supporting code.
- Reuse established patterns. Add abstractions or dependencies only when required.
- Protect performance, especially in hot paths such as poll loops, per-message, and per-read code.
- Avoid unnecessary allocations, copies, locks, syscalls, clock reads, and logging in repeated loops.
- Reuse buffers and pools instead of allocating per call. Keep capacity across uses.
- Measure performance-sensitive changes when practical. State when measurement is not possible.
- Stop when the requested outcome is complete. Report unrelated issues instead of fixing them.

## Build and lint

```bash
just fmt          # Format with the nightly toolchain that rustfmt.toml requires.
just fmt-check    # The CI formatting check.
just clippy       # Clippy for all features and targets. Treat warnings as errors.
```

- Run `just fmt`, then `just clippy`, only at the end before handing off or pushing changes, unless the user specifies otherwise.
- Do not run broad workspace or whole-crate `cargo test` sweeps.
- Do not run plain `cargo fmt`. The stable toolchain ignores the nightly options in `rustfmt.toml` and reformats unrelated files.
- After you format, run `git diff --stat`. Make sure the diff contains only your change.

## Comments

Write a comment only when its removal would hide an invariant, ordering requirement, workaround, or non-obvious reason.

Do not use a comment for these purposes:

- To restate an operation, a nearby limit, or the next block.
- To explain edit history, a migration, or how a previous version behaved.
- To repeat information from a chat, pull request, or design document.

## Tests

Do not add tests by default. Add a test only when:

- The user explicitly requests it.
- It covers a demonstrated defect and fails without the fix.
- A changed safety-critical invariant or stable external contract has no existing coverage.

For permitted tests:

- Search existing coverage first; extend it where possible. Use the smallest test and data that protect observable behavior. Run only tests scoped to the change.
- Do not test internal predicates, metadata, wiring, direct conversions, implementation details, standard-library behavior, or Rust type-system guarantees.
- Do not duplicate production algorithms or add separate tests for equivalent cases.
- Do not change production APIs just for tests.
- Use mocks only when the real boundary is unavailable, unsafe, or too expensive.
- For async tests, wait for conditions with deadlines, not fixed sleeps. Repeat timing-sensitive tests before claiming stability.

## Git and pull requests

- Do not commit, push, or open a pull request unless the user asks.
- Before you reset, rebase, or discard changes, check `git status` and `git reflog`.
- Do not commit agent worktrees or other local tool state.
- Describe changes in generic terms. Do not name downstream users or internal projects in commits or pull requests.
- Write PR descriptions for engineers without the code open: explain the problem, resulting behavior, necessary implementation details, and evidence for measurable claims. Do not include `Validation`/`Testing` sections or lists of commands run.
- If requested, include benchmark numbers with their method: machine, cores, load, and what the numbers measure.

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

Follow [creating a version tag](README.md#creating-a-version-tag) in the README.
