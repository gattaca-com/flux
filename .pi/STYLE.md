# Flux Code Style Guide

Style conventions distilled from the `main` branch. Follow these in all new and
modified code.

## Formatting

Formatting is enforced by **nightly rustfmt** via `just fmt` / `just fmt-check`.
The `rustfmt.toml` at the workspace root is the source of truth. Key settings:

- `group_imports = "StdExternalCrate"` — std first, then external crates, then local
- `imports_granularity = "Crate"` — one `use` per crate, braces for multiple items
- `use_small_heuristics = "Max"` — allow long single-line expressions
- `trailing_comma = "Vertical"` — trailing commas only in multi-line lists
- `use_field_init_shorthand = true` — `Self { inner }` not `Self { inner: inner }`
- `overflow_delimited_expr = true`

Always run `just fmt` before committing.

## Linting

`just clippy` runs `cargo clippy --all-features --no-deps -- -D warnings -A clippy::collapsible_if`.
All warnings are errors except `collapsible_if`.

## Language & Toolchain

- **Edition 2024**, `rust-version = "1.91.0"`
- Explicit `unsafe` blocks (edition 2024 semantics)
- Workspace dependency table in root `Cargo.toml` — crates reference
  `workspace = true`; don't duplicate version specs in leaf crates

## Code Organisation

### Module structure
- Flat `mod.rs` (or `lib.rs`) that declares submodules and re-exports the
  public API via `pub use`.
- Private implementation lives in submodule files.
- Prefer one type per file when it has meaningful impl blocks.

### Functions belong on types
- **Heavily favour methods / associated functions inside `impl` blocks** over
  free-standing functions. If a function operates primarily on a type, it should
  be a method on that type.
- Free functions are acceptable only for thin wrappers that delegate to a method
  (e.g. `shmem_queue()` → `Queue::create_or_open_shared()`), or truly
  type-independent utilities.

### Naming
- Types: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `SCREAMING_SNAKE_CASE`
- Crate names: `flux-*` (kebab-case)
- Short, descriptive names; abbreviations are fine when domain-standard
  (e.g. `shmem`, `flink`, `msg`, `prio`)

## Type Design

### Shared-memory types
- Always `#[repr(C)]` (often `#[repr(C, align(64))]` for cache-line alignment)
- Must be `Copy` — no heap pointers, no `String`, no `Vec`
- Use `ArrayVec<T, N>` / `ArrayStr<N>` for inline collections
- Derive `Serialize, Deserialize` (via `serde`) and `TypeHash` where needed

### Error handling
- **No `anyhow`**. Use `thiserror` derive for typed error enums, or
  `Box<dyn std::error::Error>` for ad-hoc propagation.
- Error variants are `#[repr(u8)]` when they cross FFI / shared-memory
  boundaries.

### Newtypes
- Wrap primitives in newtypes for domain clarity (e.g. `Duration(u64)`,
  `Nanos(u64)`, `IngestionTime(...)`)
- Implement arithmetic traits via `impl Add / Sub / Mul / Div` as appropriate.

## Performance Conventions

### Inlining
- `#[inline]` or `#[inline(always)]` on small, hot-path functions — especially
  on types like `ArrayVec`, `Duration`, queue producers/consumers.
- Omit on cold / complex functions; let the compiler decide.

### Iteration on hot paths
- Prefer manual `while` loops with explicit index management over iterator
  chains when the loop is on a latency-critical path.
  ```rust
  let mut i = self.conns.len();
  while i != 0 {
      i -= 1;
      // ...
  }
  ```
- Iterator chains are fine on cold paths or when clarity wins.

### Time
- Use `flux_timing::{Duration, Instant, Nanos}` — **not** `std::time`.
- `Duration::as_secs()` returns `f64`, not `u64`.
- `Instant` uses `elapsed_since()`, not `duration_since()`.

## Logging & Tracing

- Use the **`tracing`** crate (`warn!`, `error!`, `info!`, `debug!`), never
  `println!` or the `log` crate.
- Structured fields: `tracing::warn!(%code, ?param, "message")`.

## Documentation

- Doc comments on public trait methods and non-obvious public APIs.
- Skip boilerplate `/// Returns the ...` on trivial getters/setters.
- `///` for item docs; `//` for inline clarifications.
- Comments should explain *why*, not *what*, when the code is self-evident.

## Testing

- Tests live in `tests/` directories (integration) or inline `#[cfg(test)]`
  modules.
- `cargo test --workspace` must pass with zero failures.
- Use `tempfile` for any test that touches the filesystem.

## Commits

- Loosely conventional: `fix:`, `feat:`, or terse imperative description.
- PR number suffix when merging via GitHub (e.g. `fix: drain backlogs (#30)`).

## Things to Avoid

- `anyhow` — use `thiserror` or `Box<dyn Error>`
- `libc` outside of `flux-utils/src/thread.rs` — prefer `/proc` or `std` APIs
- `std::time` — use `flux_timing`
- Modifying `crates/spine-derive/` without explicit approval
- Free-standing functions when a method on the relevant type would suffice
