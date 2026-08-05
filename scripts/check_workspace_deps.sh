#!/usr/bin/env bash
# Fail if any workspace dependency is not used by any crate.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
workspace_toml="$root/Cargo.toml"

# Internal crates remain available through `workspace = true` even when no
# current member depends on them.
ignored=(
  flux-network
  flux-profiler
  flux-timekeeper
)

# Member crates must inherit every dependency from [workspace.dependencies].
python3 - "$root"/crates/*/Cargo.toml <<'PY'
import sys
import tomllib

errors = []
for manifest in sys.argv[1:]:
    with open(manifest, "rb") as file:
        data = tomllib.load(file)

    tables = []
    for name in ("dependencies", "dev-dependencies", "build-dependencies"):
        tables.append((name, data.get(name, {})))
    for target, target_config in data.get("target", {}).items():
        for name in ("dependencies", "dev-dependencies", "build-dependencies"):
            tables.append((f"target.{target}.{name}", target_config.get(name, {})))

    for table, dependencies in tables:
        for dependency, config in dependencies.items():
            if not isinstance(config, dict) or config.get("workspace") is not True:
                errors.append(f"{manifest}: [{table}] {dependency} must use workspace = true")

if errors:
    print("\n".join(errors))
    raise SystemExit(1)
PY

# Extract dep names: lines in [workspace.dependencies] that look like `name = ...` or `name.something`
deps=$(sed -n '/^\[workspace\.dependencies\]/,/^\[/{
  /^\[/d
  /^#/d
  /^$/d
  /^[a-zA-Z]/!d
  s/\s*[=.].*//
  p
}' "$workspace_toml")

unused=""
for dep in $deps; do
  if [[ " ${ignored[*]} " == *" $dep "* ]]; then
    continue
  fi
  if ! grep -rq "^${dep}[. =]" "$root"/crates/*/Cargo.toml 2>/dev/null; then
    unused="$unused $dep"
  fi
done

if [ -n "$unused" ]; then
  echo "Unused workspace dependencies:"
  for dep in $unused; do echo "  - $dep"; done
  exit 1
fi
