#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "typehash: jq is required" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if (($#)); then
  cargo_args=("$@")
else
  cargo_args=(--workspace --all-targets)
fi

run_check() {
  local output=$1
  set +e
  RUSTC_WRAPPER= cargo check "${cargo_args[@]}" --message-format=json >"$output"
  local status=$?
  set -e
  return "$status"
}

first_json="$tmp_dir/first.jsonl"
first_status=0
run_check "$first_json" || first_status=$?

missing_tsv="$tmp_dir/missing.tsv"
jq -r '
  select(.reason == "compiler-message")
  | .message as $diagnostic
  | ($diagnostic.message | capture("^(?<name>[A-Za-z_][A-Za-z0-9_]*) is missing a type hash lock;")?) as $match
  | select($match != null)
  | ($diagnostic.spans[] | select(.is_primary)) as $span
  | [$span.file_name, ($span.line_start | tostring), $match.name]
  | @tsv
' "$first_json" | sort -u >"$missing_tsv"

if [[ ! -s "$missing_tsv" ]]; then
  if ((first_status)); then
    jq -r 'select(.reason == "compiler-message" and .message.level == "error") | .message.rendered // .message.message' "$first_json" >&2
    exit "$first_status"
  fi
  echo "All compiled versioned types already have type-hash locks."
  exit 0
fi

# Insert from the bottom of each file so earlier diagnostic line numbers remain
# valid. These are the only attributes this script is allowed to modify.
sort -t $'\t' -k1,1 -k2,2nr "$missing_tsv" |
while IFS=$'\t' read -r file line name; do
  indent="$(sed -n "${line}s/[^[:space:]].*//p" "$file")"
  sed -i "${line}i\\${indent}#[type_hash_lock(hash = 0)]" "$file"
  printf '%s\t%s\n' "$file" "$name" >>"$tmp_dir/inserted.tsv"
done

hash_json="$tmp_dir/hashes.jsonl"
run_check "$hash_json" || true

hashes_tsv="$tmp_dir/hashes.tsv"
jq -r '
  select(.reason == "compiler-message")
  | .message as $diagnostic
  | (
      (($diagnostic.rendered // "") | capture("found struct `[^`]*TypeHashLock<(?<hash>[0-9]+)>`")?)
      // ($diagnostic.message | capture("0_usize - (?<hash>[0-9]+)_usize")?)
    ) as $match
  | select($match != null)
  | ($diagnostic.spans[] | select(.is_primary)) as $span
  | (($span.text[0].text // "") | capture("(?<name>[A-Za-z_][A-Za-z0-9_]*)")?) as $source
  | select($source != null)
  | [$span.file_name, ($span.line_start | tostring), $source.name, $match.hash]
  | @tsv
' "$hash_json" | sort -u >"$hashes_tsv"

while IFS=$'\t' read -r file name; do
  mapfile -t lines <"$file"
  name_index=-1
  for index in "${!lines[@]}"; do
    if [[ ${lines[index]} =~ (^|[^A-Za-z0-9_])${name}([^A-Za-z0-9_]|$) ]]; then
      lower_bound=$((index > 7 ? index - 7 : 0))
      for ((candidate = index - 1; candidate >= lower_bound; candidate--)); do
        if [[ ${lines[candidate]} == *'#[type_hash_lock(hash = 0)]'* ]]; then
          name_index=$index
          placeholder_index=$candidate
          break 2
        fi
      done
    fi
  done
  if ((name_index < 0)); then
    echo "typehash: refusing to modify an existing lock for $name in $file" >&2
    exit 1
  fi

  hash="$(awk -F '\t' -v file="$file" -v line="$((name_index + 1))" -v name="$name" '
    $1 == file && $3 == name {
      distance = $2 - line
      if (distance < 0) distance = -distance
      if (!found || distance < best) {
        found = 1
        best = distance
        hash = $4
      }
    }
    END { if (found) print hash }
  ' "$hashes_tsv")"
  if [[ -z "$hash" ]]; then
    echo "typehash: could not determine the hash for $name in $file" >&2
    exit 1
  fi

  indent="${lines[placeholder_index]%%#*}"
  lines[placeholder_index]="${indent}#[type_hash_lock(hash = $hash)]"
  printf '%s\n' "${lines[@]}" >"$file"
  echo "Added #[type_hash_lock(hash = $hash)] to $name in $file"
done <"$tmp_dir/inserted.tsv"

final_json="$tmp_dir/final.jsonl"
final_status=0
run_check "$final_json" || final_status=$?
if ((final_status)); then
  jq -r 'select(.reason == "compiler-message" and .message.level == "error") | .message.rendered // .message.message' "$final_json" >&2
  exit "$final_status"
fi
