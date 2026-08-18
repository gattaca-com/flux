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

enclosing_module_line() {
  local file=$1
  local target_line=$2
  local target_indent
  target_indent="$(sed -n "${target_line}s/[^[:space:]].*//p" "$file")"
  awk -v target="$target_line" -v target_indent="${#target_indent}" '
    NR >= target { exit }
    match($0, /^[[:space:]]*/) {
      indent = RLENGTH
      if ($0 ~ /^[[:space:]]*}/) {
        for (level in modules) if (level >= indent) delete modules[level]
      }
      if (indent < target_indent && $0 ~ /^[[:space:]]*(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?mod[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\{/) modules[indent] = NR
    }
    END {
      line = 0
      for (level in modules) if (modules[level] > line) line = modules[level]
      print line
    }
  ' "$file"
}

has_type_hash_lock_import() {
  local file=$1
  local module_line=$2
  local target_line=$3
  awk -v start="$module_line" -v end="$target_line" '
    NR <= start || NR >= end { next }
    /^[[:space:]]*(pub[[:space:]]+)?use[[:space:]]/ {
      statement = $0
      while (statement !~ /;[[:space:]]*$/ && getline > 0) statement = statement " " $0
      if (statement ~ /(^|[^A-Za-z0-9_])type_hash_lock([^A-Za-z0-9_]|$)/) found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$file"
}

add_type_hash_lock_import() {
  local file=$1
  local module_line=$2
  if ((module_line)); then
    local module_indent
    module_indent="$(sed -n "${module_line}s/[^[:space:]].*//p" "$file")"
    sed -i "$((module_line + 1))i\\${module_indent}    use flux::type_hash_derive::type_hash_lock;" "$file"
    return
  fi
  local first_use
  first_use="$(grep -nEm1 '^[[:space:]]*(pub[[:space:]]+)?use[[:space:]]' "$file" | cut -d: -f1)"
  if [[ -z "$first_use" ]]; then
    echo "typehash: cannot find a safe import insertion point in $file" >&2
    exit 1
  fi
  sed -i "${first_use}i\\use flux::type_hash_derive::type_hash_lock;" "$file"
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
  module_line="$(enclosing_module_line "$file" "$line")"
  if ! has_type_hash_lock_import "$file" "$module_line" "$line"; then
    printf '%s\t%s\n' "$file" "$module_line" >>"$tmp_dir/needs-import.tsv"
  fi
  indent="$(sed -n "${line}s/[^[:space:]].*//p" "$file")"
  sed -i "${line}i\\${indent}#[type_hash_lock(hash = 0)]" "$file"
  printf '%s\t%s\n' "$file" "$name" >>"$tmp_dir/inserted.tsv"
done

if [[ -s "$tmp_dir/needs-import.tsv" ]]; then
  sort -u -t $'\t' -k1,1 -k2,2nr "$tmp_dir/needs-import.tsv" |
  while IFS=$'\t' read -r file module_line; do
    add_type_hash_lock_import "$file" "$module_line"
  done
fi

cp "$tmp_dir/inserted.tsv" "$tmp_dir/pending.tsv"
iteration=0
while [[ -s "$tmp_dir/pending.tsv" ]]; do
  iteration=$((iteration + 1))
  hash_json="$tmp_dir/hashes-$iteration.jsonl"
  run_check "$hash_json" || true

  hashes_tsv="$tmp_dir/hashes-$iteration.tsv"
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

  : >"$tmp_dir/next-pending.tsv"
  progress=0
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
      printf '%s\t%s\n' "$file" "$name" >>"$tmp_dir/next-pending.tsv"
      continue
    fi

    indent="${lines[placeholder_index]%%#*}"
    lines[placeholder_index]="${indent}#[type_hash_lock(hash = $hash)]"
    printf '%s\n' "${lines[@]}" >"$file"
    echo "Added #[type_hash_lock(hash = $hash)] to $name in $file"
    progress=$((progress + 1))
  done <"$tmp_dir/pending.tsv"

  if [[ -s "$tmp_dir/next-pending.tsv" ]] && ((progress == 0)); then
    echo "typehash: could not determine the remaining hashes:" >&2
    sed 's/^/  /' "$tmp_dir/next-pending.tsv" >&2
    exit 1
  fi
  mv "$tmp_dir/next-pending.tsv" "$tmp_dir/pending.tsv"
done

final_json="$tmp_dir/final.jsonl"
final_status=0
run_check "$final_json" || final_status=$?
if ((final_status)); then
  next_missing="$tmp_dir/next-missing.tsv"
  jq -r '
    select(.reason == "compiler-message")
    | .message as $diagnostic
    | ($diagnostic.message | capture("^(?<name>[A-Za-z_][A-Za-z0-9_]*) is missing a type hash lock;")?) as $match
    | select($match != null)
    | ($diagnostic.spans[] | select(.is_primary)) as $span
    | [$span.file_name, ($span.line_start | tostring), $match.name]
    | @tsv
  ' "$final_json" | sort -u >"$next_missing"

  if [[ -s "$next_missing" ]]; then
    pass="${TYPEHASH_PASS:-1}"
    if ((pass >= 10)); then
      echo "typehash: exceeded 10 missing-lock discovery passes" >&2
      exit 1
    fi
    echo "More missing type-hash locks became visible; continuing with pass $((pass + 1))..."
    exec env TYPEHASH_PASS="$((pass + 1))" bash "$0" "${cargo_args[@]}"
  fi

  jq -r 'select(.reason == "compiler-message" and .message.level == "error") | .message.rendered // .message.message' "$final_json" >&2
  exit "$final_status"
fi
