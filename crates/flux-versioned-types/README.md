# flux-versioned-types

`flux-versioned-types` defines Rust structs and enums as an explicit series of schema versions. It generates migrations and can decode a bincode vector using the `TypeHash` stored alongside it.

```rust
use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_versioned_types::{VersionedDeserialize, versioned_struct};

versioned_struct!(Reading =>
    #[type_hash_lock(hash = 17013878556110425249)]
    ReadingV1 { pub value: u32 }

    #[type_hash_lock(hash = 761223436273093920)]
    ReadingV2 {
        modify { value: u64 = u64::from }
        add { pub valid: bool = true }
    }
);

let old = vec![ReadingV1 { value: 7 }];
let bytes = bincode::serialize(&old)?;
let stored_hash = ReadingV1::TYPE_HASH ^ 123456;
let latest = Reading::versioned_deserialize_vec(stored_hash, &bytes)?;
assert_eq!(latest[0].value, 7);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The bincode representation, `TypeHash` values, and stored type-hash XOR value (`123456`) are compatibility-sensitive. Evolve a type by adding a new version; do not rewrite an already-persisted version.

## Adding missing type hashes

Type-hash locks make accidental schema changes fail at compile time instead of silently changing the identifiers used to deserialise data.

Run the packaged script from the root of any downstream repository that depends on `flux-versioned-types` to add its missing type-hash locks:

```bash
bash "$(dirname "$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "flux-versioned-types") | .manifest_path')")/scripts/add-missing-typehashes.sh"
```

If you already use [`just`](https://just.systems/), add this recipe, it also accepts optional Cargo arguments e.g. `just typehash -p common`:

```just
# Add locks to versioned types that are missing one. Existing locks are never modified.
typehash *cargo_args:
  manifest="$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "flux-versioned-types") | .manifest_path')"; \
  bash "$(dirname "$manifest")/scripts/add-missing-typehashes.sh" {{cargo_args}}
```

The script adds imports and locks only for versioned types that do not already have a lock. It does not replace an existing but incorrect hash, and it requires `jq`.
