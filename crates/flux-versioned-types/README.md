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

## Zero-copy leaves (`zerocopy` feature)

Opt in per consumer crate with `flux-versioned-types/zerocopy`, plus a direct
`zerocopy = { version = "0.8", features = ["derive"] }` dependency (needed for
the generated `#[derive(::zerocopy::...)]` paths, just as `bincode` is already
required; the locked zerocopy-derive only accepts a bare ident for its
`crate` attribute, so the re-export cannot be used there). Every version must
then be a padding-free `repr(C)` struct or `repr(u8)` fieldless enum; padded
types fail to compile at the derive, which is intended.

Each `versioned_struct!`/`versioned_enum!` chain then also implements
`Versioned` (plain `TYPE_HASH`es in `VERSION_HASHES`, oldest first, and a
`decode_versions` that casts raw bytes as the stored version and migrates)
and the trivial `HasVersionedLeaves` (the leaf is itself).

```rust
use flux_versioned_types::versioned_struct;

versioned_struct!(#[wire_name = "Relay.NewBidSubmission"] NewBidSubmission =>
    #[type_hash_lock(hash = 17013878556110425249)]
    NewBidSubmissionV1 { pub value: u64 }
);
assert_eq!(NewBidSubmission::NAME, "Relay.NewBidSubmission");
```

`#[wire_name = "..."]` overrides `Versioned::NAME`; without it `NAME` is the
alias name. Existing callers without the attribute compile unchanged.

Family enums (an enum of leaves or other families) use the derive, re-exported
as `flux_versioned_types::HasVersionedLeaves`:

```rust
use flux_versioned_types::HasVersionedLeaves;

#[derive(Clone, Copy, HasVersionedLeaves)]
enum Family {
    A(LeafA),
    B(LeafB),
    #[leaves(skip)]
    Other,
}
```

Every non-skipped variant must be a newtype with exactly one unnamed field;
the same field type in two variants is an error (ambiguous `From`). The derive
generates `HasVersionedLeaves` (matching `visit_leaf` down to the leaf,
`decode_blob` trying each variant in order) plus `From<Field> for Enum` for
each kept variant.
