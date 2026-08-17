# flux-versioned-types

`flux-versioned-types` defines Rust structs and enums as an explicit sequence of
schema versions. It generates adjacent and transitive migrations and can decode
a bincode vector using the `TypeHash` stored alongside it.

The crate is intentionally policy-free: it does not choose directories, define
telemetry metadata, register application messages, or prescribe storage. Those
belong in downstream applications.

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

The bincode representation, `TypeHash` values, and stored type-hash XOR value
(`123456`) are compatibility-sensitive. Evolve a type by
adding a new version; do not rewrite an already-persisted version.
