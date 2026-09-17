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

## Zero-copy leaves

Every `versioned_struct!`/`versioned_enum!` chain derives `ByteStable` and
implements `Versioned` and `HasVersionedLeaves`. Versions must be padding-free
`repr(C)` structs or `repr(u8)` fieldless enums of `ByteStable` fields; flux
provides impls for ints, `bool`, arrays, `ArrayStr`, `Nanos`, and behind
`byte-stable` features `Uuid`, `FixedBytes`/`B256`/`Address`, `Uint`/`U256`.
Consumers need no direct `byte-stable` dependency. `#[wire_skip]` keeps the
bincode codec only; `#[wire_name = ".."]` sets `Versioned::NAME`.

```rust
versioned_struct!(#[wire_name = "Relay.NewBidSubmission"] NewBidSubmission =>
    #[type_hash_lock(hash = 17013878556110425249)]
    NewBidSubmissionV1 { pub value: u64 }
);

#[derive(Clone, Copy, VersionedLeaves)]
enum Family {
    A(LeafA),
    B(LeafB),
    #[leaves(skip)]
    Other,
}
```

Family variants are newtypes of leaves or families; the same field type twice
is an error.

## Zero-copy blobs

```text
[BlobHeader 144 B][user metadata, padded to 8][zstd( [TrackingTimestampWire x n][Leaf x n] )]
```

The bytes of a `Blob` are the format: `as_bytes` to send, `from_bytes` to
receive, concatenate for a file. The user metadata is one uncompressed
`Versioned` value; `publish_t_first`/`publish_t_last` give the batch's time
span without decompressing. `BlobCache::push` buffers `InternalMessage`s per
leaf type; `flush` emits one `Blob` per type, valid only inside the callback.

```rust
let mut cache = BlobCache::new();
cache.push(&msg);
cache.flush(&meta, 3, |blob| {
    let (_, msgs) = blob.decode::<Meta, Bid>(&mut scratch).unwrap();
});
```

`from_bytes` needs an 8-aligned slice: `flux-network` payloads are, `DiskIo`
reads are not (use `Scratch::load`). Receive families with
`Family::decode_blob`, one call per blob. Files have no resync marker: rotate
per slot. Rebuilt `publish_t` carries sub-millisecond clock noise across
hosts; `ingestion_time().real()` and `tile_id` are exact.
