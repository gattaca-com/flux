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

Every `versioned_struct!`/`versioned_enum!` chain is wire-ready by default:
the generated code names the re-exported `ByteStable` derive, so consumers
need no direct `byte-stable` dependency. Every version must be a padding-free
`repr(C)` struct or `repr(u8)` fieldless enum; padded types fail to compile
at the derive, which is intended.

Every field must implement `ByteStable` too. Flux provides it for ints, bool,
arrays, `ArrayStr`, `Nanos`, and — behind `byte-stable` features — `Uuid`,
`FixedBytes`/`B256`/`Address`, and `Uint`/`U256`. Foreign types are supported
through these flux-owned impls, so leaves can name them directly.

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

Family enums (an enum of leaves or other families) use the `VersionedLeaves`
derive, re-exported from `flux_versioned_types`; it implements the
`HasVersionedLeaves` trait:

```rust
use flux_versioned_types::VersionedLeaves;

#[derive(Clone, Copy, VersionedLeaves)]
enum Family {
    A(LeafA),
    B(LeafB),
    #[leaves(skip)]
    Other,
}
```

Every non-skipped variant must be a newtype with exactly one unnamed field;
the same field type in two variants is an error (ambiguous `From`).

Chains that cannot satisfy padding-free `Copy` layouts (padding, `String`,
...) opt out with a leading `#[wire_skip]` and keep the bincode codec only.

The derive
generates `HasVersionedLeaves` (matching `visit_leaf` down to the leaf,
`decode_blob` trying each variant in order) plus `From<Field> for Enum` for
each kept variant.

## Zero-copy blobs (`Blob`, `Scratch`, `BlobCache`)

`flux_versioned_types::raw` batches versioned leaves into one wire and disk
format. A `Blob` is an unsized `repr(C)` struct whose bytes in memory *are*
the format:

```text
[header 144 B][user metadata, padded to 8][zstd( [TrackingTimestampWire x n][Leaf x n] )]
```

The header is exactly 144 bytes with no padding: magic `b"FLUXBLOB"`, format
version, user metadata length, message count, the `TYPE_HASH` of the leaf
version that wrote the blob, the user metadata's `TYPE_HASH`, compressed and
decompressed tail lengths, the earliest and latest publish wall clocks in the
batch (`publish_t_first`/`publish_t_last`, advisory, for ordering and
indexing without decompressing), and the leaf's `NAME` truncated to 64 bytes. The user metadata
section carries one uncompressed `Versioned` value (e.g. slot and instance)
so routers can read it without decompressing; both it and the zstd tail are
zero-padded to a multiple of 8, so every blob's total length is a multiple
of 8 and concatenated blobs in a file stay aligned.

Sending is a memcpy of `Blob::as_bytes`; receiving is a validated cast in
`Blob::from_bytes`, which checks alignment, magic, version, header lengths,
and `type_name`. The leaf section decodes through `Versioned::decode_versions`,
so a blob written as an older version migrates on read. `BlobCache`
accumulates `InternalMessage`s per leaf type (`push` appends the message's
projected timestamp and the leaf's bytes, no serialization) and `flush`
builds one `Blob` per non-empty leaf type with a caller-chosen user metadata
value and zstd level. `Scratch` is the 8-aligned buffer behind both
decompression and blob building, and behind `load` for realigning
untrusted input.

```rust
use flux_versioned_types::{BlobCache, Scratch};

let mut cache = BlobCache::new();
// cache.push(&msg);
let meta = NewBidSubmission { value: 7 };
let mut scratch = Scratch::new();
cache.flush(&meta, 3, |blob| {
    assert_eq!(blob.user_metadata::<NewBidSubmission>().unwrap(), meta);
    let (_, msgs) = blob.decode::<NewBidSubmission, Bid>(&mut scratch).unwrap();
});
```

### Receiving: alignment, families, and clocks

`Blob::from_bytes` needs the slice 8-byte aligned. `flux-network`
`TcpStream`/`NetworkDriver` payloads are 8-aligned, so blobs received there
can be cast directly; `DiskIo` and other read buffers carry no such
guarantee, so realign them through `Scratch::load` first (a misaligned
`from_bytes` fails with `DecodeError::Unaligned`, while `load` copies and
then decodes).

`BlobCache::flush` emits one blob per non-empty leaf type, in stable
(sorted) key order. Arrival order across types is not preserved (within one
type it is), the `sink` may fire zero times when every buffer is empty, and
the `&Blob` handed to `sink` borrows the cache's internal `Scratch`: it is
valid only inside the callback, so copy `as_bytes` out if it must outlive
the call.

Receiving into a family mirrors the `family_blobs_decode_to_variants` test:
for each incoming blob call `Family::decode_blob::<Meta>` and extend a
single `Vec<InternalMessage<Family>>`, skipping blobs of unknown leaf types
(`None`).

`decode` rebuilds each `TrackingTimestamp` from the live clock, so a
`publish_t` that crossed hosts carries sub-millisecond rebuild noise;
`ingestion_time().real()` and `tile_id` are exact.

Disk files are concatenated blobs with no resync marker: decoding walks
blob to blob via `as_bytes().len()`, and a corrupt blob ends the walk.
Writers should rotate files per slot so one bad blob cannot hide the rest.

Leaf alignment must be at most 8, checked at compile time on `push`.
`Scratch` is single-threaded: keep one per decoding thread, plus one for
building if a thread also flushes.
