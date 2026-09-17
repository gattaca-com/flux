use core::mem::offset_of;

use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_timing::{
    Duration, IngestionTime, Instant, InternalMessage, Nanos, PublishDelta, TrackingTimestamp,
};
use flux_utils::ArrayStr;
use flux_versioned_types::{
    Blob, BlobCache, BlobHeader, ByteStable, DecodeError, HasVersionedLeaves, Scratch,
    TrackingTimestampWire, TrackingTimestampWireV1, Versioned, VersionedLeaves,
    VisitorVersionedLeaf,
    byte_stable::slice_as_bytes,
    raw::{FORMAT_VERSION, MAGIC},
    versioned_enum, versioned_struct,
};

versioned_struct!(Leaf =>
    #[type_hash_lock(hash = 4125045508827104188)]
    LeafV1 { pub slot: u64 }

    #[type_hash_lock(hash = 371124184812242291)]
    LeafV2 {
        add { pub extra: u32 = 0, pub flags: u32 = 0 }
    }
);

versioned_struct!(Meta =>
    #[type_hash_lock(hash = 6493374068890055319)]
    MetaV1 { pub slot: u64, pub instance: ArrayStr<32> }
);

versioned_enum!(Kind =>
    #[type_hash_lock(hash = 6855142534858181385)]
    KindV1 { A, B }
);

versioned_struct!(Other =>
    #[type_hash_lock(hash = 11106094297253002543)]
    OtherV1 { pub x: u64 }
);

versioned_struct!(Flag =>
    #[type_hash_lock(hash = 17693854425529480628)]
    FlagV1 { pub ok: bool }
);

#[derive(Clone, Copy, Debug, PartialEq, VersionedLeaves)]
enum Sub {
    A(Leaf),
    #[leaves(skip)]
    Ignored,
}

struct Counter {
    n: usize,
}

impl VisitorVersionedLeaf for Counter {
    fn visit_leaf<L: Versioned>(&mut self, _leaf: &L) {
        self.n += 1;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, VersionedLeaves)]
enum Fam {
    S(Sub),
    K(Kind),
    #[leaves(skip)]
    Unrelated,
}

fn stamp(tile: u16, slot: u64) -> TrackingTimestamp {
    // Wall clocks must be near-live and in the past: `IngestionTime::from`
    // projects them back onto the TSC, which saturates for times far in the
    // past, and `elapsed` panics for times in the future (including the
    // publish time 5ms later).
    let ingestion = IngestionTime::new(
        Nanos(Nanos::now().0 - 10_000_000 - slot * 1_000_000),
        Instant(1_000_000 + u64::from(tile) * 1_000 + slot),
    );
    let publish = ingestion.internal() + Duration::from_millis(5);
    TrackingTimestamp {
        ingestion_t: ingestion,
        publish_delta: PublishDelta::new(tile)
            .from_ingestion_and_publish_t(ingestion.internal(), publish),
    }
}

fn meta_bytes(meta: &MetaV1) -> Vec<u8> {
    ByteStable::as_bytes(meta).to_vec()
}

fn hand_build(
    meta_hash: u64,
    meta: &[u8],
    leaf_hash: u64,
    leaf_name: &str,
    plain_tail: &[u8],
    n: u32,
    leaf_stride: usize,
) -> Vec<u8> {
    let comp = zstd::bulk::compress(plain_tail, 3).unwrap();
    let mut bytes = vec![0u8; size_of::<BlobHeader>()];
    // Field offsets come from the header type, values are written by hand,
    // so this stays independent of `BlobCache` while following the layout.
    let mut put = |at: usize, v: &[u8]| bytes[at..at + v.len()].copy_from_slice(v);
    put(offset_of!(BlobHeader, magic), &MAGIC);
    put(offset_of!(BlobHeader, version), &FORMAT_VERSION.to_le_bytes());
    put(offset_of!(BlobHeader, metadata_len), &(meta.len() as u32).to_le_bytes());
    put(offset_of!(BlobHeader, n_messages), &n.to_le_bytes());
    put(offset_of!(BlobHeader, type_hash), &leaf_hash.to_le_bytes());
    put(offset_of!(BlobHeader, metadata_type_hash), &meta_hash.to_le_bytes());
    put(offset_of!(BlobHeader, compressed_len), &(comp.len() as u64).to_le_bytes());
    put(
        offset_of!(BlobHeader, decompressed_len),
        &(u64::from(n) * (size_of::<TrackingTimestampWire>() as u64 + leaf_stride as u64))
            .to_le_bytes(),
    );
    put(offset_of!(BlobHeader, publish_t_first), &1u64.to_le_bytes());
    put(offset_of!(BlobHeader, publish_t_last), &2u64.to_le_bytes());
    let name = leaf_name.as_bytes();
    let name_at = offset_of!(BlobHeader, type_name);
    put(name_at, &(name.len() as u64).to_le_bytes());
    put(name_at + 8, name);
    bytes.extend_from_slice(meta);
    bytes.resize(bytes.len().next_multiple_of(8), 0);
    bytes.extend_from_slice(&comp);
    bytes.resize(bytes.len().next_multiple_of(8), 0);
    bytes
}

fn leaf_blob(n: u32) -> Vec<u8> {
    let mut stamps = Vec::new();
    let mut leaves = Vec::new();
    for i in 0..n {
        stamps.extend_from_slice(ByteStable::as_bytes(&TrackingTimestampWire::from(stamp(
            1,
            u64::from(i),
        ))));
        leaves.extend_from_slice(ByteStable::as_bytes(&Leaf {
            slot: u64::from(i),
            extra: 0,
            flags: 0,
        }));
    }
    let meta = MetaV1 { slot: 1, instance: ArrayStr::try_from("t").unwrap() };
    hand_build(
        MetaV1::TYPE_HASH,
        &meta_bytes(&meta),
        Leaf::TYPE_HASH,
        Leaf::NAME,
        &[stamps, leaves].concat(),
        n,
        size_of::<Leaf>(),
    )
}

#[test]
fn end_to_end_cache_flush_decode() {
    let mut cache = BlobCache::new();
    assert!(cache.is_empty());
    let mut originals = Vec::new();
    for i in 0..8u16 {
        let msg = InternalMessage::new(stamp(i, u64::from(i)), Leaf {
            slot: u64::from(i) * 10,
            extra: u32::from(i),
            flags: u32::from(i) + 1,
        });
        cache.push(&msg);
        originals.push(msg);
    }
    assert_eq!(cache.n_messages(), 8);
    let meta = Meta { slot: 42, instance: ArrayStr::try_from("tile-0").unwrap() };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].len() % 8, 0);
    assert!(cache.is_empty());
    assert_eq!(cache.n_messages(), 0);

    let mut probe = Scratch::new();
    let blob = probe.load(&blobs[0]).unwrap();
    assert!(blob.is::<Leaf>());
    assert!(!blob.is::<Other>());
    assert_eq!(blob.type_name(), Leaf::NAME);
    assert_eq!(blob.user_metadata::<Meta>().unwrap(), meta);
    assert_eq!(blob.as_bytes(), blobs[0].as_slice());
    let publishes: Vec<Nanos> = originals.iter().map(InternalMessage::publish_t).collect();
    assert_eq!(blob.header.publish_t_first, *publishes.iter().min().unwrap());
    assert_eq!(blob.header.publish_t_last, *publishes.iter().max().unwrap());
    assert!(blob.header.publish_t_first < blob.header.publish_t_last);

    let mut work = Scratch::new();
    let blob = work.load(&blobs[0]).unwrap();
    let (got_meta, msgs): (Meta, Vec<InternalMessage<Leaf>>) = blob.decode(&mut probe).unwrap();
    assert_eq!(got_meta, meta);
    assert_eq!(msgs.len(), originals.len());
    for (got, want) in msgs.iter().zip(&originals) {
        assert_eq!(got.data(), want.data());
        assert_eq!(got.ingestion_time().real(), want.ingestion_time().real());
        assert_eq!(got.tile_id(), want.tile_id());
        // The portable record keeps nanosecond wall clocks, but rebuilding a
        // local timestamp re-reads the live clock, so allow a tiny drift.
        let drift = got.publish_t().0 as i64 - want.publish_t().0 as i64;
        assert!(drift.abs() <= 1_000_000, "publish_t drifted by {drift}ns");
    }
}

#[test]
fn hand_built_v1_blob_migrates_to_latest() {
    let stamps =
        [TrackingTimestampWire::from(stamp(3, 7)), TrackingTimestampWire::from(stamp(4, 8))];
    let leaves = [LeafV1 { slot: 11 }, LeafV1 { slot: 22 }];
    let meta = MetaV1 { slot: 9, instance: ArrayStr::try_from("m").unwrap() };
    let bytes = hand_build(
        MetaV1::TYPE_HASH,
        &meta_bytes(&meta),
        LeafV1::TYPE_HASH,
        Leaf::NAME,
        &[slice_as_bytes(&stamps), slice_as_bytes(&leaves)].concat(),
        2,
        size_of::<LeafV1>(),
    );
    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&bytes).unwrap();
    assert!(blob.is::<Leaf>());
    let (got_meta, msgs): (Meta, Vec<InternalMessage<Leaf>>) = blob.decode(&mut b).unwrap();
    assert_eq!(got_meta.slot, 9);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].data().slot, 11);
    assert_eq!(msgs[0].data().extra, 0);
    assert_eq!(msgs[0].data().flags, 0);
    assert_eq!(msgs[1].data().slot, 22);
    assert_eq!(msgs[0].tile_id(), 3);
    assert_eq!(msgs[1].tile_id(), 4);
}

#[test]
fn rejects_misaligned_truncated_and_bad_header() {
    let good = leaf_blob(1);
    let mut backing = vec![0u8; good.len() + 8];
    let shift = (8 - backing.as_ptr() as usize % 8) % 8 + 1;
    backing[shift..shift + good.len()].copy_from_slice(&good);
    assert!(matches!(
        Blob::from_bytes(&backing[shift..shift + good.len()]),
        Err(DecodeError::Unaligned)
    ));
    assert!(matches!(Blob::from_bytes(&good[..good.len() - 1]), Err(DecodeError::TooShort { .. })));
    let mut bad = good.clone();
    bad[0] ^= 0xff;
    assert!(matches!(Blob::from_bytes(&bad), Err(DecodeError::BadMagic)));
    let mut bad = good.clone();
    let at = offset_of!(BlobHeader, version);
    bad[at..at + 4].copy_from_slice(&2u32.to_le_bytes());
    assert!(matches!(Blob::from_bytes(&bad), Err(DecodeError::UnsupportedVersion(2))));
    let name_at = offset_of!(BlobHeader, type_name);
    let mut bad = good.clone();
    bad[name_at + 8] = 0xff;
    assert!(matches!(Blob::from_bytes(&bad), Err(DecodeError::BadTypeName)));
    let mut bad = good;
    bad[name_at..name_at + 8].copy_from_slice(&65u64.to_le_bytes());
    assert!(matches!(Blob::from_bytes(&bad), Err(DecodeError::BadTypeName)));
}

#[test]
fn rejects_type_and_payload_mismatches() {
    let good = leaf_blob(1);
    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&good).unwrap();
    assert!(matches!(blob.decode::<Meta, Other>(&mut b), Err(DecodeError::UnknownTypeHash(_))));
    let mut tampered = good.clone();
    tampered[16..20].copy_from_slice(&2u32.to_le_bytes());
    let blob = b.load(&tampered).unwrap();
    assert!(matches!(blob.decode::<Meta, Leaf>(&mut a), Err(DecodeError::LengthMismatch { .. })));
    let stamps = [TrackingTimestampWire::from(stamp(1, 1))];
    let meta = MetaV1 { slot: 1, instance: ArrayStr::try_from("t").unwrap() };
    let bytes = hand_build(
        MetaV1::TYPE_HASH,
        &meta_bytes(&meta),
        FlagV1::TYPE_HASH,
        Flag::NAME,
        &[slice_as_bytes(&stamps), [2u8].as_slice()].concat(),
        1,
        size_of::<FlagV1>(),
    );
    let blob = a.load(&bytes).unwrap();
    assert!(matches!(blob.decode::<Meta, Flag>(&mut b), Err(DecodeError::InvalidValue)));
}

#[test]
fn family_blobs_decode_to_variants() {
    let mut cache = BlobCache::new();
    let leaf_msgs = [
        InternalMessage::new(stamp(1, 1), Fam::S(Sub::A(Leaf { slot: 1, extra: 0, flags: 0 }))),
        InternalMessage::new(stamp(2, 2), Fam::S(Sub::A(Leaf { slot: 2, extra: 0, flags: 0 }))),
    ];
    let kind_msg = InternalMessage::new(stamp(3, 3), Fam::K(Kind::A));
    for msg in leaf_msgs.iter().chain(std::iter::once(&kind_msg)) {
        cache.push(msg);
    }
    let meta = MetaV1 { slot: 1, instance: ArrayStr::try_from("f").unwrap() };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 2);

    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let mut saw_leaf = false;
    let mut saw_kind = false;
    for bytes in &blobs {
        let blob = a.load(bytes).unwrap();
        let Some(Ok((_, msgs))) = Fam::decode_blob::<MetaV1>(blob, &mut b) else {
            panic!("family blob did not decode");
        };
        if blob.is::<Leaf>() {
            assert_eq!(msgs.len(), 2);
            assert_eq!(msgs[0].data(), leaf_msgs[0].data());
            assert_eq!(msgs[1].data(), leaf_msgs[1].data());
            saw_leaf = true;
        } else if blob.is::<Kind>() {
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].data(), &Fam::K(Kind::A));
            saw_kind = true;
        } else {
            panic!("unexpected blob type");
        }
    }
    assert!(saw_leaf && saw_kind);

    let mut counter = Counter { n: 0 };
    Fam::Unrelated.visit_leaf(&mut counter);
    Sub::Ignored.visit_leaf(&mut counter);
    assert_eq!(counter.n, 0);

    let other_meta = MetaV1 { slot: 1, instance: ArrayStr::try_from("o").unwrap() };
    let other = hand_build(
        MetaV1::TYPE_HASH,
        &meta_bytes(&other_meta),
        OtherV1::TYPE_HASH,
        Other::NAME,
        &[
            slice_as_bytes(&[TrackingTimestampWire::from(stamp(9, 9))]),
            slice_as_bytes(&[OtherV1 { x: 1 }]),
        ]
        .concat(),
        1,
        size_of::<OtherV1>(),
    );
    let blob = a.load(&other).unwrap();
    assert!(Fam::decode_blob::<MetaV1>(blob, &mut b).is_none());
}

#[test]
fn concatenated_blobs_walk_off_disk() {
    let mut cache = BlobCache::new();
    cache.push(&InternalMessage::new(stamp(1, 1), Leaf { slot: 1, extra: 0, flags: 0 }));
    cache.push(&InternalMessage::new(stamp(2, 2), Other { x: 9 }));
    let meta = MetaV1 { slot: 3, instance: ArrayStr::try_from("d").unwrap() };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 2);
    let mut cat = Vec::new();
    for blob in &blobs {
        cat.extend_from_slice(blob);
    }
    let mut disk = Scratch::new();
    disk.load(&cat).unwrap();
    let bytes = disk.as_bytes();
    let first = Blob::from_bytes(bytes).unwrap();
    let second = Blob::from_bytes(&bytes[first.as_bytes().len()..]).unwrap();
    assert_eq!(first.as_bytes().len() + second.as_bytes().len(), bytes.len());

    let mut work = Scratch::new();
    let mut leaf_count = 0;
    let mut other_count = 0;
    for blob in [first, second] {
        if blob.is::<Leaf>() {
            let (_, msgs): (MetaV1, Vec<InternalMessage<Leaf>>) = blob.decode(&mut work).unwrap();
            leaf_count += msgs.len();
            assert_eq!(msgs[0].data().slot, 1);
        } else {
            let (_, msgs): (MetaV1, Vec<InternalMessage<Other>>) = blob.decode(&mut work).unwrap();
            other_count += msgs.len();
            assert_eq!(msgs[0].data().x, 9);
        }
    }
    assert_eq!((leaf_count, other_count), (1, 1));
}

#[test]
fn corrupt_zstd_tail_fails_decode() {
    let mut bytes = leaf_blob(2);
    // First byte of the zstd frame: flipping it breaks the frame magic, so
    // the decoder must fail instead of returning garbage.
    let comp_off = size_of::<BlobHeader>() + size_of::<MetaV1>().next_multiple_of(8);
    bytes[comp_off] ^= 0xff;
    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&bytes).unwrap();
    assert!(matches!(
        blob.decode::<Meta, Leaf>(&mut b),
        Err(DecodeError::Zstd(_) | DecodeError::LengthMismatch { .. })
    ));
}

#[test]
fn bogus_metadata_hash_rejected_for_meta_and_decode() {
    const BOGUS: u64 = 0xB0B0_1234_5678_9ABC;
    let stamps = [TrackingTimestampWire::from(stamp(1, 1))];
    let leaves = [Leaf { slot: 5, extra: 0, flags: 0 }];
    let meta = MetaV1 { slot: 1, instance: ArrayStr::try_from("t").unwrap() };
    let bytes = hand_build(
        BOGUS,
        &meta_bytes(&meta),
        Leaf::TYPE_HASH,
        Leaf::NAME,
        &[slice_as_bytes(&stamps), slice_as_bytes(&leaves)].concat(),
        1,
        size_of::<Leaf>(),
    );
    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&bytes).unwrap();
    assert!(matches!(
        blob.user_metadata::<Meta>(),
        Err(DecodeError::UnknownTypeHash(h)) if h == BOGUS
    ));
    assert!(matches!(
        blob.decode::<Meta, Leaf>(&mut b),
        Err(DecodeError::UnknownTypeHash(h)) if h == BOGUS
    ));
}

#[test]
fn wrong_decompressed_len_fails_without_allocating() {
    let mut bytes = leaf_blob(1);
    // 1 TiB: returning here proves the header was validated before any
    // allocation, since actually allocating it would OOM the test.
    let at = offset_of!(BlobHeader, decompressed_len);
    bytes[at..at + 8].copy_from_slice(&(1u64 << 40).to_le_bytes());
    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&bytes).unwrap();
    match blob.decode::<Meta, Leaf>(&mut b) {
        Err(DecodeError::LengthMismatch { expected, got }) => {
            assert_eq!(expected, size_of::<TrackingTimestampWire>() + size_of::<Leaf>());
            assert_eq!(got, 1usize << 40);
        }
        other => panic!("expected LengthMismatch, got {other:?}"),
    }
}

#[test]
fn wrong_metadata_len_rejected() {
    let mut bytes = leaf_blob(1);
    let at = offset_of!(BlobHeader, metadata_len);
    let meta_len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    bytes[at..at + 4].copy_from_slice(&(meta_len - 1).to_le_bytes());
    let mut a = Scratch::new();
    let blob = a.load(&bytes).unwrap();
    match blob.user_metadata::<Meta>() {
        Err(DecodeError::LengthMismatch { expected, got }) => {
            assert_eq!(expected, size_of::<MetaV1>());
            assert_eq!(got, meta_len as usize - 1);
        }
        other => panic!("expected LengthMismatch, got {other:?}"),
    }
}

#[test]
fn flushed_blob_padding_is_zero() {
    let mut cache = BlobCache::new();
    cache.push(&InternalMessage::new(stamp(1, 1), Leaf { slot: 7, extra: 0, flags: 0 }));
    // Single-byte metadata leaves a 7-byte pad; the asserts below are not vacuous.
    let meta = Flag { ok: true };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 1);
    let mut scratch = Scratch::new();
    let blob = scratch.load(&blobs[0]).unwrap();
    let meta_len = blob.header.metadata_len as usize;
    let comp_len = blob.header.compressed_len as usize;
    assert_eq!(meta_len, size_of::<FlagV1>());
    let meta_pad = meta_len.next_multiple_of(8);
    assert_eq!(meta_pad - meta_len, 7);
    let bytes = blob.as_bytes();
    assert!(
        bytes[size_of::<BlobHeader>() + meta_len..size_of::<BlobHeader>() + meta_pad]
            .iter()
            .all(|b| *b == 0)
    );
    let tail = size_of::<BlobHeader>() + meta_pad;
    assert_eq!(bytes.len(), tail + comp_len.next_multiple_of(8));
    assert!(bytes[tail + comp_len..].iter().all(|b| *b == 0));
}

#[test]
fn fresh_flush_blobs_are_8_aligned() {
    let mut cache = BlobCache::new();
    for i in 0..3u16 {
        cache.push(&InternalMessage::new(stamp(i, u64::from(i)), Leaf {
            slot: u64::from(i),
            extra: 0,
            flags: 0,
        }));
    }
    let meta = Meta { slot: 1, instance: ArrayStr::try_from("a").unwrap() };
    let mut n = 0;
    cache.flush(&meta, 3, |blob| {
        assert_eq!(blob.as_bytes().as_ptr() as usize % 8, 0);
        assert_eq!(blob.as_bytes().len() % 8, 0);
        n += 1;
    });
    assert_eq!(n, 1);
}

#[test]
fn internal_metadata_bincode_layout_is_pinned() {
    let stamp = TrackingTimestamp::new(3);
    let meta = TrackingTimestampWire::from(stamp);
    let bytes = bincode::serialize(&meta).unwrap();
    // 8-byte ingestion + 8-byte publish + 2-byte tile id; the 6-byte
    // pad is serde-skipped so legacy blobs are unchanged.
    assert_eq!(bytes.len(), 18);
    let back: TrackingTimestampWire = bincode::deserialize(&bytes).unwrap();
    assert_eq!(back.ingestion_t_real, meta.ingestion_t_real);
    assert_eq!(back.publish_t_real, meta.publish_t_real);
    assert_eq!(back.tile_id, meta.tile_id);

    let v1 = TrackingTimestampWireV1 {
        ingestion_t_real: stamp.ingestion_t().real(),
        publish_t_real: stamp.publish_t(),
    };
    let v1_bytes = bincode::serialize(&v1).unwrap();
    let v1_back: TrackingTimestampWireV1 = bincode::deserialize(&v1_bytes).unwrap();
    let up: TrackingTimestampWire = v1_back.into();
    assert_eq!(up.ingestion_t_real, v1.ingestion_t_real);
    assert_eq!(up.publish_t_real, v1.publish_t_real);
    assert_eq!(up.tile_id, 0);
}

#[test]
fn scratch_load_tolerates_misalignment() {
    let good = leaf_blob(1);
    let mut backing = vec![0u8; good.len() + 8];
    let base = backing.as_ptr() as usize;
    let shift = (8 - base % 8) % 8 + 1;
    backing[shift..shift + good.len()].copy_from_slice(&good);
    let skewed = &backing[shift..shift + good.len()];
    assert_ne!(skewed.as_ptr() as usize % 8, 0);
    assert!(matches!(Blob::from_bytes(skewed), Err(DecodeError::Unaligned)));
    let mut aligned = Scratch::new();
    let mut work = Scratch::new();
    let blob = aligned.load(skewed).unwrap();
    let (meta, msgs): (Meta, Vec<InternalMessage<Leaf>>) = blob.decode(&mut work).unwrap();
    assert_eq!(meta.slot, 1);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].data().slot, 0);
}

versioned_struct!(Foreign =>
    #[type_hash_lock(hash = 2268921598638542771)]
    ForeignV1 { pub id: uuid::Uuid, pub hash: alloy_primitives::B256 }
);

#[test]
fn foreign_leaf_fields_round_trip() {
    let mut cache = BlobCache::new();
    let first = Foreign {
        id: uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0),
        hash: alloy_primitives::B256::from([0xabu8; 32]),
    };
    let second = Foreign { id: uuid::Uuid::nil(), hash: alloy_primitives::B256::ZERO };
    cache.push(&InternalMessage::new(stamp(1, 1), first));
    cache.push(&InternalMessage::new(stamp(2, 2), second));
    let meta = MetaV1 { slot: 7, instance: ArrayStr::try_from("foreign").unwrap() };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 1);

    let mut a = Scratch::new();
    let mut b = Scratch::new();
    let blob = a.load(&blobs[0]).unwrap();
    assert_eq!(blob.user_metadata::<Meta>().unwrap().slot, 7);
    let (_, msgs): (Meta, Vec<InternalMessage<Foreign>>) = blob.decode(&mut b).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].data(), &first);
    assert_eq!(msgs[1].data(), &second);
}
