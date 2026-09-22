//! Production -> compression -> persistence -> decompression -> lazy iteration.
//!
//! Behavioral coverage for persisted current/historical leaves, lazy ownership,
//! corruption, positional skipping, and allocation-free record iteration.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_gather::{
    BlobCache, BlobReader, BlobWriter, DecodeError, DecompressedBlob, HasVersionedLeaves,
    MessageIter, Scratch, Versioned,
};
use flux_timing::{InternalMessage, TrackingTimestamp};
use flux_versioned_types::{
    ByteStable, TrackingTimestampWire, VersionedLeaves, versioned_enum, versioned_struct,
};

thread_local! {
    static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    static MAX_ALLOC: Cell<usize> = const { Cell::new(0) };
    static MIGRATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.with(|count| count.set(count.get() + 1));
        MAX_ALLOC.with(|max| max.set(max.get().max(layout.size())));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

fn allocs() -> usize {
    ALLOC_COUNT.with(|count| count.get())
}

versioned_struct!(Quote =>
    #[type_hash_lock(hash = 10296486615234503952)]
    QuoteV1 { pub id: u64, pub px: u64 }

    #[type_hash_lock(hash = 2400401184665586320)]
    QuoteV2 {
        add {
            pub qty: u64 = { MIGRATIONS.with(|n| n.set(n.get() + 1)); 0 },
        }
    }
);

versioned_struct!(Mark =>
    #[type_hash_lock(hash = 1389844797850445404)]
    MarkV1 { pub tag: u64 }
);

versioned_struct!(IterMeta =>
    #[type_hash_lock(hash = 10917218653099105827)]
    IterMetaV1 { pub slot: u64 }
);

versioned_struct!(Zed =>
    #[type_hash_lock(hash = 12315054074540755641)]
    ZedV1 {}
    #[type_hash_lock(hash = 12315059572098922320)]
    ZedV2 {}
);

mod old {
    use super::*;
    versioned_struct!(Zed =>
        #[type_hash_lock(hash = 12315054074540755641)]
        ZedV1 {}
    );
    versioned_struct!(Quote =>
        #[type_hash_lock(hash = 10296486615234503952)]
        QuoteV1 { pub id: u64, pub px: u64 }
    );
}

mod grown {
    use super::*;
    versioned_struct!(Zed =>
        #[type_hash_lock(hash = 12315054074540755641)]
        ZedV1 {}
        #[type_hash_lock(hash = 10740569519623233490)]
        ZedV2 { add { pub value: u64 = 42 } }
    );
}

versioned_struct!(Flag =>
    #[type_hash_lock(hash = 17693854425529480628)]
    FlagV1 { pub ok: bool }
);

versioned_enum!(Sparse =>
    #[type_hash_lock(hash = 15518227905406261725)]
    SparseV1 { A = 1, B = 7 }
);

#[derive(Clone, Copy, Debug, VersionedLeaves)]
enum Nested {
    Feed(Feed),
}

#[derive(Clone, Copy, Debug, VersionedLeaves)]
enum ZeroFeed {
    Zero(grown::Zed),
}

#[derive(Clone, Copy, Debug, VersionedLeaves)]
enum Checked {
    Flag(Flag),
    Sparse(Sparse),
}

#[derive(Clone, Copy, Debug, VersionedLeaves)]
enum Feed {
    Quote(Quote),
    Mark(Mark),
}

/// Produce `n` current-version quotes through the real [`BlobCache`] path.
fn produce_quotes(n: u64, tile: u16) -> (Vec<InternalMessage<Quote>>, Vec<u8>) {
    let mut cache = BlobCache::new();
    let mut originals = Vec::new();
    for i in 0..n {
        let msg = InternalMessage::new(TrackingTimestamp::new(tile), Quote {
            id: i,
            px: 1000 + i,
            qty: i % 7,
        });
        originals.push(msg);
        cache.push(&msg);
    }
    let meta = IterMeta { slot: 9 };
    let mut wire = Vec::new();
    cache.flush(&meta, 1, |blob| {
        assert!(wire.is_empty(), "one leaf type produces one blob");
        wire.extend_from_slice(blob.as_bytes());
    });
    (originals, wire)
}

fn load_blob<'a>(scratch: &'a mut Scratch, wire: &[u8]) -> &'a flux_gather::Blob {
    scratch.load(wire).expect("test wire bytes are valid")
}

fn read_back(wire: &[u8]) -> Result<DecompressedBlob, flux_gather::ReadError> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.bin");
    let mut writer = BlobWriter::new();
    let mut scratch = Scratch::new();
    let blob = scratch.load(wire).map_err(flux_gather::ReadError::Decode)?;
    assert!(writer.write(blob, &path));
    writer.drain();
    assert_eq!(std::fs::read(&path).unwrap(), wire, "producer bytes unchanged");
    let mut reader = BlobReader::new();
    let mut stores = reader.read_decompressed(&path)?;
    assert_eq!(stores.len(), 1);
    Ok(stores.pop().unwrap())
}

/// Rebuild a produced blob with a different stored hash and plain payload.
fn retype_wire(template: &[u8], type_hash: u64, n_messages: u32, plain: &[u8]) -> Vec<u8> {
    assert!(template.len() >= 144, "template holds a header");
    let meta_len = u32::from_le_bytes(template[12..16].try_into().unwrap()) as usize;
    let comp = zstd::bulk::compress(plain, 1).expect("recompress");
    let meta_pad = meta_len.next_multiple_of(8);
    let comp_pad = comp.len().next_multiple_of(8);
    let mut out = vec![0u8; 144 + meta_pad + comp_pad];
    out[..144].copy_from_slice(&template[..144]);
    out[16..20].copy_from_slice(&n_messages.to_le_bytes());
    out[24..32].copy_from_slice(&type_hash.to_le_bytes());
    out[40..48].copy_from_slice(&(comp.len() as u64).to_le_bytes());
    out[48..56].copy_from_slice(&(plain.len() as u64).to_le_bytes());
    out[144..144 + meta_len].copy_from_slice(&template[144..144 + meta_len]);
    out[144 + meta_pad..144 + meta_pad + comp.len()].copy_from_slice(&comp);
    out
}

/// Old-version plain payload: current timestamp records with V1 leaves.
fn old_plain(n: u64, tile: u16) -> Vec<u8> {
    let mut plain = Vec::new();
    let mut leaves = Vec::new();
    for i in 0..n {
        let msg = InternalMessage::new(TrackingTimestamp::new(tile), QuoteV1 { id: i, px: 50 + i });
        plain.extend_from_slice(ByteStable::as_bytes(&TrackingTimestampWire::from(
            msg.tracking_timestamp(),
        )));
        leaves.extend_from_slice(ByteStable::as_bytes(msg.data()));
    }
    plain.extend_from_slice(&leaves);
    plain
}

fn collect<T: Versioned>(iter: MessageIter<'_, T>) -> Vec<InternalMessage<T>> {
    iter.map(|result| result.expect("record decodes")).collect()
}

#[test]
fn lazy_matches_eager_end_to_end() {
    let t0 = flux_timing::Nanos::now();
    let (originals, wire) = produce_quotes(64, 3);
    // Persist through the real writer, as the receiver does.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("quotes.bin");
    let mut writer = BlobWriter::new();
    let mut scratch = Scratch::new();
    assert!(writer.write(load_blob(&mut scratch, &wire), &path));
    writer.drain();
    // Receiver side: read the file, decompress into an owned store.
    let mut reader = BlobReader::new();
    let mut stores = reader.read_decompressed(&path).expect("read back");
    assert_eq!(stores.len(), 1);
    let store = stores.pop().expect("one blob");
    assert_eq!(store.n_messages(), 64);
    assert_eq!(store.type_name(), Quote::NAME);
    assert!(store.is::<Quote>());
    // Eager baseline from the same wire bytes.
    let mut decode_scratch = Scratch::new();
    let (eager_meta, eager) = load_blob(&mut decode_scratch, &wire)
        .decode::<IterMeta, Quote>(&mut Scratch::new())
        .expect("eager decode");
    assert_eq!(eager_meta.slot, 9);
    // Lazy path, one message at a time.
    let (meta, iter) = store.messages::<IterMeta, Quote>().expect("lazy open");
    assert_eq!(meta.slot, 9);
    assert_eq!(iter.len(), 64);
    let lazy = collect(iter);
    // `InternalMessage` equality is data-only by design; timestamps follow.
    assert_eq!(lazy, eager);
    assert_eq!(lazy.len(), originals.len());
    for (i, msg) in lazy.iter().enumerate() {
        assert_eq!(msg.tile_id(), 3, "tile id survives the roundtrip");
        assert_eq!(
            msg.ingestion_time().real(),
            originals[i].ingestion_time().real(),
            "ingestion wall clock is exact"
        );
        assert!(msg.publish_t() >= t0, "rebuilt publish time stays in the test window");
    }
    // Portable wire timestamps survive byte-identically.
    let stamps = store.stamps().expect("stamps");
    assert_eq!(stamps.len(), 64);
    for (i, stamp) in stamps.iter().enumerate() {
        let wire_stamp = TrackingTimestampWire::from(originals[i].tracking_timestamp());
        assert_eq!(stamp.ingestion_t_real, wire_stamp.ingestion_t_real);
        assert_eq!(stamp.publish_t_real, wire_stamp.publish_t_real);
        assert_eq!(stamp.tile_id, 3);
    }
    assert!(flux_timing::Nanos::now() >= t0);
}

#[test]
fn owning_iter_survives_wire_drop_and_skips_positionally() {
    let (_, wire) = produce_quotes(32, 5);
    let (meta, mut iter) = {
        let mut scratch = Scratch::new();
        // The wire `Scratch` is dropped here; iteration must not borrow it.
        load_blob(&mut scratch, &wire)
            .decompress()
            .expect("decompress")
            .into_messages::<IterMeta, Quote>()
            .expect("lazy open")
    };
    assert_eq!(meta.slot, 9);
    assert_eq!(iter.len(), 32);
    let first = iter.next().expect("first").expect("decodes");
    assert_eq!((first.id, first.qty), (0, 0));
    iter.advance(10);
    assert_eq!(iter.len(), 21);
    let rest: Vec<_> = iter.by_ref().map(|result| result.expect("decodes")).collect();
    assert_eq!(rest.len(), 21);
    assert_eq!(rest[0].id, 11);
    assert_eq!(rest[20].id, 31);
    assert_eq!(iter.len(), 0);
    assert!(iter.next().is_none(), "exhausted");
    let store = read_back(&wire).unwrap();
    let (_, mut borrowed) = store.messages::<IterMeta, Quote>().unwrap();
    borrowed.next().unwrap().unwrap();
    borrowed.advance(usize::MAX);
    assert_eq!(borrowed.len(), 0);
    assert_eq!(borrowed.size_hint(), (0, Some(0)));
    assert!(borrowed.next().is_none());
    borrowed.advance(usize::MAX);
    let (_, mut owned) = store.into_messages::<IterMeta, Quote>().unwrap();
    owned.next().unwrap().unwrap();
    owned.advance(usize::MAX);
    assert_eq!(owned.len(), 0);
    assert_eq!(owned.size_hint(), (0, Some(0)));
    assert!(owned.next().is_none());
    owned.advance(usize::MAX);
}

#[test]
fn borrow_iter_pauses_across_scratch_reuse() {
    let (_, wire_a) = produce_quotes(16, 1);
    let (_, wire_b) = produce_quotes(8, 2);
    let mut scratch = Scratch::new();
    let store_a = load_blob(&mut scratch, &wire_a).decompress().expect("decompress A");
    let (_, mut iter_a) = store_a.messages::<IterMeta, Quote>().expect("open A");
    assert_eq!(iter_a.next().expect("first A").expect("decodes").tile_id(), 1);
    // Pause A: reuse the wire scratch for an unrelated blob mid-iteration.
    let store_b = load_blob(&mut scratch, &wire_b).decompress().expect("decompress B");
    let (_, iter_b) = store_b.messages::<IterMeta, Quote>().expect("open B");
    let got_b = collect(iter_b);
    assert_eq!(got_b.len(), 8);
    assert!(got_b.iter().all(|msg| msg.tile_id() == 2));
    // Resume A on its own untouched store.
    let rest_a: Vec<_> = iter_a.by_ref().map(|result| result.expect("decodes")).collect();
    assert_eq!(rest_a.len(), 15);
    assert!(rest_a.iter().all(|msg| msg.tile_id() == 1));
    assert_eq!(rest_a[0].id, 1);
    assert_eq!(iter_a.len(), 0);
}

#[test]
fn old_version_wire_migrates_one_item_at_a_time() {
    let (_, template) = produce_quotes(1, 0);
    let old_wire = retype_wire(&template, QuoteV1::TYPE_HASH, 24, &old_plain(24, 7));
    let store = read_back(&old_wire).expect("decompress old");
    assert!(store.is::<Quote>(), "stored V1 hash is a known Quote version");
    let (meta, iter) = store.messages::<IterMeta, Quote>().expect("lazy open");
    assert_eq!(meta.slot, 9);
    assert_eq!(iter.len(), 24);
    let got = collect(iter);
    assert_eq!(got.len(), 24);
    for (i, msg) in got.iter().enumerate() {
        let i = i as u64;
        assert_eq!((msg.id, msg.px, msg.qty), (i, 50 + i, 0), "V1 migrates with defaults");
        assert_eq!(msg.tile_id(), 7);
    }
    // Eager baseline agrees on data.
    let mut decode_scratch = Scratch::new();
    let (_, eager) = load_blob(&mut decode_scratch, &old_wire)
        .decode::<IterMeta, Quote>(&mut Scratch::new())
        .expect("eager old decode");
    assert_eq!(got, eager);
}

#[test]
fn empty_blob_yields_nothing() {
    let (_, template) = produce_quotes(1, 0);
    let wire = retype_wire(&template, Quote::TYPE_HASH, 0, &[]);
    let store = read_back(&wire).expect("decompress empty");
    assert_eq!(store.n_messages(), 0);
    let (meta, mut iter) = store.messages::<IterMeta, Quote>().expect("open empty");
    assert_eq!(meta.slot, 9);
    assert_eq!(iter.len(), 0);
    assert!(iter.next().is_none());
    assert!(store.stamps().expect("stamps").is_empty());
}

#[test]
fn zero_sized_leaves_and_metadata_roundtrip_and_migrate() {
    for historical in [false, true] {
        let mut cache = BlobCache::new();
        let stamps: Vec<_> = (0..3).map(|_| TrackingTimestamp::new(8)).collect();
        for stamp in &stamps {
            if historical {
                cache.push(&InternalMessage::new(*stamp, old::Zed {}));
            } else {
                cache.push(&InternalMessage::new(*stamp, Zed {}));
            }
        }
        let mut wire = Vec::new();
        if historical {
            cache.flush(&old::Zed {}, 1, |blob| wire.extend_from_slice(blob.as_bytes()));
        } else {
            cache.flush(&Zed {}, 1, |blob| wire.extend_from_slice(blob.as_bytes()));
        }
        let store = read_back(&wire).unwrap();
        let (_, mut borrowed) = store.messages::<Zed, Zed>().unwrap();
        assert_eq!(borrowed.len(), 3);
        for (got, stamp) in borrowed.by_ref().zip(&stamps) {
            let got = got.unwrap();
            assert_eq!(got.data(), &Zed {});
            assert_eq!(got.ingestion_time().real(), stamp.ingestion_t.real());
            assert_eq!(got.tile_id(), 8);
        }
        assert_eq!(borrowed.len(), 0);
        let (_, owned) = read_back(&wire).unwrap().into_messages::<Zed, Zed>().unwrap();
        assert_eq!(
            owned.fold(0, |count, msg| {
                msg.unwrap();
                count + 1
            }),
            3
        );
        let mut scratch = Scratch::new();
        let (_, eager) =
            load_blob(&mut scratch, &wire).decode::<Zed, Zed>(&mut Scratch::new()).unwrap();
        assert_eq!(eager.len(), 3);
        if historical {
            let (meta, mut family) =
                ZeroFeed::into_decode_iter::<grown::Zed>(store).unwrap().unwrap();
            assert_eq!(meta.value, 42, "zero-sized metadata migrates");
            assert_eq!(family.len(), 3);
            for got in family.by_ref() {
                let ZeroFeed::Zero(grown) = got.unwrap().into_data();
                assert_eq!(grown.value, 42, "zero-sized leaf migrates");
            }
            assert_eq!(family.len(), 0);
            let (_, eager) = load_blob(&mut scratch, &wire)
                .decode::<grown::Zed, grown::Zed>(&mut Scratch::new())
                .unwrap();
            assert!(eager.iter().all(|m| m.value == 42));
        }
        let plain =
            zstd::bulk::decompress(load_blob(&mut scratch, &wire).compressed(), 1024).unwrap();
        let mut extra = plain.clone();
        extra.push(1);
        let bad = retype_wire(&wire, load_blob(&mut scratch, &wire).header.type_hash, 3, &extra);
        assert!(read_back(&bad).unwrap().messages::<Zed, Zed>().is_err());
    }
}

#[test]
fn corrupt_wire_fails_before_any_message() {
    let (_, wire) = produce_quotes(8, 1);
    let mut scratch = Scratch::new();
    // Truncated frame.
    assert!(Scratch::new().load(&wire[..wire.len() - 10]).is_err(), "truncated wire rejected");
    // Bad magic.
    let mut bad_magic = wire.clone();
    bad_magic[0] ^= 0xFF;
    assert!(Scratch::new().load(&bad_magic).is_err(), "bad magic rejected");
    // Unknown stored hash: decompression is hash-agnostic, iteration is not.
    let mut bad_hash = wire.clone();
    bad_hash[24..32].copy_from_slice(&0xDEAD_BEEF_DEAD_BEEFu64.to_le_bytes());
    let store = load_blob(&mut scratch, &bad_hash).decompress().expect("decompress");
    assert!(
        matches!(store.messages::<IterMeta, Quote>(), Err(DecodeError::UnknownTypeHash(_))),
        "unknown leaf hash rejected at open"
    );
    // Unknown metadata hash.
    let mut bad_meta = wire.clone();
    bad_meta[32..40].copy_from_slice(&0xDEAD_BEEF_DEAD_BEEFu64.to_le_bytes());
    let store = load_blob(&mut scratch, &bad_meta).decompress().expect("decompress");
    assert!(store.user_metadata::<IterMeta>().is_err(), "unknown meta hash rejected");
    // Lying decompressed length.
    let mut bad_len = wire;
    let len = u64::from_le_bytes(bad_len[48..56].try_into().unwrap()) + 100;
    bad_len[48..56].copy_from_slice(&len.to_le_bytes());
    assert!(load_blob(&mut scratch, &bad_len).decompress().is_err(), "length lie rejected");
}

#[test]
fn family_iter_yields_each_blob_lazily() {
    let mut cache = BlobCache::new();
    for i in 0..48u64 {
        cache.push(&InternalMessage::new(TrackingTimestamp::new(4), Quote {
            id: i,
            px: i,
            qty: i,
        }));
    }
    for tag in 0..5u64 {
        cache.push(&InternalMessage::new(TrackingTimestamp::new(4), Mark { tag }));
    }
    let meta = IterMeta { slot: 2 };
    let mut wire = Vec::new();
    cache.flush(&meta, 1, |blob| wire.extend_from_slice(blob.as_bytes()));
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("feed.bin");
    std::fs::write(&path, &wire).expect("persist feed");
    let mut reader = BlobReader::new();
    let stores = reader.read_decompressed(&path).expect("read feed");
    assert_eq!(stores.len(), 2, "one blob per leaf type");
    let mut quotes = 0;
    let mut marks = 0;
    for store in &stores {
        let (got_meta, iter) = Feed::decode_iter::<IterMeta>(store)
            .expect("feed holds a Feed leaf")
            .expect("lazy open");
        assert_eq!(got_meta.slot, 2);
        for result in iter {
            match result.expect("decodes").into_data() {
                Feed::Quote(quote) => {
                    assert_eq!((quote.px, quote.qty), (quote.id, quote.id));
                    quotes += 1;
                }
                Feed::Mark(_) => marks += 1,
            }
        }
    }
    assert_eq!((quotes, marks), (48, 5));
    // A blob whose name matches no Feed position declines.
    let mut cache = BlobCache::new();
    cache.push(&InternalMessage::new(TrackingTimestamp::new(0), IterMeta { slot: 1 }));
    let mut foreign_wire = Vec::new();
    cache.flush(&IterMeta { slot: 1 }, 1, |blob| {
        foreign_wire.extend_from_slice(blob.as_bytes());
    });
    let mut scratch = Scratch::new();
    let foreign = load_blob(&mut scratch, &foreign_wire).decompress().expect("decompress");
    assert!(Feed::decode_iter::<IterMeta>(&foreign).is_none(), "foreign blob is not a Feed");
    assert!(
        Feed::decode_blob::<IterMeta>(load_blob(&mut scratch, &foreign_wire), &mut Scratch::new())
            .is_none(),
        "eager path agrees it is foreign"
    );
}

#[test]
fn owned_family_pauses_after_reader_and_source_drop() {
    let mut pending = {
        let (_, wire) = produce_quotes(32, 5);
        let store = read_back(&wire).unwrap();
        let (meta, mut iter) = Nested::into_decode_iter::<IterMeta>(store).unwrap().unwrap();
        assert_eq!(meta.slot, 9);
        let Nested::Feed(Feed::Quote(first)) = iter.next().unwrap().unwrap().into_data() else {
            panic!("quote")
        };
        assert_eq!(first.id, 0);
        iter
    };
    let (_, unrelated) = produce_quotes(4, 2);
    let mut scratch = Scratch::new();
    load_blob(&mut scratch, &unrelated).decompress().unwrap();
    scratch.resize(0);
    assert_eq!(pending.len(), 31);
    let Nested::Feed(Feed::Quote(next)) = pending.nth(10).unwrap().unwrap().into_data() else {
        panic!("quote")
    };
    assert_eq!(next.id, 11);
    assert_eq!(pending.len(), 20);
    assert!(pending.nth(usize::MAX).is_none());
    assert_eq!(pending.size_hint(), (0, Some(0)));
    assert!(pending.next().is_none());
}

#[test]
fn huge_claims_fail_without_claim_sized_allocations() {
    let (_, wire) = produce_quotes(3, 1);
    for claimed in [1u64 << 40, u64::MAX] {
        for forge_frame in [false, true] {
            let mut bad = wire.clone();
            bad[48..56].copy_from_slice(&claimed.to_le_bytes());
            if forge_frame {
                // A single-segment frame also claims a huge window/content size.
                let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
                frame.extend_from_slice(&claimed.to_le_bytes());
                frame.extend_from_slice(&[1, 0, 0]);
                bad[40..48].copy_from_slice(&(frame.len() as u64).to_le_bytes());
                bad.truncate(152);
                bad.extend_from_slice(&frame);
                bad.resize(bad.len().next_multiple_of(8), 0);
            }
            assert!(read_back(&bad).is_err());
            let mut scratch = Scratch::new();
            let blob = load_blob(&mut scratch, &bad);
            MAX_ALLOC.with(|max| max.set(0));
            assert!(blob.decompress().is_err());
            assert!(
                MAX_ALLOC.with(Cell::get) < 1024 * 1024,
                "tiny malformed input allocated a large Rust buffer"
            );
        }
    }
    let mut bad_count = wire;
    bad_count[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    bad_count[48..56].copy_from_slice(&(u64::from(u32::MAX) * 48).to_le_bytes());
    assert!(read_back(&bad_count).is_err());
}

#[test]
fn invalid_middle_record_is_local_and_skips_do_not_decode_it() {
    for sparse in [false, true] {
        let mut cache = BlobCache::new();
        for i in 0..3 {
            let stamp = TrackingTimestamp::new(9);
            if sparse {
                cache
                    .push(&InternalMessage::new(stamp, if i == 1 { Sparse::B } else { Sparse::A }));
            } else {
                cache.push(&InternalMessage::new(stamp, Flag { ok: i == 1 }));
            }
        }
        let mut wire = Vec::new();
        cache.flush(&IterMeta { slot: 7 }, 1, |blob| wire.extend_from_slice(blob.as_bytes()));
        let mut scratch = Scratch::new();
        let blob = load_blob(&mut scratch, &wire);
        let mut plain = zstd::bulk::decompress(blob.compressed(), 1024).unwrap();
        plain[3 * size_of::<TrackingTimestampWire>() + 1] = 2;
        let bad = retype_wire(&wire, blob.header.type_hash, 3, &plain);
        let store = read_back(&bad).unwrap();
        let (_, mut iter) = Checked::decode_iter::<IterMeta>(&store).unwrap().unwrap();
        assert!(iter.next().unwrap().is_ok());
        assert!(matches!(iter.next(), Some(Err(DecodeError::InvalidValue))));
        assert!(iter.next().unwrap().is_ok());
        assert_eq!(iter.len(), 0);
        let (_, mut skipped) = Checked::decode_iter::<IterMeta>(&store).unwrap().unwrap();
        assert!(skipped.next().unwrap().is_ok());
        assert!(skipped.nth(1).unwrap().is_ok());
        assert_eq!(skipped.len(), 0);
        drop((iter, skipped));
        let (_, mut owned) = Checked::into_decode_iter::<IterMeta>(store).unwrap().unwrap();
        assert!(owned.next().unwrap().is_ok());
        assert!(matches!(owned.next(), Some(Err(DecodeError::InvalidValue))));
        match owned.next().unwrap().unwrap().into_data() {
            Checked::Flag(flag) => assert!(!flag.ok),
            Checked::Sparse(value) => assert_eq!(value, Sparse::A),
        }
        let (_, mut skipped) =
            Checked::into_decode_iter::<IterMeta>(read_back(&bad).unwrap()).unwrap().unwrap();
        assert!(skipped.next().unwrap().is_ok());
        assert!(skipped.nth(1).unwrap().is_ok());
        assert!(skipped.nth(usize::MAX).is_none());
        plain.pop();
        let short = retype_wire(&wire, blob.header.type_hash, 3, &plain);
        let store = read_back(&short).unwrap();
        assert!(Checked::decode_iter::<IterMeta>(&store).unwrap().is_err());
    }
}

#[test]
fn iteration_performs_no_per_record_allocation() {
    for historical in [false, true] {
        let mut cache = BlobCache::new();
        for id in 0..512 {
            let stamp = TrackingTimestamp::new(6);
            if historical {
                cache.push(&InternalMessage::new(stamp, old::Quote { id, px: id }));
            } else {
                cache.push(&InternalMessage::new(stamp, Quote { id, px: id, qty: 0 }));
            }
        }
        let mut wire = Vec::new();
        cache.flush(&IterMeta { slot: 9 }, 1, |blob| wire.extend_from_slice(blob.as_bytes()));
        let mut scratch = Scratch::new();
        let blob = load_blob(&mut scratch, &wire);
        let before = allocs();
        let (_, eager) = blob.decode::<IterMeta, Quote>(&mut Scratch::new()).unwrap();
        let eager_allocs = allocs() - before;
        assert_eq!(eager.len(), 512);
        assert!(eager_allocs >= 2);
        let store = read_back(&wire).unwrap();
        let before = allocs();
        let (_, iter) = store.messages::<IterMeta, Quote>().unwrap();
        let sum: u64 = iter.map(|m| m.unwrap().id).sum();
        assert_eq!(allocs() - before, 0, "borrowed typed");
        assert_eq!(sum, (0..512).sum());
        let before = allocs();
        let (_, iter) = Nested::decode_iter::<IterMeta>(&store).unwrap().unwrap();
        let borrowed_setup = allocs() - before;
        assert!(borrowed_setup <= 3, "setup does not grow with record count");
        let before = allocs();
        let sum: u64 = iter
            .map(|m| {
                let Nested::Feed(Feed::Quote(q)) = m.unwrap().into_data() else { panic!("quote") };
                q.id
            })
            .sum();
        assert_eq!(allocs() - before, 0, "borrowed nested family");
        assert_eq!(sum, (0..512).sum());
        let before = allocs();
        let (_, iter) = store.into_messages::<IterMeta, Quote>().unwrap();
        let sum: u64 = iter.map(|m| m.unwrap().id).sum();
        assert_eq!(allocs() - before, 0, "owning typed transition and iteration");
        assert_eq!(sum, (0..512).sum());
        let store = read_back(&wire).unwrap();
        let before = allocs();
        let (_, iter) = Nested::into_decode_iter::<IterMeta>(store).unwrap().unwrap();
        let owned_setup = allocs() - before;
        assert!(owned_setup <= 3);
        let before = allocs();
        let sum: u64 = iter
            .map(|m| {
                let Nested::Feed(Feed::Quote(q)) = m.unwrap().into_data() else { panic!("quote") };
                q.id
            })
            .sum();
        assert_eq!(allocs() - before, 0, "owning nested family");
        assert_eq!(sum, (0..512).sum());
        eprintln!(
            "historical={historical}: family setup borrowed={borrowed_setup}, owned={owned_setup}; all record paths=0 allocations"
        );

        if historical {
            let store = read_back(&wire).unwrap();
            MIGRATIONS.with(|n| n.set(0));
            let (_, mut iter) = store.messages::<IterMeta, Quote>().unwrap();
            iter.advance(500);
            assert_eq!(MIGRATIONS.with(Cell::get), 0);
            assert_eq!(iter.next().unwrap().unwrap().id, 500);
            iter.advance(usize::MAX);
            assert_eq!(MIGRATIONS.with(Cell::get), 1);
            let (_, mut iter) = store.into_messages::<IterMeta, Quote>().unwrap();
            iter.advance(500);
            assert_eq!(MIGRATIONS.with(Cell::get), 1);
            assert_eq!(iter.next().unwrap().unwrap().id, 500);
            iter.advance(usize::MAX);
            assert_eq!(MIGRATIONS.with(Cell::get), 2);
            let store = read_back(&wire).unwrap();
            let (_, mut iter) = Nested::decode_iter::<IterMeta>(&store).unwrap().unwrap();
            iter.nth(500).unwrap().unwrap();
            assert!(iter.nth(usize::MAX).is_none());
            assert_eq!(MIGRATIONS.with(Cell::get), 3);
            drop(iter);
            let (_, mut iter) = Nested::into_decode_iter::<IterMeta>(store).unwrap().unwrap();
            iter.nth(500).unwrap().unwrap();
            assert!(iter.nth(usize::MAX).is_none());
            assert_eq!(MIGRATIONS.with(Cell::get), 4, "skipped records never migrate");
        }
    }
}
