use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_timing::{
    Duration, IngestionTime, Instant, InternalMessage, Nanos, PublishDelta, TrackingTimestamp,
};
use flux_versioned_types::{
    BlobCache, ByteStable, DecodeError, HasVersionedLeaves, Scratch, Versioned, VersionedLeaves,
    VisitorLeafType, VisitorVersionedLeaf, versioned_struct,
};

versioned_struct!(Price =>
    #[type_hash_lock(hash = 5743759228102669910)]
    PriceV1 { pub id: u64, pub value: u64 }
);

versioned_struct!(Fill =>
    #[type_hash_lock(hash = 12390809355146961144)]
    FillV1 { pub id: u64, pub qty: u64 }
);

versioned_struct!(Ping =>
    #[type_hash_lock(hash = 5607447243038438877)]
    PingV1 { pub at: u64 }
);

versioned_struct!(Meta =>
    #[type_hash_lock(hash = 12371976189249938237)]
    MetaV1 { pub batch: u64 }
);

#[derive(Clone, Copy, Debug, PartialEq, VersionedLeaves)]
enum Trade {
    Price(Price),
    Fill(Fill),
}

#[derive(Clone, Copy, Debug, PartialEq, VersionedLeaves)]
enum Telemetry {
    Trade(Trade),
    Ping(Ping),
}

const TAGGED: u64 = 0x5441_4747_4544_2121;

#[derive(Clone, Copy, Debug, ByteStable)]
#[repr(C)]
struct Tagged<L> {
    tag: u64,
    data: L,
}

impl<L: Versioned> TypeHash for Tagged<L> {
    const TYPE_HASH: u64 = L::TYPE_HASH ^ TAGGED;
}

impl<L: Versioned> Versioned for Tagged<L> {
    const NAME: &'static str = L::NAME;

    fn version_size(h: u64) -> Option<usize> {
        L::version_size(h ^ TAGGED).map(|s| s + size_of::<u64>())
    }

    fn decode_versions(h: u64, bytes: &[u8]) -> Result<Vec<Self>, DecodeError> {
        let Some(inner) = L::version_size(h ^ TAGGED) else {
            return Err(DecodeError::UnknownTypeHash(h));
        };
        let stride = size_of::<u64>() + inner;
        if !bytes.len().is_multiple_of(stride) {
            return Err(DecodeError::LengthMismatch {
                expected: bytes.len() / stride * stride,
                got: bytes.len(),
            });
        }
        let mut tags = Vec::with_capacity(bytes.len() / stride);
        let mut packed = Scratch::new();
        packed.resize(bytes.len() / stride * inner);
        for (i, rec) in bytes.chunks_exact(stride).enumerate() {
            tags.push(u64::from_le_bytes(rec[..8].try_into().expect("8 tag bytes")));
            packed.as_mut_bytes()[i * inner..(i + 1) * inner].copy_from_slice(&rec[8..]);
        }
        Ok(L::decode_versions(h ^ TAGGED, packed.as_bytes())?
            .into_iter()
            .zip(tags)
            .map(|(data, tag)| Self { tag, data })
            .collect())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Update {
    tag: u64,
    inner: Telemetry,
}

struct Wrap<'a, V> {
    tag: u64,
    inner: &'a mut V,
}

impl<V: VisitorVersionedLeaf> VisitorVersionedLeaf for Wrap<'_, V> {
    fn visit_leaf<L: Versioned>(&mut self, name: &'static str, leaf: &L) {
        self.inner.visit_leaf(name, &Tagged { tag: self.tag, data: *leaf });
    }
}

struct TypesTagged<'a, V, Root> {
    inner: &'a mut V,
    wrap: &'a dyn Fn(Update) -> Root,
}

impl<V: VisitorLeafType<Root>, Root> VisitorLeafType<Telemetry> for TypesTagged<'_, V, Root> {
    type Out = V::Out;

    fn visit_type<L: Versioned>(
        &mut self,
        name: &'static str,
        wrap: &dyn Fn(L) -> Telemetry,
    ) -> Option<V::Out> {
        let outer = self.wrap;
        self.inner.visit_type::<Tagged<L>>(name, &|t: Tagged<L>| {
            outer(Update { tag: t.tag, inner: wrap(t.data) })
        })
    }
}

impl HasVersionedLeaves for Update {
    const LEAF_NAMES: &'static [&'static str] = Telemetry::LEAF_NAMES;

    fn visit_leaf<V: VisitorVersionedLeaf>(&self, visitor: &mut V) {
        self.inner.visit_leaf(&mut Wrap { tag: self.tag, inner: visitor });
    }

    fn visit_leaf_types<Root, V: VisitorLeafType<Root>>(
        visitor: &mut V,
        wrap: &dyn Fn(Self) -> Root,
    ) -> Option<V::Out> {
        Telemetry::visit_leaf_types(&mut TypesTagged { inner: visitor, wrap }, &|t| t)
    }
}

struct Collect {
    names: Vec<&'static str>,
}

impl VisitorLeafType<Update> for Collect {
    type Out = ();

    fn visit_type<L: Versioned>(
        &mut self,
        name: &'static str,
        _wrap: &dyn Fn(L) -> Update,
    ) -> Option<()> {
        self.names.push(name);
        None
    }
}

fn stamp(tile: u16, slot: u64) -> TrackingTimestamp {
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

#[test]
fn wrapped_leaves_round_trip_through_type_walk() {
    let pushed = [
        (11u64, Telemetry::Trade(Trade::Price(Price { id: 1, value: 100 }))),
        (22u64, Telemetry::Trade(Trade::Fill(Fill { id: 2, qty: 7 }))),
        (33u64, Telemetry::Ping(Ping { at: 99 })),
    ];
    let mut cache = BlobCache::new();
    let mut originals = Vec::new();
    for (i, (tag, inner)) in pushed.iter().enumerate() {
        let msg =
            InternalMessage::new(stamp(i as u16, i as u64), Update { tag: *tag, inner: *inner });
        cache.push(&msg);
        originals.push(msg);
    }
    let meta = Meta { batch: 5 };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 3);

    let mut load = Scratch::new();
    let mut work = Scratch::new();
    let mut seen = Vec::new();
    for bytes in &blobs {
        let blob = load.load(bytes).unwrap();
        assert!(["Price", "Fill", "Ping"].contains(&blob.type_name()));
        assert!(Telemetry::decode_blob::<Meta>(blob, &mut work).is_none());
        let (got_meta, msgs) = Update::decode_blob::<Meta>(blob, &mut work).unwrap().unwrap();
        assert_eq!(got_meta, meta);
        assert_eq!(msgs.len(), 1);
        let got = &msgs[0];
        let want = originals.iter().find(|m| m.data().tag == got.data().tag).unwrap();
        assert_eq!(got.data(), want.data());
        assert_eq!(got.ingestion_time().real(), want.ingestion_time().real());
        assert_eq!(got.tile_id(), want.tile_id());
        seen.push((blob.type_name().to_owned(), got.data().tag));
    }
    seen.sort();
    assert_eq!(seen, vec![
        ("Fill".to_owned(), 22),
        ("Ping".to_owned(), 33),
        ("Price".to_owned(), 11),
    ]);

    let mut collect = Collect { names: Vec::new() };
    Update::visit_leaf_types(&mut collect, &|u| u);
    assert_eq!(collect.names, ["Price", "Fill", "Ping"]);
}

#[test]
fn plain_family_still_decodes() {
    let mut cache = BlobCache::new();
    let msg = InternalMessage::new(stamp(9, 9), Telemetry::Ping(Ping { at: 7 }));
    cache.push(&msg);
    let meta = Meta { batch: 1 };
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    cache.flush(&meta, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    assert_eq!(blobs.len(), 1);

    let mut load = Scratch::new();
    let mut work = Scratch::new();
    let blob = load.load(&blobs[0]).unwrap();
    let (got_meta, msgs) = Telemetry::decode_blob::<Meta>(blob, &mut work).unwrap().unwrap();
    assert_eq!(got_meta, meta);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].data(), msg.data());
}
