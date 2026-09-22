use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_timing::{
    Duration, IngestionTime, Instant, InternalMessage, Nanos, PublishDelta, TrackingTimestamp,
};
use flux_versioned_types::{
    BlobCache, DecodeError, Scratch, Versioned, VersionedDeserialize, versioned_enum,
    versioned_struct,
};

versioned_struct!(Foo =>
    #[wire_skip]
    #[type_hash_lock(hash = 292551272382219431)]
    FooV1 { pub a: u8, pub b: u64 }
    #[type_hash_lock(hash = 668313194888403579)]
    FooV2 {
        add { pub c: u64 = 0 }
        modify { a: u64 = |a: u8| a as u64 }
    }
);

versioned_enum!(Bar =>
    #[wire_skip]
    #[type_hash_lock(hash = 3870828793263257854)]
    BarV1 { A, B }
    #[type_hash_lock(hash = 6466126499958559011)]
    BarV2 { add { C } }
);

versioned_struct!(WMeta =>
    #[type_hash_lock(hash = 4315918564078253665)]
    WMetaV1 { pub slot: u64 }
);

#[test]
fn skipped_versions_are_bincode_only() {
    assert_eq!(Foo::VERSION_HASHES, [FooV2::TYPE_HASH]);
    assert_eq!(Bar::VERSION_HASHES, [BarV2::TYPE_HASH]);
    assert!(matches!(
        Foo::decode_versions(FooV1::TYPE_HASH, &[], 0),
        Err(DecodeError::UnknownTypeHash(_))
    ));
    let bytes = bincode::serialize(&vec![FooV1 { a: 3, b: 4 }]).unwrap();
    let hash = FooV1::TYPE_HASH ^ 123_456;
    let latest = <Foo as VersionedDeserialize>::versioned_deserialize_vec(hash, &bytes).unwrap();
    assert_eq!(latest, vec![Foo { a: 3, b: 4, c: 0 }]);
    let bytes = bincode::serialize(&vec![BarV1::B]).unwrap();
    let hash = BarV1::TYPE_HASH ^ 123_456;
    let latest = <Bar as VersionedDeserialize>::versioned_deserialize_vec(hash, &bytes).unwrap();
    assert_eq!(latest, vec![Bar::B]);

    let ingestion = IngestionTime::new(Nanos(Nanos::now().0 - 10_000_000), Instant(1_000_000));
    let publish = ingestion.internal() + Duration::from_millis(5);
    let stamp = TrackingTimestamp {
        ingestion_t: ingestion,
        publish_delta: PublishDelta::new(1)
            .from_ingestion_and_publish_t(ingestion.internal(), publish),
    };
    let msg = InternalMessage::new(stamp, Foo { a: 1, b: 10, c: 2 });
    let mut cache = BlobCache::new();
    cache.push(&msg);
    let mut blobs = Vec::new();
    cache.flush(&WMeta { slot: 7 }, 3, |blob| blobs.push(blob.as_bytes().to_vec()));
    let (mut load, mut work) = (Scratch::new(), Scratch::new());
    let blob = load.load(&blobs[0]).unwrap();
    let (meta, msgs): (WMeta, Vec<InternalMessage<Foo>>) = blob.decode(&mut work).unwrap();
    assert_eq!(meta, WMeta { slot: 7 });
    assert_eq!(msgs[0].data(), msg.data());
}
