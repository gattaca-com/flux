use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_versioned_types::{VersionedDeserialize, versioned_enum, versioned_struct};

versioned_struct!(Reading =>
    #[type_hash_lock(hash = 17013878556110425249)]
    ReadingV1 { pub value: u32 }

    #[type_hash_lock(hash = 761223436273093920)]
    ReadingV2 {
        modify { value: u64 = u64::from }
        add { pub valid: bool = true }
    }
);

versioned_enum!(Status =>
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, flux::type_hash_derive::TypeHash)]
        #[type_hash(skip_typename_on_derive)]
        #[repr(u8)]
    }

    #[type_hash_lock(hash = 13811032913305574456)]
    StatusV1 { Ready(u32), #[default] Unknown }
    #[type_hash_lock(hash = 1629192207022183331)]
    StatusV2 {
        modify { Ready(u64) = u64::from }
        add { Complete }
    }
);

#[test]
fn decodes_and_migrates_an_old_struct_vector() {
    let old = vec![ReadingV1 { value: 7 }];
    let bytes = bincode::serialize(&old).unwrap();
    let stored_hash = ReadingV1::TYPE_HASH ^ 123_456;

    let latest =
        <Reading as VersionedDeserialize>::versioned_deserialize_vec(stored_hash, &bytes).unwrap();

    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].value, 7);
    assert!(latest[0].valid);
}

#[test]
fn decodes_and_migrates_an_old_enum_vector() {
    let old = vec![StatusV1::Ready(9)];
    let bytes = bincode::serialize(&old).unwrap();
    let stored_hash = StatusV1::TYPE_HASH ^ 123_456;

    let latest =
        <Status as VersionedDeserialize>::versioned_deserialize_vec(stored_hash, &bytes).unwrap();

    assert_eq!(latest, vec![StatusV2::Ready(9)]);
}

#[test]
fn rejects_an_unknown_hash() {
    let error = <Reading as VersionedDeserialize>::versioned_deserialize_vec(0, &[]).unwrap_err();
    assert!(error.to_string().contains("Invalid type hash: 0"));
}
