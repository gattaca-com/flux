use flux::{type_hash::TypeHash, type_hash_derive::TypeHash};

#[allow(dead_code)]
#[derive(TypeHash)]
struct NestedMessage {
    enabled: bool,
    value: u64,
}

#[allow(dead_code)]
#[derive(TypeHash)]
#[type_hash(name = "OpenMessage")]
enum OpenMessage {
    #[wincode(tag = 0)]
    #[type_hash_lock(hash = 13065817970361848941)]
    Unit,
    #[wincode(tag = 1)]
    #[type_hash_lock(hash = 2862583741733307329)]
    Tuple(u32),
    #[wincode(tag = 2)]
    #[type_hash_lock(hash = 6744190838328046502)]
    Nested(NestedMessage),
}

const _: u64 = OpenMessage::TYPE_HASH;

#[test]
fn locks_enum_variants_with_wincode_tags() {
    assert_ne!(OpenMessage::TYPE_HASH, 0);
}

#[cfg(feature = "wincode")]
mod wire_compatibility {
    use flux::{type_hash::TypeHash as _, type_hash_derive::TypeHash};
    use wincode_derive::{SchemaRead, SchemaWrite};

    #[allow(dead_code)]
    #[derive(SchemaRead, SchemaWrite, TypeHash)]
    #[type_hash(name = "CompatibilityMessage")]
    enum MessageV1 {
        #[wincode(tag = 7)]
        #[type_hash_lock(hash = 7942717339589396332)]
        Shared(u64),
        #[wincode(tag = 8)]
        Removed,
    }

    #[allow(dead_code)]
    #[derive(Debug, PartialEq, SchemaRead, SchemaWrite, TypeHash)]
    #[type_hash(name = "CompatibilityMessage")]
    enum MessageV2 {
        #[wincode(tag = 6)]
        AddedBefore,
        #[wincode(tag = 7)]
        #[type_hash_lock(hash = 7942717339589396332)]
        Shared(u64),
        #[wincode(tag = 9)]
        AddedAfter,
    }

    const _: u64 = MessageV1::TYPE_HASH;
    const _: u64 = MessageV2::TYPE_HASH;

    #[test]
    fn matching_tag_deserializes_across_enum_versions() {
        let encoded = wincode::serialize(&MessageV1::Shared(42)).unwrap();
        let decoded = wincode::deserialize::<MessageV2>(&encoded).unwrap();

        assert_eq!(decoded, MessageV2::Shared(42));
    }

    #[test]
    fn missing_tag_fails_to_deserialize() {
        let encoded = wincode::serialize(&MessageV1::Removed).unwrap();

        assert!(wincode::deserialize::<MessageV2>(&encoded).is_err());
    }
}
