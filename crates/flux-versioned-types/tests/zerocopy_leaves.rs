#![cfg(feature = "zerocopy")]

use flux::{type_hash::TypeHash, type_hash_derive::type_hash_lock};
use flux_versioned_types::{
    DecodeError, HasVersionedLeaves, Versioned, VisitorVersionedLeaf, versioned_enum,
    versioned_struct, zerocopy::IntoBytes,
};

versioned_struct!(Leaf =>
    #[type_hash_lock(hash = 4125045508827104188)]
    LeafV1 { pub slot: u64 }

    #[type_hash_lock(hash = 371124184812242291)]
    LeafV2 {
        add { pub extra: u32 = 0, pub flags: u32 = 0 }
    }
);

versioned_enum!(Kind =>
    #[type_hash_lock(hash = 6855142534858181385)]
    KindV1 { A, B }
    #[type_hash_lock(hash = 14019602368473989324)]
    KindV2 { add { C } }
);

versioned_struct!(#[wire_name = "Relay.NewBidSubmission"] Wired =>
    #[type_hash_lock(hash = 334959009063145319)]
    WiredV1 { pub slot: u64 }
);

#[derive(Clone, Copy, Debug, PartialEq, HasVersionedLeaves)]
pub enum Sub {
    A(Leaf),
    #[leaves(skip)]
    Ignored,
}

#[derive(Clone, Copy, Debug, PartialEq, HasVersionedLeaves)]
pub enum Family {
    S(Sub),
    K(Kind),
    #[leaves(skip)]
    Other,
}

struct Rec {
    names: Vec<&'static str>,
}

impl VisitorVersionedLeaf for Rec {
    fn visit_leaf<L: Versioned>(&mut self, _leaf: &L) {
        self.names.push(L::NAME);
    }
}

const _: fn(
    &flux_versioned_types::Blob,
    &mut flux_versioned_types::Scratch,
) -> Option<flux_versioned_types::Decoded<Leaf, Family>> = Family::decode_blob::<Leaf>;

#[test]
fn struct_versions_decode_and_migrate() {
    assert_eq!(Leaf::VERSION_HASHES, &[LeafV1::TYPE_HASH, LeafV2::TYPE_HASH]);
    let vals = [LeafV1 { slot: 1 }, LeafV1 { slot: 2 }, LeafV1 { slot: 3 }];
    let bytes = vals.as_slice().as_bytes();
    let out = Leaf::decode_versions(LeafV1::TYPE_HASH, bytes).unwrap();
    assert_eq!(out, vals.iter().map(|v| (*v).into()).collect::<Vec<Leaf>>());
    assert_eq!(out[0].extra, 0);

    let latest = [Leaf { slot: 9, extra: 1, flags: 2 }];
    let latest_bytes = latest.as_slice().as_bytes();
    let back = Leaf::decode_versions(LeafV2::TYPE_HASH, latest_bytes).unwrap();
    assert_eq!(back, latest);
}

#[test]
fn struct_decode_rejects_bad_input() {
    let vals = [LeafV1 { slot: 7 }];
    let bytes = vals.as_slice().as_bytes().to_vec();
    assert!(matches!(
        Leaf::decode_versions(0xDEAD_BEEF, &bytes),
        Err(DecodeError::UnknownTypeHash(0xDEAD_BEEF))
    ));
    assert!(matches!(
        Leaf::decode_versions(LeafV1::TYPE_HASH, &bytes[..bytes.len() - 1]),
        Err(DecodeError::LengthMismatch { .. })
    ));
    let mut backing = vec![0u64; 8];
    let wide = backing.as_mut_slice().as_mut_bytes();
    wide[1..=bytes.len()].copy_from_slice(&bytes);
    let misaligned = &wide[1..=bytes.len()];
    assert!(matches!(
        Leaf::decode_versions(LeafV1::TYPE_HASH, misaligned),
        Err(DecodeError::Unaligned)
    ));
}

#[test]
fn enum_versions_decode_and_validate() {
    assert_eq!(Kind::VERSION_HASHES, &[KindV1::TYPE_HASH, KindV2::TYPE_HASH]);
    let vals = [KindV1::A, KindV1::B, KindV1::A];
    let bytes = vals.as_slice().as_bytes();
    let out = Kind::decode_versions(KindV1::TYPE_HASH, bytes).unwrap();
    assert_eq!(out, vec![Kind::A, Kind::B, Kind::A]);
    assert!(matches!(
        Kind::decode_versions(KindV2::TYPE_HASH, &[200]),
        Err(DecodeError::InvalidValue)
    ));
    assert!(matches!(
        Kind::decode_versions(0xDEAD_BEEF, bytes),
        Err(DecodeError::UnknownTypeHash(0xDEAD_BEEF))
    ));
}

#[test]
fn wire_names() {
    assert_eq!(Wired::NAME, "Relay.NewBidSubmission");
    assert_eq!(Leaf::NAME, "Leaf");
    assert_eq!(Kind::NAME, "Kind");
}

#[test]
fn family_visit_reaches_leaf() {
    let leaf = Leaf { slot: 5, extra: 0, flags: 0 };
    let fam = Family::S(Sub::A(leaf));
    let mut rec = Rec { names: Vec::new() };
    fam.visit_leaf(&mut rec);
    assert_eq!(rec.names, [Leaf::NAME]);

    let mut skipped = Rec { names: Vec::new() };
    Family::Other.visit_leaf(&mut skipped);
    assert!(skipped.names.is_empty());

    let sub: Sub = leaf.into();
    assert_eq!(sub, Sub::A(leaf));
    let fam2: Family = sub.into();
    assert_eq!(fam2, fam);
    let fam3: Family = Kind::A.into();
    assert_eq!(fam3, Family::K(Kind::A));
}
