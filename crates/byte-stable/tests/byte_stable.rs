use byte_stable::{ByteStable, CastError, cast_slice, slice_as_bytes, words_as_bytes_mut};
use byte_stable_derive::ByteStable;

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[repr(C)]
struct Record {
    a: u64,
    b: u32,
    c: u32,
    d: [u8; 8],
    e: bool,
    f: [u8; 7],
}

fn record() -> Record {
    Record {
        a: 0x0102_0304_0506_0708,
        b: 0x1112_1314,
        c: 0x1516_1718,
        d: *b"12345678",
        e: true,
        f: *b"abcdefg",
    }
}

#[test]
fn derived_struct_round_trip() {
    let rec = record();
    assert_eq!(rec.as_bytes().len(), ::core::mem::size_of::<Record>());
    let recs = [rec, rec, rec];
    let bytes = slice_as_bytes(&recs);
    assert_eq!(bytes.len(), 3 * ::core::mem::size_of::<Record>());
    assert_eq!(cast_slice::<Record>(bytes).unwrap(), recs);
}

#[test]
fn derived_struct_rejects_invalid_bool() {
    let mut words = vec![0u64; 4];
    let bytes = words_as_bytes_mut(&mut words);
    assert!(cast_slice::<Record>(&bytes[..]).is_ok());
    bytes[24] = 2;
    assert_eq!(cast_slice::<Record>(&bytes[..]), Err(CastError::Invalid { index: 0 }));
}

#[test]
fn misaligned_and_bad_length_errors() {
    let mut words = vec![0u64; 8];
    let bytes = words_as_bytes_mut(&mut words);
    assert_eq!(cast_slice::<Record>(&bytes[1..]), Err(CastError::Unaligned));
    assert_eq!(
        cast_slice::<Record>(&bytes[..30]),
        Err(CastError::Length { got: 30, size: ::core::mem::size_of::<Record>() })
    );
}

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[repr(u8)]
enum Code {
    A = 1,
    B = 7,
}

#[test]
fn derived_enum_validates_discriminants() {
    assert_eq!(cast_slice::<Code>(&[7]).unwrap(), [Code::B]);
    assert_eq!(cast_slice::<Code>(&[2]), Err(CastError::Invalid { index: 0 }));
}

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[repr(C)]
struct Inner {
    flag: bool,
    tag: [u8; 7],
}

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[repr(C)]
struct Outer {
    v: u64,
    inner: Inner,
}

#[test]
fn nested_struct_validates_inner_field() {
    let good = Outer { v: 42, inner: Inner { flag: true, tag: *b"abcdefg" } };
    let arr = [good];
    let bytes = slice_as_bytes(&arr);
    assert_eq!(cast_slice::<Outer>(bytes).unwrap(), [good]);
    let mut words = vec![0u64; 2];
    let buf = words_as_bytes_mut(&mut words);
    buf.copy_from_slice(bytes);
    buf[8] = 2;
    assert_eq!(cast_slice::<Outer>(&buf[..]), Err(CastError::Invalid { index: 0 }));
}

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[repr(C)]
struct Pair<T> {
    a: T,
    b: T,
}

#[test]
fn generic_struct_for_u64_and_u32() {
    let p = Pair { a: 1u64, b: 2u64 };
    assert_eq!(p.as_bytes().len(), 16);
    let arr = [p];
    assert_eq!(cast_slice::<Pair<u64>>(slice_as_bytes(&arr)).unwrap(), [p]);
    let q = Pair { a: 1u32, b: 2u32 };
    let arr = [q];
    assert_eq!(cast_slice::<Pair<u32>>(slice_as_bytes(&arr)).unwrap(), [q]);
}

#[derive(Clone, Copy, Debug, PartialEq, ByteStable)]
#[byte_stable(crate = "::byte_stable")]
#[repr(C)]
struct AttrPath {
    x: u64,
}

#[test]
fn crate_attribute_path_compiles() {
    let v = AttrPath { x: 9 };
    let arr = [v];
    assert_eq!(cast_slice::<AttrPath>(slice_as_bytes(&arr)).unwrap(), [v]);
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_round_trip() {
    let id = uuid::Uuid::from_bytes([9u8; 16]);
    assert_eq!(id.as_bytes().len(), 16);
    let arr = [id];
    assert_eq!(cast_slice::<uuid::Uuid>(slice_as_bytes(&arr)).unwrap(), [id]);
}

#[cfg(feature = "alloy")]
#[test]
fn alloy_fixed_bytes_and_address_round_trip() {
    let fb = alloy_primitives::FixedBytes::<32>([7u8; 32]);
    let arr = [fb];
    assert_eq!(cast_slice::<alloy_primitives::FixedBytes<32>>(slice_as_bytes(&arr)).unwrap(), [fb]);
    let addr = alloy_primitives::Address::from([2u8; 20]);
    let arr = [addr];
    assert_eq!(cast_slice::<alloy_primitives::Address>(slice_as_bytes(&arr)).unwrap(), [addr]);
}

#[cfg(feature = "alloy")]
#[test]
fn alloy_u256_round_trip() {
    let v = alloy_primitives::U256::from(0xdead_beefu64);
    let arr = [v];
    assert_eq!(cast_slice::<alloy_primitives::U256>(slice_as_bytes(&arr)).unwrap(), [v]);
}

#[cfg(feature = "alloy")]
#[test]
fn alloy_masked_uint_rejects_top_bit() {
    type U255 = alloy_primitives::Uint<255, 4>;
    let mut words = vec![0u64; 4];
    let bytes = words_as_bytes_mut(&mut words);
    assert!(cast_slice::<U255>(&bytes[..]).is_ok());
    bytes[31] = 0x80;
    assert_eq!(cast_slice::<U255>(&bytes[..]), Err(CastError::Invalid { index: 0 }));
    bytes[31] = 0;
    assert!(cast_slice::<U255>(&bytes[..]).is_ok());
}
