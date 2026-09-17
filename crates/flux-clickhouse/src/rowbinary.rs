//! `RowBinary` encoding of [`serde::Serialize`] rows for insert bodies.
//!
//! Field order is column order, so name the columns in the statement:
//! [`insert_statement`] builds `INSERT INTO t (a, b, ...) FORMAT RowBinary`
//! from a row's field names and [`encode`] appends one row to a body.
//!
//! Integers and floats are little-endian, `bool` is one byte, `str` and bytes
//! are a LEB128 length then the bytes (`String` or `Array(UInt8)`), `Option`
//! is a `Nullable` flag byte then the value, sequences and maps are a LEB128
//! count then the elements, and tuples and nested structs are their elements
//! back to back (`Tuple`). Enums, `char`, unit, and sequences of unknown
//! length are rejected.

use std::fmt;

use serde::{Serialize, ser};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Unsupported(&'static str),
    Custom(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(what) => write!(f, "{what} has no RowBinary encoding"),
            Self::Custom(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::Custom(message.to_string())
    }
}

/// Appends `row` to `out`.
pub fn encode<T: Serialize + ?Sized>(out: &mut Vec<u8>, row: &T) -> Result<(), Error> {
    row.serialize(&mut Encoder { out, columns: None, depth: 0 })
}

/// Builds `INSERT INTO <table> (<fields>) FORMAT RowBinary` from the field
/// names of `row`, which must be a struct.
pub fn insert_statement<T: Serialize + ?Sized>(table: &str, row: &T) -> Result<String, Error> {
    let mut scratch = Vec::new();
    let mut encoder = Encoder { out: &mut scratch, columns: Some(Vec::new()), depth: 0 };
    row.serialize(&mut encoder)?;
    let columns = encoder.columns.take().unwrap_or_default();
    if columns.is_empty() {
        return Err(Error::Unsupported("a row that is not a struct with named fields"))
    }
    Ok(format!("INSERT INTO {table} ({}) FORMAT RowBinary", columns.join(", ")))
}

struct Encoder<'a> {
    out: &'a mut Vec<u8>,
    columns: Option<Vec<&'static str>>,
    depth: usize,
}

impl Encoder<'_> {
    fn leb128(&mut self, mut n: u64) {
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.out.push(byte);
                return
            }
            self.out.push(byte | 0x80);
        }
    }
    fn count(&mut self, len: Option<usize>) -> Result<(), Error> {
        let len = len.ok_or(Error::Unsupported("a sequence of unknown length"))?;
        self.leb128(len as u64);
        Ok(())
    }
}

macro_rules! little_endian {
    ($($method:ident: $type:ty),* $(,)?) => {$(
        fn $method(self, v: $type) -> Result<(), Error> {
            self.out.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
    )*};
}

impl ser::Serializer for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn is_human_readable(&self) -> bool {
        false
    }
    little_endian!(
        serialize_i8: i8, serialize_i16: i16, serialize_i32: i32, serialize_i64: i64,
        serialize_i128: i128, serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
        serialize_u64: u64, serialize_u128: u128, serialize_f32: f32, serialize_f64: f64,
    );
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.out.push(u8::from(v));
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), Error> {
        Err(Error::Unsupported("char"))
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.serialize_bytes(v.as_bytes())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.leb128(v.len() as u64);
        self.out.extend_from_slice(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.out.push(1);
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), Error> {
        self.out.push(0);
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        Err(Error::Unsupported("unit"))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        Err(Error::Unsupported("a unit struct"))
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(Error::Unsupported("an enum"))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), Error> {
        Err(Error::Unsupported("an enum"))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self, Error> {
        self.count(len)?;
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Unsupported("an enum"))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self, Error> {
        self.count(len)?;
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        self.depth += 1;
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Unsupported("an enum"))
    }
}

impl ser::SerializeSeq for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeMap for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        key.serialize(&mut **self)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        if self.depth == 1 &&
            let Some(columns) = &mut self.columns
        {
            columns.push(key);
        }
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        self.depth -= 1;
        Ok(())
    }
}
