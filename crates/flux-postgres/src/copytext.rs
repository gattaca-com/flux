//! `COPY ... (FORMAT TEXT)` encoding of `serde::Serialize` rows.
//!
//! [`encode`] appends one row; [`encode_batch`] names the columns
//! after the first row's fields. Values are written in their
//! text form, so types that serialize as strings reach `Postgres` as the
//! literal it parses for the column: `chrono` timestamps, `BigDecimal`
//! numerics, and unit enum variants all work here, unlike
//! [`crate::copybinary`]. Nested containers, maps, and sequences are
//! rejected; wrap byte strings in [`crate::Bytea`].

use std::fmt::{self, Write as _};

use serde::{Serialize, ser};

use crate::EncodeError as Error;

fn unsupported(what: &str) -> Error {
    Error(format!("{what} has no COPY TEXT encoding"))
}

pub fn encode<T: Serialize + ?Sized>(out: &mut Vec<u8>, row: &T) -> Result<(), Error> {
    encode_inner(out, row, None)
}

/// Encodes `rows` as one body and returns the first row's column names.
pub fn encode_batch<T: Serialize>(
    out: &mut Vec<u8>,
    rows: &[T],
) -> Result<Vec<&'static str>, Error> {
    let Some((first, rest)) = rows.split_first() else { return Err(unsupported("an empty batch")) };
    let mut columns = Vec::new();
    encode_inner(out, first, Some(&mut columns))?;
    if columns.is_empty() {
        return Err(unsupported("a row that is not a struct"));
    }
    for row in rest {
        encode_inner(out, row, None)?;
    }
    Ok(columns)
}

fn encode_inner<T: Serialize + ?Sized>(
    out: &mut Vec<u8>,
    row: &T,
    columns: Option<&mut Vec<&'static str>>,
) -> Result<(), Error> {
    let mut encoder = Encoder { out, columns, depth: 0, first: true };
    row.serialize(&mut encoder)?;
    encoder.out.push(b'\n');
    Ok(())
}

const HEX: &[u8; 16] = b"0123456789abcdef";

struct Encoder<'a> {
    out: &'a mut Vec<u8>,
    columns: Option<&'a mut Vec<&'static str>>,
    depth: usize,
    first: bool,
}

impl Encoder<'_> {
    fn separator(&mut self) {
        if !self.first {
            self.out.push(b'\t');
        }
        self.first = false;
    }

    fn text(&mut self, value: &str) {
        for byte in value.bytes() {
            match byte {
                b'\\' => self.out.extend_from_slice(b"\\\\"),
                b'\n' => self.out.extend_from_slice(b"\\n"),
                b'\r' => self.out.extend_from_slice(b"\\r"),
                b'\t' => self.out.extend_from_slice(b"\\t"),
                0x08 => self.out.extend_from_slice(b"\\b"),
                0x0b => self.out.extend_from_slice(b"\\v"),
                0x0c => self.out.extend_from_slice(b"\\f"),
                _ => self.out.push(byte),
            }
        }
    }

    fn container(&mut self) -> Result<(), Error> {
        if self.depth > 0 {
            return Err(unsupported("a nested container"));
        }
        self.depth += 1;
        self.first = true;
        Ok(())
    }
}

macro_rules! display {
    ($($method:ident: $type:ty),* $(,)?) => {$(
        fn $method(self, v: $type) -> Result<(), Error> {
            write!(self.out_writer(), "{v}").unwrap();
            Ok(())
        }
    )*};
}

impl Encoder<'_> {
    fn out_writer(&mut self) -> impl fmt::Write {
        struct Sink<'a>(&'a mut Vec<u8>);
        impl fmt::Write for Sink<'_> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.0.extend_from_slice(s.as_bytes());
                Ok(())
            }
        }
        Sink(self.out)
    }

    fn float(&mut self, finite: bool, nan: bool, positive: bool, value: impl fmt::Display) {
        if finite {
            write!(self.out_writer(), "{value}").unwrap();
        } else if nan {
            self.out.extend_from_slice(b"NaN");
        } else if positive {
            self.out.extend_from_slice(b"Infinity");
        } else {
            self.out.extend_from_slice(b"-Infinity");
        }
    }
}

impl ser::Serializer for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn is_human_readable(&self) -> bool {
        true
    }
    display!(
        serialize_i8: i8, serialize_i16: i16, serialize_i32: i32, serialize_i64: i64,
        serialize_i128: i128, serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
        serialize_u64: u64, serialize_u128: u128,
    );
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.float(v.is_finite(), v.is_nan(), v > 0.0, v);
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.float(v.is_finite(), v.is_nan(), v > 0.0, v);
        Ok(())
    }
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.out.push(if v { b't' } else { b'f' });
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), Error> {
        Err(unsupported("char"))
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.text(v);
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.out.extend_from_slice(b"\\\\x");
        for byte in v {
            self.out.extend_from_slice(&[HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]);
        }
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.out.extend_from_slice(b"\\N");
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<(), Error> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        Err(unsupported("unit"))
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        Err(unsupported("a unit struct"))
    }
    /// Unit variants carry the label `Postgres` enums store.
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), Error> {
        self.text(variant);
        Ok(())
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
        Err(unsupported("a data-carrying enum"))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(unsupported("a sequence"))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, Error> {
        self.container()?;
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        self.container()?;
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(unsupported("a data-carrying enum"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(unsupported("a map"))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        self.container()?;
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(unsupported("a data-carrying enum"))
    }
}

macro_rules! elements {
    ($($trait:ident::$method:ident),* $(,)?) => {$(
        impl ser::$trait for &mut Encoder<'_> {
            type Ok = ();
            type Error = Error;
            fn $method<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
                self.separator();
                v.serialize(&mut **self)
            }
            fn end(self) -> Result<(), Error> {
                Ok(())
            }
        }
    )*};
}

elements!(SerializeTuple::serialize_element, SerializeTupleStruct::serialize_field,);

impl ser::SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        if let Some(columns) = &mut self.columns {
            columns.push(key);
        }
        self.separator();
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}
