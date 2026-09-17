//! `COPY ... (FORMAT BINARY)` encoding of `serde::Serialize` rows.
//!
//! [`encode`] appends one tuple to a body framed by [`header`] and
//! [`trailer`]; [`copy_statement`] names the columns after the row's fields.
//! Integers are big-endian and widen to the smallest holding Postgres type
//! (`u128`/`i128` to `numeric`); enums, `char`, maps, and sequences are
//! rejected; wrap byte strings in [`crate::Bytea`].

use std::fmt;

use serde::{Serialize, ser};

#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

fn unsupported(what: &str) -> Error {
    Error(format!("{what} has no COPY BINARY encoding"))
}

pub fn header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
}

pub fn trailer(out: &mut Vec<u8>) {
    out.extend_from_slice(&(-1i16).to_be_bytes());
}

pub fn encode<T: Serialize + ?Sized>(out: &mut Vec<u8>, row: &T) -> Result<(), Error> {
    let pos = out.len();
    out.extend_from_slice(&[0, 0]);
    let mut encoder = Encoder { out, columns: None, depth: 0, fields: 0 };
    row.serialize(&mut encoder)?;
    let count =
        i16::try_from(encoder.fields).map_err(|_| Error("row has too many fields".to_owned()))?;
    encoder.out[pos..pos + 2].copy_from_slice(&count.to_be_bytes());
    Ok(())
}

pub fn copy_statement<T: Serialize + ?Sized>(table: &str, row: &T) -> Result<String, Error> {
    let mut scratch = Vec::new();
    let mut encoder = Encoder { out: &mut scratch, columns: Some(Vec::new()), depth: 0, fields: 0 };
    row.serialize(&mut encoder)?;
    let columns = encoder.columns.take().unwrap_or_default();
    if columns.is_empty() {
        return Err(unsupported("a row that is not a struct"))
    }
    let quoted = columns
        .iter()
        .map(|name| format!("\"{}\"", name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("COPY {table} ({quoted}) FROM STDIN (FORMAT BINARY)"))
}

struct Encoder<'a> {
    out: &'a mut Vec<u8>,
    columns: Option<Vec<&'static str>>,
    depth: usize,
    fields: usize,
}

impl Encoder<'_> {
    fn top_level(&mut self, len: usize) {
        if self.depth == 0 {
            self.fields = len;
        }
    }
    fn single(&mut self) {
        if self.depth == 0 && self.fields == 0 {
            self.fields = 1;
        }
    }
    fn sized(&mut self, len: usize, bytes: &[u8]) {
        self.out.extend_from_slice(&(len as u32).to_be_bytes());
        self.out.extend_from_slice(bytes);
    }
    fn numeric(&mut self, negative: bool, mut abs: u128) {
        let mut digits = [0u16; 10];
        let mut len = 0;
        while abs > 0 {
            digits[len] = (abs % 10000) as u16;
            abs /= 10000;
            len += 1;
        }
        if len == 0 {
            len = 1;
        }
        let weight = len - 1;
        let mut start = 0;
        while len - start > 1 && digits[start] == 0 {
            start += 1;
        }
        let kept = len - start;
        self.out.extend_from_slice(&((8 + 2 * kept) as u32).to_be_bytes());
        self.out.extend_from_slice(&(kept as u16).to_be_bytes());
        self.out.extend_from_slice(&(weight as u16).to_be_bytes());
        self.out.extend_from_slice(&(if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        self.out.extend_from_slice(&0u16.to_be_bytes());
        for digit in digits[start..len].iter().rev() {
            self.out.extend_from_slice(&digit.to_be_bytes());
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
        false
    }
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.single();
        self.sized(2, &(v as i16).to_be_bytes());
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.single();
        self.sized(2, &v.to_be_bytes());
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.single();
        self.sized(4, &v.to_be_bytes());
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        self.single();
        self.sized(8, &v.to_be_bytes());
        Ok(())
    }
    fn serialize_i128(self, v: i128) -> Result<(), Error> {
        self.single();
        self.numeric(v < 0, v.unsigned_abs());
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.single();
        self.sized(2, &(v as i16).to_be_bytes());
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.single();
        self.sized(4, &(v as i32).to_be_bytes());
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.single();
        self.sized(8, &(v as i64).to_be_bytes());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        if v > i64::MAX as u64 {
            return Err(Error(format!("u64 value {v} exceeds bigint")))
        }
        self.single();
        self.sized(8, &(v as i64).to_be_bytes());
        Ok(())
    }
    fn serialize_u128(self, v: u128) -> Result<(), Error> {
        self.single();
        self.numeric(false, v);
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.single();
        self.sized(4, &v.to_be_bytes());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.single();
        self.sized(8, &v.to_be_bytes());
        Ok(())
    }
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.single();
        self.sized(1, &[u8::from(v)]);
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), Error> {
        Err(unsupported("char"))
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.single();
        self.sized(v.len(), v.as_bytes());
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.single();
        self.sized(v.len(), v);
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.single();
        self.out.extend_from_slice(&(-1i32).to_be_bytes());
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
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(unsupported("an enum"))
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
        Err(unsupported("an enum"))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(unsupported("a sequence"))
    }
    fn serialize_tuple(self, len: usize) -> Result<Self, Error> {
        self.top_level(len);
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Self, Error> {
        self.top_level(len);
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(unsupported("an enum"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(unsupported("a map"))
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Self, Error> {
        self.top_level(len);
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
        Err(unsupported("an enum"))
    }
}

macro_rules! elements {
    ($($trait:ident::$method:ident),* $(,)?) => {$(
        impl ser::$trait for &mut Encoder<'_> {
            type Ok = ();
            type Error = Error;
            fn $method<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
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
