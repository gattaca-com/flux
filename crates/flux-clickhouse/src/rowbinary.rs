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
    Error(format!("{what} has no RowBinary encoding"))
}

pub fn encode<T: Serialize + ?Sized>(out: &mut Vec<u8>, row: &T) -> Result<(), Error> {
    row.serialize(&mut Encoder { out, columns: None, depth: 0 })
}

pub fn insert_statement<T: Serialize + ?Sized>(table: &str, row: &T) -> Result<String, Error> {
    let mut scratch = Vec::new();
    let mut encoder = Encoder { out: &mut scratch, columns: Some(Vec::new()), depth: 0 };
    row.serialize(&mut encoder)?;
    let columns = encoder.columns.take().unwrap_or_default();
    if columns.is_empty() {
        return Err(unsupported("a row that is not a struct"))
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
        let len = len.ok_or_else(|| unsupported("a sequence of unknown length"))?;
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
    type SerializeMap = ser::Impossible<(), Error>;
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
        Err(unsupported("char"))
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
        Err(unsupported("an enum"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(unsupported("a map"))
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

elements!(
    SerializeSeq::serialize_element,
    SerializeTuple::serialize_element,
    SerializeTupleStruct::serialize_field,
);

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
