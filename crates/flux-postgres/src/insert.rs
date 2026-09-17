//! Multi-row `INSERT` statements for `serde::Serialize` rows.
//!
//! [`rows`] builds one `INSERT INTO ... VALUES ...` statement naming the
//! columns after the row's fields. Values render as text literals; strings
//! and bytes are escaped, `Option` is `NULL` or the bare value. Enums,
//! `char`, maps, and sequences are rejected; wrap byte strings in
//! [`crate::Bytea`].

use std::fmt::{self, Write as _};

use serde::{Serialize, ser};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnConflict {
    None,
    DoNothing,
}

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
    Error(format!("{what} has no INSERT encoding"))
}

pub fn rows<T: Serialize>(
    table: &str,
    rows: &[T],
    on_conflict: OnConflict,
) -> Result<String, Error> {
    let first = rows.first().ok_or_else(|| ser::Error::custom("empty batch"))?;
    let mut scratch = String::new();
    let mut probe = Encoder {
        out: &mut scratch,
        columns: Some(Vec::new()),
        named: false,
        depth: 0,
        first: true,
    };
    first.serialize(&mut probe)?;
    let (named, columns) = (probe.named, probe.columns.take().unwrap_or_default());
    if named && columns.is_empty() {
        return Err(unsupported("a row without columns"));
    }
    let mut out = format!("INSERT INTO {table} ");
    if named {
        write!(out, "({}) ", columns.join(", ")).unwrap();
    }
    out.push_str("VALUES ");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('(');
        let mut encoder =
            Encoder { out: &mut out, columns: None, named: false, depth: 0, first: true };
        row.serialize(&mut encoder)?;
        out.push(')');
    }
    match on_conflict {
        OnConflict::None => {}
        OnConflict::DoNothing => out.push_str(" ON CONFLICT DO NOTHING"),
    }
    Ok(out)
}

struct Encoder<'a> {
    out: &'a mut String,
    columns: Option<Vec<&'static str>>,
    named: bool,
    depth: usize,
    first: bool,
}

impl Encoder<'_> {
    fn value(&mut self) {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
    }

    fn text(&mut self, value: &str) -> Result<(), Error> {
        if value.contains('\0') {
            return Err(Error("string contains NUL".to_owned()));
        }
        self.out.push_str("E'");
        for c in value.chars() {
            match c {
                '\'' => self.out.push_str("''"),
                '\\' => self.out.push_str("\\\\"),
                '\n' => self.out.push_str("\\n"),
                '\r' => self.out.push_str("\\r"),
                '\t' => self.out.push_str("\\t"),
                _ => self.out.push(c),
            }
        }
        self.out.push('\'');
        Ok(())
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
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_i128(self, v: i128) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_u128(self, v: u128) -> Result<(), Error> {
        write!(self.out, "{v}").unwrap();
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        if v.is_finite() {
            write!(self.out, "{v}").unwrap();
        } else if v.is_nan() {
            self.out.push_str("E'NaN'");
        } else if v > 0.0 {
            self.out.push_str("E'Infinity'");
        } else {
            self.out.push_str("E'-Infinity'");
        }
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        if v.is_finite() {
            write!(self.out, "{v}").unwrap();
        } else if v.is_nan() {
            self.out.push_str("E'NaN'");
        } else if v > 0.0 {
            self.out.push_str("E'Infinity'");
        } else {
            self.out.push_str("E'-Infinity'");
        }
        Ok(())
    }
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.out.push_str(if v { "TRUE" } else { "FALSE" });
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), Error> {
        Err(unsupported("char"))
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.text(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.out.push_str("E'\\\\x");
        for byte in v {
            write!(self.out, "{byte:02x}").unwrap();
        }
        self.out.push('\'');
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.out.push_str("NULL");
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
    fn serialize_tuple(self, _: usize) -> Result<Self, Error> {
        self.first = true;
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Error> {
        self.first = true;
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
        if self.depth == 0 {
            self.named = true;
        }
        self.first = true;
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
                self.value();
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
        self.value();
        v.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        self.depth -= 1;
        Ok(())
    }
}
