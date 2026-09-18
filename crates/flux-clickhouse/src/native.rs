//! Native block format: cursor primitives, type parsing, block decoding,
//! and transposition of `RowBinary` rows into a column-major block.

use std::collections::HashMap;

const LOW_CARDINALITY_VERSION: u64 = 1;
const NEEDS_GLOBAL_DICTIONARY: u64 = 1 << 8;
const HAS_ADDITIONAL_KEYS: u64 = 1 << 9;
const NEEDS_DICTIONARY_UPDATE: u64 = 1 << 10;

/// One column of a native block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub type_name: String,
}

/// A decoded block: the column headers and one entry per cell.
///
/// Cells carry the value's native bytes, `None` for a NULL, little-endian for
/// numbers, unprefixed for strings, and `RowBinary` for the composite types:
/// count then elements for an array or a map, fields back to back for a
/// tuple.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Output {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn put_string(out: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

const COMPRESS_METHOD_LZ4: u8 = 0x82;
const CHECKSUM_LEN: usize = 16;
/// The compressed envelope header: the method byte, the envelope size
/// including the header, and the decompressed size.
const COMPRESSED_HEADER_LEN: usize = 9;

/// Wraps `raw` in one compressed envelope: the checksum over the header and
/// the payload, then the header, then the `LZ4` payload.
pub(crate) fn compress_block(raw: &[u8]) -> Vec<u8> {
    let payload = lz4_flex::block::compress(raw);
    let mut envelope = Vec::with_capacity(COMPRESSED_HEADER_LEN + payload.len());
    envelope.push(COMPRESS_METHOD_LZ4);
    envelope.extend_from_slice(&((COMPRESSED_HEADER_LEN + payload.len()) as u32).to_le_bytes());
    envelope.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    envelope.extend_from_slice(&payload);
    let mut out = Vec::with_capacity(CHECKSUM_LEN + envelope.len());
    out.extend_from_slice(&checksum(&envelope));
    out.extend_from_slice(&envelope);
    out
}

/// Reads one compressed envelope, verifying the checksum before anything is
/// trusted. `budget` caps the declared size before it is allocated; breaching
/// it sets [`Cursor::too_large`] rather than `invalid`.
pub(crate) fn read_compressed(cursor: &mut Cursor<'_>, budget: usize) -> Option<Vec<u8>> {
    let wire = cursor.bytes(CHECKSUM_LEN)?;
    if cursor.u8()? != COMPRESS_METHOD_LZ4 {
        return cursor.fail("unknown compression method")
    }
    let compressed = cursor.u32()? as usize;
    let original = cursor.u32()? as usize;
    let Some(payload_len) = compressed.checked_sub(COMPRESSED_HEADER_LEN) else {
        return cursor.fail("compressed size smaller than its header")
    };
    if original > budget {
        cursor.too_large = true;
        return None
    }
    let payload = cursor.bytes(payload_len)?;
    let mut envelope = Vec::with_capacity(compressed);
    envelope.push(COMPRESS_METHOD_LZ4);
    envelope.extend_from_slice(&(compressed as u32).to_le_bytes());
    envelope.extend_from_slice(&(original as u32).to_le_bytes());
    envelope.extend_from_slice(payload);
    if wire != checksum(&envelope) {
        return cursor.fail("compressed block failed its checksum")
    }
    if let Ok(raw) = lz4_flex::block::decompress(payload, original) {
        return Some(raw)
    }
    cursor.fail("compressed block does not decompress")
}

/// The envelope checksum: `CityHash` 1.0.2, the version the server pins, with
/// its words on the wire high word first.
fn checksum(envelope: &[u8]) -> [u8; CHECKSUM_LEN] {
    let hash = cityhash_rs::cityhash_102_128(envelope);
    let mut out = [0; CHECKSUM_LEN];
    out[..8].copy_from_slice(&((hash >> 64) as u64).to_le_bytes());
    out[8..].copy_from_slice(&(hash as u64).to_le_bytes());
    out
}

/// Appends `raw` as one block, compressed when the query negotiated it.
pub(crate) fn push_block(out: &mut Vec<u8>, raw: &[u8], compressed: bool) {
    if compressed {
        out.extend_from_slice(&compress_block(raw));
    } else {
        out.extend_from_slice(raw);
    }
}

/// Reader over one receive buffer. Every method yields `None` when the bytes
/// ran out, which the caller reads as "wait for more"; a stream that can never
/// parse is caught by the client's output budget instead. Malformed content
/// that is recognisably wrong sets [`Cursor::invalid`].
pub(crate) struct Cursor<'a> {
    data: &'a [u8],
    pub(crate) invalid: Option<&'static str>,
    pub(crate) too_large: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, invalid: None, too_large: false }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn u8(&mut self) -> Option<u8> {
        let (first, rest) = self.data.split_first()?;
        self.data = rest;
        Some(*first)
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.data.len() < len {
            return None
        }
        let (head, rest) = self.data.split_at(len);
        self.data = rest;
        Some(head)
    }

    pub(crate) fn uvarint(&mut self) -> Option<u64> {
        let mut value = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value)
            }
        }
        self.fail("varint longer than 64 bits")
    }

    pub(crate) fn string(&mut self) -> Option<&'a [u8]> {
        let len = self.uvarint()? as usize;
        self.bytes(len)
    }

    pub(crate) fn owned_string(&mut self) -> Option<String> {
        let bytes = self.string()?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    pub(crate) fn i32(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes(self.bytes(4)?.try_into().ok()?))
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.bytes(4)?.try_into().ok()?))
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.bytes(8)?.try_into().ok()?))
    }

    pub(crate) fn fail<T>(&mut self, reason: &'static str) -> Option<T> {
        self.invalid = Some(reason);
        None
    }
}

/// The wire layout of a column, which is all the client needs of a type: its
/// fixed width, or how its parts nest.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Type {
    Fixed(usize),
    Str,
    Nullable(Box<Type>),
    Array(Box<Type>),
    Tuple(Vec<Type>),
    LowCardinality(Box<Type>),
}

impl Type {
    fn parse(name: &str) -> Option<Self> {
        let name = name.trim();
        if let Some(args) = Self::args(name, "Nullable") {
            return Some(Self::Nullable(Box::new(Self::parse(args)?)))
        }
        if let Some(args) = Self::args(name, "Array") {
            return Some(Self::Array(Box::new(Self::parse(args)?)))
        }
        if let Some(args) = Self::args(name, "LowCardinality") {
            return Some(Self::LowCardinality(Box::new(Self::parse(args)?)))
        }
        if let Some(args) = Self::args(name, "Tuple") {
            return Some(Self::Tuple(
                Self::split(args)
                    .iter()
                    .map(|arg| Self::parse_field(arg))
                    .collect::<Option<_>>()?,
            ))
        }
        // A map is laid out like the array of pairs it is.
        if let Some(args) = Self::args(name, "Map") {
            let [key, value] = Self::split(args)[..] else { return None };
            let pair = Self::Tuple(vec![Self::parse(key)?, Self::parse(value)?]);
            return Some(Self::Array(Box::new(pair)))
        }
        if let Some(len) = Self::args(name, "FixedString") {
            return Some(Self::Fixed(len.trim().parse().ok()?))
        }
        if let Some(args) = Self::args(name, "Decimal") {
            let precision: u32 = Self::split(args).first()?.parse().ok()?;
            return Some(Self::Fixed(match precision {
                0..=9 => 4,
                10..=18 => 8,
                19..=38 => 16,
                39..=76 => 32,
                _ => return None,
            }))
        }
        Some(match name.split('(').next().unwrap_or(name) {
            "String" => Self::Str,
            "Bool" | "Int8" | "UInt8" | "Enum8" => Self::Fixed(1),
            "Int16" | "UInt16" | "Enum16" | "Date" => Self::Fixed(2),
            "Int32" | "UInt32" | "Float32" | "Date32" | "DateTime" | "IPv4" | "Decimal32" => {
                Self::Fixed(4)
            }
            "Int64" | "UInt64" | "Float64" | "DateTime64" | "Decimal64" => Self::Fixed(8),
            "Int128" | "UInt128" | "UUID" | "IPv6" | "Decimal128" => Self::Fixed(16),
            "Int256" | "UInt256" | "Decimal256" => Self::Fixed(32),
            _ => return None,
        })
    }

    /// A tuple field, which may carry a name before its type.
    fn parse_field(field: &str) -> Option<Self> {
        Self::parse(field).or_else(|| Self::parse(field.split_once(' ')?.1))
    }

    /// The arguments of `Name(...)`, if that is what this type is.
    fn args<'a>(name: &'a str, outer: &str) -> Option<&'a str> {
        name.strip_prefix(outer)?.strip_prefix('(')?.strip_suffix(')')
    }

    /// Splits type arguments on the commas that separate them, leaving those
    /// nested inside parentheses or enum labels alone.
    fn split(args: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let (mut depth, mut quoted, mut start) = (0u32, false, 0);
        for (at, character) in args.char_indices() {
            match character {
                '\'' => quoted = !quoted,
                '(' if !quoted => depth += 1,
                ')' if !quoted => depth = depth.saturating_sub(1),
                ',' if !quoted && depth == 0 => {
                    parts.push(args[start..at].trim());
                    start = at + 1;
                }
                _ => {}
            }
        }
        parts.push(args[start..].trim());
        parts
    }

    /// The type a low cardinality dictionary stores, and whether index zero is
    /// reserved for NULL.
    fn dictionary(&self) -> (&Self, bool) {
        match self {
            Self::Nullable(inner) => (inner, true),
            other => (other, false),
        }
    }

    /// Reads one `RowBinary` value into the column buffers.
    fn transpose(&self, cursor: &mut Cursor<'_>, buf: &mut ColumnBuf) -> Option<()> {
        match (self, buf) {
            (Self::Fixed(width), ColumnBuf::Leaf(values)) => {
                values.extend_from_slice(cursor.bytes(*width)?);
            }
            (Self::Str, ColumnBuf::Leaf(values)) => {
                let text = cursor.string()?;
                put_string(values, text);
            }
            (Self::Nullable(inner), ColumnBuf::Nullable { nulls, values }) => {
                let is_null = cursor.u8()?;
                nulls.push(is_null);
                if is_null == 0 {
                    inner.transpose(cursor, values)?;
                } else {
                    inner.write_default(values);
                }
            }
            (Self::Array(inner), ColumnBuf::Array { offsets, len, values }) => {
                let count = cursor.uvarint()?;
                *len += count;
                offsets.extend_from_slice(&len.to_le_bytes());
                for _ in 0..count {
                    inner.transpose(cursor, values)?;
                }
            }
            (Self::Tuple(fields), ColumnBuf::Tuple(values)) => {
                for (field, value) in fields.iter().zip(values) {
                    field.transpose(cursor, value)?;
                }
            }
            (Self::LowCardinality(inner), ColumnBuf::Dictionary { dict, index, keys, entries }) => {
                let (leaf, nullable) = inner.dictionary();
                if nullable && cursor.u8()? == 1 {
                    keys.push(0);
                    return Some(())
                }
                let mut value = ColumnBuf::Leaf(Vec::new());
                leaf.transpose(cursor, &mut value)?;
                let ColumnBuf::Leaf(value) = value else { unreachable!("a leaf stays a leaf") };
                let next = index.len() as u64 + u64::from(nullable);
                keys.push(*index.entry(value.clone()).or_insert_with(|| {
                    dict.extend_from_slice(&value);
                    *entries += 1;
                    next
                }));
            }
            _ => return cursor.fail("column buffer does not match its type"),
        }
        Some(())
    }

    /// Writes the placeholder a NULL still occupies in the values array.
    fn write_default(&self, buf: &mut ColumnBuf) {
        match (self, buf) {
            (Self::Fixed(width), ColumnBuf::Leaf(values)) => values.resize(values.len() + width, 0),
            (Self::Str, ColumnBuf::Leaf(values)) => values.push(0),
            (Self::Nullable(inner), ColumnBuf::Nullable { nulls, values }) => {
                nulls.push(1);
                inner.write_default(values);
            }
            (Self::Array(_), ColumnBuf::Array { offsets, len, .. }) => {
                offsets.extend_from_slice(&len.to_le_bytes());
            }
            (Self::Tuple(fields), ColumnBuf::Tuple(values)) => {
                for (field, value) in fields.iter().zip(values) {
                    field.write_default(value);
                }
            }
            (Self::LowCardinality(_), ColumnBuf::Dictionary { keys, .. }) => keys.push(0),
            _ => unreachable!("column buffer is built from its own type"),
        }
    }

    /// Reads a whole column, returning each row's value in `RowBinary` form.
    fn read_column(&self, cursor: &mut Cursor<'_>, rows: usize) -> Option<Vec<Vec<u8>>> {
        match self {
            Self::Fixed(width) => {
                (0..rows).map(|_| cursor.bytes(*width).map(<[u8]>::to_vec)).collect()
            }
            Self::Str => (0..rows)
                .map(|_| {
                    let text = cursor.string()?;
                    let mut cell = Vec::with_capacity(text.len() + 1);
                    put_string(&mut cell, text);
                    Some(cell)
                })
                .collect(),
            Self::Nullable(inner) => {
                let nulls = cursor.bytes(rows)?.to_vec();
                let values = inner.read_column(cursor, rows)?;
                Some(
                    nulls
                        .into_iter()
                        .zip(values)
                        .map(|(is_null, value)| {
                            let mut cell = vec![is_null];
                            if is_null == 0 {
                                cell.extend_from_slice(&value);
                            }
                            cell
                        })
                        .collect(),
                )
            }
            Self::Array(inner) => {
                let mut ends = Vec::with_capacity(rows);
                for _ in 0..rows {
                    ends.push(cursor.u64()? as usize);
                }
                let total = ends.last().copied().unwrap_or(0);
                if ends.windows(2).any(|pair| pair[0] > pair[1]) {
                    return cursor.fail("array offsets are not monotonic")
                }
                let values = inner.read_column(cursor, total)?;
                let mut cells = Vec::with_capacity(rows);
                let mut start = 0;
                for end in ends {
                    let mut cell = Vec::new();
                    put_uvarint(&mut cell, (end - start) as u64);
                    for value in &values[start..end] {
                        cell.extend_from_slice(value);
                    }
                    cells.push(cell);
                    start = end;
                }
                Some(cells)
            }
            Self::Tuple(fields) => {
                let mut cells = vec![Vec::new(); rows];
                for field in fields {
                    for (cell, value) in cells.iter_mut().zip(field.read_column(cursor, rows)?) {
                        cell.extend_from_slice(&value);
                    }
                }
                Some(cells)
            }
            Self::LowCardinality(inner) => Self::read_dictionary(cursor, inner, rows),
        }
    }

    /// Reads a low cardinality column: the dictionary, then one key per row.
    fn read_dictionary(cursor: &mut Cursor<'_>, inner: &Self, rows: usize) -> Option<Vec<Vec<u8>>> {
        if rows == 0 {
            return Some(Vec::new())
        }
        if cursor.u64()? != LOW_CARDINALITY_VERSION {
            return cursor.fail("unknown low cardinality version")
        }
        let flags = cursor.u64()?;
        if flags & NEEDS_GLOBAL_DICTIONARY != 0 {
            return cursor.fail("low cardinality global dictionaries are not supported")
        }
        let key_width = match flags & 0xff {
            0 => 1,
            1 => 2,
            2 => 4,
            3 => 8,
            _ => return cursor.fail("unknown low cardinality key width"),
        };
        let dictionary_len = cursor.u64()? as usize;
        let (leaf, nullable) = inner.dictionary();
        let dictionary = leaf.read_column(cursor, dictionary_len)?;
        if cursor.u64()? as usize != rows {
            return cursor.fail("low cardinality key count does not match the block")
        }
        let mut cells = Vec::with_capacity(rows);
        for _ in 0..rows {
            let mut key = 0;
            for (shift, byte) in cursor.bytes(key_width)?.iter().enumerate() {
                key |= usize::from(*byte) << (8 * shift);
            }
            let Some(value) = dictionary.get(key) else {
                return cursor.fail("low cardinality key outside the dictionary")
            };
            cells.push(match (nullable, key) {
                (true, 0) => vec![1],
                (true, _) => [&[0][..], value].concat(),
                (false, _) => value.clone(),
            });
        }
        Some(cells)
    }

    /// Strips the `RowBinary` framing a caller does not want to see: the null
    /// flag becomes `None`, and a string loses its length prefix.
    fn cell(&self, mut value: Vec<u8>) -> Option<Vec<u8>> {
        match self {
            Self::Nullable(inner) => {
                let is_null = value.remove(0) == 1;
                if is_null { None } else { inner.cell(value) }
            }
            Self::LowCardinality(inner) => inner.cell(value),
            Self::Str => {
                let prefix = value.iter().position(|byte| byte & 0x80 == 0)? + 1;
                Some(value.split_off(prefix))
            }
            _ => Some(value),
        }
    }
}

/// A block half-decoded when the bytes ran out: the columns done so far and
/// how many block bytes they spanned, so the next attempt resumes after them.
pub(crate) struct BlockResume {
    columns: Vec<Column>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    row_count: usize,
    column_count: usize,
    consumed: usize,
}

impl BlockResume {
    /// Reads the block header: info fields and the column and row counts.
    fn read_header(cursor: &mut Cursor<'_>) -> Option<Self> {
        let start = cursor.remaining();
        loop {
            match cursor.uvarint()? {
                0 => break,
                1 => cursor.bytes(1)?,
                2 => cursor.bytes(4)?,
                _ => return cursor.fail("unknown block info field"),
            };
        }
        let column_count = cursor.uvarint()? as usize;
        let row_count = cursor.uvarint()? as usize;
        Some(Self {
            columns: Vec::with_capacity(column_count),
            rows: vec![Vec::with_capacity(column_count); row_count],
            row_count,
            column_count,
            consumed: start - cursor.remaining(),
        })
    }
}

/// Column-major buffers mirroring one [`Type`]'s nesting.
enum ColumnBuf {
    Leaf(Vec<u8>),
    Nullable { nulls: Vec<u8>, values: Box<ColumnBuf> },
    Array { offsets: Vec<u8>, len: u64, values: Box<ColumnBuf> },
    Tuple(Vec<ColumnBuf>),
    Dictionary { dict: Vec<u8>, index: HashMap<Vec<u8>, u64>, keys: Vec<u64>, entries: u64 },
}

impl ColumnBuf {
    fn new(ty: &Type) -> Self {
        match ty {
            Type::Fixed(_) | Type::Str => Self::Leaf(Vec::new()),
            Type::Nullable(inner) => {
                Self::Nullable { nulls: Vec::new(), values: Box::new(Self::new(inner)) }
            }
            Type::Array(inner) => {
                Self::Array { offsets: Vec::new(), len: 0, values: Box::new(Self::new(inner)) }
            }
            Type::Tuple(fields) => Self::Tuple(fields.iter().map(Self::new).collect()),
            Type::LowCardinality(inner) => {
                // A nullable dictionary reserves index zero for the NULL a key
                // of zero stands for.
                let (leaf, nullable) = inner.dictionary();
                let mut dict = Self::Leaf(Vec::new());
                if nullable {
                    leaf.write_default(&mut dict);
                }
                let Self::Leaf(dict) = dict else { unreachable!("a leaf stays a leaf") };
                Self::Dictionary {
                    dict,
                    index: HashMap::new(),
                    keys: Vec::new(),
                    entries: u64::from(nullable),
                }
            }
        }
    }

    fn write(self, out: &mut Vec<u8>) {
        match self {
            Self::Leaf(values) => out.extend_from_slice(&values),
            Self::Nullable { nulls, values } => {
                out.extend_from_slice(&nulls);
                values.write(out);
            }
            Self::Array { offsets, values, .. } => {
                out.extend_from_slice(&offsets);
                values.write(out);
            }
            Self::Tuple(fields) => {
                for field in fields {
                    field.write(out);
                }
            }
            Self::Dictionary { dict, keys, entries, .. } => {
                if keys.is_empty() {
                    return;
                }
                let (key_type, key_width) = match entries {
                    0..=0xff => (0, 1),
                    0x100..=0xffff => (1, 2),
                    0x1_0000..=0xffff_ffff => (2, 4),
                    _ => (3, 8),
                };
                out.extend_from_slice(&LOW_CARDINALITY_VERSION.to_le_bytes());
                let flags = key_type | HAS_ADDITIONAL_KEYS | NEEDS_DICTIONARY_UPDATE;
                out.extend_from_slice(&flags.to_le_bytes());
                out.extend_from_slice(&entries.to_le_bytes());
                out.extend_from_slice(&dict);
                out.extend_from_slice(&(keys.len() as u64).to_le_bytes());
                for key in keys {
                    out.extend_from_slice(&key.to_le_bytes()[..key_width]);
                }
            }
        }
    }
}

impl Output {
    /// Appends an empty block, which ends external tables and insert data.
    pub(crate) fn write_empty(out: &mut Vec<u8>) {
        Self::write_info(out);
        put_uvarint(out, 0);
        put_uvarint(out, 0);
    }

    /// Appends the `RowBinary` `body` as a block laid out for `columns`, whose
    /// types the server sent in its header block.
    pub(crate) fn write_rowbinary(
        columns: &[Column],
        body: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        let types = columns
            .iter()
            .map(|column| Type::parse(&column.type_name))
            .collect::<Option<Vec<_>>>()
            .ok_or("insert into a column type this client cannot encode")?;
        let mut bufs: Vec<_> = types.iter().map(ColumnBuf::new).collect();
        let mut cursor = Cursor::new(body);
        let mut rows = 0;
        while cursor.remaining() > 0 {
            for (ty, buf) in types.iter().zip(&mut bufs) {
                if ty.transpose(&mut cursor, buf).is_none() {
                    return Err(cursor.invalid.unwrap_or("insert row ended mid-value"))
                }
            }
            rows += 1;
        }
        Self::write_info(out);
        put_uvarint(out, columns.len() as u64);
        put_uvarint(out, rows);
        for (column, buf) in columns.iter().zip(bufs) {
            put_string(out, column.name.as_bytes());
            put_string(out, column.type_name.as_bytes());
            buf.write(out);
        }
        Ok(())
    }

    /// Decodes a block, resuming after the columns a previous attempt finished
    /// instead of decoding them again: native packets carry no length, so a
    /// block split across segments would otherwise be re-decoded per segment.
    pub(crate) fn read_resume(
        cursor: &mut Cursor<'_>,
        resume: &mut Option<BlockResume>,
    ) -> Option<Self> {
        let state = match resume {
            Some(state) => {
                cursor.bytes(state.consumed)?;
                state
            }
            None => resume.insert(BlockResume::read_header(cursor)?),
        };
        while state.columns.len() < state.column_count {
            let before = cursor.remaining();
            let name = cursor.owned_string()?;
            let type_name = cursor.owned_string()?;
            let Some(ty) = Type::parse(&type_name) else {
                return cursor.fail("server sent a column type this client cannot decode")
            };
            for (row, value) in state.rows.iter_mut().zip(ty.read_column(cursor, state.row_count)?)
            {
                row.push(ty.cell(value));
            }
            state.columns.push(Column { name, type_name });
            state.consumed += before - cursor.remaining();
        }
        Some(Self {
            columns: std::mem::take(&mut state.columns),
            rows: std::mem::take(&mut state.rows),
        })
    }

    fn write_info(out: &mut Vec<u8>) {
        put_uvarint(out, 1);
        out.push(0);
        put_uvarint(out, 2);
        out.extend_from_slice(&(-1i32).to_le_bytes());
        put_uvarint(out, 0);
    }
}
