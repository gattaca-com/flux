use crate::ArrayStr;

/// Short type name capacity. Covers realistic tile/message names.
pub const SHORT_TYPENAME_CAP: usize = 32;

/// Shortened type name, suitable for logging/metrics/shm identifiers.
pub type ShortTypename = ArrayStr<SHORT_TYPENAME_CAP>;

/// Return the short, unqualified name of type `T`, removing all module paths.
/// Truncates if result exceeds 32 bytes.
///
/// Example:
///   `this::is::my::Tile<crate::inner::Spine>` → `"Tile<Spine>"`
#[inline]
pub fn short_typename<T>() -> ShortTypename {
    let s = std::any::type_name::<T>();
    let mut out = ShortTypename::new();
    let bytes = s.as_bytes();
    let mut seg_start = 0usize;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'<' | b',' | b'>' => {
                if seg_start < i {
                    let seg = &s[seg_start..i].trim();
                    if !seg.is_empty() {
                        let short = seg.rsplit("::").next().unwrap_or(seg);
                        out.push_str_truncate(short);
                    }
                }
                if !out.is_full() {
                    out.push_byte(b);
                }
                if b == b',' && !out.is_full() {
                    out.push_byte(b' ');
                }
                seg_start = i + 1;
            }
            _ => {}
        }
    }

    if seg_start < s.len() {
        let seg = &s[seg_start..].trim();
        if !seg.is_empty() {
            let short = seg.rsplit("::").next().unwrap_or(seg);
            out.push_str_truncate(short);
        }
    }

    out
}
