//! Chunked request bodies. `Err` is the status refusing the request.

use super::{STACK_HEADERS, crlf_only, header_slots, transfer_chunked};

/// Decodes the chunked body of `req` at the start of `bytes` into `body` once
/// it is all in, returning where it ends.
pub(super) fn decode(
    req: &httparse::Request<'_, '_>,
    bytes: &[u8],
    max_body_bytes: usize,
    max_headers: usize,
    scan: &mut (usize, usize),
    body: &mut Vec<u8>,
) -> Result<Option<usize>, u16> {
    if transfer_chunked(req.headers) != Some(true) {
        return Err(501)
    }
    // HTTP/1.0 has no transfer codings, and a length beside one is ambiguous.
    if req.version == Some(0) ||
        req.headers.iter().any(|h| h.name.eq_ignore_ascii_case("content-length"))
    {
        return Err(400)
    }
    let Some((end, body_len)) = end(bytes, max_body_bytes, max_headers, scan)? else {
        return Ok(None)
    };
    copy(bytes, body_len, body);
    Ok(Some(end))
}

/// Copies the data of a chunked body that a scan accepted into `body`.
pub(super) fn copy(bytes: &[u8], body_len: usize, body: &mut Vec<u8>) {
    body.clear();
    body.reserve(body_len);
    let mut at = 0;
    while let Ok(httparse::Status::Complete((consumed, size))) =
        httparse::parse_chunk_size(&bytes[at..])
    {
        let size = size as usize;
        if size == 0 {
            break
        }
        at += consumed;
        body.extend_from_slice(&bytes[at..at + size]);
        at += size + 2;
    }
}

/// Where a chunked body ends and its length, once it is all in. `scan` is
/// where the last call stopped, moved past each whole chunk.
pub(super) fn end(
    bytes: &[u8],
    max_body_bytes: usize,
    max_headers: usize,
    scan: &mut (usize, usize),
) -> Result<Option<(usize, usize)>, u16> {
    let (mut at, mut body_len) = *scan;
    loop {
        let (consumed, size) = match httparse::parse_chunk_size(&bytes[at..]) {
            // A bare LF in the size line, extensions included, is refused.
            Ok(httparse::Status::Complete((consumed, _)))
                if !crlf_only(&bytes[at..at + consumed]) =>
            {
                return Err(400)
            }
            Ok(httparse::Status::Complete((consumed, size))) => {
                (consumed, usize::try_from(size).map_err(|_| 413u16)?)
            }
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(_) => return Err(400),
        };
        if size > max_body_bytes.saturating_sub(body_len) {
            return Err(413)
        }
        at += consumed;
        if size == 0 {
            let (mut stack_slots, mut heap_slots) =
                ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
            let headers = header_slots(&mut stack_slots, &mut heap_slots, max_headers);
            let httparse::Status::Complete((consumed, _)) =
                httparse::parse_headers(&bytes[at..], headers).map_err(|_| 400u16)?
            else {
                return Ok(None)
            };
            if !crlf_only(&bytes[at..at + consumed]) {
                return Err(400)
            }
            return Ok(Some((at + consumed, body_len)))
        }
        let Some(chunk_end) = at.checked_add(size).and_then(|end| end.checked_add(2)) else {
            return Err(413)
        };
        let crlf = bytes.get(at + size..chunk_end.min(bytes.len())).unwrap_or_default();
        if crlf != &b"\r\n"[..crlf.len()] {
            return Err(400)
        }
        if bytes.len() < chunk_end {
            return Ok(None)
        }
        body_len += size;
        at = chunk_end;
        *scan = (at, body_len);
    }
}
