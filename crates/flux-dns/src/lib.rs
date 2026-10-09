//! Non-blocking IPv4 lookups, so a tile never blocks on DNS. IP literals and
//! `/etc/hosts` names are answered locally; other names get one `A` query over
//! UDP to one nameserver, without retries, search domains, IPv6 records, or
//! answers over 512 bytes.

use std::{
    collections::hash_map::RandomState,
    fs,
    hash::BuildHasher,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
};

use flux_timing::{Duration, Instant};
use flux_utils::ArrayVec;

const TIMEOUT_SECS: u64 = 2;
const MAX_MESSAGE: usize = 512;
/// Longest host name: 253 characters encode to 255 bytes.
const MAX_HOST: usize = 253;
/// The encoded name (the first label's length byte, the host, the closing root
/// label), then type and class.
const MAX_QUESTION: usize = 1 + MAX_HOST + 1 + 4;
/// More `A` records than a 512-byte message can hold.
const MAX_ADDRS: usize = 32;

pub struct Resolver {
    socket: UdpSocket,
    hosts: Vec<(String, Ipv4Addr)>,
    /// Answered without a query, for the next poll.
    ready: Vec<(usize, Ipv4Addr)>,
    pending: Vec<Pending>,
    rng: u64,
    timeout: Duration,
    buf: [u8; MAX_MESSAGE],
    addrs: ArrayVec<Ipv4Addr, MAX_ADDRS>,
}

struct Pending {
    key: usize,
    id: u16,
    sent: Instant,
    /// The question as sent, which the answer must repeat.
    question: ArrayVec<u8, MAX_QUESTION>,
}

impl Resolver {
    /// Answers from `/etc/hosts`, else queries the first `nameserver` in
    /// `/etc/resolv.conf`, or `127.0.0.1` as glibc does without one.
    #[cfg(target_os = "linux")]
    pub fn system() -> io::Result<Self> {
        let server = fs::read_to_string("/etc/resolv.conf")
            .ok()
            .and_then(|conf| {
                conf.lines()
                    .find_map(|line| line.strip_prefix("nameserver")?.trim().parse::<IpAddr>().ok())
            })
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let mut resolver = Self::new(SocketAddr::new(server, 53))?;
        resolver.hosts =
            fs::read_to_string("/etc/hosts").map(|hosts| parse_hosts(&hosts)).unwrap_or_default();
        Ok(resolver)
    }

    pub fn new(server: SocketAddr) -> io::Result<Self> {
        let local = match server {
            SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
        };
        let socket = UdpSocket::bind(local)?;
        // Connected, so only the server's datagrams are read.
        socket.connect(server)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            hosts: Vec::new(),
            ready: Vec::new(),
            pending: Vec::new(),
            rng: RandomState::new().hash_one(0) | 1,
            timeout: Duration::from_secs(TIMEOUT_SECS),
            buf: [0; MAX_MESSAGE],
            addrs: ArrayVec::new(),
        })
    }

    /// Asks for `host`'s IPv4 addresses, which [`Self::poll`] reports under
    /// `key`. A query for a `key` that is still pending replaces it.
    pub fn query(&mut self, key: usize, host: &str) -> io::Result<()> {
        self.pending.retain(|pending| pending.key != key);
        self.ready.retain(|(ready, _)| *ready != key);
        let host = host.trim_end_matches('.');
        let local = host.parse().ok().or_else(|| {
            self.hosts.iter().find(|(name, _)| name.eq_ignore_ascii_case(host)).map(|(_, ip)| *ip)
        });
        if let Some(ip) = local {
            self.ready.push((key, ip));
            return Ok(());
        }
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let id = self.rng as u16;
        let invalid =
            || io::Error::new(io::ErrorKind::InvalidInput, format!("invalid host name {host}"));
        if host.len() > MAX_HOST {
            return Err(invalid());
        }
        let mut question = ArrayVec::<u8, MAX_QUESTION>::new();
        for label in host.split('.') {
            let len = u8::try_from(label.len())
                .ok()
                .filter(|len| (1..64).contains(len))
                .ok_or_else(invalid)?;
            question.push(len);
            question.extend(label.bytes());
        }
        // Root label, type A, class IN.
        question.extend([0, 0, 1, 0, 1]);
        let mut query = ArrayVec::<u8, { 12 + MAX_QUESTION }>::new();
        query.extend(id.to_be_bytes());
        // Recursion desired, one question.
        query.extend([0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
        query.extend(question.iter().copied());
        self.socket.send(query.as_slice())?;
        self.pending.push(Pending { key, id, sent: Instant::now(), question });
        Ok(())
    }

    /// Calls `f` once per finished query with its key and addresses, which are
    /// empty when the host has none, the lookup failed, or it timed out.
    pub fn poll(&mut self, mut f: impl FnMut(usize, &[Ipv4Addr])) {
        for (key, ip) in self.ready.drain(..) {
            f(key, &[ip]);
        }
        if self.pending.is_empty() {
            return;
        }
        while let Ok(len) = self.socket.recv(&mut self.buf) {
            let msg = &self.buf[..len];
            let Some(id) = msg.get(..2).map(|id| u16::from_be_bytes([id[0], id[1]])) else {
                continue
            };
            let Some(index) = self.pending.iter().position(|pending| {
                pending.id == id &&
                    msg.get(12..12 + pending.question.len()).is_some_and(|question| {
                        question.eq_ignore_ascii_case(pending.question.as_slice())
                    })
            }) else {
                continue
            };
            let key = self.pending.swap_remove(index).key;
            self.addrs.clear();
            parse_a(msg, &mut self.addrs);
            f(key, self.addrs.as_slice());
        }
        let now = Instant::now();
        self.pending.retain(|pending| {
            let waiting = now.saturating_sub(pending.sent) < self.timeout;
            if !waiting {
                f(pending.key, &[]);
            }
            waiting
        });
    }
}

fn parse_a(msg: &[u8], addrs: &mut ArrayVec<Ipv4Addr, MAX_ADDRS>) {
    let u16_at =
        |at: usize| msg.get(at..at + 2).map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]));
    let (Some(flags), Some(questions), Some(answers)) = (u16_at(2), u16_at(4), u16_at(6)) else {
        return
    };
    // A response, not truncated, response code 0.
    if flags & 0x8000 == 0 || flags & 0x020F != 0 {
        return;
    }
    let mut at = 12;
    for _ in 0..questions {
        let Some(end) = skip_name(msg, at) else { return };
        at = end + 4;
    }
    for _ in 0..answers {
        let Some(end) = skip_name(msg, at) else { return };
        let (Some(kind), Some(class), Some(len)) = (u16_at(end), u16_at(end + 2), u16_at(end + 8))
        else {
            return
        };
        let data = end + 10;
        let Some(rdata) = msg.get(data..data + usize::from(len)) else { return };
        if kind == 1 &&
            class == 1 &&
            let Ok(ip) = <[u8; 4]>::try_from(rdata) &&
            addrs.try_push(Ipv4Addr::from(ip)).is_some()
        {
            return;
        }
        at = data + usize::from(len);
    }
}

fn skip_name(msg: &[u8], mut at: usize) -> Option<usize> {
    loop {
        match *msg.get(at)? {
            0 => return Some(at + 1),
            len if len & 0xC0 == 0xC0 => return Some(at + 2),
            len => at += 1 + usize::from(len),
        }
    }
}

/// The first address each name has, as glibc uses it.
fn parse_hosts(file: &str) -> Vec<(String, Ipv4Addr)> {
    let mut hosts: Vec<(String, Ipv4Addr)> = Vec::new();
    for line in file.lines() {
        let mut fields = line.split('#').next().unwrap_or_default().split_whitespace();
        let Some(Ok(ip)) = fields.next().map(str::parse::<Ipv4Addr>) else { continue };
        for name in fields {
            if !hosts.iter().any(|(known, _)| known.eq_ignore_ascii_case(name)) {
                hosts.push((name.to_owned(), ip));
            }
        }
    }
    hosts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_records_behind_a_cname() {
        let mut msg = vec![0x12, 0x34, 0x81, 0x80, 0, 1, 0, 2, 0, 0, 0, 0];
        // Question: www.github.com A IN.
        msg.extend_from_slice(b"\x03www\x06github\x03com\x00\x00\x01\x00\x01");
        // www.github.com CNAME github.com, both names compressed.
        msg.extend_from_slice(&[0xC0, 12, 0, 5, 0, 1, 0, 0, 0, 60, 0, 2, 0xC0, 16]);
        // github.com A 140.82.121.4.
        msg.extend_from_slice(&[0xC0, 16, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 140, 82, 121, 4]);
        let mut addrs = ArrayVec::new();
        parse_a(&msg, &mut addrs);
        assert_eq!(addrs.as_slice(), [Ipv4Addr::new(140, 82, 121, 4)]);
    }
}
