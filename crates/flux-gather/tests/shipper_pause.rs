use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_gather::{BlobCache, BlobShipper};
use flux_network::{NetworkDriver, PollEvent};
use flux_timing::{InternalMessage, TrackingTimestamp};
use flux_utils::ArrayStr;
use flux_versioned_types::versioned_struct;
use type_hash_derive::type_hash_lock;

versioned_struct!(Ping =>
    #[type_hash_lock(hash = 8348789466763183282)]
    PingV1 { pub n: u64 }
);

versioned_struct!(Meta =>
    #[type_hash_lock(hash = 11940900062794036198)]
    MetaV1 { pub slot: u64, pub instance: ArrayStr<16> }
);

fn blob_bytes(n: u64) -> Vec<u8> {
    let mut cache = BlobCache::new();
    cache.push(&InternalMessage::new(TrackingTimestamp::new_without_tile(), Ping { n }));
    let meta = Meta { slot: n, instance: ArrayStr::from_str_truncate("t") };
    let mut out = Vec::new();
    cache.flush(&meta, 1, |blob| out = blob.as_bytes().to_vec());
    out
}

fn pump(peer: &mut NetworkDriver, got: &mut Vec<Vec<u8>>, for_how_long: Duration) {
    let deadline = std::time::Instant::now() + for_how_long;
    while std::time::Instant::now() < deadline {
        peer.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                got.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
}

/// A fresh dial is reported to the caller the same way a reconnect is, and a
/// paused endpoint sits out `ship` while still taking `ship_to`.
#[test]
fn a_paused_endpoint_takes_only_what_is_addressed_to_it() {
    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24741));
    let mut peer = NetworkDriver::default();
    peer.listen_at(addr).expect("couldn't listen");

    // The first drive dials; the caller learns the token through the event
    // rather than by asking, since a reconnect arrives the same way.
    let mut shipper = BlobShipper::new(vec![addr]);
    let mut connected = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while connected.is_none() && std::time::Instant::now() < deadline {
        shipper.drive_with(|event| {
            if let PollEvent::Reconnect { token } = event {
                connected = Some(token);
            }
        });
        peer.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    let token = connected.expect("dial never reported");
    assert_eq!(shipper.endpoint_of(token), Some(0));

    let first = blob_bytes(1);
    shipper.ship(flux_gather::Blob::from_bytes(&first).unwrap());
    let mut got = Vec::new();
    for _ in 0..50 {
        shipper.drive();
        pump(&mut peer, &mut got, Duration::from_millis(5));
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got, vec![first]);

    // Paused: the broadcast passes it by, the addressed frame does not.
    shipper.pause_broadcast(token);
    assert!(shipper.is_broadcast_paused(token));
    let dropped = blob_bytes(2);
    let addressed = blob_bytes(3);
    shipper.ship(flux_gather::Blob::from_bytes(&dropped).unwrap());
    shipper.ship_to(token, flux_gather::Blob::from_bytes(&addressed).unwrap());
    got.clear();
    for _ in 0..50 {
        shipper.drive();
        pump(&mut peer, &mut got, Duration::from_millis(5));
        if !got.is_empty() {
            break;
        }
    }
    pump(&mut peer, &mut got, Duration::from_millis(50));
    assert_eq!(got, vec![addressed]);

    // Resumed: back in the broadcast.
    shipper.resume_broadcast(token);
    assert!(!shipper.is_broadcast_paused(token));
    let after = blob_bytes(4);
    shipper.ship(flux_gather::Blob::from_bytes(&after).unwrap());
    got.clear();
    for _ in 0..50 {
        shipper.drive();
        pump(&mut peer, &mut got, Duration::from_millis(5));
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got, vec![after]);
}
