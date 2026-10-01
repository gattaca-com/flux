//! Unary echo plus a ticking subscription on one caller-driven loop.
//!
//! - `/example.Echo/Echo` returns its request.
//! - `/example.Echo/Subscribe` streams a counter every second. The request is a
//!   byte count of ticks to send; the server then ends the stream with `OK`. A
//!   request of 0 is refused once the stream is open, to show ending a stream
//!   with an error, as a revoked subscription would.
//!
//! Messages are opaque protobuf bytes here; with Prost, `M::decode(message)`
//! the request and `response.encode(reply)` the reply.
//!
//! This network serves only gRPC, so `poll` does everything. When the network
//! also carries other groups, keep your own `net.poll_with`, pass each event to
//! `server.on_event`, and call `server.drive(&mut net)` once per iteration.
//!
//! Run: `cargo run -p flux-network --example grpc_server -- 127.0.0.1:50051`
use std::time::{Duration, Instant};

use flux_network::{
    Network,
    grpc::{Code, GrpcConfig, GrpcServer, Response, Status, Stream},
};

struct Subscriber {
    stream: Stream,
    remaining: u8,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:50051".into()).parse()?;
    let mut net = Network::default();
    let mut server = GrpcServer::new(&mut net, GrpcConfig::default())?;
    server.listen(&mut net, addr)?;
    println!("{addr}");

    let mut subscribers: Vec<Subscriber> = Vec::new();
    let mut next_tick = Instant::now();
    let mut count = 0u64;
    let mut tick = Vec::new();
    loop {
        server.poll(&mut net, |request, reply| match request.path() {
            b"/example.Echo/Echo" => {
                reply.extend_from_slice(request.message());
                Response::Message
            }
            b"/example.Echo/Subscribe" => {
                let remaining = request.message().first().copied().unwrap_or(10);
                subscribers.push(Subscriber { stream: request.stream(), remaining });
                Response::Stream
            }
            _ => Status::new(Code::Unimplemented, "unknown method").into(),
        });
        if Instant::now() < next_tick {
            std::hint::spin_loop();
            continue;
        }
        next_tick += Duration::from_secs(1);
        count += 1;
        // Field 1, varint: encoded once into a reused buffer for everyone.
        tick.clear();
        tick.push(0x08);
        varint(count, &mut tick);
        // Order doesn't matter, so ended streams are swap-removed.
        let mut i = 0;
        while i < subscribers.len() {
            let subscriber = &mut subscribers[i];
            let ended = if subscriber.remaining == 0 {
                // Ending with an error, e.g. when access is revoked.
                let denied = Status::new(Code::PermissionDenied, "subscription revoked");
                let _ = server.finish(subscriber.stream, &denied);
                true
            } else if server.send(subscriber.stream, &tick).is_err() {
                // The client left or lagged: just forget the stream.
                true
            } else {
                subscriber.remaining -= 1;
                if subscriber.remaining == 0 {
                    // Ending normally after the last message.
                    let _ = server.finish(subscriber.stream, &Status::ok());
                    true
                } else {
                    false
                }
            };
            if ended {
                subscribers.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}
