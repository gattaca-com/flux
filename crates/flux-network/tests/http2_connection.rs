use flux_network::http2::{
    CLIENT_PREFACE, DEFAULT_MAX_FRAME_SIZE, Decoder, Error, Event, Http2Config, Payload, SendError,
    ServerConnection, encode_frame, flags, kind,
};
use http::{HeaderMap, StatusCode};

// RFC 7541 C.4.1: GET http://www.example.com/, with Huffman-coded authority.
const REQUEST: &[u8] = &[
    0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4,
    0xff,
];

fn frame(kind: u8, flags: u8, id: u32, body: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    encode_frame(kind, flags, id, body, DEFAULT_MAX_FRAME_SIZE, &mut wire).unwrap();
    wire
}

fn connect(config: Http2Config, settings: &[u8]) -> ServerConnection {
    let mut server = ServerConnection::new(config).unwrap();
    let mut wire = CLIENT_PREFACE.to_vec();
    wire.extend(frame(kind::SETTINGS, 0, 0, settings));
    let mut buffered = Vec::new();
    for byte in wire {
        buffered.push(byte);
        let used =
            server.receive(&buffered, flux_timing::IngestionTime::default(), |_| {}).unwrap();
        buffered.drain(..used);
    }
    assert!(buffered.is_empty());
    server.consume_output(server.output().len());
    server
}

fn take_frames(server: &mut ServerConnection, mut inspect: impl FnMut(u32, Payload<'_>)) {
    let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 64 * 1024).unwrap();
    let mut input = server.output();
    while let Some((frame, used)) = decoder.decode(input).unwrap() {
        inspect(frame.stream_id, frame.payload);
        input = &input[used..];
    }
    assert!(input.is_empty());
    server.consume_output(server.output().len());
}

#[test]
fn exchanges_headers_data_and_trailers_with_backpressure() {
    let mut server = connect(Http2Config::default(), &[0, 4, 0, 0, 0, 3]);
    let mut wire = frame(kind::HEADERS, flags::END_STREAM, 1, &REQUEST[..8]);
    wire.extend(frame(kind::CONTINUATION, flags::END_HEADERS, 1, &REQUEST[8..]));
    let mut requests = 0;
    server
        .receive(&wire, flux_timing::IngestionTime::default(), |event| {
            if let Event::Request { stream_id, head, end_stream, .. } = event {
                assert_eq!(stream_id, 1);
                assert_eq!(head.method, "GET");
                assert_eq!(head.authority.unwrap(), b"www.example.com");
                assert!(end_stream);
                requests += 1;
            }
        })
        .unwrap();
    assert_eq!(requests, 1);
    server.send_headers(1, StatusCode::OK, &HeaderMap::new(), false).unwrap();
    assert_eq!(server.send_data(1, b"abcd", true), Ok(3));
    assert_eq!(server.send_data(1, b"d", true), Err(SendError::WouldBlock));
    server
        .receive(
            &frame(kind::WINDOW_UPDATE, 0, 1, &1u32.to_be_bytes()),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    assert_eq!(server.send_data(1, b"d", true), Ok(1));
    let mut body = Vec::new();
    let mut ends = Vec::new();
    take_frames(&mut server, |_, payload| match payload {
        Payload::Headers { fragment, .. } => assert_eq!(fragment, [0x88]),
        Payload::Data { data, end_stream, .. } => {
            body.extend_from_slice(data);
            ends.push(end_stream);
        }
        _ => panic!("unexpected response frame"),
    });
    assert_eq!(body, b"abcd");
    assert_eq!(ends, [false, true]);

    // POST, reusing the authority from the previous stream's dynamic table.
    let headers = [0x83, 0x86, 0x84, 0xbe, 0x0f, 0x0d, 1, b'4'];
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 3, &headers),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    let mut body = Vec::new();
    server
        .receive(
            &frame(kind::DATA, flags::PADDED, 3, &[2, b'a', b'b', b'c', b'd', 0, 0]),
            flux_timing::IngestionTime::default(),
            |event| {
                if let Event::Data { data, .. } = event {
                    body.extend_from_slice(data);
                }
            },
        )
        .unwrap();
    assert_eq!(body, b"abcd");
    let mut credits = Vec::new();
    take_frames(&mut server, |id, payload| {
        if let Payload::WindowUpdate { increment } = payload {
            credits.push((id, increment));
        }
    });
    // Connection credit waits until half the window is owed.
    assert_eq!(credits, [(3, 3)]);
    assert_eq!(server.release_capacity(3, 5), Err(SendError::InvalidState));
    server.release_capacity(3, 4).unwrap();
    let mut trailers = false;
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS | flags::END_STREAM, 3, &[
                0, 5, b'x', b'-', b'e', b'n', b'd', 2, b'o', b'k',
            ]),
            flux_timing::IngestionTime::default(),
            |event| {
                if let Event::Trailers { fields, .. } = event {
                    assert_eq!(fields.get("x-end"), Some(&b"ok"[..]));
                    trailers = true;
                }
            },
        )
        .unwrap();
    assert!(trailers);
    server.send_headers(3, StatusCode::OK, &HeaderMap::new(), false).unwrap();
    server.send_trailers(3, &HeaderMap::new()).unwrap();
}

#[test]
fn resets_release_credit_and_shutdown_preserves_the_goaway_boundary() {
    let config = Http2Config { max_concurrent_streams: 1, ..Http2Config::default() };
    let mut server = connect(config, &[]);
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 1, REQUEST),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    server
        .receive(&frame(kind::DATA, 0, 1, b"abc"), flux_timing::IngestionTime::default(), |_| {})
        .unwrap();
    server.reset(1, 8).unwrap();
    let mut frames = Vec::new();
    take_frames(&mut server, |id, payload| match payload {
        Payload::Reset { error_code } => frames.push((id, error_code)),
        Payload::WindowUpdate { increment } => frames.push((id, increment)),
        _ => panic!("unexpected reset output"),
    });
    assert_eq!(frames, [(1, 8)], "connection credit is batched");
    server
        .receive(&frame(kind::DATA, 0, 1, b"late"), flux_timing::IngestionTime::default(), |_| {
            panic!("reset stream delivered data")
        })
        .unwrap();
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS | flags::END_STREAM, 3, &[
                0x82, 0x86, 0x84, 0xbe,
            ]),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    server.go_away().unwrap();
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS | flags::END_STREAM, 5, &[
                0x82, 0x86, 0x84, 0xbe,
            ]),
            flux_timing::IngestionTime::default(),
            |_| panic!("accepted while draining"),
        )
        .unwrap();
    assert_eq!(
        server.receive(
            &frame(kind::DATA, 0, 7, b"idle"),
            flux_timing::IngestionTime::default(),
            |_| {}
        ),
        Err(Error::Protocol)
    );
    let mut boundaries = Vec::new();
    take_frames(&mut server, |_, payload| {
        if let Payload::GoAway { last_stream_id, .. } = payload {
            boundaries.push(last_stream_id);
        }
    });
    assert_eq!(boundaries, [3, 3]);
}

#[test]
fn rejects_invalid_compression_and_message_boundaries() {
    for (block, expected) in [
        (&[0x82, 0x20, 0x86, 0x84][..], Error::Compression),
        (&[0x3f, 0xe1, 0x3f, 0x82][..], Error::Compression),
        (&[0x82, 0x82, 0x86, 0x84][..], Error::Protocol),
        (&[0x82, 0x86, 0x04, 2, b'/', b'#'][..], Error::Protocol),
        (&[0x82, 0x86, 0x84, 0x0f, 0x0d, 1, b'1'][..], Error::Protocol),
    ] {
        let mut server = connect(Http2Config::default(), &[]);
        assert_eq!(
            server.receive(
                &frame(kind::HEADERS, flags::END_HEADERS | flags::END_STREAM, 1, block),
                flux_timing::IngestionTime::default(),
                |_| {}
            ),
            Err(expected)
        );
    }
    let mut server =
        connect(Http2Config { max_header_list_size: 64, ..Http2Config::default() }, &[]);
    assert_eq!(
        server.receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 1, REQUEST),
            flux_timing::IngestionTime::default(),
            |_| {}
        ),
        Err(Error::LimitExceeded)
    );

    let mut server =
        connect(Http2Config { max_header_list_size: 1_000_000, ..Http2Config::default() }, &[]);
    let mut oversized = REQUEST.to_vec();
    oversized.extend_from_slice(&[0x90; 256]);
    assert_eq!(
        server.receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 1, &oversized),
            flux_timing::IngestionTime::default(),
            |_| {}
        ),
        Err(Error::LimitExceeded)
    );

    // The protocol's default window: 64 KiB less one byte.
    let small = Http2Config { recv_window: 65_535, ..Http2Config::default() };
    let mut server = connect(small, &[]);
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 1, REQUEST),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    for _ in 0..3 {
        server
            .receive(
                &frame(kind::DATA, 0, 1, &vec![0; 16_384]),
                flux_timing::IngestionTime::default(),
                |_| {},
            )
            .unwrap();
    }
    assert_eq!(
        server.receive(
            &frame(kind::DATA, 0, 1, &vec![0; 16_384]),
            flux_timing::IngestionTime::default(),
            |_| {}
        ),
        Err(Error::FlowControl)
    );
}

#[test]
fn connection_credit_is_returned_once_half_the_window_is_owed() {
    let mut server = connect(Http2Config { recv_window: 65_535, ..Http2Config::default() }, &[]);
    server
        .receive(
            &frame(kind::HEADERS, flags::END_HEADERS, 1, REQUEST),
            flux_timing::IngestionTime::default(),
            |_| {},
        )
        .unwrap();
    let mut updates = Vec::new();
    for _ in 0..3 {
        server
            .receive(
                &frame(kind::DATA, 0, 1, &[0; 16_000]),
                flux_timing::IngestionTime::default(),
                |_| {},
            )
            .unwrap();
        server.release_capacity(1, 16_000).unwrap();
        take_frames(&mut server, |id, payload| {
            if let Payload::WindowUpdate { increment } = payload {
                updates.push((id, increment));
            }
        });
    }
    // The open stream gets its credit each time; the connection's arrives once
    // 32 KiB is owed, as one update.
    assert_eq!(updates, [(1, 16_000), (1, 16_000), (1, 16_000), (0, 48_000)]);
}

#[test]
fn requests_report_when_their_headers_frame_arrived() {
    let at = |t| flux_timing::IngestionTime::new(flux_timing::Nanos(t), flux_timing::Instant(t));
    let mut server = connect(Http2Config::default(), &[]);
    let mut received = Vec::new();
    let mut on_event = |event: Event<'_>| {
        if let Event::Request { received_at, .. } = event {
            received.push(received_at);
        }
    };
    let headers = frame(kind::HEADERS, flags::END_STREAM, 1, &REQUEST[..8]);
    server.receive(&headers, at(10), &mut on_event).unwrap();
    let continuation = frame(kind::CONTINUATION, flags::END_HEADERS, 1, &REQUEST[8..]);
    server.receive(&continuation, at(20), &mut on_event).unwrap();
    let single = frame(kind::HEADERS, flags::END_HEADERS | flags::END_STREAM, 3, REQUEST);
    server.receive(&single, at(30), &mut on_event).unwrap();
    assert_eq!(received, [at(10), at(30)]);
}
