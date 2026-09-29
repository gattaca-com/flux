use flux_network::http2::{
    DEFAULT_MAX_FRAME_SIZE, Decoder, Error, Payload, Priority, encode_frame, flags, kind,
};

#[test]
fn decodes_fragmented_wire_frames_without_copying_payloads() {
    let frames: &[(&[u8], Payload<'_>)] = &[
        (
            &[0, 0, 9, 1, 0x28, 0x80, 0, 0, 1, 1, 0x80, 0, 0, 0, 255, 0x82, 0x84, 0],
            Payload::Headers {
                fragment: &[0x82, 0x84],
                end_headers: false,
                end_stream: false,
                priority: Some(Priority { dependency: 0, exclusive: true, weight: 256 }),
            },
        ),
        (&[0, 0, 1, 9, 4, 0, 0, 0, 1, 0x86], Payload::Continuation {
            fragment: &[0x86],
            end_headers: true,
        }),
        (&[0, 0, 7, 0, 9, 0, 0, 0, 1, 3, b'a', b'b', b'c', 0, 0, 0], Payload::Data {
            data: b"abc",
            end_stream: true,
            flow_controlled_len: 7,
        }),
        (
            &[0, 0, 5, 2, 0xff, 0, 0, 0, 1, 0, 0, 0, 0, 0],
            Payload::Priority(Priority { dependency: 0, exclusive: false, weight: 1 }),
        ),
        (&[0, 0, 4, 3, 0xff, 0, 0, 0, 1, 0, 0, 0, 8], Payload::Reset { error_code: 8 }),
        (&[0, 0, 5, 5, 4, 0, 0, 0, 1, 0x80, 0, 0, 2, 0x82], Payload::PushPromise {
            promised_stream_id: 2,
            fragment: &[0x82],
            end_headers: true,
        }),
        (&[0, 0, 8, 6, 0xff, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8], Payload::Ping {
            ack: true,
            opaque: [1, 2, 3, 4, 5, 6, 7, 8],
        }),
        (&[0, 0, 9, 7, 0xff, 0, 0, 0, 0, 0x80, 0, 0, 1, 0, 0, 0, 0, b'x'], Payload::GoAway {
            last_stream_id: 1,
            error_code: 0,
            debug_data: b"x",
        }),
        (&[0, 0, 4, 8, 0xff, 0, 0, 0, 1, 0x80, 0, 0, 1], Payload::WindowUpdate { increment: 1 }),
        (&[0, 0, 1, 0xff, 0xff, 0, 0, 0, 1, b'x'], Payload::Unknown {
            kind: 0xff,
            flags: 0xff,
            data: b"x",
        }),
    ];
    let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 64).unwrap();
    for &(wire, expected) in frames {
        for end in 0..wire.len() {
            assert_eq!(decoder.decode(&wire[..end]), Ok(None));
        }
        let (frame, consumed) = decoder.decode(wire).unwrap().unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(frame.stream_id, u32::from(wire[8]));
        assert_eq!(frame.payload, expected);
        if let Payload::Data { data, .. } = frame.payload {
            assert_eq!(data.as_ptr(), wire[10..].as_ptr());
        }
    }

    let wire = [
        0, 0, 18, 4, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0, 0, 0, 4, 1, 0, 0, 0, 0,
    ];
    let (frame, used) = decoder.decode(&wire).unwrap().unwrap();
    let Payload::Settings { ack, settings } = frame.payload else { panic!("expected settings") };
    assert!(!ack);
    assert_eq!(settings.iter().collect::<Vec<_>>(), [(2, 0), (2, 1), (0xffff, u32::MAX)]);
    let (frame, used_ack) = decoder.decode(&wire[used..]).unwrap().unwrap();
    assert!(matches!(frame.payload, Payload::Settings { ack: true, .. }));
    assert_eq!(used + used_ack, wire.len());
}

#[test]
fn rejects_malformed_frames_and_header_sequences() {
    let malformed: &[(&[u8], Error)] = &[
        (&[0, 0x40, 1, 0, 0, 0, 0, 0, 1], Error::FrameSize),
        (&[0, 0, 0, 0, 0, 0, 0, 0, 0], Error::Protocol),
        (&[0, 0, 0, 4, 0, 0, 0, 0, 1], Error::Protocol),
        (&[0, 0, 7, 6, 0, 0, 0, 0, 0], Error::FrameSize),
        (&[0, 0, 6, 4, 1, 0, 0, 0, 0], Error::FrameSize),
        (&[0, 0, 1, 4, 0, 0, 0, 0, 0], Error::FrameSize),
        (&[0, 0, 0, 0, 8, 0, 0, 0, 1], Error::FrameSize),
        (&[0, 0, 1, 0, 8, 0, 0, 0, 1, 1], Error::Protocol),
        (&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 0, 0], Error::Protocol),
        (&[0, 0, 5, 2, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0], Error::Protocol),
        (&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2], Error::Protocol),
        (&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 4, 0x80, 0, 0, 0], Error::FlowControl),
        (&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0], Error::Protocol),
        (&[0, 0, 0, 9, 4, 0, 0, 0, 1], Error::Protocol),
    ];
    for &(wire, expected) in malformed {
        let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 64).unwrap();
        assert_eq!(decoder.decode(wire), Err(expected), "{wire:?}");
    }

    for interruption in [&[0, 0, 0, 9, 4, 0, 0, 0, 3][..], &[0, 0, 0, 0xff, 0, 0, 0, 0, 1][..]] {
        let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 64).unwrap();
        decoder.decode(&[0, 0, 1, 1, 0, 0, 0, 0, 1, 0x82]).unwrap().unwrap();
        assert_eq!(decoder.decode(interruption), Err(Error::Protocol));
    }

    for end_headers in [0, flags::END_HEADERS] {
        let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 1).unwrap();
        assert_eq!(
            decoder.decode(&[0, 0, 2, 1, end_headers, 0, 0, 0, 1, 0x82, 0x84]),
            Err(Error::LimitExceeded),
        );
    }
    let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 1).unwrap();
    decoder.decode(&[0, 0, 1, 1, 0, 0, 0, 0, 1, 0x82]).unwrap().unwrap();
    assert_eq!(decoder.decode(&[0, 0, 1, 9, 4, 0, 0, 0, 1, 0x84]), Err(Error::LimitExceeded),);
}

#[test]
fn encodes_wire_bytes_and_preserves_output_on_error() {
    let mut output = Vec::new();
    encode_frame(kind::DATA, flags::END_STREAM, 1, b"abc", DEFAULT_MAX_FRAME_SIZE, &mut output)
        .unwrap();
    assert_eq!(output, [0, 0, 3, 0, 1, 0, 0, 0, 1, b'a', b'b', b'c']);
    let saved = output.clone();
    for (kind, stream, payload, max) in [
        (kind::DATA, 1, &b"abc"[..], 0),
        (kind::DATA, 0x8000_0001, &b"abc"[..], DEFAULT_MAX_FRAME_SIZE),
        (kind::PING, 0, &b"abc"[..], DEFAULT_MAX_FRAME_SIZE),
    ] {
        assert!(encode_frame(kind, 0, stream, payload, max, &mut output).is_err());
        assert_eq!(output, saved);
    }
    assert!(Decoder::new(0, 1).is_err());
    assert!(Decoder::new(0x0100_0000, 1).is_err());
}
