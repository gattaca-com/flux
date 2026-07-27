use thiserror::Error;

const CONTROL_VERSION: u8 = 1;
const CONTROL_PREFIX_SIZE: usize = 2; // version + message

const SUBSCRIBE: u8 = 1;
const STATE: u8 = 2;
const REPAIR_REQUEST: u8 = 3;
const REPAIR_DATA: u8 = 4;
const UNAVAILABLE: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubscriberMessage {
    Subscribe { udp_port: u16 },
    Repair { session_id: u32, sequence: u64 },
}

impl SubscriberMessage {
    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Subscribe { udp_port } => {
                begin_message(output, SUBSCRIBE);
                output.extend_from_slice(&udp_port.to_le_bytes());
            }
            Self::Repair { session_id, sequence } => {
                begin_message(output, REPAIR_REQUEST);
                output.extend_from_slice(&session_id.to_le_bytes());
                output.extend_from_slice(&sequence.to_le_bytes());
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        let (kind, mut reader) = begin_decode(bytes)?;
        let message = match kind {
            SUBSCRIBE => Self::Subscribe { udp_port: reader.u16()? },
            REPAIR_REQUEST => Self::Repair { session_id: reader.u32()?, sequence: reader.u64()? },
            other => return Err(ControlError::UnsupportedMessage(other)),
        };
        finish_decode(&reader)?;
        validate_subscriber(&message)?;
        Ok(message)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublisherMessage<'a> {
    State { session_id: u32, next_sequence: u64 },
    RepairData { session_id: u32, sequence: u64, payload: &'a [u8] },
    Unavailable { session_id: u32, sequence: u64 },
}

impl<'a> PublisherMessage<'a> {
    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::State { session_id, next_sequence } => {
                begin_message(output, STATE);
                output.extend_from_slice(&session_id.to_le_bytes());
                output.extend_from_slice(&next_sequence.to_le_bytes());
            }
            Self::RepairData { session_id, sequence, payload } => {
                begin_message(output, REPAIR_DATA);
                output.extend_from_slice(&session_id.to_le_bytes());
                output.extend_from_slice(&sequence.to_le_bytes());
                output.extend_from_slice(payload);
            }
            Self::Unavailable { session_id, sequence } => {
                begin_message(output, UNAVAILABLE);
                output.extend_from_slice(&session_id.to_le_bytes());
                output.extend_from_slice(&sequence.to_le_bytes());
            }
        }
    }

    pub(crate) fn decode(bytes: &'a [u8]) -> Result<Self, ControlError> {
        let (kind, mut reader) = begin_decode(bytes)?;
        let message = match kind {
            STATE => Self::State { session_id: reader.u32()?, next_sequence: reader.u64()? },
            REPAIR_DATA => {
                let session_id = reader.u32()?;
                let sequence = reader.u64()?;
                let payload = reader.remaining();
                Self::RepairData { session_id, sequence, payload }
            }
            UNAVAILABLE => Self::Unavailable { session_id: reader.u32()?, sequence: reader.u64()? },
            other => return Err(ControlError::UnsupportedMessage(other)),
        };
        finish_decode(&reader)?;
        Ok(message)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum ControlError {
    #[error("truncated UDP control message")]
    Truncated,
    #[error("unsupported UDP control version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported UDP control message kind {0}")]
    UnsupportedMessage(u8),
    #[error("trailing bytes in UDP control message")]
    TrailingBytes,
    #[error("subscription UDP port must be nonzero")]
    InvalidUdpPort,
}

fn begin_message(output: &mut Vec<u8>, kind: u8) {
    output.clear();
    output.push(CONTROL_VERSION);
    output.push(kind);
}

fn begin_decode(bytes: &[u8]) -> Result<(u8, Reader<'_>), ControlError> {
    if bytes.len() < CONTROL_PREFIX_SIZE {
        return Err(ControlError::Truncated);
    }
    if bytes[0] != CONTROL_VERSION {
        return Err(ControlError::UnsupportedVersion(bytes[0]));
    }
    Ok((bytes[1], Reader::new(&bytes[CONTROL_PREFIX_SIZE..])))
}

fn finish_decode(reader: &Reader<'_>) -> Result<(), ControlError> {
    if reader.is_empty() { Ok(()) } else { Err(ControlError::TrailingBytes) }
}

fn validate_subscriber(message: &SubscriberMessage) -> Result<(), ControlError> {
    match message {
        SubscriberMessage::Subscribe { udp_port: 0 } => Err(ControlError::InvalidUdpPort),
        SubscriberMessage::Subscribe { .. } | SubscriberMessage::Repair { .. } => Ok(()),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ControlError> {
        let end = self.cursor.checked_add(length).ok_or(ControlError::Truncated)?;
        let bytes = self.bytes.get(self.cursor..end).ok_or(ControlError::Truncated)?;
        self.cursor = end;
        Ok(bytes)
    }

    fn u16(&mut self) -> Result<u16, ControlError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, ControlError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ControlError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn remaining(&mut self) -> &'a [u8] {
        let remaining = &self.bytes[self.cursor..];
        self.cursor = self.bytes.len();
        remaining
    }

    fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriber_message_roundtrip() {
        let mut buf = Vec::new();
        for message in
            [SubscriberMessage::Subscribe { udp_port: 4567 }, SubscriberMessage::Repair {
                session_id: 0xdead_beef,
                sequence: u64::MAX,
            }]
        {
            message.encode(&mut buf);
            assert_eq!(SubscriberMessage::decode(&buf), Ok(message));
        }
    }

    #[test]
    fn publisher_message_roundtrip() {
        let mut buf = Vec::new();
        for message in [
            PublisherMessage::State { session_id: 1, next_sequence: 2 },
            PublisherMessage::RepairData { session_id: 3, sequence: 4, payload: b"repair bytes" },
            PublisherMessage::RepairData { session_id: 3, sequence: 4, payload: b"" },
            PublisherMessage::Unavailable { session_id: 5, sequence: 6 },
        ] {
            message.encode(&mut buf);
            assert_eq!(PublisherMessage::decode(&buf), Ok(message));
        }
    }

    #[test]
    fn decode_rejects_invalid_messages() {
        let mut subscribe = Vec::new();
        SubscriberMessage::Subscribe { udp_port: 4567 }.encode(&mut subscribe);

        assert_eq!(SubscriberMessage::decode(&subscribe[..1]), Err(ControlError::Truncated));
        assert_eq!(
            SubscriberMessage::decode(&subscribe[..subscribe.len() - 1]),
            Err(ControlError::Truncated)
        );

        let mut bad_version = subscribe.clone();
        bad_version[0] = CONTROL_VERSION + 1;
        assert_eq!(
            SubscriberMessage::decode(&bad_version),
            Err(ControlError::UnsupportedVersion(CONTROL_VERSION + 1))
        );

        let mut bad_kind = subscribe.clone();
        bad_kind[1] = 0xff;
        assert_eq!(
            SubscriberMessage::decode(&bad_kind),
            Err(ControlError::UnsupportedMessage(0xff))
        );

        let mut trailing = subscribe.clone();
        trailing.push(0);
        assert_eq!(SubscriberMessage::decode(&trailing), Err(ControlError::TrailingBytes));

        let mut zero_port = Vec::new();
        SubscriberMessage::Subscribe { udp_port: 0 }.encode(&mut zero_port);
        assert_eq!(SubscriberMessage::decode(&zero_port), Err(ControlError::InvalidUdpPort));

        // messages must not decode as the opposite direction
        let mut state = Vec::new();
        PublisherMessage::State { session_id: 1, next_sequence: 2 }.encode(&mut state);
        assert_eq!(SubscriberMessage::decode(&state), Err(ControlError::UnsupportedMessage(STATE)));
        assert_eq!(
            PublisherMessage::decode(&subscribe),
            Err(ControlError::UnsupportedMessage(SUBSCRIBE))
        );
    }
}
