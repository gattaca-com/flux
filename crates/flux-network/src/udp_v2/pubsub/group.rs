use std::{io, net::SocketAddr};

use flux_timing::Duration;
use flux_utils::DCache;
use mio::{Registry, Token, event::Event as MioEvent};

use super::{publisher::PublisherState, subscriber::SubscriberState};
use crate::{
    UdpConfig,
    network::{Event, Group, PayloadBuf, RxPayload, Tokens, UdpGroupConfigV2},
    udp_v2::sys::RecvBatch,
};

// One per group, boxed in the network's group list.
#[allow(clippy::large_enum_variant)]
enum Role {
    Publisher(PublisherState),
    Subscriber(SubscriberState),
}

/// A publisher or subscriber group. Operations of the other role do
/// nothing, as other groups' do, except that a subscriber group can't
/// `listen` and a publisher group can't `connect`.
pub(crate) struct PubSubGroup {
    max_message_size: usize,
    recv: RecvBatch,
    role: Role,
}

impl PubSubGroup {
    pub(crate) fn publisher(config: UdpGroupConfigV2, group: Group) -> Self {
        Self::new(config.udp, Role::Publisher(PublisherState::new(config, group)))
    }

    pub(crate) fn subscriber(config: UdpGroupConfigV2, group: Group) -> Self {
        Self::new(config.udp, Role::Subscriber(SubscriberState::new(config, group)))
    }

    fn new(udp: UdpConfig, role: Role) -> Self {
        Self {
            max_message_size: udp.max_message_size,
            recv: RecvBatch::new(udp.max_datagram_size),
            role,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.role {
            Role::Publisher(state) => state.is_empty(),
            Role::Subscriber(state) => state.is_empty(),
        }
    }

    pub(crate) fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    pub(crate) fn tick_interval(&self) -> Duration {
        match &self.role {
            Role::Publisher(state) => state.tick_interval(),
            Role::Subscriber(state) => state.tick_interval(),
        }
    }

    pub(crate) fn close_all(&mut self, registry: &Registry) {
        match &mut self.role {
            Role::Publisher(state) => state.close_all(registry),
            Role::Subscriber(state) => state.close_all(registry),
        }
    }

    pub(crate) fn listen(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> io::Result<Token> {
        match &mut self.role {
            Role::Publisher(state) => state.listen(registry, addr, tokens),
            Role::Subscriber(_) => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "subscriber groups can't listen"))
            }
        }
    }

    pub(crate) fn connect(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> Token {
        match &mut self.role {
            Role::Subscriber(state) => state.subscribe(registry, addr, tokens),
            Role::Publisher(_) => {
                panic!("publisher groups can't connect; subscribe from a subscriber group")
            }
        }
    }

    pub(crate) fn broadcast_with<F>(&mut self, registry: &Registry, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        match &mut self.role {
            Role::Publisher(state) => state.broadcast_with(registry, serialise),
            Role::Subscriber(_) => 0,
        }
    }

    pub(crate) fn broadcast_many_with<I, F>(
        &mut self,
        registry: &Registry,
        items: I,
        serialise: F,
    ) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        match &mut self.role {
            Role::Publisher(state) => state.broadcast_many_with(registry, items, serialise),
            Role::Subscriber(_) => 0,
        }
    }

    pub(crate) fn disconnect(&mut self, token: Token) -> bool {
        match &mut self.role {
            Role::Publisher(state) => state.disconnect(token),
            Role::Subscriber(state) => state.resubscribe(token),
        }
    }

    pub(crate) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        match &mut self.role {
            Role::Publisher(state) => state.disconnect_when_drained(token),
            Role::Subscriber(_) => false,
        }
    }

    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        match &mut self.role {
            Role::Publisher(state) => state.remove(registry, token),
            Role::Subscriber(state) => state.unsubscribe(registry, token),
        }
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> + '_ {
        match &self.role {
            Role::Subscriber(state) => Some(state.waiting()),
            Role::Publisher(_) => None,
        }
        .into_iter()
        .flatten()
    }

    pub(crate) fn force_reconnect(&mut self) {
        if let Role::Subscriber(state) = &mut self.role {
            state.retry_now();
        }
    }

    pub(crate) fn disconnect_outbound(&mut self) {
        if let Role::Subscriber(state) = &mut self.role {
            state.resubscribe_all();
        }
    }

    pub(crate) fn pre_poll<F>(&mut self, registry: &Registry, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        match &mut self.role {
            Role::Publisher(state) => state.pre_poll(registry, deliver),
            Role::Subscriber(state) => state.pre_poll(registry, deliver),
        }
    }

    pub(crate) fn handle_event<F>(
        &mut self,
        registry: &Registry,
        event: &MioEvent,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        match &mut self.role {
            Role::Publisher(state) => {
                state.handle_event(&mut self.recv, registry, event, tokens, dcache, deliver);
            }
            Role::Subscriber(state) => {
                state.handle_event(&mut self.recv, registry, event, dcache, deliver);
            }
        }
    }

    pub(crate) fn post_poll<F>(&mut self, tokens: &mut Tokens, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        match &mut self.role {
            Role::Publisher(state) => state.post_poll(tokens, deliver),
            Role::Subscriber(state) => state.post_poll(deliver),
        }
    }
}
