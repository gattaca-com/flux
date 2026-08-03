//! Multi-host throughput and sampled one-way-latency benchmark for Flux TCP and
//! UDP.
//!
//! This is deliberately an example binary rather than a Criterion benchmark:
//! the publisher and subscribers need to run on separate machines for useful
//! results.

#![recursion_limit = "256"]

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hint::spin_loop,
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    path::Path,
    str::FromStr,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use flux_network::{
    tcp::{ClientEvent, SendBehavior, ServerEvent, TcpClient, TcpServer},
    udp::{
        MAX_XDP_UNICAST_DESTINATIONS, PublisherEvent, SubscriberEvent, UdpConfig,
        UdpMulticastConfig, UdpPublisher, UdpSendBatchMode, UdpSubscriber, UdpXdpConfig,
        UdpXdpMode, UdpXdpUnicastDestination,
    },
};
use flux_timing::Nanos;
use mio::Token;
use serde_json::{Map, Value, json};

const MESSAGE_MAGIC: [u8; 8] = *b"FLUXBN01";
const CLOCK_MAGIC: [u8; 8] = *b"FLUXCLK1";
const HEADER_SIZE: usize = 56;
const DEFAULT_SOCKET_BUFFER: usize = 64 * 1024 * 1024;
const LATENCY_MAX_US: usize = 200_000;
const MAX_TRACKED_SEQUENCE: u64 = 250_000_000;

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser, Debug)]
#[command(about = "Run a realistic multi-host Flux UDP/TCP benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the single publisher process.
    Publisher(PublisherArgs),
    /// Run one subscriber process.
    Subscriber(SubscriberArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Transport {
    Tcp,
    Udp,
}

impl Transport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PublisherLoop {
    /// Continuously poll transport I/O while pacing and between publications.
    SpinPoll,
    /// Sleep/spin while pacing and poll I/O only at the configured interval.
    Paced,
}

impl PublisherLoop {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SpinPoll => "spin-poll",
            Self::Paced => "paced",
        }
    }
}

#[derive(Args, Debug)]
#[allow(clippy::struct_excessive_bools)]
struct PublisherArgs {
    #[arg(long, value_enum)]
    transport: Transport,
    #[arg(long, default_value = "0.0.0.0:9000")]
    bind: SocketAddr,
    /// Separate raw TCP endpoint used only for NTP-style clock calibration.
    #[arg(long)]
    control_bind: Option<SocketAddr>,
    #[arg(long, default_value_t = 3)]
    subscribers: usize,
    #[arg(long, default_value_t = 1_232)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 100_000)]
    rate: u64,
    #[arg(long, default_value_t = 5.0)]
    warmup_secs: f64,
    #[arg(long, default_value_t = 20.0)]
    duration_secs: f64,
    /// Messages emitted back-to-back at each pacing deadline.
    #[arg(long, default_value_t = 1)]
    burst: usize,
    /// Publisher loop model. `spin-poll` matches a pinned production network
    /// thread.
    #[arg(long, value_enum, default_value = "spin-poll")]
    publisher_loop: PublisherLoop,
    /// In `paced` mode, poll control/repair/writability after this many publish
    /// calls.
    #[arg(long, default_value_t = 64)]
    io_poll_every: u64,
    /// In `spin-poll` mode, limit control/repair polls to this interval. Zero
    /// preserves the legacy behavior of polling every spin iteration and
    /// before every publication.
    #[arg(long, default_value_t = 0)]
    control_poll_interval_us: u64,
    /// Timestamp one message in every N; zero disables latency sampling.
    #[arg(long, default_value_t = 100)]
    latency_sample_every: u64,
    #[arg(long, default_value_t = 3.0)]
    drain_secs: f64,
    /// Number of identical end markers sent after measurement. Values above
    /// one are useful for loss diagnostics with repair deliberately delayed.
    #[arg(long, default_value_t = 1)]
    end_markers: usize,
    #[arg(long, default_value_t = 30.0)]
    connect_timeout_secs: f64,
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    socket_buffer_bytes: usize,
    #[arg(long, default_value_t = 4_096)]
    tcp_max_backlog_messages: usize,
    #[arg(long, default_value_t = 25)]
    tcp_backlog_timeout_ms: u64,
    #[arg(long, default_value_t = 1_400)]
    udp_datagram_bytes: usize,
    #[arg(long, default_value_t = 65_536)]
    udp_sequence_window: usize,
    /// Logical UDP publications coalesced before entering the send path.
    #[arg(long, default_value_t = 1)]
    udp_send_batch_size: usize,
    /// Cooperative flush deadline for a partial UDP publication batch.
    #[arg(long, default_value_t = 50)]
    udp_send_batch_delay_us: u64,
    /// Submit equal-sized UDP datagrams with Linux `UDP_SEGMENT` GSO.
    #[arg(long)]
    udp_gso: bool,
    /// Copy each GSO group into a contiguous buffer instead of scatter-gather.
    #[arg(long)]
    udp_gso_copy: bool,
    /// Bypass the publication batch wait after an idle interval.
    #[arg(long)]
    udp_adaptive_batching: bool,
    /// IPv4 multicast destination, including the group port.
    #[arg(long)]
    udp_multicast_group: Option<SocketAddrV4>,
    /// Local IPv4 address of the interface used for multicast traffic.
    #[arg(long)]
    udp_multicast_interface: Option<Ipv4Addr>,
    /// Enable `AF_XDP` TX on this physical interface index.
    #[arg(long)]
    udp_xdp_interface_index: Option<u32>,
    /// Source MAC for `AF_XDP` Ethernet frames, for example 02:00:00:00:00:01.
    #[arg(long, requires = "udp_xdp_interface_index")]
    udp_xdp_source_mac: Option<String>,
    /// Source IPv4 address for `AF_XDP` unicast fanout.
    #[arg(long, requires = "udp_xdp_interface_index")]
    udp_xdp_source_ip: Option<Ipv4Addr>,
    /// Static `AF_XDP` unicast target as IP:port@MAC. Repeat once per receiver.
    #[arg(long, requires = "udp_xdp_interface_index")]
    udp_xdp_unicast_destination: Vec<String>,
    /// Physical NIC queue bound by the `AF_XDP` socket.
    #[arg(long, default_value_t = 0)]
    udp_xdp_queue: u32,
    /// Optional 802.1Q VLAN identifier emitted by `AF_XDP`.
    #[arg(long)]
    udp_xdp_vlan_id: Option<u16>,
    #[arg(long, default_value_t = 4_096)]
    udp_xdp_ring_size: u32,
    #[arg(long, default_value_t = 8_192)]
    udp_xdp_frame_count: u32,
    /// Require the native zero-copy driver path instead of copy mode.
    #[arg(long)]
    udp_xdp_zero_copy: bool,
    /// Attach a temporary native-mode `XDP_PASS` program for the XSK lifetime.
    #[arg(long)]
    udp_xdp_attach_pass: bool,
    /// Fail publisher startup instead of using kernel UDP if `AF_XDP` setup
    /// fails.
    #[arg(long)]
    udp_xdp_no_fallback: bool,
    #[arg(long)]
    run_id: Option<u64>,
    #[arg(long, default_value = "publisher")]
    label: String,
    #[arg(long)]
    interface: Option<String>,
    /// Publish Flux Profiler rings under this name (requires --features
    /// profiling).
    #[arg(long)]
    profile_name: Option<String>,
}

#[derive(Args, Debug)]
struct SubscriberArgs {
    #[arg(long, value_enum)]
    transport: Transport,
    #[arg(long)]
    publisher: SocketAddr,
    #[arg(long)]
    control: Option<SocketAddr>,
    /// Used only by UDP. Port zero lets the OS choose the receive port.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 120.0)]
    timeout_secs: f64,
    #[arg(long, default_value_t = 3.0)]
    drain_secs: f64,
    #[arg(long, default_value_t = 64)]
    clock_samples: u32,
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    socket_buffer_bytes: usize,
    #[arg(long, default_value_t = 1_400)]
    udp_datagram_bytes: usize,
    #[arg(long, default_value_t = 65_536)]
    udp_sequence_window: usize,
    /// Delay before requesting TCP repair for a missing UDP publication.
    #[arg(long, default_value_t = 1)]
    udp_repair_delay_ms: u64,
    /// Poll the TCP subscription/repair control plane at this interval while
    /// continuously polling UDP data. Zero preserves the combined legacy poll.
    #[arg(long, default_value_t = 0)]
    control_poll_interval_us: u64,
    /// Minimum interval between nonblocking UDP receive polls while using the
    /// split data/control loop. The thread busy-spins between polls; zero
    /// preserves continuous `recvmmsg` probing.
    #[arg(long, default_value_t = 0)]
    data_poll_interval_us: u64,
    /// Receive coalesced UDP packets with Linux `UDP_GRO` and split them in
    /// userspace.
    #[arg(long)]
    udp_gro: bool,
    /// IPv4 multicast destination, including the group port.
    #[arg(long)]
    udp_multicast_group: Option<SocketAddrV4>,
    /// Local IPv4 address of the interface used to join the multicast group.
    #[arg(long)]
    udp_multicast_interface: Option<Ipv4Addr>,
    #[arg(long, default_value = "subscriber")]
    label: String,
    #[arg(long)]
    interface: Option<String>,
    /// Publish Flux Profiler rings under this name (requires --features
    /// profiling).
    #[arg(long)]
    profile_name: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum MessageKind {
    Warmup = 1,
    Start = 2,
    Data = 3,
    End = 4,
}

impl TryFrom<u8> for MessageKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Warmup),
            2 => Ok(Self::Start),
            3 => Ok(Self::Data),
            4 => Ok(Self::End),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchMessage {
    kind: MessageKind,
    run_id: u64,
    sequence: u64,
    send_ns: u64,
    arg0: u64,
    arg1: u64,
}

impl BenchMessage {
    fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < HEADER_SIZE || payload[..8] != MESSAGE_MAGIC {
            return None;
        }
        Some(Self {
            kind: MessageKind::try_from(payload[8]).ok()?,
            run_id: u64::from_le_bytes(payload[16..24].try_into().ok()?),
            sequence: u64::from_le_bytes(payload[24..32].try_into().ok()?),
            send_ns: u64::from_le_bytes(payload[32..40].try_into().ok()?),
            arg0: u64::from_le_bytes(payload[40..48].try_into().ok()?),
            arg1: u64::from_le_bytes(payload[48..56].try_into().ok()?),
        })
    }
}

fn encode_message(
    payload: &mut [u8],
    kind: MessageKind,
    run_id: u64,
    sequence: u64,
    send_ns: u64,
    arg0: u64,
    arg1: u64,
) {
    payload[..8].copy_from_slice(&MESSAGE_MAGIC);
    payload[8] = kind as u8;
    payload[9..16].fill(0);
    payload[16..24].copy_from_slice(&run_id.to_le_bytes());
    payload[24..32].copy_from_slice(&sequence.to_le_bytes());
    payload[32..40].copy_from_slice(&send_ns.to_le_bytes());
    payload[40..48].copy_from_slice(&arg0.to_le_bytes());
    payload[48..56].copy_from_slice(&arg1.to_le_bytes());
}

#[derive(Default)]
struct Lifecycle {
    connects: u64,
    disconnects: u64,
    min_active_during_measurement: usize,
    measuring: bool,
}

trait PublisherTransport {
    fn activate(&mut self) -> BenchResult<()> {
        Ok(())
    }
    fn poll(&mut self, lifecycle: &mut Lifecycle);
    fn poll_data(&mut self, lifecycle: &mut Lifecycle) {
        self.poll(lifecycle);
    }
    fn poll_control(&mut self, _lifecycle: &mut Lifecycle) {}
    fn publish(&mut self, payload: &[u8], lifecycle: &mut Lifecycle);
    fn flush(&mut self, _lifecycle: &mut Lifecycle) {}
    fn active(&self) -> usize;
    fn reset_stats(&mut self) {}
    fn stats_json(&self) -> Value {
        json!({})
    }
}

struct TcpPublisherTransport {
    server: TcpServer,
    active: HashSet<Token>,
}

impl TcpPublisherTransport {
    fn new(args: &PublisherArgs) -> BenchResult<Self> {
        let mut server = TcpServer::default()
            .with_nodelay(true)
            .with_socket_buf_size(args.socket_buffer_bytes)
            .with_max_backlog(
                args.tcp_max_backlog_messages,
                Duration::from_millis(args.tcp_backlog_timeout_ms).into(),
            )
            .with_drop_backlog_on_disconnect(true);
        server
            .listen_at(args.bind)
            .ok_or_else(|| format!("could not bind TCP publisher at {}", args.bind))?;
        Ok(Self { server, active: HashSet::new() })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_event(
        active: &mut HashSet<Token>,
        lifecycle: &mut Lifecycle,
        event: ServerEvent<&[u8]>,
    ) {
        match event {
            ServerEvent::Accept { stream, .. } => {
                active.insert(stream);
                lifecycle.connects += 1;
            }
            ServerEvent::Disconnect { token } => {
                active.remove(&token);
                lifecycle.disconnects += 1;
            }
            ServerEvent::Message { .. } => {}
        }
    }
}

impl PublisherTransport for TcpPublisherTransport {
    fn poll(&mut self, lifecycle: &mut Lifecycle) {
        let active = &mut self.active;
        self.server.poll_with(|event| Self::handle_event(active, lifecycle, event));
        if lifecycle.measuring {
            lifecycle.min_active_during_measurement =
                lifecycle.min_active_during_measurement.min(self.active.len());
        }
    }

    fn publish(&mut self, payload: &[u8], _lifecycle: &mut Lifecycle) {
        self.server
            .write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(payload));
    }

    fn active(&self) -> usize {
        self.active.len()
    }
}

struct UdpPublisherTransport {
    publisher: UdpPublisher,
}

impl UdpPublisherTransport {
    fn new(args: &PublisherArgs) -> BenchResult<Self> {
        let mut config = UdpConfig::default_for_addr(args.bind);
        config.max_datagram_size = args.udp_datagram_bytes;
        config.max_message_size = args.payload_bytes;
        config.sequence_window = args.udp_sequence_window;
        config.socket_buf_size = Some(args.socket_buffer_bytes);
        config.send_batch_size = args.udp_send_batch_size;
        config.send_batch_max_delay = Duration::from_micros(args.udp_send_batch_delay_us);
        config.send_batch_mode = if args.udp_adaptive_batching {
            UdpSendBatchMode::Adaptive
        } else {
            UdpSendBatchMode::Fixed
        };
        config.use_udp_segment = args.udp_gso;
        config.copy_udp_segment_payloads = args.udp_gso_copy;
        config.multicast =
            multicast_config(args.udp_multicast_group, args.udp_multicast_interface)?;
        config.xdp = if let Some(interface_index) = args.udp_xdp_interface_index {
            let source_mac = parse_mac(
                args.udp_xdp_source_mac
                    .as_deref()
                    .ok_or("--udp-xdp-source-mac is required with --udp-xdp-interface-index")?,
            )?;
            let mut xdp = UdpXdpConfig::new(interface_index, args.udp_xdp_queue, source_mac);
            xdp.source_ip = args.udp_xdp_source_ip;
            let destinations = args
                .udp_xdp_unicast_destination
                .iter()
                .map(|destination| parse_xdp_unicast_destination(destination))
                .collect::<Result<Vec<_>, _>>()?;
            if destinations.len() > MAX_XDP_UNICAST_DESTINATIONS {
                return Err(format!(
                    "AF_XDP supports at most {MAX_XDP_UNICAST_DESTINATIONS} unicast destinations"
                )
                .into());
            }
            for (slot, destination) in xdp.unicast_destinations.iter_mut().zip(destinations) {
                *slot = Some(destination);
            }
            xdp.vlan_id = args.udp_xdp_vlan_id;
            xdp.ring_size = args.udp_xdp_ring_size;
            xdp.frame_count = args.udp_xdp_frame_count;
            xdp.mode = if args.udp_xdp_zero_copy { UdpXdpMode::ZeroCopy } else { UdpXdpMode::Copy };
            xdp.attach_xdp_pass = args.udp_xdp_attach_pass;
            xdp.fallback_to_socket = !args.udp_xdp_no_fallback;
            Some(xdp)
        } else {
            None
        };
        Ok(Self { publisher: UdpPublisher::new_with_config(args.bind, config)? })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn on_event(lifecycle: &mut Lifecycle, event: PublisherEvent) {
        match event {
            PublisherEvent::Connected { .. } => lifecycle.connects += 1,
            PublisherEvent::Disconnect { .. } => lifecycle.disconnects += 1,
        }
    }
}

impl PublisherTransport for UdpPublisherTransport {
    fn activate(&mut self) -> BenchResult<()> {
        Ok(())
    }

    fn poll(&mut self, lifecycle: &mut Lifecycle) {
        self.publisher.poll_with(|event| Self::on_event(lifecycle, event));
        if lifecycle.measuring {
            lifecycle.min_active_during_measurement =
                lifecycle.min_active_during_measurement.min(self.publisher.active_subscribers());
        }
    }

    fn poll_data(&mut self, lifecycle: &mut Lifecycle) {
        self.publisher.poll_data_with(|event| Self::on_event(lifecycle, event));
    }

    fn poll_control(&mut self, lifecycle: &mut Lifecycle) {
        self.publisher.poll_control_with(|event| Self::on_event(lifecycle, event));
        if lifecycle.measuring {
            lifecycle.min_active_during_measurement =
                lifecycle.min_active_during_measurement.min(self.publisher.active_subscribers());
        }
    }

    fn publish(&mut self, payload: &[u8], lifecycle: &mut Lifecycle) {
        self.publisher.publish_with(
            |event| Self::on_event(lifecycle, event),
            |buf| buf.extend_from_slice(payload),
        );
    }

    fn active(&self) -> usize {
        self.publisher.active_subscribers()
    }

    fn flush(&mut self, lifecycle: &mut Lifecycle) {
        self.publisher.flush_with(|event| Self::on_event(lifecycle, event));
    }

    fn reset_stats(&mut self) {
        self.publisher.reset_stats();
    }

    fn stats_json(&self) -> Value {
        let stats = self.publisher.stats();
        json!({
            "using_xdp": self.publisher.using_xdp(),
            "publication_flushes": stats.publication_flushes,
            "publications_flushed": stats.publications_flushed,
            "max_publications_per_flush": stats.max_publications_per_flush,
            "immediate_flushes": stats.immediate_flushes,
            "adaptive_immediate_flushes": stats.adaptive_immediate_flushes,
            "adaptive_batch_activations": stats.adaptive_batch_activations,
            "adaptive_idle_resets": stats.adaptive_idle_resets,
            "full_batch_flushes": stats.full_batch_flushes,
            "deadline_flushes": stats.deadline_flushes,
            "explicit_flushes": stats.explicit_flushes,
            "total_batch_dwell_ns": stats.total_batch_dwell_ns,
            "max_batch_dwell_ns": stats.max_batch_dwell_ns,
            "sendmmsg_calls": stats.sendmmsg_calls,
            "total_sendmmsg_ns": stats.total_sendmmsg_ns,
            "max_sendmmsg_ns": stats.max_sendmmsg_ns,
            "sendmmsg_would_block": stats.sendmmsg_would_block,
            "send_entries": stats.send_entries,
            "wire_datagrams": stats.wire_datagrams,
            "xdp_enqueued_datagrams": stats.xdp_enqueued_datagrams,
            "xdp_tx_producer": stats.xdp_tx_producer,
            "xdp_tx_consumer": stats.xdp_tx_consumer,
            "xdp_completion_producer": stats.xdp_completion_producer,
            "xdp_completion_consumer": stats.xdp_completion_consumer,
            "xdp_tx_needs_wakeup": stats.xdp_tx_needs_wakeup,
            "xdp_free_frames": stats.xdp_free_frames,
            "xdp_completed_datagrams": stats.xdp_completed_datagrams,
            "xdp_ring_full_drops": stats.xdp_ring_full_drops,
            "xdp_frame_exhaustion_drops": stats.xdp_frame_exhaustion_drops,
            "xdp_kick_calls": stats.xdp_kick_calls,
            "xdp_kick_errors": stats.xdp_kick_errors,
            "xdp_setup_fallbacks": stats.xdp_setup_fallbacks,
            "xdp_wire_bytes": stats.xdp_wire_bytes,
        })
    }
}

#[derive(Default)]
struct PaceStats {
    messages: u64,
    elapsed_ns: u64,
    late_bursts: u64,
    max_late_ns: u64,
    io_polls: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct SchedulerSnapshot {
    runtime_ns: u64,
    runqueue_wait_ns: u64,
    timeslices: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

#[derive(Clone, Copy, Debug)]
struct CpuCoreSnapshot {
    cpu: i32,
    ticks: [u64; 10],
}

#[derive(Clone, Copy, Debug)]
struct SoftirqSnapshot {
    cpu: i32,
    counts: [u64; 10],
}

#[allow(clippy::too_many_arguments)]
fn pace_phase<T: PublisherTransport>(
    transport: &mut T,
    lifecycle: &mut Lifecycle,
    payload: &mut [u8],
    kind: MessageKind,
    run_id: u64,
    rate: u64,
    duration_secs: f64,
    burst: usize,
    publisher_loop: PublisherLoop,
    io_poll_every: u64,
    control_poll_interval_us: u64,
    latency_sample_every: u64,
) -> PaceStats {
    let started = Instant::now();
    let deadline = started + Duration::from_secs_f64(duration_secs);
    let mut stats = PaceStats::default();
    let control_poll_interval = Duration::from_micros(control_poll_interval_us);
    let mut next_control_poll = started;

    while Instant::now() < deadline {
        let target_ns = ((stats.messages as u128 * 1_000_000_000) / rate as u128) as u64;
        let target = started + Duration::from_nanos(target_ns);
        wait_until(
            target.min(deadline),
            transport,
            lifecycle,
            publisher_loop,
            control_poll_interval,
            &mut next_control_poll,
            &mut stats.io_polls,
        );
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let late_ns = now.saturating_duration_since(target).as_nanos() as u64;
        if late_ns > 50_000 {
            stats.late_bursts += 1;
            stats.max_late_ns = stats.max_late_ns.max(late_ns);
        }

        for _ in 0..burst {
            if Instant::now() >= deadline {
                break;
            }
            if publisher_loop == PublisherLoop::SpinPoll {
                if !control_poll_interval.is_zero() {
                    transport.poll_data(lifecycle);
                }
                poll_control_if_due(
                    transport,
                    lifecycle,
                    control_poll_interval,
                    &mut next_control_poll,
                    &mut stats.io_polls,
                );
            }
            let sequence = stats.messages;
            let sampled = kind == MessageKind::Data &&
                latency_sample_every != 0 &&
                sequence % latency_sample_every == run_id % latency_sample_every;
            let send_ns = if sampled { Nanos::now().0 } else { 0 };
            encode_message(payload, kind, run_id, sequence, send_ns, 0, 0);
            transport.publish(payload, lifecycle);
            stats.messages += 1;
            if publisher_loop == PublisherLoop::Paced &&
                stats.messages.is_multiple_of(io_poll_every)
            {
                transport.poll(lifecycle);
                stats.io_polls += 1;
            }
        }
    }
    transport.flush(lifecycle);
    transport.poll(lifecycle);
    stats.io_polls += 1;
    stats.elapsed_ns = started.elapsed().as_nanos() as u64;
    stats
}

fn wait_until<T: PublisherTransport>(
    target: Instant,
    transport: &mut T,
    lifecycle: &mut Lifecycle,
    publisher_loop: PublisherLoop,
    control_poll_interval: Duration,
    next_control_poll: &mut Instant,
    io_polls: &mut u64,
) {
    loop {
        let now = Instant::now();
        if now >= target {
            return;
        }
        if publisher_loop == PublisherLoop::SpinPoll {
            if !control_poll_interval.is_zero() {
                transport.poll_data(lifecycle);
            }
            poll_control_if_due(
                transport,
                lifecycle,
                control_poll_interval,
                next_control_poll,
                io_polls,
            );
            spin_loop();
            continue;
        }
        let remaining = target - now;
        if remaining > Duration::from_micros(200) {
            thread::sleep((remaining - Duration::from_micros(100)).min(Duration::from_micros(100)));
        } else {
            spin_loop();
        }
    }
}

fn poll_control_if_due<T: PublisherTransport>(
    transport: &mut T,
    lifecycle: &mut Lifecycle,
    interval: Duration,
    next_poll: &mut Instant,
    poll_count: &mut u64,
) {
    let now = Instant::now();
    if !interval.is_zero() && now < *next_poll {
        return;
    }
    if interval.is_zero() {
        transport.poll(lifecycle);
    } else {
        transport.poll_control(lifecycle);
    }
    *poll_count += 1;
    *next_poll = if interval.is_zero() { now } else { now + interval };
}

fn poll_until<T: PublisherTransport>(
    target: Instant,
    transport: &mut T,
    lifecycle: &mut Lifecycle,
) {
    while Instant::now() < target {
        transport.poll(lifecycle);
        spin_loop();
    }
}

fn run_publisher(args: &PublisherArgs) -> BenchResult<()> {
    validate_publisher(args)?;
    enable_requested_profiler(args.profile_name.as_deref())?;
    let control_bind = args.control_bind.unwrap_or(port_after(args.bind)?);
    let control_thread = start_clock_server(control_bind, args.subscribers)?;
    match args.transport {
        Transport::Tcp => run_publisher_with(
            args,
            TcpPublisherTransport::new(args)?,
            control_bind,
            control_thread,
        ),
        Transport::Udp => run_publisher_with(
            args,
            UdpPublisherTransport::new(args)?,
            control_bind,
            control_thread,
        ),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn enable_requested_profiler(name: Option<&str>) -> BenchResult<()> {
    #[cfg(feature = "profiling")]
    if let Some(name) = name {
        flux_profiler::enable_profiler(name);
    }

    #[cfg(not(feature = "profiling"))]
    if name.is_some() {
        return Err(
            "--profile-name requires building transport_bench with --features profiling".into()
        );
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_publisher_with<T: PublisherTransport>(
    args: &PublisherArgs,
    mut transport: T,
    control_bind: SocketAddr,
    control_thread: thread::JoinHandle<Result<(), String>>,
) -> BenchResult<()> {
    println!(
        "READY {}",
        json!({
            "role": "publisher",
            "label": args.label,
            "transport": args.transport.as_str(),
            "bind": args.bind.to_string(),
            "control_bind": control_bind.to_string(),
        })
    );

    let mut lifecycle = Lifecycle::default();
    let deadline = Instant::now() + Duration::from_secs_f64(args.connect_timeout_secs);
    while transport.active() < args.subscribers {
        transport.poll(&mut lifecycle);
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out with {}/{} transport subscribers",
                transport.active(),
                args.subscribers
            )
            .into());
        }
        thread::sleep(Duration::from_micros(100));
    }
    control_thread
        .join()
        .map_err(|_| "clock calibration server panicked")?
        .map_err(|err| format!("clock calibration failed: {err}"))?;
    transport.activate()?;
    println!("ACTIVATED {}", json!({ "role": "publisher" }));
    std::io::stdout().flush()?;

    let run_id = args.run_id.unwrap_or_else(wall_nanos);
    let mut payload = vec![0xA5; args.payload_bytes];
    let warmup = pace_phase(
        &mut transport,
        &mut lifecycle,
        &mut payload,
        MessageKind::Warmup,
        run_id,
        args.rate,
        args.warmup_secs,
        args.burst,
        args.publisher_loop,
        args.io_poll_every,
        args.control_poll_interval_us,
        0,
    );

    let expected = (args.rate as f64 * args.duration_secs).round() as u64;
    encode_message(
        &mut payload,
        MessageKind::Start,
        run_id,
        args.burst as u64,
        args.latency_sample_every,
        expected,
        Duration::from_secs_f64(args.duration_secs).as_nanos() as u64,
    );
    transport.publish(&payload, &mut lifecycle);
    transport.poll(&mut lifecycle);
    let start_settle = Instant::now() + Duration::from_millis(10);
    poll_until(start_settle, &mut transport, &mut lifecycle);

    transport.reset_stats();
    let cpu_before = process_cpu_ns();
    let scheduler_before = scheduler_snapshot();
    let cpu_core_before = cpu_core_snapshot();
    let softirq_before = softirq_snapshot();
    let kernel_before = kernel_snapshot(args.interface.as_deref());
    lifecycle.measuring = true;
    lifecycle.min_active_during_measurement = transport.active();
    let measured = pace_phase(
        &mut transport,
        &mut lifecycle,
        &mut payload,
        MessageKind::Data,
        run_id,
        args.rate,
        args.duration_secs,
        args.burst,
        args.publisher_loop,
        args.io_poll_every,
        args.control_poll_interval_us,
        args.latency_sample_every,
    );
    lifecycle.measuring = false;
    let cpu_after = process_cpu_ns();
    let scheduler_after = scheduler_snapshot();
    let cpu_core_after = cpu_core_snapshot();
    let softirq_after = softirq_snapshot();
    let kernel_after = kernel_snapshot(args.interface.as_deref());
    let transport_stats = transport.stats_json();

    // Overload diagnostics can end with a full nonblocking TX ring. This is
    // outside the measured interval and gives it time to drain before End.
    poll_until(Instant::now() + Duration::from_millis(10), &mut transport, &mut lifecycle);

    encode_message(
        &mut payload,
        MessageKind::End,
        run_id,
        measured.messages,
        0,
        measured.messages,
        measured.elapsed_ns,
    );
    for marker in 0..args.end_markers {
        transport.publish(&payload, &mut lifecycle);
        transport.flush(&mut lifecycle);
        transport.poll(&mut lifecycle);
        if marker + 1 < args.end_markers {
            thread::sleep(Duration::from_micros(100));
        }
    }

    let drain_deadline = Instant::now() + Duration::from_secs_f64(args.drain_secs);
    while Instant::now() < drain_deadline {
        transport.poll(&mut lifecycle);
        thread::sleep(Duration::from_micros(50));
    }

    let elapsed_secs = measured.elapsed_ns as f64 / 1e9;
    let pps = measured.messages as f64 / elapsed_secs;
    let per_subscriber_bytes_per_sec = pps * args.payload_bytes as f64;
    println!(
        "RESULT {}",
        json!({
            "role": "publisher",
            "label": args.label,
            "transport": args.transport.as_str(),
            "run_id": run_id,
            "subscribers_requested": args.subscribers,
            "active_before_measurement": args.subscribers,
            "min_active_during_measurement": lifecycle.min_active_during_measurement,
            "connects": lifecycle.connects,
            "disconnects_total_including_drain": lifecycle.disconnects,
            "payload_bytes": args.payload_bytes,
            "target_rate_pps": args.rate,
            "burst": args.burst,
            "publisher_loop": args.publisher_loop.as_str(),
            "io_poll_every": args.io_poll_every,
            "control_poll_interval_us": args.control_poll_interval_us,
            "io_poll_calls": measured.io_polls,
            "udp_send_batch_size": args.udp_send_batch_size,
            "udp_send_batch_delay_us": args.udp_send_batch_delay_us,
            "udp_adaptive_batching": args.udp_adaptive_batching,
            "udp_gso": args.udp_gso,
            "udp_gso_copy": args.udp_gso_copy,
            "udp_multicast": args.udp_multicast_group.is_some(),
            "udp_multicast_group": args.udp_multicast_group.map(|group| group.to_string()),
            "udp_xdp_requested": args.udp_xdp_interface_index.is_some(),
            "udp_xdp_interface_index": args.udp_xdp_interface_index,
            "udp_xdp_queue": args.udp_xdp_queue,
            "udp_xdp_vlan_id": args.udp_xdp_vlan_id,
            "udp_xdp_zero_copy": args.udp_xdp_zero_copy,
            "udp_xdp_attach_pass": args.udp_xdp_attach_pass,
            "warmup_messages": warmup.messages,
            "end_markers": args.end_markers,
            "messages": measured.messages,
            "elapsed_ns": measured.elapsed_ns,
            "achieved_publish_calls_per_sec": pps,
            "application_bytes_per_sec_per_subscriber": per_subscriber_bytes_per_sec,
            "aggregate_application_bytes_per_sec": per_subscriber_bytes_per_sec * args.subscribers as f64,
            "late_bursts_over_50us": measured.late_bursts,
            "max_pacing_lateness_ns": measured.max_late_ns,
            "process_cpu_ns": cpu_after.saturating_sub(cpu_before),
            "process_cpu_utilization_one_core": cpu_after.saturating_sub(cpu_before) as f64 / measured.elapsed_ns as f64,
            "scheduler_delta": scheduler_delta(scheduler_before, scheduler_after),
            "cpu_core_delta": cpu_core_delta(cpu_core_before, cpu_core_after),
            "softirq_delta": softirq_delta(softirq_before, softirq_after),
            "kernel_delta": snapshot_delta(&kernel_before, &kernel_after),
            "transport_stats": transport_stats,
        })
    );
    Ok(())
}

trait SubscriberTransport {
    fn poll(&mut self, receiver: &mut Receiver);
    fn poll_data(&mut self, receiver: &mut Receiver) {
        self.poll(receiver);
    }
    fn poll_control(&mut self, _receiver: &mut Receiver) {}
    fn reset_stats(&mut self) {}
    fn stats_json(&self) -> Value {
        json!({})
    }
}

struct TcpSubscriberTransport {
    client: TcpClient,
}

impl TcpSubscriberTransport {
    fn new(args: &SubscriberArgs) -> Self {
        let mut client = TcpClient::default()
            .with_nodelay(true)
            .with_socket_buf_size(args.socket_buffer_bytes)
            .with_drop_backlog_on_disconnect(true);
        let _ = client.connect(args.publisher);
        Self { client }
    }
}

impl SubscriberTransport for TcpSubscriberTransport {
    fn poll(&mut self, receiver: &mut Receiver) {
        self.client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => receiver.connects += 1,
            ClientEvent::Disconnect { .. } => receiver.disconnects += 1,
            ClientEvent::Message { payload, .. } => receiver.on_payload(payload, None),
        });
    }
}

struct UdpSubscriberTransport {
    subscriber: UdpSubscriber,
}

impl UdpSubscriberTransport {
    fn new(args: &SubscriberArgs) -> BenchResult<Self> {
        let mut config = UdpConfig::default_for_addr(args.publisher);
        config.max_datagram_size = args.udp_datagram_bytes;
        config.max_message_size = 16 * 1024 * 1024;
        config.sequence_window = args.udp_sequence_window;
        config.socket_buf_size = Some(args.socket_buffer_bytes);
        config.repair_delay = Duration::from_millis(args.udp_repair_delay_ms);
        config.use_udp_gro = args.udp_gro;
        config.multicast =
            multicast_config(args.udp_multicast_group, args.udp_multicast_interface)?;
        Ok(Self { subscriber: UdpSubscriber::new_with_config(args.publisher, args.bind, config)? })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn on_event(receiver: &mut Receiver, event: SubscriberEvent<'_>) {
        match event {
            SubscriberEvent::Connected { .. } => receiver.connects += 1,
            SubscriberEvent::Disconnect { .. } => receiver.disconnects += 1,
            SubscriberEvent::Message { payload, ingest_ts } => {
                receiver.on_payload(payload, Some(ingest_ts.0));
            }
        }
    }
}

impl SubscriberTransport for UdpSubscriberTransport {
    fn poll(&mut self, receiver: &mut Receiver) {
        self.subscriber.poll_with(|event| Self::on_event(receiver, event));
    }

    fn poll_data(&mut self, receiver: &mut Receiver) {
        self.subscriber.poll_data_with(|event| Self::on_event(receiver, event));
    }

    fn poll_control(&mut self, receiver: &mut Receiver) {
        self.subscriber.poll_control_with(|event| Self::on_event(receiver, event));
    }

    fn reset_stats(&mut self) {
        self.subscriber.reset_stats();
    }

    fn stats_json(&self) -> Value {
        let stats = self.subscriber.stats();
        json!({
            "recvmmsg_calls": stats.recvmmsg_calls,
            "nonempty_recvmmsg_calls": stats.nonempty_recvmmsg_calls,
            "socket_messages_received": stats.socket_messages_received,
            "datagrams_received": stats.datagrams_received,
            "max_datagrams_per_recvmmsg": stats.max_datagrams_per_recvmmsg,
            "gro_packets_received": stats.gro_packets_received,
            "gro_segments_received": stats.gro_segments_received,
            "max_gro_segments": stats.max_gro_segments,
            "udp_messages_delivered": stats.udp_messages_delivered,
            "repair_requests": stats.repair_requests,
            "repair_messages_delivered": stats.repair_messages_delivered,
            "unavailable_messages": stats.unavailable_messages,
        })
    }
}

struct LatencyHistogram {
    buckets: Vec<u64>,
    samples: u64,
    overflow: u64,
    negative: u64,
    max_ns: u64,
}

#[derive(Clone, Copy, Debug)]
struct WorstLatencySample {
    sequence: u64,
    latency_ns: u64,
    recv_ns: u64,
    udp_ingest_to_delivery_ns: Option<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; LATENCY_MAX_US + 1],
            samples: 0,
            overflow: 0,
            negative: 0,
            max_ns: 0,
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, latency_ns: i128) {
        if latency_ns < 0 {
            self.negative += 1;
            return;
        }
        let latency_ns = latency_ns as u64;
        self.max_ns = self.max_ns.max(latency_ns);
        self.samples += 1;
        let micros = latency_ns.div_ceil(1_000) as usize;
        if micros > LATENCY_MAX_US {
            self.overflow += 1;
        } else {
            self.buckets[micros] += 1;
        }
    }

    fn percentile_us(&self, percentile: f64) -> Option<u64> {
        if self.samples == 0 {
            return None;
        }
        let wanted = (self.samples as f64 * percentile).ceil() as u64;
        let mut observed = 0;
        for (micros, count) in self.buckets.iter().enumerate() {
            observed += count;
            if observed >= wanted {
                return Some(micros as u64);
            }
        }
        Some(LATENCY_MAX_US as u64 + 1)
    }

    fn as_json(&self) -> Value {
        json!({
            "samples": self.samples,
            "negative_samples": self.negative,
            "over_200ms_samples": self.overflow,
            "p50_us": self.percentile_us(0.50),
            "p99_us": self.percentile_us(0.99),
            "p99_9_us": self.percentile_us(0.999),
            "max_us": self.max_ns as f64 / 1_000.0,
        })
    }
}

struct Receiver {
    label: String,
    transport: Transport,
    interface: Option<String>,
    udp_repair_delay_ms: u64,
    udp_gro: bool,
    clock_offset_ns: i64,
    clock_rtt_ns: u64,
    connects: u64,
    disconnects: u64,
    run_id: Option<u64>,
    expected: Option<u64>,
    requested_messages: Option<u64>,
    publisher_elapsed_ns: Option<u64>,
    requested_duration_ns: Option<u64>,
    burst: Option<u64>,
    latency_sample_every: Option<u64>,
    seen: Vec<u64>,
    unique: u64,
    duplicates: u64,
    out_of_order: u64,
    highest_sequence: Option<u64>,
    payload_bytes: Option<usize>,
    invalid_messages: u64,
    wrong_run_messages: u64,
    end_seen_at: Option<Instant>,
    first_data_at: Option<Instant>,
    last_data_at: Option<Instant>,
    cpu_before: Option<u64>,
    scheduler_before: Option<SchedulerSnapshot>,
    cpu_core_before: Option<CpuCoreSnapshot>,
    softirq_before: Option<SoftirqSnapshot>,
    kernel_before: Option<HashMap<String, u64>>,
    latency: LatencyHistogram,
    udp_ingest_to_delivery: LatencyHistogram,
    worst_latency_samples: Vec<WorstLatencySample>,
    transport_stats_reset_requested: bool,
}

impl Receiver {
    fn new(args: &SubscriberArgs, calibration: ClockCalibration) -> Self {
        Self {
            label: args.label.clone(),
            transport: args.transport,
            interface: args.interface.clone(),
            udp_repair_delay_ms: args.udp_repair_delay_ms,
            udp_gro: args.udp_gro,
            clock_offset_ns: calibration.offset_ns,
            clock_rtt_ns: calibration.rtt_ns,
            connects: 0,
            disconnects: 0,
            run_id: None,
            expected: None,
            requested_messages: None,
            publisher_elapsed_ns: None,
            requested_duration_ns: None,
            burst: None,
            latency_sample_every: None,
            seen: Vec::new(),
            unique: 0,
            duplicates: 0,
            out_of_order: 0,
            highest_sequence: None,
            payload_bytes: None,
            invalid_messages: 0,
            wrong_run_messages: 0,
            end_seen_at: None,
            first_data_at: None,
            last_data_at: None,
            cpu_before: None,
            scheduler_before: None,
            cpu_core_before: None,
            softirq_before: None,
            kernel_before: None,
            latency: LatencyHistogram::default(),
            udp_ingest_to_delivery: LatencyHistogram::default(),
            worst_latency_samples: Vec::with_capacity(8),
            transport_stats_reset_requested: false,
        }
    }

    fn start_run(&mut self, message: BenchMessage) {
        if self.run_id.is_none() {
            self.run_id = Some(message.run_id);
            self.cpu_before = Some(process_cpu_ns());
            self.scheduler_before = Some(scheduler_snapshot());
            self.cpu_core_before = cpu_core_snapshot();
            self.softirq_before = softirq_snapshot();
            self.kernel_before = Some(kernel_snapshot(self.interface.as_deref()));
        }
        if message.kind == MessageKind::Start {
            self.transport_stats_reset_requested = true;
            self.expected = Some(message.arg0);
            self.requested_messages = Some(message.arg0);
            self.requested_duration_ns = Some(message.arg1);
            self.burst = Some(message.sequence);
            self.latency_sample_every = Some(message.send_ns);
        }
    }

    fn take_transport_stats_reset_request(&mut self) -> bool {
        core::mem::take(&mut self.transport_stats_reset_requested)
    }

    fn on_payload(&mut self, payload: &[u8], udp_ingest_ns: Option<u64>) {
        let Some(message) = BenchMessage::parse(payload) else {
            self.invalid_messages += 1;
            return;
        };
        if message.kind == MessageKind::Warmup {
            return;
        }
        self.start_run(message);
        if self.run_id != Some(message.run_id) {
            self.wrong_run_messages += 1;
            return;
        }
        match message.kind {
            MessageKind::Warmup => {}
            MessageKind::Start => {
                self.expected = Some(message.arg0);
                self.requested_messages = Some(message.arg0);
                self.requested_duration_ns = Some(message.arg1);
                self.burst = Some(message.sequence);
                self.latency_sample_every = Some(message.send_ns);
            }
            MessageKind::Data => self.on_data(message, payload.len(), udp_ingest_ns),
            MessageKind::End => {
                self.expected = Some(message.arg0);
                self.publisher_elapsed_ns = Some(message.arg1);
                self.end_seen_at.get_or_insert_with(Instant::now);
            }
        }
    }

    fn on_data(&mut self, message: BenchMessage, payload_bytes: usize, udp_ingest_ns: Option<u64>) {
        if message.sequence > MAX_TRACKED_SEQUENCE {
            self.invalid_messages += 1;
            return;
        }
        let word = message.sequence as usize / 64;
        if self.seen.len() <= word {
            self.seen.resize(word + 1, 0);
        }
        let mask = 1_u64 << (message.sequence % 64);
        if self.seen[word] & mask != 0 {
            self.duplicates += 1;
            return;
        }
        self.seen[word] |= mask;
        self.payload_bytes.get_or_insert(payload_bytes);
        if self.highest_sequence.is_some_and(|highest| message.sequence < highest) {
            self.out_of_order += 1;
        }
        self.highest_sequence = Some(
            self.highest_sequence.map_or(message.sequence, |value| value.max(message.sequence)),
        );
        self.unique += 1;
        let now_instant = Instant::now();
        self.first_data_at.get_or_insert(now_instant);
        self.last_data_at = Some(now_instant);

        if message.send_ns != 0 {
            let recv_ns = Nanos::now().0;
            let latency_ns =
                recv_ns as i128 - message.send_ns as i128 + self.clock_offset_ns as i128;
            self.latency.record(latency_ns);
            if let Ok(latency_ns) = u64::try_from(latency_ns) {
                self.worst_latency_samples.push(WorstLatencySample {
                    sequence: message.sequence,
                    latency_ns,
                    recv_ns,
                    udp_ingest_to_delivery_ns: udp_ingest_ns
                        .map(|ingest_ns| recv_ns.saturating_sub(ingest_ns)),
                });
                self.worst_latency_samples
                    .sort_unstable_by_key(|sample| core::cmp::Reverse(sample.latency_ns));
                self.worst_latency_samples.truncate(8);
            }
            if let Some(ingest_ns) = udp_ingest_ns {
                self.udp_ingest_to_delivery.record(recv_ns.saturating_sub(ingest_ns) as i128);
            }
        }
    }

    fn should_finish(&self, drain: Duration) -> bool {
        let Some(end_seen_at) = self.end_seen_at else { return false };
        let complete = self.expected.is_some_and(|expected| self.unique >= expected);
        let minimum_grace = drain.min(Duration::from_millis(250));
        (complete && end_seen_at.elapsed() >= minimum_grace) || end_seen_at.elapsed() >= drain
    }

    fn result(&self, timed_out: bool) -> Value {
        let expected = self.expected.unwrap_or(0);
        let missing = expected.saturating_sub(self.unique);
        let publisher_elapsed_ns = self.publisher_elapsed_ns.unwrap_or(0);
        let goodput = if publisher_elapsed_ns == 0 {
            0.0
        } else {
            self.unique as f64 / (publisher_elapsed_ns as f64 / 1e9)
        };
        let application_bytes_per_sec = goodput * self.payload_bytes.unwrap_or(0) as f64;
        let target_rate_pps = self.requested_duration_ns.map_or(0.0, |duration_ns| {
            self.requested_messages.unwrap_or(0) as f64 / (duration_ns as f64 / 1e9)
        });
        let receive_span_ns = self
            .first_data_at
            .zip(self.last_data_at)
            .map_or(0, |(first, last)| last.saturating_duration_since(first).as_nanos() as u64);
        let cpu_after = process_cpu_ns();
        let cpu_ns = self.cpu_before.map_or(0, |before| cpu_after.saturating_sub(before));
        let scheduler_after = scheduler_snapshot();
        let cpu_core_after = cpu_core_snapshot();
        let softirq_after = softirq_snapshot();
        let kernel_after = kernel_snapshot(self.interface.as_deref());
        let kernel_before = self.kernel_before.clone().unwrap_or_default();
        json!({
            "role": "subscriber",
            "label": self.label,
            "transport": self.transport.as_str(),
            "udp_repair_delay_ms": self.udp_repair_delay_ms,
            "udp_gro": self.udp_gro,
            "run_id": self.run_id,
            "timed_out": timed_out,
            "end_seen": self.end_seen_at.is_some(),
            "connects": self.connects,
            "disconnects": self.disconnects,
            "expected_messages": expected,
            "target_rate_pps": target_rate_pps,
            "burst": self.burst,
            "latency_sample_every": self.latency_sample_every,
            "unique_messages": self.unique,
            "missing_messages": missing,
            "loss_fraction_after_drain": if expected == 0 { 0.0 } else { missing as f64 / expected as f64 },
            "duplicates_visible_to_application": self.duplicates,
            "out_of_order_messages": self.out_of_order,
            "invalid_messages": self.invalid_messages,
            "wrong_run_messages": self.wrong_run_messages,
            "publisher_elapsed_ns": publisher_elapsed_ns,
            "receive_span_ns": receive_span_ns,
            "delivered_messages_per_sec": goodput,
            "payload_bytes": self.payload_bytes,
            "application_bytes_per_sec": application_bytes_per_sec,
            "process_cpu_ns_including_drain": cpu_ns,
            "scheduler_delta_including_drain": self.scheduler_before.map_or(Value::Null, |before| scheduler_delta(before, scheduler_after)),
            "cpu_core_delta_including_drain": cpu_core_delta(self.cpu_core_before, cpu_core_after),
            "softirq_delta_including_drain": softirq_delta(self.softirq_before, softirq_after),
            "clock_offset_server_minus_client_ns": self.clock_offset_ns,
            "clock_min_rtt_ns": self.clock_rtt_ns,
            "clock_one_way_uncertainty_bound_ns": self.clock_rtt_ns / 2,
            "latency": self.latency.as_json(),
            "worst_latency_samples": self.worst_latency_samples.iter().map(|sample| json!({
                "sequence": sample.sequence,
                "latency_us": sample.latency_ns as f64 / 1_000.0,
                "recv_ns": sample.recv_ns,
                "udp_ingest_to_application_delivery_us": sample
                    .udp_ingest_to_delivery_ns
                    .map(|value| value as f64 / 1_000.0),
            })).collect::<Vec<_>>(),
            "udp_sampled_ingest_to_application_delivery": self.udp_ingest_to_delivery.as_json(),
            "kernel_delta": snapshot_delta(&kernel_before, &kernel_after),
        })
    }
}

fn run_subscriber(args: &SubscriberArgs) -> BenchResult<()> {
    validate_subscriber(args)?;
    enable_requested_profiler(args.profile_name.as_deref())?;
    let control = args.control.unwrap_or(port_after(args.publisher)?);
    let calibration = calibrate_clock(control, args.clock_samples, Duration::from_secs(30))?;
    match args.transport {
        Transport::Tcp => {
            run_subscriber_with(args, TcpSubscriberTransport::new(args), control, calibration);
        }
        Transport::Udp => {
            run_subscriber_with(args, UdpSubscriberTransport::new(args)?, control, calibration);
        }
    }
    Ok(())
}

fn run_subscriber_with<T: SubscriberTransport>(
    args: &SubscriberArgs,
    mut transport: T,
    control: SocketAddr,
    calibration: ClockCalibration,
) {
    println!(
        "READY {}",
        json!({
            "role": "subscriber",
            "label": args.label,
            "transport": args.transport.as_str(),
            "publisher": args.publisher.to_string(),
            "control": control.to_string(),
            "clock_offset_server_minus_client_ns": calibration.offset_ns,
            "clock_min_rtt_ns": calibration.rtt_ns,
        })
    );
    let mut receiver = Receiver::new(args, calibration);
    let deadline = Instant::now() + Duration::from_secs_f64(args.timeout_secs);
    let drain = Duration::from_secs_f64(args.drain_secs);
    let mut timed_out = false;
    let control_poll_interval = Duration::from_micros(args.control_poll_interval_us);
    let data_poll_interval = Duration::from_micros(args.data_poll_interval_us);
    let split_control_poll =
        matches!(args.transport, Transport::Udp) && !control_poll_interval.is_zero();
    let mut next_control_poll = Instant::now();
    let mut next_data_poll = Instant::now();
    let mut combined_poll_calls = 0_u64;
    let mut control_poll_calls = 0_u64;
    let mut data_poll_calls = 0_u64;
    loop {
        if split_control_poll {
            let now = Instant::now();
            if now >= next_control_poll {
                transport.poll_control(&mut receiver);
                control_poll_calls += 1;
                next_control_poll = now + control_poll_interval;
            }
            if now >= next_data_poll {
                transport.poll_data(&mut receiver);
                data_poll_calls += 1;
                next_data_poll =
                    if data_poll_interval.is_zero() { now } else { now + data_poll_interval };
            }
        } else {
            transport.poll(&mut receiver);
            combined_poll_calls += 1;
        }
        if receiver.take_transport_stats_reset_request() {
            transport.reset_stats();
            combined_poll_calls = 0;
            control_poll_calls = 0;
            data_poll_calls = 0;
        }
        if receiver.should_finish(drain) {
            break;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            break;
        }
        spin_loop();
    }
    let mut result = receiver.result(timed_out);
    result["udp_multicast"] = json!(args.udp_multicast_group.is_some());
    result["udp_multicast_group"] = json!(args.udp_multicast_group.map(|group| group.to_string()));
    result["control_poll_interval_us"] = json!(args.control_poll_interval_us);
    result["data_poll_interval_us"] = json!(args.data_poll_interval_us);
    result["combined_poll_calls_including_drain"] = json!(combined_poll_calls);
    result["control_poll_calls_including_drain"] = json!(control_poll_calls);
    result["data_poll_calls_including_drain"] = json!(data_poll_calls);
    result["transport_stats"] = transport.stats_json();
    println!("RESULT {result}");
}

#[derive(Clone, Copy, Debug)]
struct ClockCalibration {
    offset_ns: i64,
    rtt_ns: u64,
}

fn start_clock_server(
    addr: SocketAddr,
    expected_clients: usize,
) -> BenchResult<thread::JoinHandle<Result<(), String>>> {
    let listener = TcpListener::bind(addr)?;
    Ok(thread::spawn(move || {
        for _ in 0..expected_clients {
            let (mut stream, _) = listener.accept().map_err(|err| err.to_string())?;
            stream.set_nodelay(true).map_err(|err| err.to_string())?;
            serve_clock_client(&mut stream)?;
        }
        Ok(())
    }))
}

fn serve_clock_client(stream: &mut TcpStream) -> Result<(), String> {
    let mut hello = [0_u8; 12];
    stream.read_exact(&mut hello).map_err(|err| err.to_string())?;
    if hello[..8] != CLOCK_MAGIC {
        return Err("invalid clock calibration magic".to_owned());
    }
    let samples = u32::from_le_bytes(hello[8..12].try_into().expect("fixed-sized sample count"));
    if !(1..=10_000).contains(&samples) {
        return Err(format!("invalid clock sample count {samples}"));
    }
    for _ in 0..samples {
        let mut request = [0_u8; 8];
        stream.read_exact(&mut request).map_err(|err| err.to_string())?;
        let recv_ns = wall_nanos();
        let send_ns = wall_nanos();
        let mut response = [0_u8; 16];
        response[..8].copy_from_slice(&recv_ns.to_le_bytes());
        response[8..].copy_from_slice(&send_ns.to_le_bytes());
        stream.write_all(&response).map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn calibrate_clock(
    control: SocketAddr,
    samples: u32,
    timeout: Duration,
) -> BenchResult<ClockCalibration> {
    if samples == 0 {
        return Err("clock sample count must be nonzero".into());
    }
    let deadline = Instant::now() + timeout;
    let mut stream = loop {
        match TcpStream::connect_timeout(&control, Duration::from_millis(500)) {
            Ok(stream) => break stream,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => {
                return Err(format!("could not connect to clock endpoint {control}: {err}").into());
            }
        }
    };
    stream.set_nodelay(true)?;
    stream.write_all(&CLOCK_MAGIC)?;
    stream.write_all(&samples.to_le_bytes())?;
    let mut best: Option<ClockCalibration> = None;
    for _ in 0..samples {
        let t1 = wall_nanos();
        stream.write_all(&t1.to_le_bytes())?;
        let mut response = [0_u8; 16];
        stream.read_exact(&mut response)?;
        let t4 = wall_nanos();
        let t2 = u64::from_le_bytes(response[..8].try_into()?);
        let t3 = u64::from_le_bytes(response[8..].try_into()?);
        let rtt = t4.saturating_sub(t1).saturating_sub(t3.saturating_sub(t2));
        let offset = i128::midpoint(t2 as i128 - t1 as i128, t3 as i128 - t4 as i128);
        let sample = ClockCalibration { offset_ns: i64::try_from(offset)?, rtt_ns: rtt };
        if best.is_none_or(|current| sample.rtt_ns < current.rtt_ns) {
            best = Some(sample);
        }
    }
    best.ok_or_else(|| "clock calibration returned no samples".into())
}

fn validate_publisher(args: &PublisherArgs) -> BenchResult<()> {
    let multicast = multicast_config(args.udp_multicast_group, args.udp_multicast_interface)?;
    if matches!(args.transport, Transport::Tcp) && multicast.is_some() {
        return Err("UDP multicast options cannot be used with TCP".into());
    }
    if args.subscribers == 0 {
        return Err("subscriber count must be nonzero".into());
    }
    if args.payload_bytes < HEADER_SIZE {
        return Err(format!("payload must be at least {HEADER_SIZE} bytes").into());
    }
    if args.payload_bytes > 16 * 1024 * 1024 {
        return Err("payload exceeds the UDP benchmark maximum of 16 MiB".into());
    }
    if matches!(args.transport, Transport::Tcp) &&
        args.payload_bytes > flux_network::tcp::TcpStream::SEND_BUF_SIZE
    {
        return Err(format!(
            "TCP payload exceeds its {}-byte send buffer",
            flux_network::tcp::TcpStream::SEND_BUF_SIZE
        )
        .into());
    }
    if args.rate == 0 || args.burst == 0 || args.io_poll_every == 0 || args.end_markers == 0 {
        return Err("rate, burst, I/O poll interval, and end marker count must be nonzero".into());
    }
    if !args.duration_secs.is_finite() || args.duration_secs <= 0.0 {
        return Err("duration must be positive and finite".into());
    }
    if !args.warmup_secs.is_finite() || args.warmup_secs < 0.0 {
        return Err("warmup must be nonnegative and finite".into());
    }
    validate_interface(args.interface.as_deref())?;
    Ok(())
}

fn validate_subscriber(args: &SubscriberArgs) -> BenchResult<()> {
    let multicast = multicast_config(args.udp_multicast_group, args.udp_multicast_interface)?;
    if matches!(args.transport, Transport::Tcp) && multicast.is_some() {
        return Err("UDP multicast options cannot be used with TCP".into());
    }
    if !args.timeout_secs.is_finite() || args.timeout_secs <= 0.0 {
        return Err("timeout must be positive and finite".into());
    }
    if !args.drain_secs.is_finite() || args.drain_secs < 0.0 {
        return Err("drain must be nonnegative and finite".into());
    }
    if matches!(args.transport, Transport::Udp) && args.udp_repair_delay_ms == 0 {
        return Err("UDP repair delay must be nonzero".into());
    }
    validate_interface(args.interface.as_deref())?;
    Ok(())
}

fn multicast_config(
    group: Option<SocketAddrV4>,
    interface: Option<Ipv4Addr>,
) -> BenchResult<Option<UdpMulticastConfig>> {
    match (group, interface) {
        (None, None) => Ok(None),
        (Some(group), Some(interface)) => Ok(Some(UdpMulticastConfig::new(group, interface))),
        _ => Err("UDP multicast group and interface must be provided together".into()),
    }
}

fn parse_mac(value: &str) -> Result<[u8; 6], String> {
    let mut address = [0_u8; 6];
    let mut parts = value.split(':');
    for byte in &mut address {
        let part = parts.next().ok_or_else(|| format!("invalid MAC address: {value}"))?;
        *byte =
            u8::from_str_radix(part, 16).map_err(|_| format!("invalid MAC address: {value}"))?;
    }
    if parts.next().is_some() {
        return Err(format!("invalid MAC address: {value}"));
    }
    Ok(address)
}

fn parse_xdp_unicast_destination(value: &str) -> Result<UdpXdpUnicastDestination, String> {
    let (address, mac) = value
        .split_once('@')
        .ok_or_else(|| format!("invalid AF_XDP destination (expected IP:port@MAC): {value}"))?;
    let address = address
        .parse::<SocketAddrV4>()
        .map_err(|_| format!("invalid AF_XDP IPv4 destination: {address}"))?;
    Ok(UdpXdpUnicastDestination::new(address, parse_mac(mac)?))
}

fn validate_interface(interface: Option<&str>) -> BenchResult<()> {
    if let Some(interface) = interface &&
        !interface
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
    {
        return Err(format!("invalid interface name {interface:?}").into());
    }
    Ok(())
}

fn port_after(addr: SocketAddr) -> BenchResult<SocketAddr> {
    let port = addr.port().checked_add(1).ok_or("data port 65535 has no following control port")?;
    Ok(SocketAddr::new(addr.ip(), port))
}

fn wall_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos() as u64
}

fn process_cpu_ns() -> u64 {
    let mut value = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &raw mut value) };
    if result != 0 {
        return 0;
    }
    value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64
}

fn scheduler_snapshot() -> SchedulerSnapshot {
    let mut snapshot = SchedulerSnapshot::default();
    if let Ok(raw) = std::fs::read_to_string("/proc/self/schedstat") {
        let mut fields = raw.split_whitespace().filter_map(|field| field.parse::<u64>().ok());
        snapshot.runtime_ns = fields.next().unwrap_or(0);
        snapshot.runqueue_wait_ns = fields.next().unwrap_or(0);
        snapshot.timeslices = fields.next().unwrap_or(0);
    }
    if let Ok(raw) = std::fs::read_to_string("/proc/self/status") {
        for line in raw.lines() {
            let Some((key, value)) = line.split_once(':') else { continue };
            let Ok(value) = value.trim().parse::<u64>() else { continue };
            match key {
                "voluntary_ctxt_switches" => snapshot.voluntary_context_switches = value,
                "nonvoluntary_ctxt_switches" => snapshot.involuntary_context_switches = value,
                _ => {}
            }
        }
    }
    snapshot
}

fn scheduler_delta(before: SchedulerSnapshot, after: SchedulerSnapshot) -> Value {
    json!({
        "runtime_ns": after.runtime_ns.saturating_sub(before.runtime_ns),
        "runqueue_wait_ns": after.runqueue_wait_ns.saturating_sub(before.runqueue_wait_ns),
        "timeslices": after.timeslices.saturating_sub(before.timeslices),
        "voluntary_context_switches": after
            .voluntary_context_switches
            .saturating_sub(before.voluntary_context_switches),
        "involuntary_context_switches": after
            .involuntary_context_switches
            .saturating_sub(before.involuntary_context_switches),
    })
}

fn cpu_core_snapshot() -> Option<CpuCoreSnapshot> {
    let cpu = unsafe { libc::sched_getcpu() };
    if cpu < 0 {
        return None;
    }
    let raw = std::fs::read_to_string("/proc/stat").ok()?;
    let prefix = format!("cpu{cpu}");
    let line = raw.lines().find(|line| line.split_whitespace().next() == Some(&prefix))?;
    let mut ticks = [0; 10];
    for (target, value) in ticks
        .iter_mut()
        .zip(line.split_whitespace().skip(1).filter_map(|field| field.parse::<u64>().ok()))
    {
        *target = value;
    }
    Some(CpuCoreSnapshot { cpu, ticks })
}

fn cpu_core_delta(before: Option<CpuCoreSnapshot>, after: Option<CpuCoreSnapshot>) -> Value {
    let (Some(before), Some(after)) = (before, after) else { return Value::Null };
    if before.cpu != after.cpu {
        return json!({"cpu_before": before.cpu, "cpu_after": after.cpu, "migrated": true});
    }
    let mut delta = [0; 10];
    for (index, value) in delta.iter_mut().enumerate() {
        *value = after.ticks[index].saturating_sub(before.ticks[index]);
    }
    json!({
        "cpu": before.cpu,
        "total_ticks": delta.iter().sum::<u64>(),
        "user_ticks": delta[0],
        "nice_ticks": delta[1],
        "system_ticks": delta[2],
        "idle_ticks": delta[3],
        "iowait_ticks": delta[4],
        "irq_ticks": delta[5],
        "softirq_ticks": delta[6],
        "steal_ticks": delta[7],
        "guest_ticks": delta[8],
        "guest_nice_ticks": delta[9],
        "migrated": false,
    })
}

const SOFTIRQ_NAMES: [&str; 10] =
    ["HI", "TIMER", "NET_TX", "NET_RX", "BLOCK", "IRQ_POLL", "TASKLET", "SCHED", "HRTIMER", "RCU"];

fn softirq_snapshot() -> Option<SoftirqSnapshot> {
    let cpu = unsafe { libc::sched_getcpu() };
    if cpu < 0 {
        return None;
    }
    let raw = std::fs::read_to_string("/proc/softirqs").ok()?;
    let mut counts = [0; 10];
    for line in raw.lines().skip(1) {
        let Some((name, values)) = line.split_once(':') else { continue };
        let Some(index) = SOFTIRQ_NAMES.iter().position(|candidate| *candidate == name.trim())
        else {
            continue;
        };
        counts[index] = values
            .split_whitespace()
            .nth(cpu as usize)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
    }
    Some(SoftirqSnapshot { cpu, counts })
}

fn softirq_delta(before: Option<SoftirqSnapshot>, after: Option<SoftirqSnapshot>) -> Value {
    let (Some(before), Some(after)) = (before, after) else { return Value::Null };
    if before.cpu != after.cpu {
        return json!({"cpu_before": before.cpu, "cpu_after": after.cpu, "migrated": true});
    }
    let mut result = Map::new();
    for (index, name) in SOFTIRQ_NAMES.iter().enumerate() {
        result.insert(
            (*name).to_owned(),
            Value::from(after.counts[index].saturating_sub(before.counts[index])),
        );
    }
    Value::Object(result)
}

fn kernel_snapshot(interface: Option<&str>) -> HashMap<String, u64> {
    let mut values = parse_proc_net_snmp();
    if let Some(interface) = interface {
        for stat in [
            "rx_packets",
            "rx_bytes",
            "rx_dropped",
            "rx_errors",
            "tx_packets",
            "tx_bytes",
            "tx_dropped",
            "tx_errors",
        ] {
            let path = format!("/sys/class/net/{interface}/statistics/{stat}");
            if let Ok(raw) = std::fs::read_to_string(path) &&
                let Ok(value) = u64::from_str(raw.trim())
            {
                values.insert(format!("Nic.{stat}"), value);
            }
        }
    }
    values
}

fn parse_proc_net_snmp() -> HashMap<String, u64> {
    let Ok(raw) = std::fs::read_to_string(Path::new("/proc/net/snmp")) else {
        return HashMap::new();
    };
    let mut values = HashMap::new();
    let mut lines = raw.lines();
    while let (Some(header), Some(data)) = (lines.next(), lines.next()) {
        let Some((header_proto, header_fields)) = header.split_once(':') else { continue };
        let Some((data_proto, data_values)) = data.split_once(':') else { continue };
        if header_proto != data_proto {
            continue;
        }
        for (field, value) in header_fields.split_whitespace().zip(data_values.split_whitespace()) {
            if let Ok(value) = u64::from_str(value) {
                values.insert(format!("{header_proto}.{field}"), value);
            }
        }
    }
    values
}

fn snapshot_delta(before: &HashMap<String, u64>, after: &HashMap<String, u64>) -> Value {
    let mut result = Map::new();
    for (key, after_value) in after {
        if let Some(before_value) = before.get(key) {
            let delta = after_value.saturating_sub(*before_value);
            if delta != 0 ||
                key.contains("Errors") ||
                key.contains("dropped") ||
                key.contains("Retrans")
            {
                result.insert(key.clone(), Value::from(delta));
            }
        }
    }
    Value::Object(result)
}

fn main() -> BenchResult<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Publisher(args) => run_publisher(&args),
        Command::Subscriber(args) => run_subscriber(&args),
    }
}
