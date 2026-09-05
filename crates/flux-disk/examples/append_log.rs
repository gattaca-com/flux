//! Minimal `flux-disk` walkthrough: `cargo run -p flux-disk --example
//! append_log`.
//!
//! Shows the poll-driven pattern `DiskIo` shares with `flux-network`'s
//! `TcpNetwork`: `open`/`write_with`/`sync_all`/`close` queue work and return
//! immediately — operations submitted before a file finishes opening are
//! simply queued — and `poll_with` delivers completions as they arrive from
//! `io_uring`, never blocking.

use std::io::Write as _;

use flux_disk::{DiskEvent, DiskIo, OpenOptions};

fn main() {
    let path = std::env::temp_dir().join("flux_disk_example.log");
    let mut disk = DiskIo::default();

    let options = OpenOptions::new().write(true).create(true).truncate(true);
    let file = disk.open(&path, options).expect("options and path are valid");
    for line in 0..10 {
        disk.write_with(file, |buf| writeln!(buf, "line {line}").unwrap());
    }
    let sync_id = disk.sync_all(file).expect("file is available");
    disk.close(file);

    let mut done = false;
    while !done {
        disk.poll_with(|event| match event {
            DiskEvent::Opened { .. } => println!("opened {} for writing", path.display()),
            DiskEvent::Written { offset, len, .. } => {
                println!("wrote {len} bytes at offset {offset}");
            }
            DiskEvent::Synced { operation_id, .. } => {
                assert_eq!(operation_id, sync_id);
                println!("synced operation {}", operation_id.get());
            }
            DiskEvent::Closed { .. } => {
                println!("closed after writing");
                done = true;
            }
            DiskEvent::Failed { op, error, .. } => panic!("write phase failed: {op:?}: {error}"),
            DiskEvent::Read { .. } | DiskEvent::Truncated { .. } | DiskEvent::Renamed { .. } => {
                unreachable!("no reads or structural changes issued in the write phase")
            }
        });
    }

    // Read the file back to show the completion side of the API.
    let file = disk.open(&path, OpenOptions::new().read(true)).expect("options and path are valid");
    disk.read_to_end(file, 0);
    disk.close(file);

    let mut done = false;
    while !done {
        disk.poll_with(|event| match event {
            DiskEvent::Opened { .. } => println!("opened {} for reading", path.display()),
            DiskEvent::Read { payload, eof, .. } => {
                print!("{}", String::from_utf8_lossy(payload));
                println!("(eof: {eof})");
            }
            DiskEvent::Closed { .. } => done = true,
            DiskEvent::Failed { op, error, .. } => panic!("read phase failed: {op:?}: {error}"),
            DiskEvent::Written { .. } |
            DiskEvent::Synced { .. } |
            DiskEvent::Truncated { .. } |
            DiskEvent::Renamed { .. } => {
                unreachable!("no writes or syncs issued in the read phase")
            }
        });
    }
}
