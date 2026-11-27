//! Forge storage engine.
//!
//! An acknowledged write has been fsynced. Other readers see it only after that
//! fsync. If fsync returns an error the store stops acknowledging writes.

mod bloom;
mod checksum;
mod clock;
mod disk;
mod error;
mod failpoint;
mod gate;
mod manifest;
mod memtable;
mod record;
mod sstable;
mod store;
mod syncfile;
mod util;
mod wal;

pub use clock::{Clock, ManualClock, SystemClock};
pub use memtable::Memtable;
pub use disk::{Disk, Faults, Span, SyncRange};
pub use error::Error;
pub use failpoint::Failpoint;
pub use gate::Gate;
pub use record::{OpKind, Prepared, Record, Reply, UserOp, ValueRec};
pub use store::{ScanPage, Stats, Store, StoreOpts, WriteAck};
pub use syncfile::{durable_sync, fullfsync_calls};
pub use util::{glob_match, hex_decode, hex_encode};
pub use wal::expiry_of;

pub fn poll_backend() -> &'static str {
    if cfg!(target_os = "macos") {
        "kqueue"
    } else if cfg!(target_os = "linux") {
        "epoll"
    } else {
        "mio"
    }
}
