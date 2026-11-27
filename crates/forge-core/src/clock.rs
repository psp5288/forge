use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Time source for TTL. Tests inject a manual clock. Expiry is stored as an
/// absolute millisecond timestamp; replay does not recompute it.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    ms: AtomicU64,
}

impl ManualClock {
    pub fn new(ms: u64) -> Self {
        Self {
            ms: AtomicU64::new(ms),
        }
    }

    pub fn set(&self, ms: u64) {
        self.ms.store(ms, Ordering::Relaxed);
    }

    pub fn advance(&self, by: u64) {
        self.ms.fetch_add(by, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.ms.load(Ordering::Relaxed)
    }
}
