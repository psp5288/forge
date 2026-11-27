use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Test hook. While closed, `sync_wal` waits and does not fsync.
#[derive(Debug)]
pub struct Gate {
    closed: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    pub fn open() -> Self {
        Self {
            closed: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    pub fn close(&self) {
        *self.closed.lock().unwrap() = true;
    }

    pub fn release(&self) {
        let mut g = self.closed.lock().unwrap();
        *g = false;
        self.cv.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }

    /// Returns true when the gate is open. Returns false if `stop` is set.
    pub fn wait_interruptible(&self, stop: &std::sync::atomic::AtomicBool) -> bool {
        let mut g = self.closed.lock().unwrap();
        while *g {
            if stop.load(std::sync::atomic::Ordering::Relaxed) {
                return false;
            }
            let (next, _) = self
                .cv
                .wait_timeout(g, Duration::from_millis(50))
                .unwrap();
            g = next;
        }
        true
    }
}
