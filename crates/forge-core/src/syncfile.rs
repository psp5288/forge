use std::fs::File;
use std::io::{self, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Successful `F_FULLFSYNC` calls on macOS. Linux increments nothing here;
/// `durable_sync` uses `File::sync_all` there.
pub static FULLFSYNC_CALLS: AtomicU64 = AtomicU64::new(0);

pub fn fullfsync_calls() -> u64 {
    FULLFSYNC_CALLS.load(Ordering::Relaxed)
}

/// Make file data durable.
///
/// macOS `fsync` does not flush the drive cache. This calls `F_FULLFSYNC`.
/// `EINTR` is retried. Any other error is returned once; the caller must not
/// retry and continue acknowledging writes.
pub fn durable_sync(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let fd = file.as_raw_fd();
        let mut last = io::Error::from_raw_os_error(libc::EINTR);
        for _ in 0..8 {
            // SAFETY: fd is owned by `file` and F_FULLFSYNC does not close it.
            let rc = unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) };
            if rc == 0 {
                FULLFSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            last = io::Error::last_os_error();
            if last.raw_os_error() != Some(libc::EINTR) {
                return Err(last);
            }
        }
        Err(last)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (&FULLFSYNC_CALLS, file.as_raw_fd());
        file.sync_all()
    }
}

pub fn sync_dir(dir: &Path) -> io::Result<()> {
    let f = File::open(dir)?;
    f.sync_all()
}

pub fn write_all_handling_short<W: Write>(mut w: W, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        match w.write(data) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short write returned 0",
                ));
            }
            Ok(n) => data = &data[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
