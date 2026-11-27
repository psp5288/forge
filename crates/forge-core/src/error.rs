use std::fmt;

/// Failures a caller can distinguish. `Crashed` is a simulated process death:
/// the in-memory store must not be used again. `Fatal` means fsync failed and
/// the store has stopped acknowledging writes.
#[derive(Debug, Clone)]
pub enum Error {
    Io(String),
    Corrupt(String),
    DiskFull(String),
    Fatal(String),
    Stopped,
    Crashed {
        point: &'static str,
        op_acked: bool,
        seq: u64,
    },
    Backpressure,
}

impl Error {
    pub fn crashed(point: &'static str, op_acked: bool, seq: u64) -> Self {
        Self::Crashed {
            point,
            op_acked,
            seq,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(s) => write!(f, "io: {s}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::DiskFull(s) => write!(f, "disk full: {s}"),
            Self::Fatal(s) => write!(f, "fatal: {s}"),
            Self::Stopped => write!(f, "stopped"),
            Self::Crashed {
                point,
                op_acked,
                seq,
            } => write!(f, "crashed at {point} (op_acked={op_acked} seq={seq})"),
            Self::Backpressure => write!(f, "backpressure"),
        }
    }
}

impl std::error::Error for Error {}

pub fn map_io(err: std::io::Error) -> Error {
    if err.raw_os_error() == Some(28) {
        Error::DiskFull(err.to_string())
    } else {
        Error::Io(err.to_string())
    }
}
