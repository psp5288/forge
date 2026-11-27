//! mio event loop, RESP2, and the HTTP admin server.
//!
//! The event-loop thread never calls fsync. It prepares records and hands them
//! to the durability thread, which group-fsyncs and wakes the loop with mio's
//! `Waker`.

mod http;
mod resp;
mod server;

pub use server::{spawn, Config, ServerHandle};
