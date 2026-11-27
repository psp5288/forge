use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::FromRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge_core::{fullfsync_calls, Clock, Disk, Gate, ManualClock};
use forge_server::{spawn, Config};

static N: AtomicU64 = AtomicU64::new(0);

fn scratch() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "forge-srv-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

struct Running {
    dir: PathBuf,
    handle: forge_server::ServerHandle,
    keep: bool,
}

impl Drop for Running {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn boot(tune: impl FnOnce(&mut Config)) -> Running {
    let dir = scratch();
    let mut cfg = Config::new(dir.clone());
    cfg.redis_addr = "127.0.0.1:0".parse().unwrap();
    cfg.http_addr = "127.0.0.1:0".parse().unwrap();
    cfg.exit_on_fatal = false;
    tune(&mut cfg);
    let handle = spawn(cfg).unwrap();
    Running {
        dir,
        handle,
        keep: false,
    }
}

fn arr(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend(format!("${}\r\n", arg.len()).into_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn connect_tiny_recv(port: u16) -> TcpStream {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    assert!(fd >= 0, "socket");
    let recv: i32 = 1024;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &recv as *const i32 as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "{}", std::io::Error::last_os_error());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let SocketAddr::V4(v4) = addr else {
        unreachable!();
    };
    let mut raw: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    raw.sin_len = std::mem::size_of::<libc::sockaddr_in>() as u8;
    raw.sin_family = libc::AF_INET as libc::sa_family_t;
    raw.sin_port = v4.port().to_be();
    raw.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes(v4.ip().octets()),
    };
    let rc = unsafe {
        libc::connect(
            fd,
            &raw as *const libc::sockaddr_in as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        panic!("connect: {}", std::io::Error::last_os_error());
    }
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    stream.set_nodelay(true).unwrap();
    stream.set_nonblocking(true).unwrap();
    stream
}

fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => {
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                return stream;
            }
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Err(err) => panic!("connect {port}: {err}"),
        }
    }
}

fn read_for(stream: &mut TcpStream, wait: Duration) -> Vec<u8> {
    stream.set_read_timeout(Some(wait)).unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let deadline = Instant::now() + wait;
    while Instant::now() < deadline {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                if !buf.is_empty() {
                    break;
                }
                break;
            }
            Err(_) => break,
        }
    }
    buf
}

fn command(port: u16, args: &[&[u8]]) -> String {
    let mut stream = connect(port);
    stream.write_all(&arr(args)).unwrap();
    let bytes = read_for(&mut stream, Duration::from_secs(2));
    String::from_utf8_lossy(&bytes).into_owned()
}

fn http(port: u16, req: &str) -> String {
    let mut stream = connect(port);
    stream.write_all(req.as_bytes()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

fn metric(body: &str, name: &str) -> Option<u64> {
    let text = body.split("\r\n\r\n").nth(1).unwrap_or(body);
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(name) {
            return parts.next()?.parse().ok();
        }
    }
    None
}

fn wait_metric(port: u16, name: &str, pred: impl Fn(u64) -> bool) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut last = 0;
    while Instant::now() < deadline {
        let body = http(
            port,
            "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        if let Some(v) = metric(&body, name) {
            last = v;
            if pred(v) {
                return v;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("metric {name} last={last} never matched");
}

#[test]
fn pipeline_keeps_reply_order_and_commands_work() {
    let srv = boot(|_| {});
    let port = srv.handle.redis_port;
    let mut stream = connect(port);
    let mut bytes = Vec::new();
    bytes.extend(arr(&[b"PING"]));
    bytes.extend(arr(&[b"SET", b"a", b"1"]));
    bytes.extend(arr(&[b"SET", b"a", b"2"]));
    bytes.extend(arr(&[b"GET", b"a"]));
    bytes.extend(arr(&[b"DEL", b"missing"]));
    bytes.extend(arr(&[b"PING", b"hi"]));
    bytes.extend(arr(&[b"SELECT", b"0"]));
    bytes.extend(arr(&[b"INFO"]));
    bytes.extend(arr(&[b"COMMAND"]));
    stream.write_all(&bytes).unwrap();
    let got = String::from_utf8_lossy(&read_for(&mut stream, Duration::from_secs(2))).into_owned();
    assert!(
        got.starts_with("+PONG\r\n+OK\r\n+OK\r\n$1\r\n2\r\n:0\r\n$2\r\nhi\r\n+OK\r\n"),
        "{got:?}"
    );
    assert!(got.contains("server_name:forge"));
    assert!(got.contains("*0\r\n"), "{got:?}");
    assert_eq!(command(port, &[b"GET", b"a"]), "$1\r\n2\r\n");
}

#[test]
fn set_then_ping_waits_for_fsync_and_hides_the_write() {
    let gate = Arc::new(Gate::open());
    gate.close();
    let gate2 = Arc::clone(&gate);
    {
        let dir = scratch();
        let disk = Disk::real(&dir).unwrap();
        let srv = boot(move |cfg| {
            cfg.data = dir.clone();
            cfg.disk = Some(disk.clone());
            cfg.sync_gate = Some(gate2);
        });
        let redis = srv.handle.redis_port;
        let http_port = srv.handle.http_port;
        let mut writer = connect(redis);
        let mut bytes = arr(&[b"SET", b"k", b"v"]);
        bytes.extend(arr(&[b"PING"]));
        writer.write_all(&bytes).unwrap();
        let early = read_for(&mut writer, Duration::from_millis(200));
        assert!(
            early.is_empty(),
            "reply before fsync: {}",
            String::from_utf8_lossy(&early)
        );
        assert_eq!(command(redis, &[b"GET", b"k"]), "$-1\r\n");
        let inflight = wait_metric(http_port, "forge_fsync_inflight", |v| v == 1);
        assert_eq!(inflight, 1);
        gate.release();
        writer
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut got = Vec::new();
        let mut tmp = [0u8; 64];
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && !got.windows(12).any(|w| w == b"+OK\r\n+PONG\r\n") {
            match writer.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&tmp[..n]),
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        let text = String::from_utf8_lossy(&got);
        assert_eq!(text, "+OK\r\n+PONG\r\n", "{text:?}");
        assert_eq!(command(redis, &[b"GET", b"k"]), "$1\r\nv\r\n");
        let body = http(
            http_port,
            "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert!(body.contains("forge_fsync_on_durability_thread 1"), "{body}");
        assert!(body.contains("fsync_thread forge-durability"), "{body}");
        srv.handle.shutdown();
    };
}

#[test]
fn pipelined_sets_share_one_fsync() {
    let gate = Arc::new(Gate::open());
    gate.close();
    let held = Arc::clone(&gate);
    let dir = scratch();
    let disk = Disk::real(&dir).unwrap();
    let watched = disk.clone();
    let srv = boot(move |cfg| {
        cfg.disk = Some(disk);
        cfg.sync_gate = Some(held);
    });
    let mut writer = connect(srv.handle.redis_port);
    let mut bytes = Vec::new();
    bytes.extend(arr(&[b"SET", b"k", b"1"]));
    bytes.extend(arr(&[b"SET", b"k", b"2"]));
    bytes.extend(arr(&[b"SET", b"k", b"3"]));
    writer.write_all(&bytes).unwrap();
    let queued = wait_metric(srv.handle.http_port, "forge_wal_queued_records", |v| v == 3);
    assert_eq!(queued, 3);
    let before = watched.sync_ok();
    assert_eq!(
        command(srv.handle.redis_port, &[b"GET", b"k"]),
        "$-1\r\n"
    );
    gate.release();
    let got = read_for(&mut writer, Duration::from_secs(2));
    assert_eq!(got, b"+OK\r\n+OK\r\n+OK\r\n");
    assert_eq!(
        watched.sync_ok(),
        before + 1,
        "pipeline was not one fsync group"
    );
    assert_eq!(command(srv.handle.redis_port, &[b"GET", b"k"]), "$1\r\n3\r\n");
    let _ = dir;
}

#[test]
fn ping_is_not_held_when_no_write_precedes_it() {
    let gate = Arc::new(Gate::open());
    gate.close();
    let held = Arc::clone(&gate);
    let srv = boot(move |cfg| {
        cfg.sync_gate = Some(held);
    });
    let mut writer = connect(srv.handle.redis_port);
    let mut bytes = arr(&[b"PING"]);
    bytes.extend(arr(&[b"SET", b"k", b"v"]));
    writer.write_all(&bytes).unwrap();
    let early = read_for(&mut writer, Duration::from_millis(500));
    assert_eq!(early, b"+PONG\r\n", "ping should not wait on a later set");
    gate.release();
    let rest = read_for(&mut writer, Duration::from_secs(2));
    assert_eq!(rest, b"+OK\r\n");
}

#[test]
fn backpressure_stops_reading_when_the_wal_queue_is_full() {
    let gate = Arc::new(Gate::open());
    gate.close();
    let held = Arc::clone(&gate);
    let srv = boot(move |cfg| {
        cfg.sync_gate = Some(held);
        cfg.wal_queue_records = 2;
        cfg.inbound_cap = 512;
        cfg.outbound_cap = 1024;
    });
    let mut writer = connect(srv.handle.redis_port);
    writer.set_nonblocking(true).unwrap();
    let mut pending = Vec::new();
    for i in 0..40 {
        pending.extend(arr(&[b"SET", format!("k{i}").as_bytes(), b"v"]));
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while !pending.is_empty() && Instant::now() < deadline {
        match writer.write(&pending) {
            Ok(0) => break,
            Ok(n) => {
                pending.drain(..n);
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::Interrupted =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("{err}"),
        }
    }
    let queued = wait_metric(srv.handle.http_port, "forge_wal_queued_records", |v| v == 2);
    assert_eq!(queued, 2);
    std::thread::sleep(Duration::from_millis(80));
    let body = http(
        srv.handle.http_port,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(metric(&body, "forge_wal_queued_records"), Some(2), "{body}");
    let inbound = metric(&body, "forge_inbound_bytes").unwrap();
    assert!(inbound <= 512, "inbound grew to {inbound}\n{body}");
    assert!(
        metric(&body, "forge_read_paused").unwrap() >= 1,
        "{body}"
    );
    gate.release();
    writer.set_nonblocking(false).unwrap();
    writer
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let _ = read_for(&mut writer, Duration::from_secs(2));
    assert_eq!(command(srv.handle.redis_port, &[b"PING"]), "+PONG\r\n");
}

#[test]
fn slow_reader_caps_outbound() {
    let srv = boot(|cfg| {
        cfg.outbound_cap = 64;
        cfg.inbound_cap = 256;
        cfg.sndbuf = Some(1024);
    });
    let mut writer = connect_tiny_recv(srv.handle.redis_port);
    let mut pending = Vec::new();
    let payload = vec![b'x'; 200];
    for _ in 0..200 {
        pending.extend(arr(&[b"PING", &payload]));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while !pending.is_empty() && Instant::now() < deadline {
        match writer.write(&pending) {
            Ok(0) => break,
            Ok(n) => {
                pending.drain(..n);
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("{err}"),
        }
    }
    std::thread::sleep(Duration::from_millis(200));
    let body = http(
        srv.handle.http_port,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    let outbound = metric(&body, "forge_outbound_bytes").unwrap_or(0);
    let inbound = metric(&body, "forge_inbound_bytes").unwrap_or(0);
    let peak = metric(&body, "forge_outbound_peak").unwrap_or(0);
    assert!(outbound <= 64 + 220, "outbound grew\n{body}");
    assert!(inbound <= 256, "inbound grew\n{body}");
    assert!(peak > 0, "no replies were buffered\n{body}");
    assert!(
        peak <= 64 + 220,
        "reply buffer grew without a cap\n{body}"
    );
    let _ = writer;
}

#[test]
fn oversize_requests_are_rejected() {
    let srv = boot(|cfg| {
        cfg.max_bulk = 32;
        cfg.http_limit = 128;
    });
    let mut stream = connect(srv.handle.redis_port);
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$100\r\n")
        .unwrap();
    let got = read_for(&mut stream, Duration::from_secs(1));
    let text = String::from_utf8_lossy(&got);
    assert!(text.starts_with("-ERR"), "{text:?}");
    assert_eq!(command(srv.handle.redis_port, &[b"PING"]), "+PONG\r\n");

    let huge = format!("GET /health HTTP/1.1\r\nHost: {}\r\n\r\n", "x".repeat(400));
    let body = http(srv.handle.http_port, &huge);
    assert!(body.starts_with("HTTP/1.1 413"), "{body}");
    let post = http(
        srv.handle.http_port,
        "POST /health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
    );
    assert!(post.starts_with("HTTP/1.1 405"), "{post}");
}

#[test]
fn http_health_metrics_and_key_lookup() {
    let srv = boot(|_| {});
    let health = http(
        srv.handle.http_port,
        "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    assert!(health.contains("ok\n"), "{health}");
    assert_eq!(
        command(srv.handle.redis_port, &[b"SET", b"a b", b"z"]),
        "+OK\r\n"
    );
    let key = http(
        srv.handle.http_port,
        "GET /key/a%20b HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(key.contains("\r\n\r\nz\n"), "{key}");
    let missing = http(
        srv.handle.http_port,
        "GET /key/nope HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");
}

#[test]
fn scan_ttl_expire_and_del_over_tcp() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let clock_bg: Arc<dyn Clock> = Arc::clone(&clock) as Arc<dyn Clock>;
    let srv = boot(move |cfg| {
        cfg.clock = clock_bg;
    });
    let port = srv.handle.redis_port;
    assert_eq!(command(port, &[b"SET", b"k", b"v", b"EX", b"5"]), "+OK\r\n");
    assert_eq!(command(port, &[b"TTL", b"k"]), ":5\r\n");
    assert_eq!(command(port, &[b"SET", b"keep", b"yes"]), "+OK\r\n");
    assert_eq!(command(port, &[b"EXPIRE", b"missing", b"1"]), ":0\r\n");
    assert_eq!(command(port, &[b"DEL", b"keep"]), ":1\r\n");
    assert_eq!(command(port, &[b"GET", b"keep"]), "$-1\r\n");
    assert_eq!(command(port, &[b"SET", b"b", b"2"]), "+OK\r\n");
    assert_eq!(command(port, &[b"SET", b"c", b"3"]), "+OK\r\n");
    let page = command(port, &[b"SCAN", b"0", b"COUNT", b"2"]);
    assert!(page.starts_with("*2\r\n"), "{page}");
    clock.advance(6_000);
    assert_eq!(command(port, &[b"GET", b"k"]), "$-1\r\n");
    assert_eq!(command(port, &[b"TTL", b"k"]), ":-2\r\n");
    let rest = command(port, &[b"SCAN", b"0", b"MATCH", b"b*"]);
    assert!(rest.contains("$1\r\nb\r\n"), "{rest}");
}

#[test]
fn parsers_do_not_panic_on_short_or_random_input() {
    let srv = boot(|_| {});
    let port = srv.handle.redis_port;
    let good = arr(&[b"GET", b"a"]);
    let mut stream = connect(port);
    for byte in good {
        stream.write_all(&[byte]).unwrap();
    }
    assert_eq!(
        String::from_utf8_lossy(&read_for(&mut stream, Duration::from_secs(1))),
        "$-1\r\n"
    );
    let mut state = 0x1234_5678u64;
    for i in 0..250 {
        let mut junk = connect(port);
        junk.set_write_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let len = (state % 48) as usize + 1;
        let mut buf = vec![0u8; len];
        for b in &mut buf {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (state >> 33) as u8;
        }
        if i % 17 == 0 {
            buf.splice(0..0, b"*1\r\n$4\r\nPING".to_vec());
        }
        let _ = junk.write_all(&buf);
        let _ = read_for(&mut junk, Duration::from_millis(30));
    }
    assert_eq!(command(port, &[b"PING"]), "+PONG\r\n");
}

#[test]
fn macos_server_uses_fullfsync() {
    let before = fullfsync_calls();
    let srv = boot(|_| {});
    assert_eq!(
        command(srv.handle.redis_port, &[b"SET", b"k", b"v"]),
        "+OK\r\n"
    );
    assert!(
        fullfsync_calls() > before,
        "SET did not call F_FULLFSYNC"
    );
}

#[test]
fn visibility_observations_come_from_acked_writes() {
    let mut srv = boot(|_| {});
    let port = srv.handle.redis_port;
    let mut acked = Vec::new();
    for i in 0..20 {
        let val = format!("v{i}");
        let reply = command(port, &[b"SET", b"k", val.as_bytes()]);
        assert_eq!(reply, "+OK\r\n");
        acked.push(val);
        let seen = command(port, &[b"GET", b"k"]);
        assert!(
            seen.contains(acked.last().unwrap()),
            "observed {seen} not in {acked:?}"
        );
    }
    let dir = srv.dir.clone();
    srv.keep = true;
    drop(srv);
    let mut cfg = Config::new(dir.clone());
    cfg.redis_addr = "127.0.0.1:0".parse().unwrap();
    cfg.http_addr = "127.0.0.1:0".parse().unwrap();
    cfg.exit_on_fatal = false;
    let again = spawn(cfg).unwrap();
    let current = command(again.redis_port, &[b"GET", b"k"]);
    assert_eq!(current, "$3\r\nv19\r\n");
    for value in &acked {
        assert!(
            acked.iter().any(|v| v == value),
            "{value} was observed without an ack"
        );
    }
    drop(again);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn sigkill_keeps_acked_keys_and_reopens() {
    let dir = scratch();
    let bin = env!("CARGO_BIN_EXE_forge-server");
    let (mut child, redis, _http) = start_bin(bin, &dir);
    let mut stream = connect(redis);
    for i in 0..20 {
        stream
            .write_all(&arr(&[b"SET", format!("k{i}").as_bytes(), b"v"]))
            .unwrap();
        let got = read_for(&mut stream, Duration::from_secs(2));
        assert_eq!(got, b"+OK\r\n", "key {i}");
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success());
    let (mut child, redis, _) = start_bin(bin, &dir);
    for i in 0..20 {
        assert_eq!(
            command(redis, &[b"GET", format!("k{i}").as_bytes()]),
            "$1\r\nv\r\n",
            "acked key {i} missing after SIGKILL"
        );
    }
    stream = connect(redis);
    stream
        .write_all(&arr(&[b"SET", b"late", b"x"]))
        .unwrap();
    child.kill().unwrap();
    let _ = child.wait();
    let (mut child, redis, _) = start_bin(bin, &dir);
    assert_eq!(command(redis, &[b"PING"]), "+PONG\r\n");
    assert_eq!(command(redis, &[b"GET", b"k0"]), "$1\r\nv\r\n");
    child.kill().unwrap();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn failpoint_after_fsync_keeps_the_key_without_an_ok() {
    let dir = scratch();
    let bin = env!("CARGO_BIN_EXE_forge-server");
    let (mut child, redis, _) = start_bin_fail(bin, &dir, "after_fsync_before_ack");
    let mut stream = connect(redis);
    stream
        .write_all(&arr(&[b"SET", b"k", b"kept"]))
        .unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(77), "{status}");
    let early = read_for(&mut stream, Duration::from_millis(200));
    assert_ne!(early, b"+OK\r\n");
    let (mut child, redis, _) = start_bin(bin, &dir);
    assert_eq!(command(redis, &[b"GET", b"k"]), "$4\r\nkept\r\n");
    child.kill().unwrap();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn failpoint_after_wal_append_exits_and_reopens() {
    let dir = scratch();
    let bin = env!("CARGO_BIN_EXE_forge-server");
    let (mut child, redis, _) = start_bin_fail(bin, &dir, "after_wal_append");
    let mut stream = connect(redis);
    let _ = stream.write_all(&arr(&[b"SET", b"k", b"maybe"]));
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(77), "{status}");
    let (mut child, redis, _) = start_bin(bin, &dir);
    assert_eq!(command(redis, &[b"PING"]), "+PONG\r\n");
    child.kill().unwrap();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}

fn start_bin(bin: &str, dir: &PathBuf) -> (std::process::Child, u16, u16) {
    launch(bin, dir, None)
}

fn start_bin_fail(bin: &str, dir: &PathBuf, point: &str) -> (std::process::Child, u16, u16) {
    launch(bin, dir, Some(point))
}

fn launch(bin: &str, dir: &PathBuf, fail: Option<&str>) -> (std::process::Child, u16, u16) {
    let mut cmd = Command::new(bin);
    cmd.arg("--data")
        .arg(dir)
        .arg("--redis")
        .arg("127.0.0.1:0")
        .arg("--http")
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(point) = fail {
        cmd.env("FORGE_FAILPOINT", point);
    }
    let mut child = cmd.spawn().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut line = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let mut byte = [0u8; 1];
        match stdout.read(&mut byte) {
            Ok(1) => {
                if byte[0] == b'\n' {
                    break;
                }
                line.push(byte[0]);
            }
            Ok(0) => break,
            Ok(_) => line.push(byte[0]),
            Err(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }
    let text = String::from_utf8_lossy(&line);
    let redis = text
        .split("redis=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("ready line: {text}"));
    let http = text
        .split("http=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("ready line: {text}"));
    (child, redis, http)
}
