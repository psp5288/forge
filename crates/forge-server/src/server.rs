use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use forge_core::{
    hex_decode, hex_encode, Clock, Disk, Error, Failpoint, Gate, Memtable, Record, Reply, Store,
    StoreOpts, SystemClock, UserOp,
};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};

use crate::http;
use crate::resp;

const REDIS: Token = Token(0);
const HTTP: Token = Token(1);
const WAKE: Token = Token(2);

pub struct Config {
    pub data: PathBuf,
    pub redis_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub memtable_bytes: usize,
    pub wal_rotate_bytes: u64,
    pub compact_threshold: usize,
    pub outbound_cap: usize,
    pub inbound_cap: usize,
    pub wal_queue_records: usize,
    pub wal_queue_bytes: usize,
    pub max_bulk: usize,
    pub http_limit: usize,
    pub sync_gate: Option<Arc<Gate>>,
    pub clock: Arc<dyn Clock>,
    pub disk: Option<Disk>,
    pub failpoint: Option<Failpoint>,
    pub exit_on_failpoint: bool,
    pub exit_on_fatal: bool,
    /// Optional `SO_SNDBUF`. Tests set a small buffer so a slow reader fills it.
    pub sndbuf: Option<i32>,
}

impl Config {
    pub fn new(data: PathBuf) -> Self {
        Self {
            data,
            redis_addr: "127.0.0.1:6379".parse().unwrap(),
            http_addr: "127.0.0.1:8080".parse().unwrap(),
            memtable_bytes: 4 * 1024 * 1024,
            wal_rotate_bytes: 1024 * 1024,
            compact_threshold: 4,
            outbound_cap: 1024 * 1024,
            inbound_cap: 1024 * 1024,
            wal_queue_records: 8192,
            wal_queue_bytes: 32 * 1024 * 1024,
            max_bulk: 8 * 1024 * 1024,
            http_limit: 16 * 1024,
            sync_gate: None,
            clock: Arc::new(SystemClock),
            disk: None,
            failpoint: None,
            exit_on_failpoint: false,
            exit_on_fatal: true,
            sndbuf: None,
        }
    }
}

pub struct ServerHandle {
    pub redis_port: u16,
    pub http_port: u16,
    shutdown: Arc<AtomicBool>,
    store: Store,
    waker: Arc<Waker>,
    thread: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.store.request_stop();
        let _ = self.waker.wake();
    }

    /// Block until the server thread exits. Used by the binary.
    pub fn wait(mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.shutdown();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

struct Shared {
    queued_records: AtomicUsize,
    queued_bytes: AtomicUsize,
    commands: AtomicU64,
    peak_out: AtomicUsize,
}

pub fn spawn(config: Config) -> io::Result<ServerHandle> {
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("forge-loop".into())
        .spawn(move || {
            if let Err(err) = run(config, tx) {
                eprintln!("forge-server: {err}");
            }
        })
        .map_err(|e| io::Error::other(e.to_string()))?;
    let ready = rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(ServerHandle {
        redis_port: ready.redis_port,
        http_port: ready.http_port,
        shutdown: ready.shutdown,
        store: ready.store,
        waker: ready.waker,
        thread: Some(thread),
    })
}

struct Ready {
    redis_port: u16,
    http_port: u16,
    shutdown: Arc<AtomicBool>,
    store: Store,
    waker: Arc<Waker>,
}

#[derive(Clone)]
struct WalItem {
    ticket: u64,
    token: Token,
    record: Record,
    reply: Vec<u8>,
    bytes: usize,
}

enum Msg {
    Writes(Vec<WalItem>),
    Flush(Arc<Memtable>),
    Stop,
}

enum Done {
    Ack(Vec<WalItem>),
    Failed { items: Vec<WalItem>, err: String },
    Fatal(String),
}

enum Kind {
    Redis,
    Http,
}

enum Cmd {
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Relative seconds. `None` means the key does not expire.
        ex_secs: Option<u64>,
    },
    Del(Vec<u8>),
    Expire {
        key: Vec<u8>,
        secs: u64,
    },
    Ttl(Vec<u8>),
    Ping(Option<Vec<u8>>),
    Scan {
        cursor: Vec<u8>,
        pattern: Option<Vec<u8>>,
        count: usize,
    },
    Dump(Vec<u8>),
    Quit,
    Select(i64),
    Info,
    Command,
    Error(String),
}

enum Slot {
    Write { ticket: u64, reply: Option<Vec<u8>> },
    Blocked(Cmd),
    Ready(Vec<u8>),
}

struct Conn {
    stream: TcpStream,
    kind: Kind,
    inbound: Vec<u8>,
    outbound: Vec<u8>,
    pending: VecDeque<Slot>,
    held: Option<Cmd>,
    pause_read: bool,
    hold_parse: bool,
    registered: bool,
    close_after_write: bool,
}

struct Server {
    poll: Poll,
    redis: TcpListener,
    http: TcpListener,
    store: Store,
    clock: Arc<dyn Clock>,
    cfg: Config,
    shared: Arc<Shared>,
    conns: HashMap<Token, Conn>,
    next_token: usize,
    next_ticket: u64,
    wal_tx: Sender<Msg>,
    done_rx: Receiver<Done>,
    /// Writes parsed together. Submitted once so one pipeline is one fsync group.
    batch: Vec<WalItem>,
    exit_code: Option<i32>,
}

fn run(mut config: Config, ready_tx: Sender<Ready>) -> io::Result<()> {
    // Ignore SIGPIPE so a client disconnect is an error, not a process death.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let disk = match config.disk.take() {
        Some(disk) => disk,
        None => Disk::real(&config.data)?,
    };
    let opts = StoreOpts {
        memtable_bytes: config.memtable_bytes,
        compact_threshold: config.compact_threshold,
        wal_rotate_bytes: config.wal_rotate_bytes,
        exit_on_failpoint: config.exit_on_failpoint,
        sync_gate: config.sync_gate.clone(),
    };
    let store = Store::open(disk, Arc::clone(&config.clock), opts).map_err(io_err)?;
    if let Some(point) = config.failpoint {
        store.arm(point);
    }
    store.set_loop_thread();

    let poll = Poll::new()?;
    let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
    let mut redis = TcpListener::bind(config.redis_addr)?;
    let mut http = TcpListener::bind(config.http_addr)?;
    poll.registry()
        .register(&mut redis, REDIS, Interest::READABLE)?;
    poll.registry()
        .register(&mut http, HTTP, Interest::READABLE)?;
    let redis_port = redis.local_addr()?.port();
    let http_port = http.local_addr()?.port();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shared = Arc::new(Shared {
        queued_records: AtomicUsize::new(0),
        queued_bytes: AtomicUsize::new(0),
        commands: AtomicU64::new(0),
        peak_out: AtomicUsize::new(0),
    });
    let (wal_tx, wal_rx) = mpsc::channel::<Msg>();
    let (done_tx, done_rx) = mpsc::channel::<Done>();
    let store_bg = store.clone();
    let waker_bg = Arc::clone(&waker);
    let exit_on_fatal = config.exit_on_fatal;
    let durable = std::thread::Builder::new()
        .name("forge-durability".into())
        .spawn(move || durability_loop(store_bg, wal_rx, done_tx, waker_bg))
        .map_err(|e| io::Error::other(e.to_string()))?;

    let _ = ready_tx.send(Ready {
        redis_port,
        http_port,
        shutdown: Arc::clone(&shutdown),
        store: store.clone(),
        waker: Arc::clone(&waker),
    });

    let mut server = Server {
        poll,
        redis,
        http,
        store,
        clock: Arc::clone(&config.clock),
        cfg: config,
        shared,
        conns: HashMap::new(),
        next_token: 10,
        next_ticket: 1,
        wal_tx,
        done_rx,
        batch: Vec::new(),
        exit_code: None,
    };
    server.event_loop(shutdown)?;
    let code = server.exit_code;
    drop(server);
    let _ = durable.join();
    if let Some(code) = code {
        if exit_on_fatal {
            eprintln!("forge-server exiting after fatal fsync");
            std::process::exit(code);
        }
    }
    Ok(())
}

fn durability_loop(store: Store, rx: Receiver<Msg>, done_tx: Sender<Done>, waker: Arc<Waker>) {
    loop {
        let msg = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => break,
        };
        if !handle_msg(&store, msg, &rx, &done_tx, &waker) {
            break;
        }
    }
}

fn handle_msg(
    store: &Store,
    msg: Msg,
    rx: &Receiver<Msg>,
    done_tx: &Sender<Done>,
    waker: &Waker,
) -> bool {
    match msg {
        Msg::Stop => false,
        Msg::Flush(table) => {
            if let Err(err) = store.flush_frozen(table) {
                fatal_or_log(store, err, done_tx, waker)
            } else {
                true
            }
        }
        Msg::Writes(mut batch) => {
            let mut flush_after: Option<Arc<Memtable>> = None;
            loop {
                match rx.try_recv() {
                    Ok(Msg::Writes(more)) => batch.extend(more),
                    Ok(Msg::Flush(table)) => {
                        flush_after = Some(table);
                        break;
                    }
                    Ok(Msg::Stop) => {
                        let _ = commit_batch(store, &batch, done_tx, waker);
                        return false;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return false,
                }
            }
            if !commit_batch(store, &batch, done_tx, waker) {
                return false;
            }
            if let Some(table) = flush_after {
                if let Err(err) = store.flush_frozen(table) {
                    return fatal_or_log(store, err, done_tx, waker);
                }
            }
            true
        }
    }
}

fn commit_batch(store: &Store, batch: &[WalItem], done_tx: &Sender<Done>, waker: &Waker) -> bool {
    if batch.is_empty() {
        return true;
    }
    let recs: Vec<Record> = batch.iter().map(|item| item.record.clone()).collect();
    match store.durable_commit(&recs) {
        Ok(()) => {
            let _ = done_tx.send(Done::Ack(batch.to_vec()));
            let _ = waker.wake();
            true
        }
        Err(Error::Fatal(err)) => {
            let _ = done_tx.send(Done::Fatal(err));
            let _ = waker.wake();
            false
        }
        Err(Error::Stopped) => false,
        Err(Error::Crashed { point, .. }) => {
            let _ = done_tx.send(Done::Fatal(format!("failpoint {point}")));
            let _ = waker.wake();
            false
        }
        Err(err) => {
            let _ = done_tx.send(Done::Failed {
                items: batch.to_vec(),
                err: err.to_string(),
            });
            let _ = waker.wake();
            true
        }
    }
}

fn fatal_or_log(store: &Store, err: Error, done_tx: &Sender<Done>, waker: &Waker) -> bool {
    match err {
        Error::Fatal(err) => {
            let _ = done_tx.send(Done::Fatal(err));
            let _ = waker.wake();
            false
        }
        Error::Crashed { point, .. } => {
            let _ = done_tx.send(Done::Fatal(format!("failpoint {point}")));
            let _ = waker.wake();
            false
        }
        Error::Stopped => false,
        other => {
            if store.is_fatal() {
                let _ = done_tx.send(Done::Fatal(other.to_string()));
                let _ = waker.wake();
                false
            } else {
                eprintln!("forge flush: {other}");
                true
            }
        }
    }
}

impl Server {
    fn event_loop(&mut self, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        let mut events = Events::with_capacity(256);
        loop {
            if shutdown.load(Ordering::Relaxed) {
                let _ = self.wal_tx.send(Msg::Stop);
                break;
            }
            self.poll
                .poll(&mut events, Some(Duration::from_millis(200)))?;
            if shutdown.load(Ordering::Relaxed) {
                let _ = self.wal_tx.send(Msg::Stop);
                break;
            }
            for event in events.iter() {
                match event.token() {
                    WAKE => self.on_wake(),
                    REDIS => self.accept_loop(true),
                    HTTP => self.accept_loop(false),
                    token => {
                        let readable = event.is_readable();
                        let writable = event.is_writable();
                        self.on_conn(token, readable, writable);
                    }
                }
            }
            if self.exit_code.is_some() {
                let _ = self.wal_tx.send(Msg::Stop);
                break;
            }
        }
        Ok(())
    }

    fn on_wake(&mut self) {
        loop {
            match self.done_rx.try_recv() {
                Ok(Done::Ack(items)) => self.finish_ack(items),
                Ok(Done::Failed { items, err }) => self.finish_fail(items, err),
                Ok(Done::Fatal(err)) => {
                    eprintln!("forge fatal: {err}");
                    self.exit_code = Some(1);
                    return;
                }
                Err(_) => break,
            }
        }
        let tokens: Vec<Token> = self.conns.keys().copied().collect();
        for token in tokens {
            self.pump_parse(token);
            self.flush_ready(token);
            self.pump_io(token);
            self.flush_ready(token);
            self.write_out(token, true);
            self.update_interest(token);
        }
    }

    fn finish_ack(&mut self, items: Vec<WalItem>) {
        for item in &items {
            if let Err(err) = self.store.make_visible(&item.record) {
                eprintln!("forge apply: {err}");
                if self.store.is_fatal() {
                    self.exit_code = Some(1);
                    return;
                }
            }
        }
        if let Some(table) = self.store.freeze_if_needed() {
            let _ = self.wal_tx.send(Msg::Flush(table));
        }
        for item in items {
            self.shared.queued_records.fetch_sub(1, Ordering::Relaxed);
            self.shared
                .queued_bytes
                .fetch_sub(item.bytes, Ordering::Relaxed);
            let Some(conn) = self.conns.get_mut(&item.token) else {
                continue;
            };
            for slot in conn.pending.iter_mut() {
                if let Slot::Write {
                    ticket, reply, ..
                } = slot
                {
                    if *ticket == item.ticket {
                        *reply = Some(item.reply.clone());
                    }
                }
            }
            if response_backlog(conn) < self.cfg.outbound_cap
                && conn.inbound.len() < self.cfg.inbound_cap
            {
                conn.pause_read = false;
            }
        }
    }

    fn finish_fail(&mut self, items: Vec<WalItem>, err: String) {
        let bytes = resp::err(&err);
        for item in items {
            self.shared.queued_records.fetch_sub(1, Ordering::Relaxed);
            self.shared
                .queued_bytes
                .fetch_sub(item.bytes, Ordering::Relaxed);
            let Some(conn) = self.conns.get_mut(&item.token) else {
                continue;
            };
            for slot in conn.pending.iter_mut() {
                if let Slot::Write {
                    ticket, reply, ..
                } = slot
                {
                    if *ticket == item.ticket && reply.is_none() {
                        *reply = Some(bytes.clone());
                    }
                }
            }
            conn.pause_read = false;
        }
    }

    fn accept_loop(&mut self, redis: bool) {
        loop {
            let accepted = if redis {
                self.redis.accept()
            } else {
                self.http.accept()
            };
            match accepted {
                Ok((mut stream, _)) => {
                    let _ = stream.set_nodelay(true);
                    if let Some(bytes) = self.cfg.sndbuf {
                        unsafe {
                            libc::setsockopt(
                                stream.as_raw_fd(),
                                libc::SOL_SOCKET,
                                libc::SO_SNDBUF,
                                &bytes as *const i32 as *const libc::c_void,
                                std::mem::size_of::<i32>() as libc::socklen_t,
                            );
                        }
                    }
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    if self
                        .poll
                        .registry()
                        .register(&mut stream, token, Interest::READABLE)
                        .is_err()
                    {
                        continue;
                    }
                    self.conns.insert(
                        token,
                        Conn {
                            stream,
                            kind: if redis { Kind::Redis } else { Kind::Http },
                            inbound: Vec::new(),
                            outbound: Vec::new(),
                            pending: VecDeque::new(),
                            held: None,
                            pause_read: false,
                            hold_parse: false,
                            registered: true,
                            close_after_write: false,
                        },
                    );
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    eprintln!("forge accept: {err}");
                    break;
                }
            }
        }
    }

    fn on_conn(&mut self, token: Token, readable: bool, _writable: bool) {
        if readable {
            self.pump_io(token);
        }
        self.flush_ready(token);
        self.write_out(token, true);
        self.update_interest(token);
    }

    fn pump_io(&mut self, token: Token) {
        let cap = self.cfg.inbound_cap;
        loop {
            enum Step {
                Closed,
                Block,
                Data,
            }
            let step = {
                let Some(conn) = self.conns.get_mut(&token) else {
                    return;
                };
                if conn.pause_read {
                    Step::Block
                } else if conn.inbound.len() >= cap {
                    conn.pause_read = true;
                    Step::Block
                } else {
                    let room = (cap - conn.inbound.len()).min(4096);
                    let mut buf = vec![0u8; room];
                    match conn.stream.read(&mut buf) {
                        Ok(0) => Step::Closed,
                        Ok(n) => {
                            conn.inbound.extend_from_slice(&buf[..n]);
                            Step::Data
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Step::Block,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(_) => Step::Closed,
                    }
                }
            };
            match step {
                Step::Closed => {
                    self.conns.remove(&token);
                    return;
                }
                Step::Block => break,
                Step::Data => {
                    self.pump_parse(token);
                    let paused = self
                        .conns
                        .get(&token)
                        .map(|c| c.pause_read)
                        .unwrap_or(true);
                    if paused {
                        break;
                    }
                }
            }
        }
        self.flush_ready(token);
    }

    fn pump_parse(&mut self, token: Token) {
        let kind_http = matches!(
            self.conns.get(&token).map(|c| &c.kind),
            Some(Kind::Http)
        );
        if kind_http {
            self.parse_http(token);
        } else {
            self.parse_redis(token);
        }
        self.submit_batch();
    }

    fn submit_batch(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let items = std::mem::take(&mut self.batch);
        if let Err(mpsc::SendError(Msg::Writes(items))) = self.wal_tx.send(Msg::Writes(items)) {
            for item in items {
                self.shared.queued_records.fetch_sub(1, Ordering::Relaxed);
                self.shared
                    .queued_bytes
                    .fetch_sub(item.bytes, Ordering::Relaxed);
                self.queue_ready(item.token, resp::err("shutting down"));
            }
        }
    }

    fn parse_http(&mut self, token: Token) {
        let limit = self.cfg.http_limit;
        let parsed = {
            let Some(conn) = self.conns.get_mut(&token) else {
                return;
            };
            if conn.close_after_write {
                return;
            }
            match http::pull_request(&conn.inbound, limit) {
                Ok(None) => return,
                Ok(Some((req, n))) => {
                    conn.inbound.drain(..n);
                    Some(req)
                }
                Err(err) => {
                    let status = if err.contains("large") { 413 } else { 400 };
                    conn.outbound
                        .extend(http::response(status, "error", &format!("{err}\n")));
                    conn.close_after_write = true;
                    conn.pause_read = true;
                    None
                }
            }
        };
        let Some(req) = parsed else {
            return;
        };
        let body = self.http_body(&req);
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        conn.outbound.extend(body);
        conn.close_after_write = true;
    }

    fn http_body(&self, req: &http::Request) -> Vec<u8> {
        if req.method != "GET" {
            return http::response(405, "Method Not Allowed", "method not allowed\n");
        }
        let path = req.path.split('?').next().unwrap_or(req.path.as_str());
        if path == "/health" {
            let body = if self.store.is_fatal() {
                "fatal\n"
            } else {
                "ok\n"
            };
            let status = if self.store.is_fatal() { 500 } else { 200 };
            return http::response(status, if status == 200 { "OK" } else { "error" }, body);
        }
        if path == "/metrics" {
            return http::response(200, "OK", &self.metrics());
        }
        if let Some(raw) = path.strip_prefix("/key/") {
            let Some(key) = http::percent_decode(raw) else {
                return http::response(400, "Bad Request", "bad key encoding\n");
            };
            return match self.store.get(&key) {
                Ok(Some(value)) => {
                    let text = String::from_utf8_lossy(&value);
                    http::response(200, "OK", &format!("{text}\n"))
                }
                Ok(None) => http::response(404, "Not Found", "not found\n"),
                Err(err) => http::response(500, "error", &format!("{err}\n")),
            };
        }
        http::response(404, "Not Found", "not found\n")
    }

    fn metrics(&self) -> String {
        let stats = self.store.stats();
        let mut inbound = 0usize;
        let mut outbound = 0usize;
        let mut paused = 0usize;
        for conn in self.conns.values() {
            inbound += conn.inbound.len();
            outbound += response_backlog(conn);
            if conn.pause_read {
                paused += 1;
            }
        }
        let keys = self.store.dump().map(|m| m.len()).unwrap_or(0);
        format!(
            "forge_up 1\n\
             forge_fatal {}\n\
             forge_fsync_total {}\n\
             forge_fsync_inflight {}\n\
             forge_fsync_on_durability_thread {}\n\
             forge_wal_queued_records {}\n\
             forge_wal_queued_bytes {}\n\
             forge_inbound_bytes {inbound}\n\
             forge_outbound_bytes {outbound}\n\
             forge_outbound_peak {}\n\
             forge_read_paused {paused}\n\
             forge_commands_total {}\n\
             forge_keys {keys}\n\
             fsync_thread {}\n",
            u8::from(stats.fatal),
            stats.fsync_total,
            stats.fsync_inflight,
            u8::from(stats.fsync_on_durability_thread),
            self.shared.queued_records.load(Ordering::Relaxed),
            self.shared.queued_bytes.load(Ordering::Relaxed),
            self.shared.peak_out.load(Ordering::Relaxed),
            self.shared.commands.load(Ordering::Relaxed),
            if stats.last_fsync_thread.is_empty() {
                "none"
            } else {
                &stats.last_fsync_thread
            }
        )
    }

    fn parse_redis(&mut self, token: Token) {
        loop {
            if self.exit_code.is_some() {
                return;
            }
            let (pause, hold, outbound_full, has_held) = {
                let Some(conn) = self.conns.get(&token) else {
                    return;
                };
                (
                    conn.pause_read,
                    conn.hold_parse,
                    response_backlog(conn) >= self.cfg.outbound_cap,
                    conn.held.is_some(),
                )
            };
            if outbound_full {
                if let Some(conn) = self.conns.get_mut(&token) {
                    conn.pause_read = true;
                }
                return;
            }
            if pause || hold {
                return;
            }
            if has_held {
                let cmd = self.conns.get_mut(&token).unwrap().held.take().unwrap();
                if !self.dispatch(token, cmd) {
                    return;
                }
                continue;
            }
            let pulled = {
                let Some(conn) = self.conns.get_mut(&token) else {
                    return;
                };
                resp::pull(&mut conn.inbound, self.cfg.max_bulk)
            };
            match pulled {
                Ok(resp::Pull::NeedMore) => return,
                Ok(resp::Pull::Command(args)) => {
                    let cmd = parse_command(&args);
                    if !self.dispatch(token, cmd) {
                        return;
                    }
                }
                Err(err) => {
                    let Some(conn) = self.conns.get_mut(&token) else {
                        return;
                    };
                    conn.outbound.extend(resp::err(&err));
                    conn.close_after_write = true;
                    conn.pause_read = true;
                    return;
                }
            }
        }
    }

    /// Returns false when the caller should stop parsing this connection.
    fn dispatch(&mut self, token: Token, cmd: Cmd) -> bool {
        if self.has_blocked(token) || (self.unacked_write(token) && !is_blind_set(&cmd)) {
            let Some(conn) = self.conns.get_mut(&token) else {
                return false;
            };
            conn.pending.push_back(Slot::Blocked(cmd));
            conn.hold_parse = true;
            return false;
        }
        if is_blind_set(&cmd) && self.unacked_write(token) {
            return self.enqueue_write(token, cmd);
        }
        match &cmd {
            Cmd::Set { .. } | Cmd::Del(_) | Cmd::Expire { .. } => self.enqueue_write(token, cmd),
            Cmd::Quit => {
                self.queue_ready(token, resp::ok());
                if let Some(conn) = self.conns.get_mut(&token) {
                    conn.close_after_write = true;
                }
                self.shared.commands.fetch_add(1, Ordering::Relaxed);
                false
            }
            Cmd::Error(msg) => {
                self.queue_ready(token, resp::err(msg));
                true
            }
            other => {
                let bytes = self.exec_read(other);
                self.queue_ready(token, bytes);
                self.shared.commands.fetch_add(1, Ordering::Relaxed);
                self.outbound_room(token)
            }
        }
    }

    fn unacked_write(&self, token: Token) -> bool {
        let Some(conn) = self.conns.get(&token) else {
            return false;
        };
        conn.pending
            .iter()
            .any(|slot| matches!(slot, Slot::Write { reply: None, .. }))
    }

    fn has_blocked(&self, token: Token) -> bool {
        let Some(conn) = self.conns.get(&token) else {
            return false;
        };
        conn.pending
            .iter()
            .any(|slot| matches!(slot, Slot::Blocked(_)))
    }

    fn enqueue_write(&mut self, token: Token, cmd: Cmd) -> bool {
        let op = match &cmd {
            Cmd::Set {
                key,
                value,
                ex_secs,
            } => UserOp::Set {
                key: key.clone(),
                value: value.clone(),
                expiry: match ex_secs {
                    None => 0,
                    Some(secs) => self
                        .clock
                        .now_ms()
                        .saturating_add(secs.saturating_mul(1000)),
                },
            },
            Cmd::Del(key) => UserOp::Del { key: key.clone() },
            Cmd::Expire { key, secs } => UserOp::Expire {
                key: key.clone(),
                expiry: self
                    .clock
                    .now_ms()
                    .saturating_add(secs.saturating_mul(1000)),
            },
            _ => return true,
        };
        let prepared = match self.store.prepare(op) {
            Ok(p) => p,
            Err(Error::Fatal(err)) => {
                eprintln!("forge fatal: {err}");
                self.exit_code = Some(1);
                return false;
            }
            Err(err) => {
                self.queue_ready(token, resp::err(&err.to_string()));
                return true;
            }
        };
        let (record, reply) = match prepared {
            forge_core::Prepared::Immediate(reply) => {
                self.queue_ready(token, encode_reply(&reply, true));
                self.shared.commands.fetch_add(1, Ordering::Relaxed);
                return self.outbound_room(token);
            }
            forge_core::Prepared::Durable { record, reply } => (record, reply),
        };
        let nbytes = record.key.len() + record.value.len() + 64;
        if nbytes > self.cfg.wal_queue_bytes {
            self.queue_ready(token, resp::err("value too large"));
            return true;
        }
        let records = self.shared.queued_records.load(Ordering::Relaxed);
        let bytes = self.shared.queued_bytes.load(Ordering::Relaxed);
        if records >= self.cfg.wal_queue_records || bytes + nbytes > self.cfg.wal_queue_bytes {
            if let Some(conn) = self.conns.get_mut(&token) {
                conn.held = Some(cmd);
                conn.pause_read = true;
            }
            return false;
        }
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        let encoded = encode_reply(&reply, false);
        let item = WalItem {
            ticket,
            token,
            record,
            reply: encoded,
            bytes: nbytes,
        };
        self.shared.queued_records.fetch_add(1, Ordering::Relaxed);
        self.shared.queued_bytes.fetch_add(nbytes, Ordering::Relaxed);
        self.shared.commands.fetch_add(1, Ordering::Relaxed);
        if let Some(conn) = self.conns.get_mut(&token) {
            conn.pending
                .push_back(Slot::Write { ticket, reply: None });
        }
        self.batch.push(item);
        true
    }

    fn exec_read(&self, cmd: &Cmd) -> Vec<u8> {
        match cmd {
            Cmd::Get(key) | Cmd::Dump(key) => match self.store.get(key) {
                Ok(Some(v)) => resp::bulk(&v),
                Ok(None) => resp::null_bulk(),
                Err(err) => resp::err(&err.to_string()),
            },
            Cmd::Ttl(key) => match self.store.ttl(key) {
                Ok(n) => resp::integer(n),
                Err(err) => resp::err(&err.to_string()),
            },
            Cmd::Ping(None) => resp::pong(),
            Cmd::Ping(Some(msg)) => resp::bulk(msg),
            Cmd::Scan {
                cursor,
                pattern,
                count,
            } => {
                let after = if cursor == b"0" {
                    None
                } else {
                    match std::str::from_utf8(cursor).ok().and_then(hex_decode) {
                        Some(raw) => Some(raw),
                        None => return resp::err("bad cursor"),
                    }
                };
                match self.store.scan(
                    after.as_deref(),
                    *count,
                    pattern.as_deref(),
                ) {
                    Ok(page) => {
                        let cur = if page.done {
                            "0".to_string()
                        } else {
                            hex_encode(page.next.as_deref().unwrap_or(b""))
                        };
                        resp::scan_reply(&cur, &page.keys)
                    }
                    Err(err) => resp::err(&err.to_string()),
                }
            }
            Cmd::Select(0) => resp::ok(),
            Cmd::Select(_) => resp::err("only db 0"),
            Cmd::Info => resp::bulk(b"server_name:forge\n"),
            Cmd::Command => resp::empty_array(),
            Cmd::Error(msg) => resp::err(msg),
            Cmd::Quit => resp::ok(),
            Cmd::Set { .. } | Cmd::Del(_) | Cmd::Expire { .. } => resp::err("internal"),
        }
    }

    fn queue_ready(&mut self, token: Token, bytes: Vec<u8>) {
        let n = {
            let Some(conn) = self.conns.get_mut(&token) else {
                return;
            };
            conn.pending.push_back(Slot::Ready(bytes));
            response_backlog(conn)
        };
        self.shared.peak_out.fetch_max(n, Ordering::Relaxed);
    }

    fn outbound_room(&self, token: Token) -> bool {
        self.conns
            .get(&token)
            .map(|c| response_backlog(c) < self.cfg.outbound_cap)
            .unwrap_or(false)
    }

    fn flush_ready(&mut self, token: Token) {
        let cap = self.cfg.outbound_cap;
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        while let Some(front) = conn.pending.front() {
            if conn.outbound.len() >= cap {
                conn.pause_read = true;
                break;
            }
            let bytes = match front {
                Slot::Ready(bytes) => bytes.clone(),
                Slot::Write {
                    reply: Some(bytes),
                    ..
                } => bytes.clone(),
                _ => break,
            };
            conn.outbound.extend_from_slice(&bytes);
            conn.pending.pop_front();
            if response_backlog(conn) >= cap {
                conn.pause_read = true;
            }
        }
        let backlog = response_backlog(conn);
        self.shared.peak_out.fetch_max(backlog, Ordering::Relaxed);
        let resume = if conn.hold_parse {
            let blocked_ready = match conn.pending.back() {
                Some(Slot::Blocked(_)) => !conn
                    .pending
                    .iter()
                    .any(|slot| matches!(slot, Slot::Write { reply: None, .. })),
                _ => false,
            };
            if blocked_ready {
                if let Some(Slot::Blocked(cmd)) = conn.pending.pop_back() {
                    conn.hold_parse = false;
                    Some(cmd)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let _ = conn;
        if let Some(cmd) = resume {
            self.dispatch(token, cmd);
            self.pump_parse(token);
            self.flush_ready(token);
        }
    }

    fn write_out(&mut self, token: Token, follow: bool) {
        let (close, read_again) = {
            let Some(conn) = self.conns.get_mut(&token) else {
                return;
            };
            let close = if conn.outbound.is_empty() {
                conn.close_after_write
            } else {
                match conn.stream.write(&conn.outbound) {
                    Ok(0) => true,
                    Ok(n) => {
                        conn.outbound.drain(..n);
                        conn.outbound.is_empty() && conn.close_after_write
                    }
                    Err(err)
                        if err.kind() == io::ErrorKind::WouldBlock
                            || err.kind() == io::ErrorKind::Interrupted =>
                    {
                        false
                    }
                    Err(_) => true,
                }
            };
            let was_paused = conn.pause_read;
            if !close
                && conn.held.is_none()
                && response_backlog(conn) < self.cfg.outbound_cap
                && conn.inbound.len() < self.cfg.inbound_cap
            {
                conn.pause_read = false;
            }
            (close, was_paused && !conn.pause_read)
        };
        if close {
            self.conns.remove(&token);
            return;
        }
        // kqueue is edge-triggered. After unpausing, read even if no new event fired.
        if follow && read_again {
            self.pump_io(token);
            self.flush_ready(token);
            self.write_out(token, false);
        }
    }

    fn update_interest(&mut self, token: Token) {
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        let want_read = !conn.pause_read && !conn.close_after_write;
        let want_write = !conn.outbound.is_empty();
        let interest = match (want_read, want_write) {
            (false, false) => None,
            (true, false) => Some(Interest::READABLE),
            (false, true) => Some(Interest::WRITABLE),
            (true, true) => Some(Interest::READABLE | Interest::WRITABLE),
        };
        let registered = conn.registered;
        let result = match (interest, registered) {
            (None, true) => self.poll.registry().deregister(&mut conn.stream),
            (None, false) => Ok(()),
            (Some(interest), true) => self
                .poll
                .registry()
                .reregister(&mut conn.stream, token, interest),
            (Some(interest), false) => self
                .poll
                .registry()
                .register(&mut conn.stream, token, interest),
        };
        if result.is_ok() {
            if let Some(conn) = self.conns.get_mut(&token) {
                conn.registered = interest.is_some();
            }
        }
    }
}

fn response_backlog(conn: &Conn) -> usize {
    let mut n = conn.outbound.len();
    for slot in &conn.pending {
        n += match slot {
            Slot::Ready(bytes) => bytes.len(),
            Slot::Write {
                reply: Some(bytes), ..
            } => bytes.len(),
            _ => 0,
        };
    }
    n
}

fn is_blind_set(cmd: &Cmd) -> bool {
    matches!(cmd, Cmd::Set { .. })
}

fn encode_reply(reply: &Reply, _int_for_ok: bool) -> Vec<u8> {
    match reply {
        Reply::Ok => resp::ok(),
        Reply::Integer(n) => resp::integer(*n),
    }
}

fn parse_command(args: &[Vec<u8>]) -> Cmd {
    if args.is_empty() {
        return Cmd::Error("empty command".into());
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match name.as_str() {
        "GET" if args.len() == 2 => Cmd::Get(args[1].clone()),
        "DUMP" if args.len() == 2 => Cmd::Dump(args[1].clone()),
        "SET" => parse_set(args),
        "DEL" if args.len() == 2 => Cmd::Del(args[1].clone()),
        "EXPIRE" if args.len() == 3 => match parse_u64(&args[2]) {
            Some(secs) => Cmd::Expire {
                key: args[1].clone(),
                secs,
            },
            None => Cmd::Error("expire seconds".into()),
        },
        "TTL" if args.len() == 2 => Cmd::Ttl(args[1].clone()),
        "PING" if args.len() == 1 => Cmd::Ping(None),
        "PING" if args.len() == 2 => Cmd::Ping(Some(args[1].clone())),
        "SCAN" if args.len() >= 2 => parse_scan(args),
        "QUIT" => Cmd::Quit,
        "SELECT" if args.len() == 2 => match parse_i64(&args[1]) {
            Some(n) => Cmd::Select(n),
            None => Cmd::Error("bad db".into()),
        },
        "INFO" => Cmd::Info,
        "COMMAND" => Cmd::Command,
        "ECHO" if args.len() == 2 => Cmd::Ping(Some(args[1].clone())),
        _ => Cmd::Error(format!("unknown command {}", name.to_ascii_lowercase())),
    }
}

fn parse_set(args: &[Vec<u8>]) -> Cmd {
    if args.len() == 3 {
        return Cmd::Set {
            key: args[1].clone(),
            value: args[2].clone(),
            ex_secs: None,
        };
    }
    if args.len() == 5 && args[3].eq_ignore_ascii_case(b"EX") {
        return match parse_u64(&args[4]) {
            Some(secs) => Cmd::Set {
                key: args[1].clone(),
                value: args[2].clone(),
                ex_secs: Some(secs),
            },
            None => Cmd::Error("set ex".into()),
        };
    }
    Cmd::Error("syntax error".into())
}

fn parse_scan(args: &[Vec<u8>]) -> Cmd {
    let mut pattern = None;
    let mut count = 10usize;
    let mut i = 2;
    while i < args.len() {
        let flag = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        if flag == "MATCH" && i + 1 < args.len() {
            pattern = Some(args[i + 1].clone());
            i += 2;
        } else if flag == "COUNT" && i + 1 < args.len() {
            count = parse_u64(&args[i + 1]).unwrap_or(10) as usize;
            i += 2;
        } else {
            return Cmd::Error("syntax error".into());
        }
    }
    Cmd::Scan {
        cursor: args[1].clone(),
        pattern,
        count,
    }
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn io_err(err: Error) -> io::Error {
    io::Error::other(err.to_string())
}
