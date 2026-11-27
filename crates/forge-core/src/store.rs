use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use crate::disk::Disk;
use crate::error::Error;
use crate::failpoint::Failpoint;
use crate::gate::Gate;
use crate::manifest::{self, Manifest, SstMeta, WalMeta};
use crate::memtable::Memtable;
use crate::record::{OpKind, Prepared, Record, Reply, UserOp, ValueRec};
use crate::sstable::{self, SsTable};
use crate::util::glob_match;
use crate::wal::{self, Decoded};
use crate::Clock;

#[derive(Clone)]
pub struct StoreOpts {
    pub memtable_bytes: usize,
    pub compact_threshold: usize,
    pub wal_rotate_bytes: u64,
    pub exit_on_failpoint: bool,
    pub sync_gate: Option<Arc<Gate>>,
}

impl Default for StoreOpts {
    fn default() -> Self {
        Self {
            memtable_bytes: 4 * 1024 * 1024,
            compact_threshold: 4,
            wal_rotate_bytes: 1024 * 1024,
            exit_on_failpoint: false,
            sync_gate: None,
        }
    }
}

pub struct WriteAck {
    pub seq: u64,
    pub reply: Reply,
}

pub struct ScanPage {
    pub keys: Vec<Vec<u8>>,
    pub done: bool,
    /// Last key returned, when the scan is not finished.
    pub next: Option<Vec<u8>>,
}

pub struct Stats {
    pub fsync_total: u64,
    pub fsync_inflight: u64,
    pub last_fsync_thread: String,
    pub fsync_on_durability_thread: bool,
    pub fatal: bool,
    pub mem_bytes: usize,
    pub sst_count: usize,
}

struct LiveSst {
    name: String,
    max_seq: u64,
    table: Arc<SsTable>,
}

struct State {
    mem: Memtable,
    frozen: Vec<Arc<Memtable>>,
    ssts: Vec<LiveSst>,
    wals: Vec<WalMeta>,
    active_wal: String,
    flushed_seq: u64,
    next_seq: u64,
    next_file_id: u64,
    manifest_gen: u64,
}

impl State {
    fn empty() -> Self {
        Self {
            mem: Memtable::default(),
            frozen: Vec::new(),
            ssts: Vec::new(),
            wals: Vec::new(),
            active_wal: String::new(),
            flushed_seq: 0,
            next_seq: 1,
            next_file_id: 1,
            manifest_gen: 0,
        }
    }
}

struct StoreInner {
    disk: Disk,
    clock: Arc<dyn Clock>,
    opts: StoreOpts,
    state: Mutex<State>,
    armed: Mutex<Option<Failpoint>>,
    dead: AtomicBool,
    fatal: AtomicBool,
    stop: AtomicBool,
    fsync_inflight: AtomicU64,
    fsync_total: AtomicU64,
    last_fsync_thread: Mutex<String>,
    fsync_on_durability: AtomicBool,
}

/// LSM store. Network code lives in `forge-server`. Writes are visible only
/// after `make_visible`, which the server calls after `durable_commit` returns.
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

impl Store {
    pub fn open(disk: Disk, clock: Arc<dyn Clock>, opts: StoreOpts) -> Result<Self, Error> {
        let store = Self {
            inner: Arc::new(StoreInner {
                disk,
                clock,
                opts,
                state: Mutex::new(State::empty()),
                armed: Mutex::new(None),
                dead: AtomicBool::new(false),
                fatal: AtomicBool::new(false),
                stop: AtomicBool::new(false),
                fsync_inflight: AtomicU64::new(0),
                fsync_total: AtomicU64::new(0),
                last_fsync_thread: Mutex::new(String::new()),
                fsync_on_durability: AtomicBool::new(false),
            }),
        };
        if !store.inner.disk.exists("CURRENT") {
            store.init_empty()?;
        } else {
            store.recover()?;
        }
        Ok(store)
    }

    pub fn disk(&self) -> &Disk {
        &self.inner.disk
    }

    pub fn arm(&self, point: Failpoint) {
        *self.inner.armed.lock().unwrap() = Some(point);
    }

    pub fn set_loop_thread(&self) {
        self.inner
            .disk
            .set_loop_thread(std::thread::current().id());
    }

    pub fn request_stop(&self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(gate) = &self.inner.opts.sync_gate {
            gate.release();
        }
    }

    pub fn is_fatal(&self) -> bool {
        self.inner.fatal.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> Stats {
        let st = self.inner.state.lock().unwrap();
        Stats {
            fsync_total: self.inner.fsync_total.load(Ordering::Relaxed),
            fsync_inflight: self.inner.fsync_inflight.load(Ordering::Relaxed),
            last_fsync_thread: self.inner.last_fsync_thread.lock().unwrap().clone(),
            fsync_on_durability_thread: self.inner.fsync_on_durability.load(Ordering::Relaxed),
            fatal: self.inner.fatal.load(Ordering::Relaxed),
            mem_bytes: st.mem.bytes(),
            sst_count: st.ssts.len(),
        }
    }

    pub fn sst_count(&self) -> usize {
        self.inner.state.lock().unwrap().ssts.len()
    }

    pub fn sst_names(&self) -> Vec<String> {
        self.inner
            .state
            .lock()
            .unwrap()
            .ssts
            .iter()
            .map(|s| s.name.clone())
            .collect()
    }

    pub fn wal_names(&self) -> Vec<String> {
        self.inner
            .state
            .lock()
            .unwrap()
            .wals
            .iter()
            .map(|w| w.name.clone())
            .collect()
    }

    pub fn active_wal(&self) -> String {
        self.inner.state.lock().unwrap().active_wal.clone()
    }

    /// Synchronous write used by tests and recovery checks. The server does not
    /// call this; it commits on the durability thread and applies on the loop.
    pub fn apply(&self, op: UserOp) -> Result<WriteAck, Error> {
        match self.prepare(op)? {
            Prepared::Immediate(reply) => Ok(WriteAck { seq: 0, reply }),
            Prepared::Durable { record, reply } => {
                let seq = record.seq;
                if let Err(e) = self.durable_commit(std::slice::from_ref(&record)) {
                    return Err(match e {
                        Error::Crashed { point, .. } => Error::Crashed {
                            point,
                            op_acked: false,
                            seq,
                        },
                        other => other,
                    });
                }
                self.make_visible(&record)?;
                match self.flush_inline() {
                    Ok(()) => Ok(WriteAck { seq, reply }),
                    Err(Error::Crashed { point, .. }) => Err(Error::Crashed {
                        point,
                        op_acked: true,
                        seq,
                    }),
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub fn prepare(&self, op: UserOp) -> Result<Prepared, Error> {
        self.check_alive()?;
        let now = self.inner.clock.now_ms();
        let mut st = self.inner.state.lock().unwrap();
        match op {
            UserOp::Set { key, value, expiry } => {
                let seq = bump_seq(&mut st);
                Ok(Prepared::Durable {
                    record: Record {
                        seq,
                        op: OpKind::Set,
                        key,
                        value,
                        expiry,
                    },
                    reply: Reply::Ok,
                })
            }
            UserOp::Del { key } => {
                let visible = lookup_in(&st, &key)?
                    .map(|v| v.is_visible(now))
                    .unwrap_or(false);
                if !visible {
                    return Ok(Prepared::Immediate(Reply::Integer(0)));
                }
                let seq = bump_seq(&mut st);
                Ok(Prepared::Durable {
                    record: Record {
                        seq,
                        op: OpKind::Del,
                        key,
                        value: Vec::new(),
                        expiry: 0,
                    },
                    reply: Reply::Integer(1),
                })
            }
            UserOp::Expire { key, expiry } => {
                let cur = lookup_in(&st, &key)?;
                let visible = cur.as_ref().map(|v| v.is_visible(now)).unwrap_or(false);
                if !visible {
                    return Ok(Prepared::Immediate(Reply::Integer(0)));
                }
                let value = cur.unwrap().value;
                let seq = bump_seq(&mut st);
                Ok(Prepared::Durable {
                    record: Record {
                        seq,
                        op: OpKind::Expire,
                        key,
                        value,
                        expiry,
                    },
                    reply: Reply::Integer(1),
                })
            }
        }
    }

    /// Append `recs` and fsync once. On fsync error the store becomes fatal and
    /// nothing in the batch is acknowledged. Failpoints abort before ack.
    pub fn durable_commit(&self, recs: &[Record]) -> Result<(), Error> {
        self.check_alive()?;
        if recs.is_empty() {
            return Ok(());
        }
        let wal = self.active_wal();
        let start = self.inner.disk.len(&wal)?;
        for rec in recs {
            if let Err(e) = self.append_record(rec) {
                let _ = self.inner.disk.truncate(&wal, start);
                self.inner.disk.drop_spans_from(&wal, start);
                return Err(e);
            }
        }
        self.reach(Failpoint::AfterWalAppend)?;
        self.sync_wal()?;
        self.reach(Failpoint::AfterFsyncBeforeAck)?;
        Ok(())
    }

    pub fn make_visible(&self, rec: &Record) -> Result<(), Error> {
        let now = self.inner.clock.now_ms();
        let mut st = self.inner.state.lock().unwrap();
        match rec.op {
            OpKind::Expire => {
                let cur = lookup_in(&st, &rec.key)?;
                apply_expire(&mut st.mem, rec, cur, now);
            }
            _ => apply_simple(&mut st.mem, rec, now),
        }
        Ok(())
    }

    pub fn freeze_if_needed(&self) -> Option<Arc<Memtable>> {
        let limit = self.inner.opts.memtable_bytes;
        let mut st = self.inner.state.lock().unwrap();
        if st.mem.is_empty() || st.mem.bytes() < limit {
            return None;
        }
        let mem = std::mem::take(&mut st.mem);
        let arc = Arc::new(mem);
        st.frozen.push(Arc::clone(&arc));
        Some(arc)
    }

    pub fn flush_frozen(&self, table: Arc<Memtable>) -> Result<(), Error> {
        if table.is_empty() {
            return Ok(());
        }
        let now = self.inner.clock.now_ms();
        let entries = table.entries_for_flush(now);
        let bytes = SsTable::build(&entries)?;
        let id = self.alloc_id();
        let name = format!("sst-{id:06}.sst");
        self.write_bytes(&name, &bytes, Failpoint::MidFlush)?;
        let sst = SsTable::open(bytes)?;
        let max_seq = table.max_seq();
        let manifest = {
            let st = self.inner.state.lock().unwrap();
            let mut m = snapshot_manifest(&st);
            m.ssts.push(SstMeta {
                name: name.clone(),
                max_seq,
            });
            m.flushed_seq = m.flushed_seq.max(max_seq);
            m
        };
        self.publish(&manifest)?;
        self.reach(Failpoint::AfterManifestWrite)?;
        {
            let mut st = self.inner.state.lock().unwrap();
            st.ssts.push(LiveSst {
                name,
                max_seq,
                table: Arc::new(sst),
            });
            st.flushed_seq = st.flushed_seq.max(max_seq);
            let flushed = st.flushed_seq;
            st.frozen.retain(|f| f.max_seq() > flushed);
        }
        self.drop_covered_wals()?;
        let wal_len = self.inner.disk.len(&self.active_wal()).unwrap_or(0);
        if wal_len >= self.inner.opts.wal_rotate_bytes {
            self.rotate_wal()?;
        }
        if self.sst_count() >= self.inner.opts.compact_threshold {
            self.compact_all()?;
        }
        Ok(())
    }

    pub fn compact_all(&self) -> Result<(), Error> {
        let (tables, names, max_seq) = {
            let st = self.inner.state.lock().unwrap();
            if st.ssts.len() < 2 {
                return Ok(());
            }
            let mut tables = Vec::new();
            let mut names = Vec::new();
            let mut max_seq = 0u64;
            for sst in &st.ssts {
                tables.push(sst.table.iter()?);
                names.push(sst.name.clone());
                max_seq = max_seq.max(sst.max_seq);
            }
            (tables, names, max_seq)
        };
        let now = self.inner.clock.now_ms();
        let merged = sstable::merge_entries(&tables, now, true);
        let bytes = SsTable::build(&merged)?;
        let id = self.alloc_id();
        let name = format!("sst-{id:06}.sst");
        self.write_bytes(&name, &bytes, Failpoint::MidCompaction)?;
        let sst = SsTable::open(bytes)?;
        let manifest = {
            let st = self.inner.state.lock().unwrap();
            let mut m = snapshot_manifest(&st);
            m.ssts = vec![SstMeta {
                name: name.clone(),
                max_seq,
            }];
            m
        };
        self.publish(&manifest)?;
        {
            let mut st = self.inner.state.lock().unwrap();
            st.ssts = vec![LiveSst {
                name: name.clone(),
                max_seq,
                table: Arc::new(sst),
            }];
        }
        for old in names {
            if old != name {
                let _ = self.inner.disk.remove(&old);
            }
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let now = self.inner.clock.now_ms();
        let st = self.inner.state.lock().unwrap();
        Ok(lookup_in(&st, key)?
            .filter(|v| v.is_visible(now))
            .map(|v| v.value))
    }

    pub fn ttl(&self, key: &[u8]) -> Result<i64, Error> {
        let now = self.inner.clock.now_ms();
        let st = self.inner.state.lock().unwrap();
        match lookup_in(&st, key)? {
            None => Ok(-2),
            Some(v) if !v.is_visible(now) => Ok(-2),
            Some(v) if v.expiry == 0 => Ok(-1),
            Some(v) => Ok((v.expiry.saturating_sub(now) / 1000) as i64),
        }
    }

    pub fn scan(
        &self,
        after: Option<&[u8]>,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> Result<ScanPage, Error> {
        let map = self.visible_map()?;
        let count = if count == 0 { 10 } else { count.min(10_000) };
        let mut keys = Vec::new();
        for (k, _) in map {
            if let Some(pat) = pattern {
                if !glob_match(pat, &k) {
                    continue;
                }
            }
            if let Some(cursor) = after {
                if k.as_slice() <= cursor {
                    continue;
                }
            }
            keys.push(k);
            if keys.len() == count + 1 {
                break;
            }
        }
        let done = keys.len() <= count;
        if !done {
            keys.pop();
        }
        let next = if done { None } else { keys.last().cloned() };
        Ok(ScanPage { keys, done, next })
    }

    pub fn dump(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, Error> {
        self.visible_map()
    }

    fn visible_map(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, Error> {
        let now = self.inner.clock.now_ms();
        let mut map: BTreeMap<Vec<u8>, ValueRec> = BTreeMap::new();
        let st = self.inner.state.lock().unwrap();
        for sst in &st.ssts {
            for (k, v) in sst.table.iter()? {
                consider(&mut map, k, v);
            }
        }
        for frozen in &st.frozen {
            for (k, v) in frozen.iter() {
                consider(&mut map, k.clone(), v.clone());
            }
        }
        for (k, v) in st.mem.iter() {
            consider(&mut map, k.clone(), v.clone());
        }
        drop(st);
        let mut out = BTreeMap::new();
        for (k, v) in map {
            if v.is_visible(now) {
                out.insert(k, v.value);
            }
        }
        Ok(out)
    }

    fn flush_inline(&self) -> Result<(), Error> {
        if let Some(table) = self.freeze_if_needed() {
            self.flush_frozen(table)?;
        }
        Ok(())
    }

    fn init_empty(&self) -> Result<(), Error> {
        let name = "wal-000001.log".to_string();
        self.inner.disk.create_wal(&name)?;
        let manifest = Manifest {
            flushed_seq: 0,
            next_file_id: 2,
            next_seq: 1,
            wals: vec![WalMeta {
                name: name.clone(),
                max_seq: 0,
            }],
            ssts: Vec::new(),
        };
        self.publish(&manifest)?;
        let mut st = self.inner.state.lock().unwrap();
        st.wals = manifest.wals;
        st.active_wal = name;
        st.next_file_id = 2;
        st.next_seq = 1;
        st.flushed_seq = 0;
        Ok(())
    }

    fn recover(&self) -> Result<(), Error> {
        let manifest = load_manifest(&self.inner.disk)?;
        let gen = read_gen(&self.inner.disk, "CURRENT").unwrap_or(0);
        let mut ssts = Vec::new();
        for meta in &manifest.ssts {
            let bytes = self.inner.disk.read(&meta.name)?;
            let table = SsTable::open(bytes).map_err(|e| {
                Error::Corrupt(format!("sstable {}: {e}", meta.name))
            })?;
            ssts.push(LiveSst {
                name: meta.name.clone(),
                max_seq: meta.max_seq,
                table: Arc::new(table),
            });
        }
        let now = self.inner.clock.now_ms();
        let mut mem = Memtable::default();
        let mut wals = Vec::new();
        let mut max_seen = 0u64;
        for meta in &manifest.wals {
            let buf = self.inner.disk.read(&meta.name)?;
            let mut off = wal::check_header(&buf)?;
            let mut good = off;
            let mut file_max = 0u64;
            loop {
                if off >= buf.len() {
                    break;
                }
                match wal::decode_one(&buf, off)? {
                    Decoded::Torn => {
                        self.inner.disk.truncate(&meta.name, good as u64)?;
                        self.inner.disk.sync(&meta.name)?;
                        break;
                    }
                    Decoded::Record { next, rec } => {
                        file_max = file_max.max(rec.seq);
                        max_seen = max_seen.max(rec.seq);
                        if rec.seq > manifest.flushed_seq {
                            apply_record(&mut mem, &ssts, &[], &rec, now)?;
                        }
                        good = next;
                        off = next;
                    }
                }
            }
            wals.push(WalMeta {
                name: meta.name.clone(),
                max_seq: file_max.max(meta.max_seq),
            });
        }
        if wals.is_empty() {
            return Err(Error::Corrupt("manifest has no wal".into()));
        }
        let active = wals.last().unwrap().name.clone();
        let next_seq = manifest.next_seq.max(max_seen.saturating_add(1)).max(1);
        let mut st = self.inner.state.lock().unwrap();
        st.mem = mem;
        st.ssts = ssts;
        st.wals = wals;
        st.active_wal = active;
        st.flushed_seq = manifest.flushed_seq;
        st.next_seq = next_seq;
        st.next_file_id = manifest.next_file_id.max(2);
        st.manifest_gen = gen;
        Ok(())
    }

    fn append_record(&self, rec: &Record) -> Result<(), Error> {
        let bytes = wal::encode(rec);
        let file = self.active_wal();
        let offset = self.inner.disk.len(&file)?;
        self.inner.disk.append_all(&file, &bytes)?;
        self.inner.disk.note_span(crate::disk::Span {
            seq: rec.seq,
            file: file.clone(),
            offset,
            len: bytes.len() as u64,
            synced: false,
            dropped: false,
        });
        let mut st = self.inner.state.lock().unwrap();
        if let Some(w) = st.wals.iter_mut().find(|w| w.name == file) {
            if rec.seq > w.max_seq {
                w.max_seq = rec.seq;
            }
        }
        Ok(())
    }

    fn sync_wal(&self) -> Result<(), Error> {
        if self.inner.fatal.load(Ordering::Relaxed) {
            return Err(Error::Fatal(
                "writes stopped after fsync error".into(),
            ));
        }
        self.inner.fsync_inflight.store(1, Ordering::Relaxed);
        if let Some(gate) = &self.inner.opts.sync_gate {
            if !gate.wait_interruptible(&self.inner.stop) {
                self.inner.fsync_inflight.store(0, Ordering::Relaxed);
                return Err(Error::Stopped);
            }
        }
        let thread_name = std::thread::current().name().unwrap_or("").to_string();
        *self.inner.last_fsync_thread.lock().unwrap() = thread_name.clone();
        if thread_name == "forge-durability" {
            self.inner
                .fsync_on_durability
                .store(true, Ordering::Relaxed);
        }
        let name = self.active_wal();
        let result = self.inner.disk.sync(&name);
        self.inner.fsync_inflight.store(0, Ordering::Relaxed);
        match result {
            Ok(()) => {
                self.inner.fsync_total.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.inner.fatal.store(true, Ordering::Relaxed);
                // The fsync did not succeed, so those bytes are not durable.
                // Do not retry. A later open sees only successfully synced bytes.
                self.inner.disk.discard_unsynced();
                Err(Error::Fatal(format!(
                    "fsync failed: {e}; refusing to continue"
                )))
            }
        }
    }

    fn write_bytes(&self, name: &str, bytes: &[u8], point: Failpoint) -> Result<(), Error> {
        let mid = if bytes.is_empty() {
            0
        } else {
            (bytes.len() / 2).clamp(1, bytes.len())
        };
        if mid > 0 {
            self.inner.disk.append_all(name, &bytes[..mid])?;
        }
        self.reach(point)?;
        if mid < bytes.len() {
            self.inner.disk.append_all(name, &bytes[mid..])?;
        }
        self.inner.disk.sync(name)?;
        Ok(())
    }

    fn rotate_wal(&self) -> Result<(), Error> {
        let id = self.alloc_id();
        let name = format!("wal-{id:06}.log");
        self.inner.disk.create_wal(&name)?;
        self.reach(Failpoint::AfterNewWalBeforeManifest)?;
        let manifest = {
            let st = self.inner.state.lock().unwrap();
            let mut m = snapshot_manifest(&st);
            m.wals.push(WalMeta {
                name: name.clone(),
                max_seq: 0,
            });
            m
        };
        self.publish(&manifest)?;
        let mut st = self.inner.state.lock().unwrap();
        st.wals.push(WalMeta {
            name: name.clone(),
            max_seq: 0,
        });
        st.active_wal = name;
        Ok(())
    }

    fn drop_covered_wals(&self) -> Result<(), Error> {
        let (drop_names, manifest) = {
            let st = self.inner.state.lock().unwrap();
            let drop_names: Vec<String> = st
                .wals
                .iter()
                .filter(|w| {
                    w.name != st.active_wal && w.max_seq > 0 && w.max_seq <= st.flushed_seq
                })
                .map(|w| w.name.clone())
                .collect();
            if drop_names.is_empty() {
                return Ok(());
            }
            let mut m = snapshot_manifest(&st);
            m.wals.retain(|w| !drop_names.contains(&w.name));
            (drop_names, m)
        };
        self.publish(&manifest)?;
        {
            let mut st = self.inner.state.lock().unwrap();
            st.wals.retain(|w| !drop_names.contains(&w.name));
            if !st.wals.iter().any(|w| w.name == st.active_wal) {
                if let Some(last) = st.wals.last() {
                    st.active_wal = last.name.clone();
                }
            }
        }
        for name in drop_names {
            let _ = self.inner.disk.remove(&name);
        }
        Ok(())
    }

    fn publish(&self, manifest: &Manifest) -> Result<(), Error> {
        let gen = self.inner.state.lock().unwrap().manifest_gen + 1;
        let name = manifest::manifest_name(gen);
        self.inner.disk.write_replace(&name, &manifest.encode())?;
        if self.inner.disk.exists("CURRENT") {
            let old = self.inner.disk.read("CURRENT")?;
            self.inner.disk.write_replace("CURRENT.bak", &old)?;
        }
        let body = format!("{:06}\n", gen);
        self.inner.disk.write_replace("CURRENT", body.as_bytes())?;
        self.inner.state.lock().unwrap().manifest_gen = gen;
        Ok(())
    }

    fn alloc_id(&self) -> u64 {
        let mut st = self.inner.state.lock().unwrap();
        let id = st.next_file_id;
        st.next_file_id += 1;
        id
    }

    fn check_alive(&self) -> Result<(), Error> {
        if self.inner.stop.load(Ordering::Relaxed) {
            return Err(Error::Stopped);
        }
        if self.inner.dead.load(Ordering::Relaxed) {
            return Err(Error::Fatal("store crashed".into()));
        }
        if self.inner.fatal.load(Ordering::Relaxed) {
            return Err(Error::Fatal(
                "writes stopped after fsync error".into(),
            ));
        }
        Ok(())
    }

    fn reach(&self, point: Failpoint) -> Result<(), Error> {
        let fire = {
            let mut armed = self.inner.armed.lock().unwrap();
            if *armed == Some(point) {
                *armed = None;
                true
            } else {
                false
            }
        };
        if !fire {
            return Ok(());
        }
        self.inner.dead.store(true, Ordering::Relaxed);
        self.inner.disk.discard_unsynced();
        if self.inner.opts.exit_on_failpoint {
            eprintln!("forge failpoint {} aborting", point.as_str());
            std::process::exit(77);
        }
        Err(Error::Crashed {
            point: point.as_str(),
            op_acked: false,
            seq: 0,
        })
    }
}

fn bump_seq(st: &mut State) -> u64 {
    let seq = st.next_seq;
    st.next_seq = st.next_seq.saturating_add(1);
    seq
}

fn snapshot_manifest(st: &State) -> Manifest {
    Manifest {
        flushed_seq: st.flushed_seq,
        next_file_id: st.next_file_id,
        next_seq: st.next_seq,
        wals: st.wals.clone(),
        ssts: st
            .ssts
            .iter()
            .map(|s| SstMeta {
                name: s.name.clone(),
                max_seq: s.max_seq,
            })
            .collect(),
    }
}

fn lookup_in(st: &State, key: &[u8]) -> Result<Option<ValueRec>, Error> {
    if let Some(v) = st.mem.get(key) {
        return Ok(Some(v.clone()));
    }
    for frozen in st.frozen.iter().rev() {
        if let Some(v) = frozen.get(key) {
            return Ok(Some(v.clone()));
        }
    }
    for sst in st.ssts.iter().rev() {
        if let Some(v) = sst.table.get(key)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn apply_simple(mem: &mut Memtable, rec: &Record, now: u64) {
    match rec.op {
        OpKind::Del => mem.insert(
            rec.key.clone(),
            ValueRec {
                seq: rec.seq,
                expiry: 0,
                tombstone: true,
                value: Vec::new(),
            },
        ),
        _ => mem.insert(rec.key.clone(), ValueRec::from_record(rec, now)),
    }
}

fn apply_expire(mem: &mut Memtable, rec: &Record, cur: Option<ValueRec>, now: u64) {
    let Some(v) = cur else {
        return;
    };
    if v.tombstone || (v.expiry != 0 && v.expiry <= now) {
        return;
    }
    if rec.expiry != 0 && rec.expiry <= now {
        mem.insert(
            rec.key.clone(),
            ValueRec {
                seq: rec.seq,
                expiry: 0,
                tombstone: true,
                value: Vec::new(),
            },
        );
    } else {
        mem.insert(
            rec.key.clone(),
            ValueRec {
                seq: rec.seq,
                expiry: rec.expiry,
                tombstone: false,
                value: v.value,
            },
        );
    }
}

fn apply_record(
    mem: &mut Memtable,
    ssts: &[LiveSst],
    frozen: &[Arc<Memtable>],
    rec: &Record,
    now: u64,
) -> Result<(), Error> {
    match rec.op {
        OpKind::Expire => {
            let cur = lookup_parts(mem, ssts, frozen, &rec.key)?;
            apply_expire(mem, rec, cur, now);
        }
        _ => apply_simple(mem, rec, now),
    }
    Ok(())
}

fn lookup_parts(
    mem: &Memtable,
    ssts: &[LiveSst],
    frozen: &[Arc<Memtable>],
    key: &[u8],
) -> Result<Option<ValueRec>, Error> {
    if let Some(v) = mem.get(key) {
        return Ok(Some(v.clone()));
    }
    for table in frozen.iter().rev() {
        if let Some(v) = table.get(key) {
            return Ok(Some(v.clone()));
        }
    }
    for sst in ssts.iter().rev() {
        if let Some(v) = sst.table.get(key)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn consider(map: &mut BTreeMap<Vec<u8>, ValueRec>, key: Vec<u8>, rec: ValueRec) {
    match map.get(&key) {
        Some(old) if old.seq > rec.seq => {}
        _ => {
            map.insert(key, rec);
        }
    }
}

fn load_manifest(disk: &Disk) -> Result<Manifest, Error> {
    let gen = read_gen(disk, "CURRENT")?;
    if let Ok(m) = read_manifest(disk, gen) {
        return Ok(m);
    }
    if gen > 1 {
        if let Ok(m) = read_manifest(disk, gen - 1) {
            return Ok(m);
        }
    }
    if disk.exists("CURRENT.bak") {
        if let Ok(prev) = read_gen(disk, "CURRENT.bak") {
            if let Ok(m) = read_manifest(disk, prev) {
                return Ok(m);
            }
        }
    }
    Err(Error::Corrupt("no usable manifest".into()))
}

fn read_manifest(disk: &Disk, gen: u64) -> Result<Manifest, Error> {
    let name = manifest::manifest_name(gen);
    let bytes = disk.read(&name)?;
    Manifest::decode(&bytes)
}

fn read_gen(disk: &Disk, name: &str) -> Result<u64, Error> {
    let bytes = disk.read(name)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| Error::Corrupt("generation".into()))?;
    text.trim()
        .parse::<u64>()
        .map_err(|_| Error::Corrupt(format!("generation {text}")))
}

