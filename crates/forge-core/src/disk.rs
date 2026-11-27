use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::{map_io, Error};
use crate::syncfile::{self, write_all_handling_short};
use crate::wal;

/// One WAL record's bytes, and whether a successful fsync covered them.
#[derive(Clone, Debug)]
pub struct Span {
    pub seq: u64,
    pub file: String,
    pub offset: u64,
    pub len: u64,
    pub synced: bool,
    pub dropped: bool,
}

#[derive(Clone, Debug)]
pub struct SyncRange {
    pub file: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Next N write calls fail with ENOSPC.
    pub enospc_writes: u32,
    /// The next write returns at most this many bytes, then clears.
    pub short_once: Option<usize>,
    /// After a short write, further writes fail with ENOSPC.
    pub enospc_after_short: bool,
    /// Next N sync calls fail.
    pub fsync_errors: u32,
}

#[derive(Clone, Debug)]
struct SimFile {
    logical: Vec<u8>,
    durable: Vec<u8>,
}

struct SimInner {
    files: Mutex<BTreeMap<String, SimFile>>,
    faults: Mutex<Faults>,
    spans: Mutex<Vec<Span>>,
    sync_ranges: Mutex<Vec<SyncRange>>,
    sync_attempts: AtomicU64,
    sync_ok: AtomicU64,
    dropped_bytes: AtomicU64,
    enospc_latched: Mutex<u32>,
}

struct RealInner {
    root: PathBuf,
    files: Mutex<BTreeMap<String, File>>,
    spans: Mutex<Vec<Span>>,
    sync_ranges: Mutex<Vec<SyncRange>>,
    sync_attempts: AtomicU64,
    sync_ok: AtomicU64,
}

enum Inner {
    Sim(SimInner),
    Real(RealInner),
}

/// File layer. The sim backend records fsynced ranges and can drop the rest.
#[derive(Clone)]
pub struct Disk {
    inner: std::sync::Arc<Inner>,
    /// When set, `sync` panics if called on this thread. The server sets it to
    /// the event-loop thread so a WAL or SSTable fsync cannot run there.
    loop_thread: std::sync::Arc<Mutex<Option<std::thread::ThreadId>>>,
}

impl Disk {
    pub fn memory() -> Self {
        Self {
            loop_thread: std::sync::Arc::new(Mutex::new(None)),
            inner: std::sync::Arc::new(Inner::Sim(SimInner {
                files: Mutex::new(BTreeMap::new()),
                faults: Mutex::new(Faults::default()),
                spans: Mutex::new(Vec::new()),
                sync_ranges: Mutex::new(Vec::new()),
                sync_attempts: AtomicU64::new(0),
                sync_ok: AtomicU64::new(0),
                dropped_bytes: AtomicU64::new(0),
                enospc_latched: Mutex::new(0),
            })),
        }
    }

    pub fn real(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            loop_thread: std::sync::Arc::new(Mutex::new(None)),
            inner: std::sync::Arc::new(Inner::Real(RealInner {
                root,
                files: Mutex::new(BTreeMap::new()),
                spans: Mutex::new(Vec::new()),
                sync_ranges: Mutex::new(Vec::new()),
                sync_attempts: AtomicU64::new(0),
                sync_ok: AtomicU64::new(0),
            })),
        })
    }

    pub fn set_faults(&self, faults: Faults) {
        if let Inner::Sim(sim) = self.inner.as_ref() {
            *sim.faults.lock().unwrap() = faults;
            *sim.enospc_latched.lock().unwrap() = 0;
        }
    }

    pub fn clear_faults(&self) {
        self.set_faults(Faults::default());
    }

    pub fn spans(&self) -> Vec<Span> {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.spans.lock().unwrap().clone(),
            Inner::Real(s) => s.spans.lock().unwrap().clone(),
        }
    }

    pub fn sync_ranges(&self) -> Vec<SyncRange> {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.sync_ranges.lock().unwrap().clone(),
            Inner::Real(s) => s.sync_ranges.lock().unwrap().clone(),
        }
    }

    pub fn sync_attempts(&self) -> u64 {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.sync_attempts.load(Ordering::Relaxed),
            Inner::Real(s) => s.sync_attempts.load(Ordering::Relaxed),
        }
    }

    pub fn sync_ok(&self) -> u64 {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.sync_ok.load(Ordering::Relaxed),
            Inner::Real(s) => s.sync_ok.load(Ordering::Relaxed),
        }
    }

    pub fn syncs_for(&self, name: &str) -> u64 {
        self.sync_ranges()
            .iter()
            .filter(|r| r.file == name)
            .count() as u64
    }

    pub fn dropped_bytes(&self) -> u64 {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.dropped_bytes.load(Ordering::Relaxed),
            Inner::Real(_) => 0,
        }
    }

    pub fn exists(&self, name: &str) -> bool {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.files.lock().unwrap().contains_key(name),
            Inner::Real(s) => s.root.join(name).exists(),
        }
    }

    pub fn list(&self) -> Vec<String> {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.files.lock().unwrap().keys().cloned().collect(),
            Inner::Real(s) => fs::read_dir(&s.root)
                .map(|rd| {
                    rd.filter_map(|e| e.ok())
                        .filter(|e| e.path().is_file())
                        .filter_map(|e| e.file_name().into_string().ok())
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub fn read(&self, name: &str) -> Result<Vec<u8>, Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let files = s.files.lock().unwrap();
                files
                    .get(name)
                    .map(|f| f.logical.clone())
                    .ok_or_else(|| Error::Io(format!("missing {name}")))
            }
            Inner::Real(s) => {
                let path = s.root.join(name);
                fs::read(&path).map_err(map_io)
            }
        }
    }

    pub fn len(&self, name: &str) -> Result<u64, Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let files = s.files.lock().unwrap();
                files
                    .get(name)
                    .map(|f| f.logical.len() as u64)
                    .ok_or_else(|| Error::Io(format!("missing {name}")))
            }
            Inner::Real(s) => {
                let path = s.root.join(name);
                let meta = fs::metadata(&path).map_err(map_io)?;
                Ok(meta.len())
            }
        }
    }

    pub fn create_wal(&self, name: &str) -> Result<(), Error> {
        self.write_replace(name, &wal::header_bytes())
    }

    pub fn write_replace(&self, name: &str, data: &[u8]) -> Result<(), Error> {
        let tmp = format!("{name}.tmp");
        self.remove(&tmp).ok();
        self.append_all(&tmp, data)?;
        self.sync(&tmp)?;
        self.rename(&tmp, name)?;
        self.sync_dir()?;
        Ok(())
    }

    pub fn append_all(&self, name: &str, data: &[u8]) -> Result<(), Error> {
        let start = if self.exists(name) {
            self.len(name)?
        } else {
            self.create_empty(name)?;
            0
        };
        match self.write_loop(name, data) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = self.truncate(name, start);
                Err(e)
            }
        }
    }

    fn write_loop(&self, name: &str, data: &[u8]) -> Result<(), Error> {
        let mut off = 0;
        while off < data.len() {
            let n = self.write_some(name, &data[off..])?;
            if n == 0 {
                return Err(Error::Io("short write returned 0".into()));
            }
            off += n;
        }
        Ok(())
    }

    fn write_some(&self, name: &str, data: &[u8]) -> Result<usize, Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => sim_write_some(s, name, data),
            Inner::Real(s) => real_write_some(s, name, data),
        }
    }

    pub fn truncate(&self, name: &str, len: u64) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let mut files = s.files.lock().unwrap();
                let f = files
                    .get_mut(name)
                    .ok_or_else(|| Error::Io(format!("missing {name}")))?;
                let n = len as usize;
                if n <= f.logical.len() {
                    f.logical.truncate(n);
                }
                if n <= f.durable.len() {
                    f.durable.truncate(n);
                }
                Ok(())
            }
            Inner::Real(s) => {
                let mut files = s.files.lock().unwrap();
                if let Some(f) = files.get_mut(name) {
                    f.set_len(len).map_err(map_io)?;
                    f.seek(SeekFrom::Start(len)).map_err(map_io)?;
                } else {
                    let path = s.root.join(name);
                    let f = OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .map_err(map_io)?;
                    f.set_len(len).map_err(map_io)?;
                }
                Ok(())
            }
        }
    }

    pub fn set_loop_thread(&self, id: std::thread::ThreadId) {
        *self.loop_thread.lock().unwrap() = Some(id);
    }

    pub fn sync(&self, name: &str) -> Result<(), Error> {
        if let Some(id) = *self.loop_thread.lock().unwrap() {
            if std::thread::current().id() == id {
                panic!("fsync on the event-loop thread");
            }
        }
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                s.sync_attempts.fetch_add(1, Ordering::Relaxed);
                {
                    let mut faults = s.faults.lock().unwrap();
                    if faults.fsync_errors > 0 {
                        faults.fsync_errors -= 1;
                        return Err(Error::Io("injected fsync error".into()));
                    }
                }
                let mut files = s.files.lock().unwrap();
                let f = files
                    .get_mut(name)
                    .ok_or_else(|| Error::Io(format!("missing {name}")))?;
                let start = common_prefix(&f.durable, &f.logical) as u64;
                let end = f.logical.len() as u64;
                if f.logical != f.durable {
                    s.sync_ranges.lock().unwrap().push(SyncRange {
                        file: name.to_string(),
                        start,
                        end,
                    });
                }
                f.durable = f.logical.clone();
                let end = f.durable.len() as u64;
                drop(files);
                mark_spans_synced(&s.spans, name, end);
                s.sync_ok.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Inner::Real(s) => {
                s.sync_attempts.fetch_add(1, Ordering::Relaxed);
                let mut files = s.files.lock().unwrap();
                let file = open_cached(s, &mut files, name)?;
                file.flush().map_err(map_io)?;
                syncfile::durable_sync(file).map_err(map_io)?;
                // The crash shim records ranges. On a real file the durability
                // thread was spending the samples after F_FULLFSYNC walking
                // every historical span. Mark only the tail, then drop spans
                // that are already synced so the walk stays short.
                let len = file.seek(SeekFrom::End(0)).map_err(map_io)?;
                mark_spans_synced(&s.spans, name, len);
                s.sync_ok.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let mut files = s.files.lock().unwrap();
                let file = files
                    .remove(from)
                    .ok_or_else(|| Error::Io(format!("missing {from}")))?;
                files.insert(to.to_string(), file);
                Ok(())
            }
            Inner::Real(s) => {
                let mut files = s.files.lock().unwrap();
                files.remove(from);
                files.remove(to);
                drop(files);
                let src = s.root.join(from);
                let dst = s.root.join(to);
                fs::rename(&src, &dst).map_err(map_io)
            }
        }
    }

    pub fn remove(&self, name: &str) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                s.files.lock().unwrap().remove(name);
                Ok(())
            }
            Inner::Real(s) => {
                s.files.lock().unwrap().remove(name);
                let path = s.root.join(name);
                if path.exists() {
                    fs::remove_file(&path).map_err(map_io)?;
                }
                Ok(())
            }
        }
    }

    pub fn sync_dir(&self) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(_) => Ok(()),
            Inner::Real(s) => syncfile::sync_dir(&s.root).map_err(map_io),
        }
    }

    fn create_empty(&self, name: &str) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                s.files.lock().unwrap().insert(
                    name.to_string(),
                    SimFile {
                        logical: Vec::new(),
                        durable: Vec::new(),
                    },
                );
                Ok(())
            }
            Inner::Real(s) => {
                let path = s.root.join(name);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(map_io)?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .read(true)
                    .open(&path)
                    .map_err(map_io)?;
                s.files.lock().unwrap().insert(name.to_string(), file);
                Ok(())
            }
        }
    }

    pub fn note_span(&self, span: Span) {
        match self.inner.as_ref() {
            Inner::Sim(s) => s.spans.lock().unwrap().push(span),
            Inner::Real(s) => s.spans.lock().unwrap().push(span),
        }
    }

    pub fn drop_spans_from(&self, file: &str, offset: u64) {
        let spans = match self.inner.as_ref() {
            Inner::Sim(s) => &s.spans,
            Inner::Real(s) => &s.spans,
        };
        let mut spans = spans.lock().unwrap();
        for span in spans.iter_mut() {
            if span.file == file && span.offset >= offset && !span.synced {
                span.dropped = true;
            }
        }
    }

    /// Drop bytes that were not covered by a successful fsync.
    pub fn discard_unsynced(&self) {
        if let Inner::Sim(s) = self.inner.as_ref() {
            let mut files = s.files.lock().unwrap();
            let mut dropped = 0u64;
            for f in files.values_mut() {
                if f.logical.len() > f.durable.len()
                    && f.logical.starts_with(&f.durable)
                {
                    dropped += (f.logical.len() - f.durable.len()) as u64;
                } else if f.logical != f.durable {
                    dropped += f.logical.len().abs_diff(f.durable.len()) as u64;
                }
                f.logical = f.durable.clone();
            }
            s.dropped_bytes.fetch_add(dropped, Ordering::Relaxed);
            drop(files);
            for span in s.spans.lock().unwrap().iter_mut() {
                if !span.synced {
                    span.dropped = true;
                }
            }
        }
    }

    /// Corrupt durable bytes in place. Used by fault tests, not by crash recovery.
    pub fn testing_flip(&self, name: &str, offset: usize) -> Result<(), Error> {
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let mut files = s.files.lock().unwrap();
                let f = files
                    .get_mut(name)
                    .ok_or_else(|| Error::Io(format!("missing {name}")))?;
                if offset >= f.durable.len() {
                    return Err(Error::Io("flip past end".into()));
                }
                f.durable[offset] ^= 0xff;
                f.logical = f.durable.clone();
                Ok(())
            }
            Inner::Real(s) => {
                let path = s.root.join(name);
                let mut bytes = fs::read(&path).map_err(map_io)?;
                if offset >= bytes.len() {
                    return Err(Error::Io("flip past end".into()));
                }
                bytes[offset] ^= 0xff;
                fs::write(&path, &bytes).map_err(map_io)?;
                if let Some(f) = s.files.lock().unwrap().get_mut(name) {
                    let _ = f.seek(SeekFrom::Start(0));
                    let _ = f.write_all(&bytes);
                    let _ = f.set_len(bytes.len() as u64);
                }
                Ok(())
            }
        }
    }

    pub fn testing_truncate_durable(&self, name: &str, len: u64) -> Result<(), Error> {
        self.truncate(name, len)
    }

    pub fn export_durable(&self, dir: &Path) -> Result<(), Error> {
        fs::create_dir_all(dir).map_err(map_io)?;
        match self.inner.as_ref() {
            Inner::Sim(s) => {
                let files = s.files.lock().unwrap();
                for (name, f) in files.iter() {
                    if name.ends_with(".tmp") {
                        continue;
                    }
                    fs::write(dir.join(name), &f.durable).map_err(map_io)?;
                }
                Ok(())
            }
            Inner::Real(s) => {
                for name in self.list() {
                    let bytes = fs::read(s.root.join(&name)).map_err(map_io)?;
                    fs::write(dir.join(name), bytes).map_err(map_io)?;
                }
                Ok(())
            }
        }
    }

    pub fn root(&self) -> Option<&Path> {
        match self.inner.as_ref() {
            Inner::Real(s) => Some(&s.root),
            Inner::Sim(_) => None,
        }
    }
}

fn mark_spans_synced(spans: &Mutex<Vec<Span>>, name: &str, end: u64) {
    let mut spans = spans.lock().unwrap();
    for span in spans.iter_mut() {
        if span.synced || span.dropped || span.file != name {
            continue;
        }
        if span.offset + span.len <= end {
            span.synced = true;
        }
    }
    // Synced spans are only an audit trail. Drop them once the list is long
    // so a multi-thousand-write run does not memcmp the whole history on
    // every fsync. Unsynced spans stay, which is what the crash checks need.
    if spans.len() > 4096 {
        spans.retain(|span| !span.synced);
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

fn sim_write_some(s: &SimInner, name: &str, data: &[u8]) -> Result<usize, Error> {
    {
        let mut latched = s.enospc_latched.lock().unwrap();
        if *latched > 0 {
            *latched -= 1;
            return Err(Error::DiskFull("no space left on device".into()));
        }
    }
    let mut faults = s.faults.lock().unwrap();
    if faults.enospc_writes > 0 {
        faults.enospc_writes -= 1;
        return Err(Error::DiskFull("no space left on device".into()));
    }
    let mut n = data.len();
    if let Some(limit) = faults.short_once.take() {
        n = n.min(limit);
        if faults.enospc_after_short {
            *s.enospc_latched.lock().unwrap() = u32::MAX / 2;
        }
    }
    drop(faults);
    if n == 0 {
        return Err(Error::Io("short write returned 0".into()));
    }
    let mut files = s.files.lock().unwrap();
    let f = files.entry(name.to_string()).or_insert_with(|| SimFile {
        logical: Vec::new(),
        durable: Vec::new(),
    });
    f.logical.extend_from_slice(&data[..n]);
    Ok(n)
}

fn open_cached<'a>(
    s: &'a RealInner,
    files: &'a mut BTreeMap<String, File>,
    name: &str,
) -> Result<&'a mut File, Error> {
    if !files.contains_key(name) {
        let path = s.root.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(map_io)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(map_io)?;
        files.insert(name.to_string(), file);
    }
    Ok(files.get_mut(name).unwrap())
}

fn real_write_some(s: &RealInner, name: &str, data: &[u8]) -> Result<usize, Error> {
    let mut files = s.files.lock().unwrap();
    let file = open_cached(s, &mut files, name)?;
    file.seek(SeekFrom::End(0)).map_err(map_io)?;
    write_all_handling_short(&mut *file, data).map_err(map_io)?;
    Ok(data.len())
}
