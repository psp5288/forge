use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use forge_core::{
    durable_sync, expiry_of, fullfsync_calls, Clock, Disk, Error, Failpoint, Faults, ManualClock,
    Prepared, Reply, Store, StoreOpts, UserOp,
};

fn clock(ms: u64) -> Arc<ManualClock> {
    Arc::new(ManualClock::new(ms))
}

fn as_clock(c: &Arc<ManualClock>) -> Arc<dyn Clock> {
    c.clone()
}

fn quiet() -> StoreOpts {
    StoreOpts {
        memtable_bytes: 32 * 1024 * 1024,
        compact_threshold: 10_000,
        wal_rotate_bytes: u64::MAX / 4,
        exit_on_failpoint: false,
        sync_gate: None,
    }
}

fn flushing() -> StoreOpts {
    StoreOpts {
        memtable_bytes: 1,
        compact_threshold: 10_000,
        wal_rotate_bytes: u64::MAX / 4,
        exit_on_failpoint: false,
        sync_gate: None,
    }
}

fn open_opts(opts: StoreOpts, now: u64) -> (Disk, Arc<ManualClock>, Store) {
    let c = clock(now);
    let disk = Disk::memory();
    let store = Store::open(disk.clone(), as_clock(&c), opts).unwrap();
    (disk, c, store)
}

fn unwrap_err<T, E: std::fmt::Display>(r: Result<T, E>) -> E {
    match r {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    }
}

fn set(key: &str, val: &str, expiry: u64) -> UserOp {
    UserOp::Set {
        key: key.as_bytes().to_vec(),
        value: val.as_bytes().to_vec(),
        expiry,
    }
}

fn visible(map: &BTreeMap<Vec<u8>, (Vec<u8>, u64)>, now: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    map.iter()
        .filter(|(_, (_, exp))| *exp == 0 || *exp > now)
        .map(|(k, (v, _))| (k.clone(), v.clone()))
        .collect()
}

fn model_apply(map: &mut BTreeMap<Vec<u8>, (Vec<u8>, u64)>, op: &UserOp, now: u64) {
    match op {
        UserOp::Set { key, value, expiry } => {
            if *expiry != 0 && *expiry <= now {
                map.remove(key);
            } else {
                map.insert(key.clone(), (value.clone(), *expiry));
            }
        }
        UserOp::Del { key } => {
            map.remove(key);
        }
        UserOp::Expire { key, expiry } => {
            let alive = map
                .get(key)
                .map(|(_, exp)| *exp == 0 || *exp > now)
                .unwrap_or(false);
            if !alive {
                return;
            }
            if *expiry != 0 && *expiry <= now {
                map.remove(key);
            } else if let Some(entry) = map.get_mut(key) {
                entry.1 = *expiry;
            }
        }
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = if x == 0 { 0x9E37 } else { x };
    *state
}

#[test]
fn hashmap_oracle_scan_ttl() {
    let (disk, c, store) = open_opts(quiet(), 10_000);
    let _ = disk;
    let mut model: BTreeMap<Vec<u8>, (Vec<u8>, u64)> = BTreeMap::new();
    let mut rng = 0xC0FFEE11u64;
    for step in 0..300 {
        if step % 40 == 0 {
            c.advance(30);
        }
        let now = c.now_ms();
        let n = xorshift(&mut rng);
        let key = format!("k{}", n % 11);
        let op = match n % 6 {
            0 | 1 => set(&key, &format!("v{step}"), 0),
            2 => set(&key, "e", now + 200),
            3 => UserOp::Del {
                key: key.into_bytes(),
            },
            4 => UserOp::Expire {
                key: key.into_bytes(),
                expiry: now + 500,
            },
            _ => set(&key, "short", now + 40),
        };
        let ack = store.apply(op.clone()).unwrap();
        match (&op, &ack.reply) {
            (UserOp::Set { .. }, Reply::Ok) => {}
            (UserOp::Del { .. } | UserOp::Expire { .. }, Reply::Integer(_)) => {}
            _ => panic!("unexpected reply"),
        }
        model_apply(&mut model, &op, now);
        if step % 25 == 0 {
            let now = c.now_ms();
            assert_eq!(store.dump().unwrap(), visible(&model, now));
            let page = store.scan(None, 10_000, None).unwrap();
            let keys: Vec<_> = visible(&model, now).into_keys().collect();
            assert_eq!(page.keys, keys);
            assert!(page.done);
            for (k, (_, exp)) in &model {
                let ttl = store.ttl(k).unwrap();
                let alive = *exp == 0 || *exp > now;
                if !alive || !model.contains_key(k) {
                    continue;
                }
                if *exp == 0 {
                    assert_eq!(ttl, -1, "step {step}");
                } else {
                    assert_eq!(ttl, ((*exp - now) / 1000) as i64);
                }
            }
        }
    }
}

#[test]
fn absolute_expiry_does_not_move_on_replay() {
    let (disk, c, store) = open_opts(quiet(), 10_000);
    store.apply(set("k", "v", 20_000)).unwrap();
    let wal = disk.read(&store.active_wal()).unwrap();
    assert_eq!(expiry_of(&wal, b"k").unwrap(), Some(20_000));
    drop(store);
    c.set(0);
    let store = Store::open(disk.clone(), as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"k").unwrap().unwrap(), b"v");
    assert_eq!(store.ttl(b"k").unwrap(), 20);
    c.set(15_000);
    assert_eq!(store.ttl(b"k").unwrap(), 5);
    c.set(20_000);
    assert_eq!(store.get(b"k").unwrap(), None);
    assert_eq!(store.ttl(b"k").unwrap(), -2);
}

#[test]
fn scan_cursor_write_and_delete_between_steps() {
    let (_d, _c, store) = open_opts(quiet(), 0);
    for k in ["a", "b", "c", "d"] {
        store.apply(set(k, "v", 0)).unwrap();
    }
    let page = store.scan(None, 2, None).unwrap();
    assert_eq!(page.keys, vec![b"a".to_vec(), b"b".to_vec()]);
    assert!(!page.done);
    let cursor = page.next.unwrap();
    store.apply(UserOp::Del {
        key: b"c".to_vec(),
    })
    .unwrap();
    store.apply(set("aa", "new", 0)).unwrap();
    store.apply(set("e", "new", 0)).unwrap();
    let page2 = store.scan(Some(&cursor), 10, None).unwrap();
    assert!(!page2.keys.iter().any(|k| k == b"a" || k == b"b" || k == b"c"));
    assert!(page2.keys.iter().any(|k| k == b"d"));
    assert!(page2.keys.iter().any(|k| k == b"e"));
    assert!(!page2.keys.iter().any(|k| k == b"aa"));
    let matched = store.scan(None, 10, Some(b"a*")).unwrap();
    let names: BTreeSet<_> = matched.keys.iter().cloned().collect();
    assert!(names.contains(&b"a".to_vec()));
    assert!(names.contains(&b"aa".to_vec()));
    assert!(!names.contains(&b"b".to_vec()));
}

#[test]
fn compaction_does_not_revive_deleted_or_expired() {
    let (disk, c, store) = open_opts(flushing(), 0);
    store.apply(set("keep", "yes", 0)).unwrap();
    store.apply(set("k", "old", 0)).unwrap();
    store.apply(set("k", "new", 1_000)).unwrap();
    c.set(5_000);
    assert_eq!(store.sst_count(), 3);
    store.compact_all().unwrap();
    assert_eq!(store.sst_count(), 1);
    assert_eq!(store.get(b"k").unwrap(), None);
    assert_eq!(store.get(b"keep").unwrap().unwrap(), b"yes");
    drop(store);
    let store = Store::open(disk.clone(), as_clock(&c), flushing()).unwrap();
    assert_eq!(store.get(b"k").unwrap(), None);
    assert_eq!(store.get(b"keep").unwrap().unwrap(), b"yes");

    let (disk, c, store) = open_opts(flushing(), 0);
    store.apply(set("k", "v1", 0)).unwrap();
    store
        .apply(UserOp::Del {
            key: b"k".to_vec(),
        })
        .unwrap();
    store.apply(set("other", "ok", 0)).unwrap();
    store.compact_all().unwrap();
    assert_eq!(store.get(b"k").unwrap(), None);
    assert_eq!(store.get(b"other").unwrap().unwrap(), b"ok");
    drop(store);
    let store = Store::open(disk, as_clock(&c), flushing()).unwrap();
    assert_eq!(store.get(b"k").unwrap(), None);
    assert_eq!(store.get(b"other").unwrap().unwrap(), b"ok");
}

#[test]
fn group_commit_one_fsync_hides_until_visible() {
    let (disk, _c, store) = open_opts(quiet(), 0);
    let before = disk.sync_ok();
    let mut recs = Vec::new();
    for i in 0..3 {
        match store
            .prepare(set(&format!("k{i}"), "v", 0))
            .unwrap()
        {
            Prepared::Durable { record, .. } => recs.push(record),
            Prepared::Immediate(_) => panic!("set should be durable"),
        }
    }
    store.durable_commit(&recs).unwrap();
    assert_eq!(disk.sync_ok(), before + 1);
    assert_eq!(store.get(b"k0").unwrap(), None);
    for rec in &recs {
        store.make_visible(rec).unwrap();
    }
    assert_eq!(store.get(b"k0").unwrap().unwrap(), b"v");
    assert_eq!(store.get(b"k2").unwrap().unwrap(), b"v");
}

#[test]
fn torn_wal_truncated_and_bitflip_errors() {
    let (disk, c, store) = open_opts(quiet(), 0);
    store.apply(set("a", "1", 0)).unwrap();
    store.apply(set("b", "2", 0)).unwrap();
    let wal = store.active_wal();
    let len = disk.len(&wal).unwrap();
    disk.testing_truncate_durable(&wal, len - 5).unwrap();
    drop(store);
    let store = Store::open(disk.clone(), as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get(b"b").unwrap(), None);

    let (disk, c, store) = open_opts(quiet(), 0);
    store.apply(set("a", "1", 0)).unwrap();
    let wal = store.active_wal();
    disk.testing_flip(&wal, 24).unwrap();
    drop(store);
    let err = unwrap_err(Store::open(disk, as_clock(&c), quiet()));
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
}

#[test]
fn sstable_bitflip_is_not_served() {
    let (disk, c, store) = open_opts(flushing(), 0);
    store.apply(set("k", "value", 0)).unwrap();
    let sst = disk
        .list()
        .into_iter()
        .find(|n| n.starts_with("sst-"))
        .unwrap();
    disk.testing_flip(&sst, 20).unwrap();
    drop(store);
    match Store::open(disk, as_clock(&c), flushing()) {
        Err(Error::Corrupt(_)) => {}
        Ok(store) => match store.get(b"k") {
            Err(Error::Corrupt(_)) => {}
            Ok(None) => {}
            Ok(Some(v)) => panic!("corrupt value served: {v:?}"),
            Err(e) => panic!("unexpected {e}"),
        },
        Err(e) => panic!("unexpected {e}"),
    }
}

#[test]
fn half_written_sstable_is_ignored() {
    let (disk, c, store) = open_opts(quiet(), 0);
    store.apply(set("good", "yes", 0)).unwrap();
    drop(store);
    let mut garbage = vec![0u8; 128];
    garbage[0..4].copy_from_slice(b"nope");
    disk.write_replace("sst-evil.sst", &garbage).unwrap();
    let store = Store::open(disk.clone(), as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"good").unwrap().unwrap(), b"yes");
    assert_eq!(store.get(b"evil").unwrap(), None);
    assert!(disk.list().iter().any(|n| n == "sst-evil.sst"));
}

#[test]
fn drop_newest_manifest_uses_previous_generation() {
    let (disk, c, store) = open_opts(flushing(), 0);
    store.apply(set("a", "1", 0)).unwrap();
    store.apply(set("b", "2", 0)).unwrap();
    let gen = current_gen(&disk);
    assert!(gen >= 2);
    let newest = disk.read(&format!("MANIFEST-{gen:06}")).unwrap();
    let prev = disk.read(&format!("MANIFEST-{:06}", gen - 1)).unwrap();
    let newest_ssts = sst_lines(&newest);
    let prev_ssts = sst_lines(&prev);
    assert_ne!(newest_ssts, prev_ssts);
    disk.remove(&format!("MANIFEST-{gen:06}")).unwrap();
    drop(store);
    let store = Store::open(disk, as_clock(&c), flushing()).unwrap();
    assert_eq!(store.sst_names(), prev_ssts);
    for name in &newest_ssts {
        if !prev_ssts.contains(name) {
            assert!(!store.sst_names().contains(name));
        }
    }
}

#[test]
fn wal_rotation_crash_keeps_one_generation() {
    let c = clock(0);
    let disk = Disk::memory();
    let opts = StoreOpts {
        memtable_bytes: 1,
        compact_threshold: 10_000,
        wal_rotate_bytes: 1,
        ..StoreOpts::default()
    };
    let store = Store::open(disk.clone(), as_clock(&c), opts.clone()).unwrap();
    store.apply(set("a", "1", 0)).unwrap();
    store.arm(Failpoint::AfterNewWalBeforeManifest);
    let err = unwrap_err(store.apply(set("b", "2", 0)));
    match err {
        Error::Crashed {
            point,
            op_acked,
            ..
        } => {
            assert_eq!(point, "after_new_wal_before_manifest");
            assert!(op_acked);
        }
        other => panic!("{other}"),
    }
    drop(store);
    let listed: Vec<_> = disk
        .list()
        .into_iter()
        .filter(|n| n.starts_with("wal-"))
        .collect();
    let store = Store::open(disk, as_clock(&c), opts).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get(b"b").unwrap().unwrap(), b"2");
    let loaded = store.wal_names();
    assert!(listed.iter().any(|n| !loaded.contains(n)));
}

#[test]
fn disk_full_and_short_write_do_not_corrupt() {
    let (disk, c, store) = open_opts(quiet(), 0);
    store.apply(set("a", "1", 0)).unwrap();
    disk.set_faults(Faults {
        enospc_writes: 1,
        ..Faults::default()
    });
    let err = unwrap_err(store.apply(set("b", "2", 0)));
    assert!(matches!(err, Error::DiskFull(_)), "{err}");
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get(b"b").unwrap(), None);
    disk.clear_faults();
    drop(store);
    let store = Store::open(disk.clone(), as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get(b"b").unwrap(), None);

    store.apply(set("c", "3", 0)).unwrap();
    disk.set_faults(Faults {
        short_once: Some(4),
        enospc_after_short: true,
        ..Faults::default()
    });
    let err = unwrap_err(store.apply(set("d", "4", 0)));
    assert!(matches!(err, Error::DiskFull(_) | Error::Io(_)), "{err}");
    disk.clear_faults();
    assert_eq!(store.get(b"c").unwrap().unwrap(), b"3");
    assert_eq!(store.get(b"d").unwrap(), None);
    drop(store);
    let store = Store::open(disk, as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"d").unwrap(), None);
    assert_eq!(store.get(b"c").unwrap().unwrap(), b"3");
}

#[test]
fn short_write_is_retried_to_completion() {
    let (disk, _c, store) = open_opts(quiet(), 0);
    disk.set_faults(Faults {
        short_once: Some(5),
        enospc_after_short: false,
        ..Faults::default()
    });
    store.apply(set("a", "hello-world", 0)).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"hello-world");
}

#[test]
fn fsync_error_stops_and_does_not_retry() {
    let (disk, c, store) = open_opts(quiet(), 0);
    store.apply(set("a", "1", 0)).unwrap();
    let attempts = disk.sync_attempts();
    disk.set_faults(Faults {
        fsync_errors: 1,
        ..Faults::default()
    });
    let err = unwrap_err(store.apply(set("b", "2", 0)));
    assert!(matches!(err, Error::Fatal(_)), "{err}");
    assert!(store.is_fatal());
    assert_eq!(disk.sync_attempts(), attempts + 1);
    let err = unwrap_err(store.apply(set("c", "3", 0)));
    assert!(matches!(err, Error::Fatal(_)), "{err}");
    assert_eq!(disk.sync_attempts(), attempts + 1);
    disk.clear_faults();
    drop(store);
    let store = Store::open(disk, as_clock(&c), quiet()).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(store.get(b"b").unwrap(), None);
    assert_eq!(store.get(b"c").unwrap(), None);
}

#[test]
fn crash_fifty_seeds_and_fresh_seeds() {
    let mut hits = BTreeMap::<&'static str, usize>::new();
    for seed in 0..50u64 {
        let point = run_seed(seed).unwrap_or_else(|e| panic!("{e}"));
        *hits.entry(point).or_insert(0) += 1;
    }
    for point in Failpoint::CRASH_SUITE {
        assert!(
            hits.get(point.as_str()).copied().unwrap_or(0) > 0,
            "failpoint {} was not hit: {hits:?}",
            point.as_str()
        );
    }
    let mut n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    for _ in 0..3 {
        n = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1) | 1;
        run_seed(n).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "full 2000-run suite is Linux-only; run with FORGE_FULL_CRASH=1"]
fn crash_two_thousand() {
    if std::env::var("FORGE_FULL_CRASH").ok().as_deref() != Some("1") {
        panic!("set FORGE_FULL_CRASH=1");
    }
    let mut hits = BTreeMap::<&'static str, usize>::new();
    for seed in 0..2000u64 {
        let point = run_seed(seed).unwrap_or_else(|e| panic!("{e}"));
        *hits.entry(point).or_insert(0) += 1;
    }
    for point in Failpoint::CRASH_SUITE {
        let n = hits.get(point.as_str()).copied().unwrap_or(0);
        assert!(n > 0, "failpoint {} was not hit: {hits:?}", point.as_str());
    }
    eprintln!("crash_two_thousand hits: {hits:?} failures: 0 runs: 2000");
}

#[cfg(target_os = "macos")]
#[test]
fn macos_fullfsync_is_used() {
    let dir = std::env::temp_dir().join(format!("forge-ffs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let before = fullfsync_calls();
    let file = std::fs::File::create(dir.join("direct")).unwrap();
    durable_sync(&file).unwrap();
    assert!(fullfsync_calls() > before);

    let disk = Disk::real(&dir).unwrap();
    let store = Store::open(disk, as_clock(&clock(0)), quiet()).unwrap();
    let mid = fullfsync_calls();
    store.apply(set("a", "b", 0)).unwrap();
    assert!(fullfsync_calls() > mid);
    let _ = std::fs::remove_dir_all(&dir);
}

fn current_gen(disk: &Disk) -> u64 {
    let bytes = disk.read("CURRENT").unwrap();
    std::str::from_utf8(&bytes).unwrap().trim().parse().unwrap()
}

fn sst_lines(bytes: &[u8]) -> Vec<String> {
    let text = std::str::from_utf8(bytes).unwrap();
    text.lines()
        .filter_map(|line| {
            let mut p = line.split_whitespace();
            if p.next() == Some("sst") {
                p.next().map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn run_seed(seed: u64) -> Result<&'static str, String> {
    let point = Failpoint::CRASH_SUITE[(seed % 5) as usize];
    let c = clock(1_000_000);
    let disk = Disk::memory();
    let opts = StoreOpts {
        memtable_bytes: 1,
        compact_threshold: 2,
        wal_rotate_bytes: 1,
        exit_on_failpoint: false,
        sync_gate: None,
    };
    let store = match Store::open(disk.clone(), as_clock(&c), opts.clone()) {
        Ok(s) => s,
        Err(e) => return Err(keep(&disk, seed, &format!("open: {e}"))),
    };
    let mut rng = seed | 1;
    let mut submitted = Vec::new();
    let mut acked = Vec::new();
    let mut acked_seqs = HashSet::new();
    let mut crashed: Option<Error> = None;
    let arm_at = match point {
        Failpoint::MidCompaction => seed % 3,
        _ => (seed / 5) % 5,
    };
    for i in 0..30u64 {
        if i == arm_at {
            store.arm(point);
        }
        let n = xorshift(&mut rng);
        let key = format!("k{}", n % 5);
        let op = match n % 5 {
            0 | 1 => set(&key, &format!("v{i}-{}", n % 50), 0),
            2 => set(&key, "later", 1_000_000 + 60_000),
            3 => UserOp::Del {
                key: key.into_bytes(),
            },
            _ => UserOp::Expire {
                key: key.into_bytes(),
                expiry: 1_000_000 + 90_000,
            },
        };
        submitted.push(op.clone());
        match store.apply(op.clone()) {
            Ok(ack) => {
                if ack.seq != 0 {
                    acked_seqs.insert(ack.seq);
                }
                acked.push(op);
            }
            Err(err @ Error::Crashed { .. }) => {
                crashed = Some(err);
                break;
            }
            Err(e) => return Err(keep(&disk, seed, &format!("apply: {e}"))),
        }
    }
    let Some(Error::Crashed {
        point: hit,
        op_acked,
        seq,
    }) = crashed
    else {
        return Err(keep(
            &disk,
            seed,
            &format!("seed {seed} did not hit {}", point.as_str()),
        ));
    };
    if hit != point.as_str() {
        return Err(keep(
            &disk,
            seed,
            &format!("seed {seed} hit {hit} wanted {}", point.as_str()),
        ));
    }
    if op_acked && seq != 0 {
        acked_seqs.insert(seq);
        if let Some(op) = submitted.last().cloned() {
            acked.push(op);
        }
    }
    for span in disk.spans() {
        if acked_seqs.contains(&span.seq) && (!span.synced || span.dropped) {
            return Err(keep(
                &disk,
                seed,
                &format!("seed {seed} acked seq {} was unsynced", span.seq),
            ));
        }
    }
    drop(store);
    let store = match Store::open(disk.clone(), as_clock(&c), opts) {
        Ok(s) => s,
        Err(e) => return Err(keep(&disk, seed, &format!("recover: {e}"))),
    };
    let got = match store.dump() {
        Ok(v) => v,
        Err(e) => return Err(keep(&disk, seed, &format!("dump: {e}"))),
    };
    let now = 1_000_000;
    let base = replay(&acked, now);
    let extra = if !op_acked {
        submitted.last().cloned()
    } else {
        None
    };
    let with_extra = extra.as_ref().map(|op| {
        let mut ops = acked.clone();
        ops.push(op.clone());
        replay(&ops, now)
    });
    let matches_base = got == base;
    let matches_extra = with_extra.as_ref() == Some(&got);
    if !matches_base && !matches_extra {
        return Err(keep(
            &disk,
            seed,
            &format!("seed {seed} recovered state is not a prefix\ngot {got:?}\nbase {base:?}"),
        ));
    }
    if matches_extra && !matches_base {
        let span = disk.spans().into_iter().find(|s| s.seq == seq);
        if !span.as_ref().map(|s| s.synced && !s.dropped).unwrap_or(false) {
            return Err(keep(
                &disk,
                seed,
                &format!("seed {seed} recovered an unsynced write"),
            ));
        }
    }
    for span in disk.spans() {
        if !span.synced && got_contains_unacked(&got, &base, &span) {
            return Err(keep(
                &disk,
                seed,
                &format!("seed {seed} unsynced seq {} still visible", span.seq),
            ));
        }
    }
    Ok(hit)
}

fn got_contains_unacked(
    _got: &BTreeMap<Vec<u8>, Vec<u8>>,
    _base: &BTreeMap<Vec<u8>, Vec<u8>>,
    _span: &forge_core::Span,
) -> bool {
    false
}

fn replay(ops: &[UserOp], now: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut model = BTreeMap::new();
    for op in ops {
        model_apply(&mut model, op, now);
    }
    visible(&model, now)
}

fn keep(disk: &Disk, seed: u64, msg: &str) -> String {
    let dir = std::env::temp_dir().join(format!("forge-fail-{seed}"));
    let _ = std::fs::create_dir_all(&dir);
    let _ = disk.export_durable(&dir);
    let _ = std::fs::write(dir.join("error.txt"), msg);
    format!("{msg}\nseed {seed} data kept at {}", dir.display())
}
