use std::collections::BTreeMap;

use crate::record::ValueRec;

#[derive(Clone, Debug, Default)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, ValueRec>,
    bytes: usize,
    max_seq: u64,
}

impl Memtable {
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub fn get(&self, key: &[u8]) -> Option<&ValueRec> {
        self.map.get(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, rec: ValueRec) {
        let key_len = key.len();
        let new_val_len = rec.value.len();
        self.max_seq = self.max_seq.max(rec.seq);
        if let Some(old) = self.map.insert(key, rec) {
            let old_cost = key_len + old.value.len() + 32;
            self.bytes = self.bytes.saturating_sub(old_cost);
        }
        self.bytes += key_len + new_val_len + 32;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &ValueRec)> {
        self.map.iter()
    }

    /// Entries for an SSTable. Already-expired values become tombstones so an
    /// older SSTable cannot revive them.
    pub fn entries_for_flush(&self, now: u64) -> Vec<(Vec<u8>, ValueRec)> {
        self.map
            .iter()
            .map(|(k, v)| {
                let mut v = v.clone();
                if !v.tombstone && v.expiry != 0 && v.expiry <= now {
                    v.tombstone = true;
                    v.value.clear();
                    v.expiry = 0;
                }
                (k.clone(), v)
            })
            .collect()
    }
}
