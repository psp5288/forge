use crate::bloom::Bloom;
use crate::checksum::crc32;
use crate::error::Error;
use crate::record::ValueRec;

const FOOTER_LEN: usize = 40;
const FOOTER_MAGIC: &[u8; 4] = b"FSTS";
const BLOCK_TARGET: usize = 4096;

#[derive(Clone, Debug)]
struct IndexEnt {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

#[derive(Clone, Debug)]
pub struct SsTable {
    data: Vec<u8>,
    index: Vec<IndexEnt>,
    bloom: Bloom,
}

impl SsTable {
    pub fn build(entries: &[(Vec<u8>, ValueRec)]) -> Result<Vec<u8>, Error> {
        let mut bloom = Bloom::new(entries.len());
        let mut blocks = Vec::new();
        let mut index = Vec::new();
        let mut block = Vec::new();
        let mut count: u32 = 0;
        let mut first: Option<Vec<u8>> = None;

        let flush_block = |block: &mut Vec<u8>,
                           count: &mut u32,
                           first: &mut Option<Vec<u8>>,
                           blocks: &mut Vec<u8>,
                           index: &mut Vec<IndexEnt>| {
            if *count == 0 {
                return;
            }
            let mut payload = Vec::new();
            payload.extend_from_slice(&count.to_le_bytes());
            payload.extend_from_slice(block);
            let sum = crc32(&payload);
            let offset = blocks.len() as u64;
            blocks.extend_from_slice(&payload);
            blocks.extend_from_slice(&sum.to_le_bytes());
            index.push(IndexEnt {
                first_key: first.take().unwrap_or_default(),
                offset,
                len: (payload.len() + 4) as u32,
            });
            block.clear();
            *count = 0;
        };

        for (key, rec) in entries {
            bloom.insert(key);
            if first.is_none() {
                first = Some(key.clone());
            }
            encode_entry(&mut block, key, rec);
            count += 1;
            if block.len() >= BLOCK_TARGET {
                flush_block(&mut block, &mut count, &mut first, &mut blocks, &mut index);
            }
        }
        flush_block(&mut block, &mut count, &mut first, &mut blocks, &mut index);

        let bloom_off = blocks.len() as u64;
        let bloom_bytes = bloom.encode();
        blocks.extend_from_slice(&bloom_bytes);
        let bloom_len = bloom_bytes.len() as u64;

        let index_off = blocks.len() as u64;
        let index_bytes = encode_index(&index)?;
        blocks.extend_from_slice(&index_bytes);
        let index_len = index_bytes.len() as u64;

        let mut footer_body = Vec::with_capacity(32);
        footer_body.extend_from_slice(&index_off.to_le_bytes());
        footer_body.extend_from_slice(&index_len.to_le_bytes());
        footer_body.extend_from_slice(&bloom_off.to_le_bytes());
        footer_body.extend_from_slice(&bloom_len.to_le_bytes());
        let sum = crc32(&footer_body);
        blocks.extend_from_slice(&footer_body);
        blocks.extend_from_slice(&sum.to_le_bytes());
        blocks.extend_from_slice(FOOTER_MAGIC);
        Ok(blocks)
    }

    pub fn open(data: Vec<u8>) -> Result<Self, Error> {
        if data.len() < FOOTER_LEN {
            return Err(Error::Corrupt("sstable too small".into()));
        }
        let footer = &data[data.len() - FOOTER_LEN..];
        if &footer[36..40] != FOOTER_MAGIC {
            return Err(Error::Corrupt("sstable footer magic".into()));
        }
        if crc32(&footer[..32]) != u32::from_le_bytes(footer[32..36].try_into().unwrap()) {
            return Err(Error::Corrupt("sstable footer checksum".into()));
        }
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let bloom_off = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
        let bloom_len = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let end = data.len() - FOOTER_LEN;
        if index_off.saturating_add(index_len) > end || bloom_off.saturating_add(bloom_len) > end {
            return Err(Error::Corrupt("sstable offsets".into()));
        }
        let (bloom, consumed) = Bloom::decode(&data[bloom_off..bloom_off + bloom_len])?;
        if consumed != bloom_len {
            return Err(Error::Corrupt("sstable bloom length".into()));
        }
        let index = decode_index(&data[index_off..index_off + index_len])?;
        Ok(Self { data, index, bloom })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<ValueRec>, Error> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let Some(ent) = self.find_block(key) else {
            return Ok(None);
        };
        let entries = self.read_block(ent)?;
        Ok(entries.into_iter().find(|(k, _)| k == key).map(|(_, v)| v))
    }

    pub fn iter(&self) -> Result<Vec<(Vec<u8>, ValueRec)>, Error> {
        let mut out = Vec::new();
        for ent in &self.index {
            out.extend(self.read_block(ent)?);
        }
        Ok(out)
    }

    fn find_block(&self, key: &[u8]) -> Option<&IndexEnt> {
        if self.index.is_empty() || key < self.index[0].first_key.as_slice() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Some(&self.index[lo])
    }

    fn read_block(&self, ent: &IndexEnt) -> Result<Vec<(Vec<u8>, ValueRec)>, Error> {
        let start = ent.offset as usize;
        let len = ent.len as usize;
        if start.saturating_add(len) > self.data.len() || len < 4 {
            return Err(Error::Corrupt("sstable block bounds".into()));
        }
        let frame = &self.data[start..start + len];
        let payload = &frame[..frame.len() - 4];
        let sum = u32::from_le_bytes(frame[frame.len() - 4..].try_into().unwrap());
        if crc32(payload) != sum {
            return Err(Error::Corrupt("sstable block checksum".into()));
        }
        if payload.len() < 4 {
            return Err(Error::Corrupt("sstable block empty".into()));
        }
        let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let mut p = &payload[4..];
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(decode_entry(&mut p)?);
        }
        if !p.is_empty() {
            return Err(Error::Corrupt("sstable block trailing".into()));
        }
        Ok(entries)
    }
}

fn encode_entry(out: &mut Vec<u8>, key: &[u8], rec: &ValueRec) {
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&rec.seq.to_le_bytes());
    out.push(u8::from(rec.tombstone));
    out.extend_from_slice(&rec.expiry.to_le_bytes());
    out.extend_from_slice(&(rec.value.len() as u32).to_le_bytes());
    out.extend_from_slice(&rec.value);
}

fn decode_entry(p: &mut &[u8]) -> Result<(Vec<u8>, ValueRec), Error> {
    if p.len() < 4 {
        return Err(Error::Corrupt("sstable entry key length".into()));
    }
    let klen = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
    *p = &p[4..];
    if p.len() < klen + 8 + 1 + 8 + 4 {
        return Err(Error::Corrupt("sstable entry truncated".into()));
    }
    let key = p[..klen].to_vec();
    *p = &p[klen..];
    let seq = u64::from_le_bytes(p[0..8].try_into().unwrap());
    let tombstone = p[8] != 0;
    let expiry = u64::from_le_bytes(p[9..17].try_into().unwrap());
    let vlen = u32::from_le_bytes(p[17..21].try_into().unwrap()) as usize;
    *p = &p[21..];
    if p.len() < vlen {
        return Err(Error::Corrupt("sstable value truncated".into()));
    }
    let value = p[..vlen].to_vec();
    *p = &p[vlen..];
    Ok((
        key,
        ValueRec {
            seq,
            expiry,
            tombstone,
            value,
        },
    ))
}

fn encode_index(index: &[IndexEnt]) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    body.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for ent in index {
        body.extend_from_slice(&(ent.first_key.len() as u32).to_le_bytes());
        body.extend_from_slice(&ent.first_key);
        body.extend_from_slice(&ent.offset.to_le_bytes());
        body.extend_from_slice(&ent.len.to_le_bytes());
    }
    let sum = crc32(&body);
    body.extend_from_slice(&sum.to_le_bytes());
    Ok(body)
}

fn decode_index(buf: &[u8]) -> Result<Vec<IndexEnt>, Error> {
    if buf.len() < 8 {
        return Err(Error::Corrupt("sstable index truncated".into()));
    }
    let body = &buf[..buf.len() - 4];
    let sum = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
    if crc32(body) != sum {
        return Err(Error::Corrupt("sstable index checksum".into()));
    }
    if body.len() < 4 {
        return Err(Error::Corrupt("sstable index body".into()));
    }
    let n = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let mut p = &body[4..];
    let mut index = Vec::with_capacity(n);
    for _ in 0..n {
        if p.len() < 4 {
            return Err(Error::Corrupt("sstable index key".into()));
        }
        let klen = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
        p = &p[4..];
        if p.len() < klen + 8 + 4 {
            return Err(Error::Corrupt("sstable index entry".into()));
        }
        let first_key = p[..klen].to_vec();
        p = &p[klen..];
        let offset = u64::from_le_bytes(p[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(p[8..12].try_into().unwrap());
        p = &p[12..];
        index.push(IndexEnt {
            first_key,
            offset,
            len,
        });
    }
    if !p.is_empty() {
        return Err(Error::Corrupt("sstable index trailing".into()));
    }
    Ok(index)
}

/// Highest seq wins. Expired or tombstoned winners suppress older values.
/// Tombstones are omitted only when the compaction covers every older SSTable.
pub fn merge_entries(
    tables: &[Vec<(Vec<u8>, ValueRec)>],
    now: u64,
    drop_tombstones: bool,
) -> Vec<(Vec<u8>, ValueRec)> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<Vec<u8>, ValueRec> = BTreeMap::new();
    for table in tables {
        for (k, v) in table {
            match map.get(k) {
                Some(old) if old.seq > v.seq => {}
                _ => {
                    map.insert(k.clone(), v.clone());
                }
            }
        }
    }
    let mut out = Vec::new();
    for (k, v) in map {
        let expired = !v.tombstone && v.expiry != 0 && v.expiry <= now;
        if v.tombstone || expired {
            if drop_tombstones {
                continue;
            }
            out.push((
                k,
                ValueRec {
                    seq: v.seq,
                    expiry: 0,
                    tombstone: true,
                    value: Vec::new(),
                },
            ));
        } else {
            out.push((k, v));
        }
    }
    out
}
