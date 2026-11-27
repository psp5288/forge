use crate::checksum::crc32;
use crate::error::Error;
use crate::record::{OpKind, Record};

pub const FILE_MAGIC: &[u8; 4] = b"FWAL";
pub const FILE_VERSION: u32 = 1;
const REC_MAGIC: u32 = 0xF0E1_A001;
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

pub fn header_bytes() -> Vec<u8> {
    let mut v = Vec::with_capacity(8);
    v.extend_from_slice(FILE_MAGIC);
    v.extend_from_slice(&FILE_VERSION.to_le_bytes());
    v
}

pub fn encode(rec: &Record) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&rec.seq.to_le_bytes());
    payload.push(rec.op as u8);
    write_bytes(&mut payload, &rec.key);
    payload.extend_from_slice(&rec.expiry.to_le_bytes());
    write_bytes(&mut payload, &rec.value);
    let sum = crc32(&payload);
    let mut out = Vec::with_capacity(12 + payload.len());
    out.extend_from_slice(&REC_MAGIC.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&sum.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

pub enum Decoded {
    /// Incomplete frame at the end of the file. Caller truncates to `offset`.
    Torn,
    Record { next: usize, rec: Record },
}

pub fn check_header(buf: &[u8]) -> Result<usize, Error> {
    if buf.len() < 8 {
        return Err(Error::Corrupt("wal header truncated".into()));
    }
    if &buf[0..4] != FILE_MAGIC {
        return Err(Error::Corrupt("wal header magic".into()));
    }
    let ver = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if ver != FILE_VERSION {
        return Err(Error::Corrupt(format!("wal version {ver}")));
    }
    Ok(8)
}

/// Decode one record at `offset`.
///
/// A frame that extends past EOF is a torn tail. A full frame whose checksum
/// or magic does not match is corruption: the caller must not apply it.
pub fn decode_one(buf: &[u8], offset: usize) -> Result<Decoded, Error> {
    if offset > buf.len() {
        return Ok(Decoded::Torn);
    }
    let rest = &buf[offset..];
    if rest.len() < 12 {
        return Ok(Decoded::Torn);
    }
    let magic = u32::from_le_bytes(rest[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(rest[4..8].try_into().unwrap()) as usize;
    let sum = u32::from_le_bytes(rest[8..12].try_into().unwrap());
    if magic != REC_MAGIC {
        return Err(Error::Corrupt("wal record magic".into()));
    }
    if len > MAX_PAYLOAD {
        return Err(Error::Corrupt("wal record length".into()));
    }
    if rest.len() < 12 + len {
        return Ok(Decoded::Torn);
    }
    let payload = &rest[12..12 + len];
    if crc32(payload) != sum {
        return Err(Error::Corrupt("wal record checksum".into()));
    }
    let rec = parse_payload(payload)?;
    Ok(Decoded::Record {
        next: offset + 12 + len,
        rec,
    })
}

fn parse_payload(mut p: &[u8]) -> Result<Record, Error> {
    if p.len() < 8 + 1 + 4 {
        return Err(Error::Corrupt("wal payload truncated".into()));
    }
    let seq = u64::from_le_bytes(p[0..8].try_into().unwrap());
    let op = OpKind::from_u8(p[8]).ok_or_else(|| Error::Corrupt("wal op".into()))?;
    p = &p[9..];
    let key = read_bytes(&mut p)?;
    if p.len() < 8 {
        return Err(Error::Corrupt("wal expiry truncated".into()));
    }
    let expiry = u64::from_le_bytes(p[0..8].try_into().unwrap());
    p = &p[8..];
    let value = read_bytes(&mut p)?;
    if !p.is_empty() {
        return Err(Error::Corrupt("wal payload trailing bytes".into()));
    }
    Ok(Record {
        seq,
        op,
        key,
        value,
        expiry,
    })
}

fn read_bytes(p: &mut &[u8]) -> Result<Vec<u8>, Error> {
    if p.len() < 4 {
        return Err(Error::Corrupt("wal length truncated".into()));
    }
    let n = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
    *p = &p[4..];
    if p.len() < n {
        return Err(Error::Corrupt("wal bytes truncated".into()));
    }
    let out = p[..n].to_vec();
    *p = &p[n..];
    Ok(out)
}

/// Absolute expiry stored in a SET/EXPIRE payload, used by tests.
pub fn expiry_of(buf: &[u8], key: &[u8]) -> Result<Option<u64>, Error> {
    if buf.len() < 8 {
        return Ok(None);
    }
    let mut off = check_header(buf)?;
    let mut found = None;
    loop {
        match decode_one(buf, off)? {
            Decoded::Torn => break,
            Decoded::Record { next, rec } => {
                if rec.key == key && matches!(rec.op, OpKind::Set | OpKind::Expire) {
                    found = Some(rec.expiry);
                }
                off = next;
            }
        }
    }
    Ok(found)
}
