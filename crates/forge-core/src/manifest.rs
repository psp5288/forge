use crate::checksum::crc32;
use crate::error::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalMeta {
    pub name: String,
    pub max_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstMeta {
    pub name: String,
    pub max_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub flushed_seq: u64,
    pub next_file_id: u64,
    pub next_seq: u64,
    pub wals: Vec<WalMeta>,
    pub ssts: Vec<SstMeta>,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut s = String::new();
        s.push_str("FORGE 1\n");
        s.push_str(&format!("flushed_seq {}\n", self.flushed_seq));
        s.push_str(&format!("next_file_id {}\n", self.next_file_id));
        s.push_str(&format!("next_seq {}\n", self.next_seq));
        for w in &self.wals {
            s.push_str(&format!("wal {} {}\n", w.name, w.max_seq));
        }
        for t in &self.ssts {
            s.push_str(&format!("sst {} {}\n", t.name, t.max_seq));
        }
        let sum = crc32(s.as_bytes());
        s.push_str(&format!("crc {sum:08x}\n"));
        s.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let text = std::str::from_utf8(bytes).map_err(|_| Error::Corrupt("manifest utf8".into()))?;
        let mut body = String::new();
        let mut crc_line: Option<u32> = None;
        for line in text.split('\n') {
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("crc ") {
                crc_line = Some(
                    u32::from_str_radix(rest.trim(), 16)
                        .map_err(|_| Error::Corrupt("manifest crc".into()))?,
                );
            } else {
                body.push_str(line);
                body.push('\n');
            }
        }
        let Some(sum) = crc_line else {
            return Err(Error::Corrupt("manifest missing crc".into()));
        };
        if crc32(body.as_bytes()) != sum {
            return Err(Error::Corrupt("manifest checksum".into()));
        }
        let mut flushed_seq = None;
        let mut next_file_id = None;
        let mut next_seq = None;
        let mut wals = Vec::new();
        let mut ssts = Vec::new();
        let mut saw_magic = false;
        for line in body.lines() {
            let mut parts = line.split_whitespace();
            let tag = parts.next().unwrap_or("");
            match tag {
                "FORGE" => {
                    let ver = parts.next().unwrap_or("");
                    if ver != "1" {
                        return Err(Error::Corrupt(format!("manifest version {ver}")));
                    }
                    saw_magic = true;
                }
                "flushed_seq" => flushed_seq = parts.next().and_then(|s| s.parse().ok()),
                "next_file_id" => next_file_id = parts.next().and_then(|s| s.parse().ok()),
                "next_seq" => next_seq = parts.next().and_then(|s| s.parse().ok()),
                "wal" => {
                    let name = parts.next().unwrap_or("").to_string();
                    let max_seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                    if name.is_empty() {
                        return Err(Error::Corrupt("manifest wal name".into()));
                    }
                    wals.push(WalMeta { name, max_seq });
                }
                "sst" => {
                    let name = parts.next().unwrap_or("").to_string();
                    let max_seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                    if name.is_empty() {
                        return Err(Error::Corrupt("manifest sst name".into()));
                    }
                    ssts.push(SstMeta { name, max_seq });
                }
                _ => return Err(Error::Corrupt(format!("manifest line {line}"))),
            }
        }
        if !saw_magic {
            return Err(Error::Corrupt("manifest magic".into()));
        }
        Ok(Self {
            flushed_seq: flushed_seq.ok_or_else(|| Error::Corrupt("manifest flushed_seq".into()))?,
            next_file_id: next_file_id
                .ok_or_else(|| Error::Corrupt("manifest next_file_id".into()))?,
            next_seq: next_seq.ok_or_else(|| Error::Corrupt("manifest next_seq".into()))?,
            wals,
            ssts,
        })
    }
}

pub fn format_gen(gen: u64) -> String {
    format!("{gen:06}")
}

pub fn manifest_name(gen: u64) -> String {
    format!("MANIFEST-{}", format_gen(gen))
}
