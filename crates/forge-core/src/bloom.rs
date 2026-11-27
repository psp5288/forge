use crate::checksum::crc32;
use crate::error::Error;

/// Bloom filter. False positives are allowed. False negatives are not.
#[derive(Clone, Debug)]
pub struct Bloom {
    bits: Vec<u8>,
    nbits: u64,
    k: u32,
}

impl Bloom {
    pub fn new(nkeys: usize) -> Self {
        let nbits = ((nkeys.max(1) as u64) * 10).max(64);
        let nbytes = ((nbits + 7) / 8) as usize;
        Self {
            bits: vec![0; nbytes],
            nbits,
            k: 7,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let positions: Vec<u64> = self.positions(key).collect();
        for bit in positions {
            let i = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if i < self.bits.len() {
                self.bits[i] |= mask;
            }
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.positions(key).all(|bit| {
            let i = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            i < self.bits.len() && self.bits[i] & mask != 0
        })
    }

    fn positions(&self, key: &[u8]) -> impl Iterator<Item = u64> + '_ {
        let h1 = fnv1a(key);
        let h2 = mix(key) | 1;
        let nbits = self.nbits;
        (0..self.k).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % nbits)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.k.to_le_bytes());
        body.extend_from_slice(&(self.nbits as u32).to_le_bytes());
        body.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        body.extend_from_slice(&self.bits);
        let sum = crc32(&body);
        body.extend_from_slice(&sum.to_le_bytes());
        body
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), Error> {
        if buf.len() < 12 {
            return Err(Error::Corrupt("bloom truncated".into()));
        }
        let k = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let nbits = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as u64;
        let nbytes = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        if k == 0 || k > 32 || buf.len() < 12 + nbytes + 4 {
            return Err(Error::Corrupt("bloom header".into()));
        }
        let bits = buf[12..12 + nbytes].to_vec();
        let sum = u32::from_le_bytes(buf[12 + nbytes..12 + nbytes + 4].try_into().unwrap());
        if crc32(&buf[..12 + nbytes]) != sum {
            return Err(Error::Corrupt("bloom checksum".into()));
        }
        Ok((
            Self { bits, nbits, k },
            12 + nbytes + 4,
        ))
    }
}

fn fnv1a(data: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in data {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn mix(data: &[u8]) -> u64 {
    let mut h = 0x84222325cbf29ce4u64;
    for b in data {
        h = h.wrapping_mul(0x100000001b3) ^ u64::from(*b);
    }
    h
}
