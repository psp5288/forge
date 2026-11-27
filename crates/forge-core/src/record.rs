#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OpKind {
    Set = 1,
    Del = 2,
    Expire = 3,
}

impl OpKind {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Set),
            2 => Some(Self::Del),
            3 => Some(Self::Expire),
            _ => None,
        }
    }
}

/// One logical mutation. `expiry` is an absolute millisecond timestamp, or 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub op: OpKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expiry: u64,
}

/// A key's winning version in memory or an SSTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueRec {
    pub seq: u64,
    pub expiry: u64,
    pub tombstone: bool,
    pub value: Vec<u8>,
}

impl ValueRec {
    pub fn is_visible(&self, now: u64) -> bool {
        if self.tombstone {
            return false;
        }
        if self.expiry != 0 && self.expiry <= now {
            return false;
        }
        true
    }

    pub fn from_record(rec: &Record, now: u64) -> Self {
        match rec.op {
            OpKind::Del => Self {
                seq: rec.seq,
                expiry: 0,
                tombstone: true,
                value: Vec::new(),
            },
            OpKind::Set | OpKind::Expire => {
                let expired = rec.expiry != 0 && rec.expiry <= now;
                if expired {
                    Self {
                        seq: rec.seq,
                        expiry: 0,
                        tombstone: true,
                        value: Vec::new(),
                    }
                } else {
                    Self {
                        seq: rec.seq,
                        expiry: rec.expiry,
                        tombstone: false,
                        value: rec.value.clone(),
                    }
                }
            }
        }
    }
}

/// What the caller asked the store to do. Expiry is absolute.
#[derive(Clone, Debug)]
pub enum UserOp {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: u64,
    },
    Del {
        key: Vec<u8>,
    },
    Expire {
        key: Vec<u8>,
        expiry: u64,
    },
}

#[derive(Clone, Debug)]
pub enum Reply {
    Ok,
    Integer(i64),
}

#[derive(Clone, Debug)]
pub enum Prepared {
    Immediate(Reply),
    Durable { record: Record, reply: Reply },
}
