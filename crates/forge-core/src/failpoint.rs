/// Named crash points. The five crash-suite points are required hits.
/// `AfterNewWalBeforeManifest` is the scripted WAL-rotation case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Failpoint {
    AfterWalAppend,
    AfterFsyncBeforeAck,
    MidFlush,
    AfterManifestWrite,
    MidCompaction,
    AfterNewWalBeforeManifest,
}

impl Failpoint {
    pub const CRASH_SUITE: [Failpoint; 5] = [
        Failpoint::AfterWalAppend,
        Failpoint::AfterFsyncBeforeAck,
        Failpoint::MidFlush,
        Failpoint::AfterManifestWrite,
        Failpoint::MidCompaction,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AfterWalAppend => "after_wal_append",
            Self::AfterFsyncBeforeAck => "after_fsync_before_ack",
            Self::MidFlush => "mid_flush",
            Self::AfterManifestWrite => "after_manifest_write",
            Self::MidCompaction => "mid_compaction",
            Self::AfterNewWalBeforeManifest => "after_new_wal_before_manifest",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::CRASH_SUITE
            .into_iter()
            .chain(std::iter::once(Self::AfterNewWalBeforeManifest))
            .find(|p| p.as_str() == name)
    }
}
