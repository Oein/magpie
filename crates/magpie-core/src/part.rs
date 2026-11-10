//! `<file>.magpiepart` sidecar: lets downloads resume across pause and daemon
//! restarts by remembering each segment's byte range and how much is done.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRecord {
    pub start: u64,
    /// Inclusive end offset; `None` for an open-ended (non-resumable) stream.
    pub end: Option<u64>,
    /// Bytes already written for this segment.
    pub done: u64,
}

impl SegmentRecord {
    /// Absolute offset where the next byte should be written.
    pub fn cursor(&self) -> u64 {
        self.start + self.done
    }
    /// Whether this segment still has bytes left (only meaningful when `end` is known).
    pub fn remaining(&self) -> Option<u64> {
        self.end.map(|e| (e + 1).saturating_sub(self.cursor()))
    }
    pub fn is_complete(&self) -> bool {
        match self.end {
            Some(e) => self.cursor() > e,
            None => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartFile {
    pub url: String,
    pub total: Option<u64>,
    pub resumable: bool,
    pub segments: Vec<SegmentRecord>,
}

impl PartFile {
    pub fn path_for(dest: &Path) -> PathBuf {
        let mut s = dest.as_os_str().to_os_string();
        s.push(".magpiepart");
        PathBuf::from(s)
    }

    pub async fn load(dest: &Path) -> Option<PartFile> {
        let p = Self::path_for(dest);
        let bytes = tokio::fs::read(&p).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Best-effort atomic save (temp file + rename).
    pub async fn save(&self, dest: &Path) {
        let p = Self::path_for(dest);
        let tmp = {
            let mut s = p.as_os_str().to_os_string();
            s.push(".tmp");
            PathBuf::from(s)
        };
        if let Ok(bytes) = serde_json::to_vec(self) {
            if tokio::fs::write(&tmp, &bytes).await.is_ok() {
                let _ = tokio::fs::rename(&tmp, &p).await;
            }
        }
    }

    pub async fn remove(dest: &Path) {
        let _ = tokio::fs::remove_file(Self::path_for(dest)).await;
    }

    pub fn downloaded(&self) -> u64 {
        self.segments.iter().map(|s| s.done).sum()
    }
}
