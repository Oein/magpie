//! Shared wire types between the browser extension, the GUI and the daemon.
//!
//! Everything here is `serde`-serializable JSON. The daemon speaks these over a
//! localhost WebSocket; the extension and GUI are both just clients.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type DownloadId = Uuid;

/// Scheduling priority. Governs how many parallel connections a download gets
/// and, when a global speed cap is active, its weighted share of bandwidth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
}

impl Priority {
    /// Relative weight used for bandwidth fair-sharing and connection scaling.
    pub fn weight(self) -> u32 {
        match self {
            Priority::Low => 1,
            Priority::Normal => 2,
            Priority::High => 4,
        }
    }
}

/// Per-download tuning. All fields optional → fall back to engine defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DownloadOptions {
    #[serde(default)]
    pub priority: Priority,
    /// Hard per-download speed cap in bytes/sec (`None` = unlimited).
    #[serde(default)]
    pub max_speed_bps: Option<u64>,
    /// Override the number of parallel connections for this download.
    #[serde(default)]
    pub connections: Option<usize>,
}

/// A captured download, as forwarded by the browser extension (or created by the
/// GUI / CLI). Carries everything needed to reproduce the browser's request —
/// cookies, headers, referrer — so authenticated downloads keep working.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub url: String,
    /// Suggested file name (from Content-Disposition or the extension). The
    /// final path is decided by the path-resolver script.
    #[serde(default)]
    pub filename: Option<String>,
    /// Raw request headers to replay (name, value).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Full `Cookie:` header value for the target origin, collected by the
    /// extension via the `cookies` API.
    #[serde(default)]
    pub cookie: Option<String>,
    #[serde(default)]
    pub referrer: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub mime: Option<String>,
    /// Size hint if the browser already knew it.
    #[serde(default)]
    pub size_hint: Option<u64>,
    /// URL of the page the download originated from (useful for scripts).
    #[serde(default)]
    pub source_page: Option<String>,
    #[serde(default)]
    pub options: DownloadOptions,
}

/// Lifecycle state of a download.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadState {
    Queued,
    /// Probing the server (HEAD / range support) and resolving the path.
    Preparing,
    Downloading,
    Paused,
    Completed,
    Failed,
    Canceled,
}

/// A point-in-time view of one download, sent to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadInfo {
    pub id: DownloadId,
    pub url: String,
    pub dest_path: Option<String>,
    pub filename: String,
    pub state: DownloadState,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    /// Bytes/sec, exponentially smoothed.
    pub speed_bps: u64,
    /// Whether the server supports resuming (HTTP range).
    pub resumable: bool,
    /// Active number of parallel connections.
    pub connections: usize,
    pub priority: Priority,
    pub max_speed_bps: Option<u64>,
    pub error: Option<String>,
    pub created_at: u64,
}

/// Client -> daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Forward a new download (from extension / GUI / CLI).
    Add { request: DownloadRequest },
    Pause { id: DownloadId },
    Resume { id: DownloadId },
    Cancel { id: DownloadId },
    Remove { id: DownloadId },
    /// Change priority / speed cap / connections of an existing download.
    SetOptions { id: DownloadId, options: DownloadOptions },
    /// Set (or clear, with `None`) the global download speed cap in bytes/sec.
    SetGlobalSpeedLimit { max_speed_bps: Option<u64> },
    /// Ask for a full snapshot of all downloads.
    List,
    /// Liveness probe used by the extension to decide whether to bootstrap.
    Ping,
}

/// Daemon -> client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Sent right after connect and in response to `List`.
    Snapshot { downloads: Vec<DownloadInfo> },
    Added { info: DownloadInfo },
    /// High-frequency progress tick (coalesced by the daemon).
    Progress {
        id: DownloadId,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bps: u64,
    },
    StateChanged { id: DownloadId, state: DownloadState, error: Option<String> },
    Removed { id: DownloadId },
    Pong { version: String },
    Error { message: String },
}

/// The metadata object handed to the user's path-resolver script.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathContext {
    pub url: String,
    pub filename: String,
    pub mime: Option<String>,
    pub size: Option<u64>,
    pub source_page: Option<String>,
    pub host: Option<String>,
    /// File extension without the dot, lowercased, if any.
    pub ext: Option<String>,
}
