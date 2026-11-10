//! `magpie-core` — the download engine.
//!
//! Design goals:
//! * **Low memory.** Each connection streams in fixed ~64 KiB buffers straight
//!   to the file via positioned writes, so peak RAM is independent of file size.
//! * **Fast (aria2-style).** A single file is split into N byte-range segments
//!   downloaded over parallel connections, then reassembled in place.
//! * **Resumable.** Segment progress is persisted to a `<file>.magpiepart` sidecar
//!   so downloads survive pause and daemon restarts (when the server supports
//!   HTTP range requests).

mod bw;
mod downloader;
mod part;
mod rate;

pub use rate::RateLimiter;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use magpie_proto::{
    DownloadId, DownloadInfo, DownloadOptions, DownloadRequest, DownloadState, PathContext,
};
use tokio::sync::{broadcast, Mutex};

pub use downloader::DownloadEntry;

/// Engine-level events, consumed by the daemon and mapped onto the wire protocol.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    Added(DownloadInfo),
    Progress {
        id: DownloadId,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bps: u64,
    },
    StateChanged {
        id: DownloadId,
        state: DownloadState,
        error: Option<String>,
    },
    Removed(DownloadId),
}

/// Tuning knobs. Defaults are conservative and memory-friendly.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Fallback directory when the resolver returns a bare filename.
    pub download_dir: PathBuf,
    /// Max parallel connections *per download* (aria2's `-x`).
    pub max_connections_per_download: usize,
    /// Don't split a file into pieces smaller than this (aria2's `-k`).
    pub min_segment_size: u64,
    /// Per-connection read buffer. Keeps memory flat regardless of file size.
    pub buffer_size: usize,
    /// How often to emit a coalesced progress tick, in milliseconds.
    pub progress_interval_ms: u64,
    /// Global default `User-Agent` when neither the request nor headers set one.
    pub user_agent: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let download_dir = dirs_download_dir();
        Self {
            download_dir,
            max_connections_per_download: 8,
            min_segment_size: 1024 * 1024, // 1 MiB
            buffer_size: 64 * 1024,        // 64 KiB
            progress_interval_ms: 500,
            user_agent: format!("Magpie/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

impl EngineConfig {
    /// Hard ceiling on connections for any single download.
    pub const MAX_CONNECTIONS_CEILING: usize = 16;

    /// How many parallel connections a download should use, given its priority
    /// and an optional explicit override.
    pub fn connections_for(&self, priority: magpie_proto::Priority, override_n: Option<usize>) -> usize {
        if let Some(n) = override_n {
            return n.clamp(1, Self::MAX_CONNECTIONS_CEILING);
        }
        let base = self.max_connections_per_download.max(1);
        let n = match priority {
            magpie_proto::Priority::Low => (base / 2).max(2),
            magpie_proto::Priority::Normal => base,
            magpie_proto::Priority::High => base * 2,
        };
        n.clamp(1, Self::MAX_CONNECTIONS_CEILING)
    }
}

fn dirs_download_dir() -> PathBuf {
    // Minimal, dependency-free resolution of a sensible default download dir.
    if let Ok(home) = std::env::var("HOME") {
        let d = PathBuf::from(&home).join("Downloads");
        return d;
    }
    std::env::temp_dir()
}

/// Resolves the destination path for a download. The daemon plugs the QuickJS
/// script engine in here; a [`DefaultResolver`] is used otherwise.
pub trait PathResolver: Send + Sync + 'static {
    fn resolve(
        &self,
        ctx: PathContext,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PathBuf>> + Send + '_>>;
}

/// Puts every file in `download_dir/<filename>`.
pub struct DefaultResolver {
    pub download_dir: PathBuf,
}

impl PathResolver for DefaultResolver {
    fn resolve(
        &self,
        ctx: PathContext,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PathBuf>> + Send + '_>> {
        let dir = self.download_dir.clone();
        Box::pin(async move { Ok(dir.join(sanitize(&ctx.filename))) })
    }
}

/// Strip path separators / traversal from a filename component.
pub fn sanitize(name: &str) -> String {
    let name = name.replace(['/', '\\'], "_");
    let name = name.trim_matches(['.', ' ']).to_string();
    if name.is_empty() {
        "download".to_string()
    } else {
        name
    }
}

/// The engine. Cheap to clone (everything shared is behind `Arc`).
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    client: reqwest::Client,
    resolver: Arc<dyn PathResolver>,
    downloads: Mutex<std::collections::HashMap<DownloadId, DownloadEntry>>,
    bandwidth: Mutex<bw::BandwidthManager>,
    events: broadcast::Sender<EngineEvent>,
}

impl Engine {
    pub fn new(config: EngineConfig, resolver: Arc<dyn PathResolver>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(config.user_agent.clone())
            // One connection pool, reused across segments and downloads.
            .pool_max_idle_per_host(config.max_connections_per_download * 2)
            .build()?;
        let (events, _) = broadcast::channel(1024);
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                client,
                resolver,
                downloads: Mutex::new(Default::default()),
                bandwidth: Mutex::new(bw::BandwidthManager::default()),
                events,
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    /// Subscribe to engine events.
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.inner.events.subscribe()
    }

    pub(crate) fn emit(&self, ev: EngineEvent) {
        // Ignore "no subscribers" errors.
        let _ = self.inner.events.send(ev);
    }

    /// Current snapshot of all downloads.
    pub async fn list(&self) -> Vec<DownloadInfo> {
        let map = self.inner.downloads.lock().await;
        map.values().map(|e| e.info()).collect()
    }

    /// Add a new download and start it.
    pub async fn add(&self, request: DownloadRequest) -> DownloadId {
        downloader::spawn_download(self.clone(), request).await
    }

    pub async fn pause(&self, id: DownloadId) {
        if let Some(entry) = self.inner.downloads.lock().await.get(&id) {
            entry.pause();
        }
    }

    pub async fn resume(&self, id: DownloadId) {
        let req = {
            let map = self.inner.downloads.lock().await;
            map.get(&id)
                .filter(|e| e.is_resumable_now())
                .map(|e| e.request())
        };
        if let Some(req) = req {
            // Re-arm a paused/failed download; the supervisor picks up from the
            // `.magpiepart` sidecar.
            downloader::restart_download(self.clone(), id, req).await;
        }
    }

    pub async fn cancel(&self, id: DownloadId) {
        if let Some(entry) = self.inner.downloads.lock().await.get(&id) {
            entry.cancel();
        }
    }

    pub async fn remove(&self, id: DownloadId) {
        let entry = self.inner.downloads.lock().await.remove(&id);
        if let Some(entry) = entry {
            entry.cancel();
            self.inner.bandwidth.lock().await.unregister(id);
            self.recompute_bandwidth().await;
            self.emit(EngineEvent::Removed(id));
        }
    }

    /// Change priority / per-download speed cap / connection count at runtime.
    pub async fn set_options(&self, id: DownloadId, options: DownloadOptions) {
        let changed = self
            .with_entry(id, |e| {
                e.set_options(options.clone());
            })
            .await
            .is_some();
        if changed {
            self.inner
                .bandwidth
                .lock()
                .await
                .set_options(id, options.priority, options.max_speed_bps);
            self.recompute_bandwidth().await;
            if let Some(info) = self.with_entry(id, |e| e.info()).await {
                self.emit(EngineEvent::StateChanged {
                    id,
                    state: info.state,
                    error: info.error,
                });
            }
        }
    }

    /// Set or clear the global aggregate speed cap (bytes/sec).
    pub async fn set_global_speed_limit(&self, max_speed_bps: Option<u64>) {
        self.inner.bandwidth.lock().await.set_global_cap(max_speed_bps);
        self.recompute_bandwidth().await;
    }

    // --- bandwidth hooks used internally and by the downloader ---
    pub(crate) async fn bw_register(
        &self,
        id: DownloadId,
        priority: magpie_proto::Priority,
        own_cap: Option<u64>,
        limiter: Arc<RateLimiter>,
    ) {
        self.inner
            .bandwidth
            .lock()
            .await
            .register(id, priority, own_cap, limiter);
    }
    pub(crate) async fn bw_set_active(&self, id: DownloadId, active: bool) {
        self.inner.bandwidth.lock().await.set_active(id, active);
        self.recompute_bandwidth().await;
    }
    pub(crate) async fn recompute_bandwidth(&self) {
        // Snapshot under the lock, then recompute (recompute awaits on limiters).
        let mgr = self.inner.bandwidth.lock().await;
        mgr.recompute().await;
    }

    // --- internal accessors used by the downloader module ---
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.inner.client
    }
    pub(crate) fn resolver(&self) -> Arc<dyn PathResolver> {
        self.inner.resolver.clone()
    }
    pub(crate) async fn insert_entry(&self, id: DownloadId, entry: DownloadEntry) {
        self.inner.downloads.lock().await.insert(id, entry);
    }
    pub(crate) async fn with_entry<R>(
        &self,
        id: DownloadId,
        f: impl FnOnce(&DownloadEntry) -> R,
    ) -> Option<R> {
        self.inner.downloads.lock().await.get(&id).map(f)
    }
}
