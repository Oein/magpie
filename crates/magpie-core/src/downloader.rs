//! Per-download supervisor and the parallel segment workers.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use magpie_proto::{
    DownloadId, DownloadInfo, DownloadOptions, DownloadRequest, DownloadState, PathContext,
    Priority,
};
use futures_util::StreamExt;
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, COOKIE, RANGE, REFERER,
    USER_AGENT,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use uuid::Uuid;

use crate::part::{PartFile, SegmentRecord};
use crate::rate::RateLimiter;
use crate::{Engine, EngineEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Run,
    Pause,
    Cancel,
}

struct Meta {
    dest_path: Option<PathBuf>,
    filename: String,
    state: DownloadState,
    total: Option<u64>,
    resumable: bool,
    error: Option<String>,
    connections: usize,
    priority: Priority,
    max_speed_bps: Option<u64>,
    conn_override: Option<usize>,
}

/// All shared per-download state. Lives in an `Arc`; the supervisor task and the
/// engine both hold references.
pub struct Shared {
    id: DownloadId,
    url: String,
    created_at: u64,
    downloaded: AtomicU64,
    speed: AtomicU64,
    meta: StdMutex<Meta>,
    control_tx: watch::Sender<Control>,
    limiter: Arc<RateLimiter>,
    request: StdMutex<DownloadRequest>,
    /// Bumped on each (re)start; a superseded supervisor won't touch state.
    generation: AtomicU64,
    /// Serializes supervisor runs so a resume never overlaps the previous run.
    run_lock: tokio::sync::Mutex<()>,
}

/// Handle stored in the engine's download map.
#[derive(Clone)]
pub struct DownloadEntry {
    shared: Arc<Shared>,
}

impl DownloadEntry {
    pub fn info(&self) -> DownloadInfo {
        let m = self.shared.meta.lock().unwrap();
        DownloadInfo {
            id: self.shared.id,
            url: self.shared.url.clone(),
            dest_path: m.dest_path.as_ref().map(|p| p.display().to_string()),
            filename: m.filename.clone(),
            state: m.state,
            total_bytes: m.total,
            downloaded_bytes: self.shared.downloaded.load(Ordering::Relaxed),
            speed_bps: self.shared.speed.load(Ordering::Relaxed),
            resumable: m.resumable,
            connections: m.connections,
            priority: m.priority,
            max_speed_bps: m.max_speed_bps,
            error: m.error.clone(),
            created_at: self.shared.created_at,
        }
    }

    pub fn pause(&self) {
        self.shared.control_tx.send_replace(Control::Pause);
    }
    pub fn cancel(&self) {
        self.shared.control_tx.send_replace(Control::Cancel);
    }
    pub fn request(&self) -> DownloadRequest {
        self.shared.request.lock().unwrap().clone()
    }
    pub fn is_resumable_now(&self) -> bool {
        matches!(
            self.shared.meta.lock().unwrap().state,
            DownloadState::Paused | DownloadState::Failed
        )
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.shared.meta.lock().unwrap().state,
            DownloadState::Paused
                | DownloadState::Failed
                | DownloadState::Completed
                | DownloadState::Canceled
        )
    }
    pub fn set_options(&self, options: DownloadOptions) {
        let mut m = self.shared.meta.lock().unwrap();
        m.priority = options.priority;
        m.max_speed_bps = options.max_speed_bps;
        m.conn_override = options.connections;
        // Connection count change takes full effect on the next (re)start.
        m.connections = connections_hint(m.priority, m.conn_override);
        self.shared.request.lock().unwrap().options = options;
    }
    pub(crate) fn shared_arc(&self) -> Arc<Shared> {
        self.shared.clone()
    }
}

fn connections_hint(priority: Priority, override_n: Option<usize>) -> usize {
    if let Some(n) = override_n {
        return n.clamp(1, 16);
    }
    match priority {
        Priority::Low => 2,
        Priority::Normal => 8,
        Priority::High => 16,
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Create and start a brand-new download.
pub async fn spawn_download(engine: Engine, request: DownloadRequest) -> DownloadId {
    let id = Uuid::new_v4();
    let opts = request.options.clone();
    let limiter = Arc::new(RateLimiter::new(opts.max_speed_bps.unwrap_or(0)));
    let filename = initial_filename(&request);
    let connections = engine
        .config()
        .connections_for(opts.priority, opts.connections);
    let (control_tx, _rx) = watch::channel(Control::Run);

    let shared = Arc::new(Shared {
        id,
        url: request.url.clone(),
        created_at: now_unix(),
        downloaded: AtomicU64::new(0),
        speed: AtomicU64::new(0),
        meta: StdMutex::new(Meta {
            dest_path: None,
            filename,
            state: DownloadState::Queued,
            total: request.size_hint,
            resumable: false,
            error: None,
            connections,
            priority: opts.priority,
            max_speed_bps: opts.max_speed_bps,
            conn_override: opts.connections,
        }),
        control_tx,
        limiter: limiter.clone(),
        request: StdMutex::new(request),
        generation: AtomicU64::new(0),
        run_lock: tokio::sync::Mutex::new(()),
    });

    let entry = DownloadEntry {
        shared: shared.clone(),
    };
    engine.insert_entry(id, entry.clone()).await;
    engine
        .bw_register(id, opts.priority, opts.max_speed_bps, limiter)
        .await;
    engine.emit(EngineEvent::Added(entry.info()));

    tokio::spawn(run(engine, shared));
    id
}

/// Re-arm a paused/failed download; the supervisor resumes from the sidecar.
pub async fn restart_download(engine: Engine, id: DownloadId, _request: DownloadRequest) {
    let shared = match engine.with_entry(id, |e| e.shared_arc()).await {
        Some(s) => s,
        None => return,
    };
    // Supersede any still-finalizing run, and authoritatively clear the control
    // back to Run (send_replace updates the value even with no live receivers).
    shared.generation.fetch_add(1, Ordering::SeqCst);
    shared.control_tx.send_replace(Control::Run);
    {
        let mut m = shared.meta.lock().unwrap();
        m.state = DownloadState::Queued;
        m.error = None;
    }
    engine.emit(EngineEvent::StateChanged {
        id,
        state: DownloadState::Queued,
        error: None,
    });
    tokio::spawn(run(engine, shared));
}

fn set_state(engine: &Engine, shared: &Arc<Shared>, state: DownloadState, error: Option<String>) {
    {
        let mut m = shared.meta.lock().unwrap();
        m.state = state;
        m.error = error.clone();
    }
    engine.emit(EngineEvent::StateChanged {
        id: shared.id,
        state,
        error,
    });
}

struct Probe {
    total: Option<u64>,
    resumable: bool,
    filename: Option<String>,
}

/// Build a request replaying the browser's cookies / headers / referrer.
fn build_request(
    engine: &Engine,
    shared: &Arc<Shared>,
    range: Option<String>,
) -> reqwest::RequestBuilder {
    let req = shared.request.lock().unwrap().clone();
    let mut b = engine.client().get(&req.url);
    for (k, v) in &req.headers {
        b = b.header(k, v);
    }
    if let Some(c) = &req.cookie {
        b = b.header(COOKIE, c);
    }
    if let Some(r) = &req.referrer {
        b = b.header(REFERER, r);
    }
    if let Some(ua) = &req.user_agent {
        b = b.header(USER_AGENT, ua);
    }
    if let Some(r) = range {
        b = b.header(RANGE, r);
    }
    b
}

async fn probe(engine: &Engine, shared: &Arc<Shared>) -> anyhow::Result<Probe> {
    let resp = build_request(engine, shared, Some("bytes=0-0".to_string()))
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("server returned status {status}");
    }
    let headers = resp.headers();
    let filename = headers
        .get(CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_disposition);

    let (total, resumable) = if status.as_u16() == 206 {
        // bytes 0-0/12345  -> total is after the slash
        let total = headers
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.rsplit('/').next())
            .and_then(|s| s.trim().parse::<u64>().ok());
        (total, true)
    } else {
        // Range ignored -> single stream, not resumable.
        let total = headers
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        let accepts = headers
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.contains("bytes"))
            .unwrap_or(false);
        (total, accepts && total.is_some())
    };
    drop(resp);
    Ok(Probe {
        total,
        resumable,
        filename,
    })
}

async fn resolve_dest(engine: &Engine, shared: &Arc<Shared>, probe: &Probe) -> anyhow::Result<PathBuf> {
    let (filename, size, mime, source_page) = {
        let m = shared.meta.lock().unwrap();
        let req = shared.request.lock().unwrap();
        (
            m.filename.clone(),
            m.total,
            req.mime.clone(),
            req.source_page.clone(),
        )
    };
    let _ = probe;
    let host = host_of(&shared.url);
    let ext = filename
        .rsplit_once('.')
        .map(|(_, e)| e.to_lowercase())
        .filter(|e| !e.is_empty() && e.len() <= 8);
    let ctx = PathContext {
        url: shared.url.clone(),
        filename,
        mime,
        size,
        source_page,
        host,
        ext,
    };
    engine.resolver().resolve(ctx).await
}

async fn run(engine: Engine, shared: Arc<Shared>) {
    let id = shared.id;
    // Serialize against any previous run of this download (e.g. a quick
    // pause→resume) so segment workers never overlap on the same file.
    let _run_guard = shared.run_lock.lock().await;
    let my_gen = shared.generation.load(Ordering::SeqCst);
    set_state(&engine, &shared, DownloadState::Preparing, None);

    // 1. Probe server for size / range support / filename.
    let probe = match probe(&engine, &shared).await {
        Ok(p) => p,
        Err(e) => {
            set_state(&engine, &shared, DownloadState::Failed, Some(e.to_string()));
            return;
        }
    };
    {
        let mut m = shared.meta.lock().unwrap();
        m.total = probe.total.or(m.total);
        m.resumable = probe.resumable;
        if let Some(f) = &probe.filename {
            m.filename = crate::sanitize(f);
        }
    }

    // 2. Resolve destination path (user script or default resolver).
    let dest = match resolve_dest(&engine, &shared, &probe).await {
        Ok(p) => p,
        Err(e) => {
            set_state(
                &engine,
                &shared,
                DownloadState::Failed,
                Some(format!("path resolver failed: {e}")),
            );
            return;
        }
    };
    if let Some(parent) = dest.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    {
        shared.meta.lock().unwrap().dest_path = Some(dest.clone());
    }

    let (total, resumable, connections) = {
        let m = shared.meta.lock().unwrap();
        (m.total, m.resumable, m.connections)
    };

    // 3. Decide the segment plan, reusing a valid sidecar if present.
    let min_seg = engine.config().min_segment_size;
    let plan: Vec<SegmentRecord> = match PartFile::load(&dest).await {
        Some(pf) if pf.url == shared.url && pf.total == total && pf.resumable == resumable => {
            pf.segments
        }
        _ => build_plan(total, resumable, connections, min_seg),
    };
    let fresh = plan.iter().all(|s| s.done == 0);

    // 4. Open / preallocate the file.
    if let Err(e) = prepare_file(&dest, total, fresh).await {
        set_state(&engine, &shared, DownloadState::Failed, Some(e.to_string()));
        return;
    }

    // Reset the downloaded counter to what the sidecar already has.
    let already: u64 = plan.iter().map(|s| s.done).sum();
    shared.downloaded.store(already, Ordering::Relaxed);

    set_state(&engine, &shared, DownloadState::Downloading, None);
    engine.bw_set_active(id, true).await;

    // 5. Spawn one worker per segment.
    let seg_done: Arc<Vec<AtomicU64>> =
        Arc::new(plan.iter().map(|s| AtomicU64::new(s.done)).collect());
    let plan_arc = Arc::new(plan.clone());

    let mut handles = Vec::new();
    for (idx, seg) in plan.iter().enumerate() {
        let engine = engine.clone();
        let shared = shared.clone();
        let seg_done = seg_done.clone();
        let dest = dest.clone();
        let seg = seg.clone();
        handles.push(tokio::spawn(async move {
            run_segment(engine, shared, idx, seg, seg_done, dest).await
        }));
    }

    // 6. Ticker: speed, progress events, periodic sidecar flush.
    let ticker = spawn_ticker(
        engine.clone(),
        shared.clone(),
        seg_done.clone(),
        plan_arc.clone(),
        dest.clone(),
        total,
        resumable,
        already,
    );

    // 7. Wait for all workers.
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap_or(SegOutcome::Failed("worker panicked".into())));
    }
    ticker.abort();
    engine.bw_set_active(id, false).await;

    // If a newer run (resume) has superseded us, leave all state to it.
    if shared.generation.load(Ordering::SeqCst) != my_gen {
        return;
    }

    // Persist the latest sidecar snapshot.
    let final_plan = snapshot_plan(&plan_arc, &seg_done);
    let pf = PartFile {
        url: shared.url.clone(),
        total,
        resumable,
        segments: final_plan,
    };

    let control = *shared.control_tx.borrow();
    match control {
        Control::Cancel => {
            PartFile::remove(&dest).await;
            let _ = tokio::fs::remove_file(&dest).await;
            set_state(&engine, &shared, DownloadState::Canceled, None);
        }
        Control::Pause => {
            pf.save(&dest).await;
            set_state(&engine, &shared, DownloadState::Paused, None);
        }
        Control::Run => {
            if let Some(SegOutcome::Failed(msg)) =
                results.iter().find(|r| matches!(r, SegOutcome::Failed(_)))
            {
                pf.save(&dest).await;
                set_state(&engine, &shared, DownloadState::Failed, Some(msg.clone()));
            } else {
                PartFile::remove(&dest).await;
                set_state(&engine, &shared, DownloadState::Completed, None);
            }
        }
    }
}

#[derive(Debug, Clone)]
enum SegOutcome {
    Completed,
    Interrupted,
    Failed(String),
}

#[allow(clippy::too_many_arguments)]
fn spawn_ticker(
    engine: Engine,
    shared: Arc<Shared>,
    seg_done: Arc<Vec<AtomicU64>>,
    plan: Arc<Vec<SegmentRecord>>,
    dest: PathBuf,
    total: Option<u64>,
    resumable: bool,
    start_downloaded: u64,
) -> tokio::task::JoinHandle<()> {
    let interval = Duration::from_millis(engine.config().progress_interval_ms.max(100));
    tokio::spawn(async move {
        let mut last = Instant::now();
        let mut last_bytes = start_downloaded;
        let mut ema = 0f64;
        loop {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            let dt = now.duration_since(last).as_secs_f64().max(0.001);
            let cur = shared.downloaded.load(Ordering::Relaxed);
            let inst = (cur.saturating_sub(last_bytes)) as f64 / dt;
            ema = if ema == 0.0 { inst } else { 0.6 * inst + 0.4 * ema };
            shared.speed.store(ema as u64, Ordering::Relaxed);
            last = now;
            last_bytes = cur;

            engine.emit(EngineEvent::Progress {
                id: shared.id,
                downloaded_bytes: cur,
                total_bytes: total,
                speed_bps: ema as u64,
            });

            // Flush sidecar so a crash loses at most one interval of progress.
            let pf = PartFile {
                url: shared.url.clone(),
                total,
                resumable,
                segments: snapshot_plan(&plan, &seg_done),
            };
            pf.save(&dest).await;
        }
    })
}

fn snapshot_plan(plan: &Arc<Vec<SegmentRecord>>, seg_done: &Arc<Vec<AtomicU64>>) -> Vec<SegmentRecord> {
    plan.iter()
        .enumerate()
        .map(|(i, s)| SegmentRecord {
            start: s.start,
            end: s.end,
            done: seg_done[i].load(Ordering::Relaxed),
        })
        .collect()
}

fn build_plan(
    total: Option<u64>,
    resumable: bool,
    connections: usize,
    min_seg: u64,
) -> Vec<SegmentRecord> {
    match (resumable, total) {
        (true, Some(total)) if total > min_seg => {
            let max_by_size = (total / min_seg).max(1) as usize;
            let n = connections.clamp(1, max_by_size);
            let chunk = total / n as u64;
            (0..n)
                .map(|i| {
                    let start = i as u64 * chunk;
                    let end = if i == n - 1 { total - 1 } else { (i as u64 + 1) * chunk - 1 };
                    SegmentRecord {
                        start,
                        end: Some(end),
                        done: 0,
                    }
                })
                .collect()
        }
        (true, Some(total)) => vec![SegmentRecord {
            start: 0,
            end: Some(total.saturating_sub(1)),
            done: 0,
        }],
        // Not resumable (or size unknown) -> one open-ended stream.
        _ => vec![SegmentRecord {
            start: 0,
            end: None,
            done: 0,
        }],
    }
}

async fn prepare_file(dest: &PathBuf, total: Option<u64>, fresh: bool) -> anyhow::Result<()> {
    let mut oo = tokio::fs::OpenOptions::new();
    oo.write(true).read(true).create(true);
    if fresh {
        oo.truncate(true);
    }
    let file = oo.open(dest).await?;
    if let Some(t) = total {
        let _ = file.set_len(t).await;
    }
    Ok(())
}

async fn run_segment(
    engine: Engine,
    shared: Arc<Shared>,
    idx: usize,
    seg: SegmentRecord,
    seg_done: Arc<Vec<AtomicU64>>,
    dest: PathBuf,
) -> SegOutcome {
    let mut control = shared.control_tx.subscribe();
    let cursor = seg.cursor();
    if let Some(end) = seg.end {
        if cursor > end {
            return SegOutcome::Completed;
        }
    }

    // Don't even open a connection if we've already been paused/canceled.
    match *control.borrow_and_update() {
        Control::Pause | Control::Cancel => return SegOutcome::Interrupted,
        Control::Run => {}
    }

    // Build the Range header for this segment (if any).
    let range = match seg.end {
        Some(end) => Some(format!("bytes={cursor}-{end}")),
        None if cursor > 0 => Some(format!("bytes={cursor}-")),
        None => None,
    };

    let resp = match build_request(&engine, &shared, range).send().await {
        Ok(r) => r,
        Err(e) => return SegOutcome::Failed(e.to_string()),
    };
    if !resp.status().is_success() {
        return SegOutcome::Failed(format!("segment status {}", resp.status()));
    }

    let mut file = match tokio::fs::OpenOptions::new().write(true).open(&dest).await {
        Ok(f) => f,
        Err(e) => return SegOutcome::Failed(e.to_string()),
    };
    if let Err(e) = file.seek(std::io::SeekFrom::Start(cursor)).await {
        return SegOutcome::Failed(e.to_string());
    }

    let buf_size = engine.config().buffer_size.max(4096);
    let mut stream = resp.bytes_stream();

    loop {
        // Check control between chunks.
        match *control.borrow_and_update() {
            Control::Pause | Control::Cancel => return SegOutcome::Interrupted,
            Control::Run => {}
        }

        tokio::select! {
            _ = control.changed() => continue,
            chunk = stream.next() => {
                match chunk {
                    None => break,
                    Some(Err(e)) => return SegOutcome::Failed(e.to_string()),
                    Some(Ok(bytes)) => {
                        // Throttle + write in buffer-sized slices to keep memory flat.
                        let mut off = 0;
                        while off < bytes.len() {
                            let end = (off + buf_size).min(bytes.len());
                            let slice = &bytes[off..end];
                            shared.limiter.acquire(slice.len()).await;
                            if let Err(e) = file.write_all(slice).await {
                                return SegOutcome::Failed(e.to_string());
                            }
                            let n = slice.len() as u64;
                            seg_done[idx].fetch_add(n, Ordering::Relaxed);
                            shared.downloaded.fetch_add(n, Ordering::Relaxed);
                            off = end;
                        }
                    }
                }
            }
        }
    }

    if let Err(e) = file.flush().await {
        return SegOutcome::Failed(e.to_string());
    }
    SegOutcome::Completed
}

fn initial_filename(request: &DownloadRequest) -> String {
    if let Some(f) = &request.filename {
        let f = crate::sanitize(f);
        if !f.is_empty() {
            return f;
        }
    }
    // Derive from URL path.
    let path = request
        .url
        .split('?')
        .next()
        .unwrap_or(&request.url)
        .split('#')
        .next()
        .unwrap_or(&request.url);
    let name = path.rsplit('/').next().unwrap_or("download");
    let name = crate::sanitize(name);
    if name.is_empty() {
        "download".to_string()
    } else {
        name
    }
}

/// Extract a filename from a `Content-Disposition` header value. Handles both
/// `filename="x"` and RFC 5987 `filename*=UTF-8''x` forms.
fn parse_content_disposition(v: &str) -> Option<String> {
    // Prefer the extended form.
    for part in v.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename*=") {
            // e.g. UTF-8''my%20file.zip
            let value = rest.rsplit("''").next().unwrap_or(rest);
            let decoded = percent_decode(value);
            let name = crate::sanitize(decoded.trim_matches('"'));
            if !name.is_empty() {
                return Some(name);
            }
        }
    }
    for part in v.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename=") {
            let name = crate::sanitize(rest.trim().trim_matches('"'));
            if !name.is_empty() {
                return Some(name);
            }
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]));
            if let (Some(a), Some(b)) = h {
                out.push(a * 16 + b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn host_of(url: &str) -> Option<String> {
    let after = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let host = after.split(['/', '?', '#']).next().unwrap_or("");
    let host = host.rsplit('@').next().unwrap_or(host); // strip userinfo
    let host = host.split(':').next().unwrap_or(host); // strip port
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}
