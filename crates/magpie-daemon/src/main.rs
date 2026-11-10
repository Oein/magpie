//! `magpie` — the better-fdm daemon and CLI.
//!
//! Subcommands:
//! * `magpie run`          start the background daemon (WebSocket server + engine)
//! * `magpie add <url>`    queue a download (auto-launches the daemon if needed)
//! * `magpie list`         print the current downloads
//! * `magpie native-host`  browser native-messaging bootstrap (auto-launch)
//! * `magpie paths`        print config / script locations

mod server;

use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use magpie_core::{Engine, EngineConfig};
use magpie_proto::{ClientMessage, DownloadRequest, ServerMessage};
use magpie_daemon::config::Config;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("run");
    match cmd {
        "run" | "daemon" => run_daemon().await,
        "add" => {
            let no_wait = args.iter().any(|a| a == "--no-wait");
            let url = args
                .iter()
                .skip(2)
                .find(|a| !a.starts_with("--"))
                .cloned()
                .unwrap_or_default();
            if url.is_empty() {
                eprintln!("usage: magpie add <url> [--no-wait]");
                std::process::exit(2);
            }
            cli_add(url, no_wait).await
        }
        "list" | "status" => cli_list().await,
        "pause" => cli_control(&args, Ctl::Pause).await,
        "resume" => cli_control(&args, Ctl::Resume).await,
        "cancel" => cli_control(&args, Ctl::Cancel).await,
        "remove" => cli_control(&args, Ctl::Remove).await,
        "priority" => cli_priority(&args).await,
        "limit" => cli_limit(&args).await,
        "limit-global" | "glimit" => cli_global_limit(&args).await,
        "native-host" => native_host().await,
        "paths" => {
            println!("config dir : {}", Config::config_dir().display());
            println!("config     : {}", Config::config_path().display());
            println!("script     : {}", Config::script_path().display());
            Ok(())
        }
        other => {
            eprintln!("unknown command: {other}");
            eprintln!(
                "commands:\n  \
                 run | add <url> [--no-wait] | list\n  \
                 pause <id> | resume <id> | cancel <id> | remove <id>\n  \
                 priority <id> <low|normal|high>\n  \
                 limit <id> <KB/s|off> | limit-global <KB/s|off>\n  \
                 native-host | paths"
            );
            std::process::exit(2);
        }
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

async fn run_daemon() -> anyhow::Result<()> {
    init_tracing();
    let cfg = Config::load_or_init()?;

    let mut engine_cfg = EngineConfig {
        download_dir: cfg.download_dir.clone(),
        ..Default::default()
    };
    if let Some(n) = cfg.max_connections_per_download {
        engine_cfg.max_connections_per_download = n;
    }

    let script = Config::read_script();
    let resolver = magpie_script::boxed(magpie_script::ScriptResolver::new(
        script,
        cfg.download_dir.clone(),
    ));
    let engine = Engine::new(engine_cfg, resolver)?;
    if let Some(limit) = cfg.global_speed_limit {
        engine.set_global_speed_limit(Some(limit)).await;
    }

    let token = Arc::new(cfg.token.clone());
    match server::run(engine, cfg.port, token).await {
        Ok(()) => Ok(()),
        Err(e) => {
            // Another daemon already holds the port — that's fine.
            if e.to_string().contains("in use") || e.to_string().contains("AddrInUse") {
                tracing::info!("daemon already running on port {}", cfg.port);
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}

/// Connect to the daemon, auto-launching it if it isn't up yet.
async fn connect_client(
    cfg: &Config,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
> {
    ensure_daemon_running(cfg).await?;
    let url = format!("ws://127.0.0.1:{}/?token={}", cfg.port, cfg.token);
    let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
    Ok(ws)
}

/// Returns once the daemon is accepting connections, launching it if needed.
async fn ensure_daemon_running(cfg: &Config) -> anyhow::Result<()> {
    if tokio::net::TcpStream::connect(("127.0.0.1", cfg.port))
        .await
        .is_ok()
    {
        return Ok(());
    }
    // Launch `magpie run` detached.
    let exe = std::env::current_exe()?;
    let _ = std::process::Command::new(exe)
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    // Poll until the port is open (up to ~5s).
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", cfg.port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("daemon did not come up on port {}", cfg.port)
}

async fn cli_add(url: String, no_wait: bool) -> anyhow::Result<()> {
    let cfg = Config::load_or_init()?;
    let ws = connect_client(&cfg).await?;
    let (mut write, mut read) = ws.split();

    let request = DownloadRequest {
        url: url.clone(),
        filename: None,
        headers: vec![],
        cookie: None,
        referrer: None,
        user_agent: None,
        mime: None,
        size_hint: None,
        source_page: None,
        options: Default::default(),
    };
    write
        .send(Message::Text(serde_json::to_string(&ClientMessage::Add {
            request,
        })?))
        .await?;

    println!("queued: {url}");
    if no_wait {
        // Wait only until the daemon acknowledges with an id, then exit.
        while let Some(Ok(Message::Text(txt))) = read.next().await {
            if let Ok(ServerMessage::Added { info }) = serde_json::from_str::<ServerMessage>(&txt) {
                if info.url == url {
                    println!("  id={}", info.id);
                    break;
                }
            }
        }
        return Ok(());
    }
    let mut my_id = None;
    while let Some(Ok(frame)) = read.next().await {
        if let Message::Text(txt) = frame {
            if let Ok(msg) = serde_json::from_str::<ServerMessage>(&txt) {
                match msg {
                    ServerMessage::Added { info } if info.url == url => {
                        my_id = Some(info.id);
                        println!("  id={} -> {}", info.id, info.filename);
                    }
                    ServerMessage::Progress {
                        id,
                        downloaded_bytes,
                        total_bytes,
                        speed_bps,
                    } if Some(id) == my_id => {
                        print_progress(downloaded_bytes, total_bytes, speed_bps);
                    }
                    ServerMessage::StateChanged { id, state, error } if Some(id) == my_id => {
                        println!("\n  state: {state:?}{}", err_suffix(&error));
                        use magpie_proto::DownloadState::*;
                        if matches!(state, Completed | Failed | Canceled) {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Ctl {
    Pause,
    Resume,
    Cancel,
    Remove,
}

/// Fetch a snapshot and resolve a (possibly abbreviated) id prefix to the full
/// download info.
async fn fetch_snapshot(cfg: &Config) -> anyhow::Result<Vec<magpie_proto::DownloadInfo>> {
    let ws = connect_client(cfg).await?;
    let (mut write, mut read) = ws.split();
    write
        .send(Message::Text(serde_json::to_string(&ClientMessage::List)?))
        .await?;
    while let Some(Ok(Message::Text(txt))) = read.next().await {
        if let Ok(ServerMessage::Snapshot { downloads }) =
            serde_json::from_str::<ServerMessage>(&txt)
        {
            return Ok(downloads);
        }
    }
    anyhow::bail!("no snapshot from daemon")
}

fn resolve_id(
    downloads: &[magpie_proto::DownloadInfo],
    prefix: &str,
) -> anyhow::Result<magpie_proto::DownloadInfo> {
    let matches: Vec<_> = downloads
        .iter()
        .filter(|d| d.id.to_string().starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [] => anyhow::bail!("no download matches id '{prefix}'"),
        [one] => Ok((*one).clone()),
        _ => anyhow::bail!("id '{prefix}' is ambiguous ({} matches)", matches.len()),
    }
}

/// Connect, send a single message, and return.
async fn send_one(cfg: &Config, msg: ClientMessage) -> anyhow::Result<()> {
    let ws = connect_client(cfg).await?;
    let (mut write, _read) = ws.split();
    write
        .send(Message::Text(serde_json::to_string(&msg)?))
        .await?;
    // Give the daemon a moment to apply before the socket closes.
    tokio::time::sleep(Duration::from_millis(120)).await;
    Ok(())
}

async fn cli_control(args: &[String], ctl: Ctl) -> anyhow::Result<()> {
    let prefix = args.get(2).cloned().unwrap_or_default();
    if prefix.is_empty() {
        anyhow::bail!("usage: magpie <pause|resume|cancel|remove> <id>");
    }
    let cfg = Config::load_or_init()?;
    let info = resolve_id(&fetch_snapshot(&cfg).await?, &prefix)?;
    let id = info.id;
    let msg = match ctl {
        Ctl::Pause => ClientMessage::Pause { id },
        Ctl::Resume => ClientMessage::Resume { id },
        Ctl::Cancel => ClientMessage::Cancel { id },
        Ctl::Remove => ClientMessage::Remove { id },
    };
    send_one(&cfg, msg).await?;
    let verb = match ctl {
        Ctl::Pause => "paused",
        Ctl::Resume => "resumed",
        Ctl::Cancel => "canceled",
        Ctl::Remove => "removed",
    };
    println!("{verb}: {} ({})", short_id(&id.to_string()), info.filename);
    Ok(())
}

async fn cli_priority(args: &[String]) -> anyhow::Result<()> {
    let prefix = args.get(2).cloned().unwrap_or_default();
    let level = args.get(3).cloned().unwrap_or_default().to_lowercase();
    let priority = match level.as_str() {
        "low" => magpie_proto::Priority::Low,
        "normal" => magpie_proto::Priority::Normal,
        "high" => magpie_proto::Priority::High,
        _ => anyhow::bail!("usage: magpie priority <id> <low|normal|high>"),
    };
    let cfg = Config::load_or_init()?;
    let info = resolve_id(&fetch_snapshot(&cfg).await?, &prefix)?;
    // Preserve the existing per-download speed cap.
    send_one(
        &cfg,
        ClientMessage::SetOptions {
            id: info.id,
            options: magpie_proto::DownloadOptions {
                priority,
                max_speed_bps: info.max_speed_bps,
                connections: None,
            },
        },
    )
    .await?;
    println!("priority {level}: {} ({})", short_id(&info.id.to_string()), info.filename);
    Ok(())
}

async fn cli_limit(args: &[String]) -> anyhow::Result<()> {
    let prefix = args.get(2).cloned().unwrap_or_default();
    let spec = args.get(3).cloned().unwrap_or_default();
    let max = parse_limit(&spec)?;
    let cfg = Config::load_or_init()?;
    let info = resolve_id(&fetch_snapshot(&cfg).await?, &prefix)?;
    send_one(
        &cfg,
        ClientMessage::SetOptions {
            id: info.id,
            options: magpie_proto::DownloadOptions {
                priority: info.priority,
                max_speed_bps: max,
                connections: None,
            },
        },
    )
    .await?;
    let shown = max.map(|b| format!("{} KB/s", b / 1024)).unwrap_or_else(|| "unlimited".into());
    println!("limit {shown}: {} ({})", short_id(&info.id.to_string()), info.filename);
    Ok(())
}

async fn cli_global_limit(args: &[String]) -> anyhow::Result<()> {
    let spec = args.get(2).cloned().unwrap_or_default();
    let max = parse_limit(&spec)?;
    let cfg = Config::load_or_init()?;
    send_one(&cfg, ClientMessage::SetGlobalSpeedLimit { max_speed_bps: max }).await?;
    let shown = max.map(|b| format!("{} KB/s", b / 1024)).unwrap_or_else(|| "unlimited".into());
    println!("global limit: {shown}");
    Ok(())
}

/// Parse a speed spec: a number in KB/s, or "off"/"none"/"0"/"" for unlimited.
fn parse_limit(spec: &str) -> anyhow::Result<Option<u64>> {
    let s = spec.trim().to_lowercase();
    if s.is_empty() || s == "off" || s == "none" || s == "0" {
        return Ok(None);
    }
    let kb: u64 = s
        .parse()
        .map_err(|_| anyhow::anyhow!("speed must be a number in KB/s, or 'off'"))?;
    Ok(Some(kb * 1024))
}

async fn cli_list() -> anyhow::Result<()> {
    let cfg = Config::load_or_init()?;
    let ws = connect_client(&cfg).await?;
    let (mut write, mut read) = ws.split();
    write
        .send(Message::Text(serde_json::to_string(&ClientMessage::List)?))
        .await?;
    while let Some(Ok(frame)) = read.next().await {
        if let Message::Text(txt) = frame {
            if let Ok(ServerMessage::Snapshot { downloads }) =
                serde_json::from_str::<ServerMessage>(&txt)
            {
                if downloads.is_empty() {
                    println!("(no downloads)");
                }
                for d in downloads {
                    println!(
                        "{:8} {:>6.1}%  {:>10}/s  {}  [{:?}]",
                        short_id(&d.id.to_string()),
                        percent(d.downloaded_bytes, d.total_bytes),
                        human(d.speed_bps),
                        d.filename,
                        d.state
                    );
                }
                break;
            }
        }
    }
    Ok(())
}

fn err_suffix(e: &Option<String>) -> String {
    e.as_ref().map(|s| format!(" ({s})")).unwrap_or_default()
}

fn print_progress(done: u64, total: Option<u64>, speed: u64) {
    print!(
        "\r  {:>6.1}%  {}  {}/s        ",
        percent(done, total),
        human(done),
        human(speed)
    );
    let _ = std::io::stdout().flush();
}

fn percent(done: u64, total: Option<u64>) -> f64 {
    match total {
        Some(t) if t > 0 => done as f64 / t as f64 * 100.0,
        _ => 0.0,
    }
}

fn short_id(s: &str) -> String {
    s.chars().take(8).collect()
}

fn human(n: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1}{}", U[i])
}

// ---------------------------------------------------------------------------
// Native messaging host (browser auto-launch bootstrap).
//
// Protocol: each message is a 4-byte little-endian length followed by UTF-8
// JSON. The extension sends `{"cmd":"bootstrap"}` and gets back
// `{"port":..,"token":".."}`, launching the daemon if necessary. It may also
// send `{"cmd":"add","request":{...}}` as a direct relay fallback.
// ---------------------------------------------------------------------------
async fn native_host() -> anyhow::Result<()> {
    let cfg = Config::load_or_init()?;
    loop {
        let msg = match read_native_message() {
            Ok(Some(m)) => m,
            Ok(None) => break, // stdin closed
            Err(_) => break,
        };
        let cmd = msg.get("cmd").and_then(|v| v.as_str()).unwrap_or("bootstrap");
        let reply = match cmd {
            "add" => {
                let resp = relay_add(&cfg, &msg).await;
                match resp {
                    Ok(()) => serde_json::json!({ "ok": true }),
                    Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                }
            }
            _ => {
                // bootstrap: ensure daemon up, return connection info.
                match ensure_daemon_running(&cfg).await {
                    Ok(()) => serde_json::json!({
                        "ok": true,
                        "port": cfg.port,
                        "token": cfg.token,
                    }),
                    Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                }
            }
        };
        write_native_message(&reply)?;
    }
    Ok(())
}

async fn relay_add(cfg: &Config, msg: &serde_json::Value) -> anyhow::Result<()> {
    let request: DownloadRequest = serde_json::from_value(
        msg.get("request").cloned().unwrap_or(serde_json::Value::Null),
    )?;
    let ws = connect_client(cfg).await?;
    let (mut write, _read) = ws.split();
    write
        .send(Message::Text(serde_json::to_string(&ClientMessage::Add {
            request,
        })?))
        .await?;
    Ok(())
}

fn read_native_message() -> anyhow::Result<Option<serde_json::Value>> {
    let mut len_buf = [0u8; 4];
    let mut stdin = std::io::stdin();
    if stdin.read_exact(&mut len_buf).is_err() {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 || len > 64 * 1024 * 1024 {
        return Ok(None);
    }
    let mut buf = vec![0u8; len];
    stdin.read_exact(&mut buf)?;
    Ok(Some(serde_json::from_slice(&buf)?))
}

fn write_native_message(v: &serde_json::Value) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(v)?;
    let mut out = std::io::stdout();
    out.write_all(&(bytes.len() as u32).to_le_bytes())?;
    out.write_all(&bytes)?;
    out.flush()?;
    Ok(())
}
