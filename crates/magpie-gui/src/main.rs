//! Magpie GUI — a Slint native front-end that talks to the daemon over the same
//! localhost WebSocket protocol as the extension and CLI.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::HashMap;
use std::process::Stdio;
use std::rc::Rc;
use std::time::Duration;

use magpie_daemon::config::Config;
use magpie_proto::{
    ClientMessage, DownloadInfo, DownloadOptions, DownloadRequest, DownloadState, Priority,
    ServerMessage,
};
use futures_util::{SinkExt, StreamExt};
use slint::{Model, ModelRc, Timer, TimerMode, VecModel};
use tokio_tungstenite::tungstenite::Message;

slint::include_modules!();

enum UiMsg {
    Server(ServerMessage),
    Connected(bool),
}

fn main() -> anyhow::Result<()> {
    let cfg = Config::load_or_init()?;

    let ui = MainWindow::new()?;
    let model: Rc<VecModel<DownloadRow>> = Rc::new(VecModel::default());
    ui.set_downloads(ModelRc::from(model.clone()));
    if let Some(limit) = cfg.global_speed_limit {
        ui.set_global_limit_text(format!("{}", limit / 1024).into());
    }

    // Channels: UI <- WS (std mpsc, polled by a Slint timer); UI -> WS (tokio).
    let (ui_tx, ui_rx) = std::sync::mpsc::channel::<UiMsg>();
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<ClientMessage>();

    // Background thread owns the WebSocket client.
    {
        let cfg = cfg.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(ws_loop(cfg, ui_tx, cmd_rx));
        });
    }

    // Wire UI callbacks -> commands.
    install_callbacks(&ui, cmd_tx);

    // Poll the WS->UI channel on the Slint event loop and apply updates.
    let model_c = model.clone();
    let ui_weak = ui.as_weak();
    let mut order: Vec<String> = Vec::new();
    let mut infos: HashMap<String, DownloadInfo> = HashMap::new();
    let timer = Timer::default();
    timer.start(TimerMode::Repeated, Duration::from_millis(120), move || {
        while let Ok(msg) = ui_rx.try_recv() {
            apply(&ui_weak, &model_c, &mut order, &mut infos, msg);
        }
    });

    ui.run()?;
    Ok(())
}

fn install_callbacks(ui: &MainWindow, cmd_tx: tokio::sync::mpsc::UnboundedSender<ClientMessage>) {
    let tx = cmd_tx.clone();
    ui.on_add_url(move |url| {
        let url = url.to_string();
        if url.trim().is_empty() {
            return;
        }
        let request = DownloadRequest {
            url,
            filename: None,
            headers: vec![],
            cookie: None,
            referrer: None,
            user_agent: None,
            mime: None,
            size_hint: None,
            source_page: None,
            options: DownloadOptions::default(),
        };
        let _ = tx.send(ClientMessage::Add { request });
    });

    macro_rules! id_cb {
        ($setter:ident, $variant:ident) => {{
            let tx = cmd_tx.clone();
            ui.$setter(move |id| {
                if let Ok(id) = uuid::Uuid::parse_str(&id) {
                    let _ = tx.send(ClientMessage::$variant { id });
                }
            });
        }};
    }
    id_cb!(on_pause, Pause);
    id_cb!(on_resume, Resume);
    id_cb!(on_cancel, Cancel);
    id_cb!(on_remove, Remove);

    let tx = cmd_tx.clone();
    ui.on_set_priority(move |id, prio| {
        if let Ok(id) = uuid::Uuid::parse_str(&id) {
            let priority = match prio.as_str() {
                "Low" => Priority::Low,
                "High" => Priority::High,
                _ => Priority::Normal,
            };
            let _ = tx.send(ClientMessage::SetOptions {
                id,
                options: DownloadOptions {
                    priority,
                    max_speed_bps: None,
                    connections: None,
                },
            });
        }
    });

    let tx = cmd_tx;
    ui.on_set_global_limit(move |text| {
        let t = text.trim();
        let max = if t.is_empty() {
            None
        } else {
            t.parse::<u64>().ok().map(|kb| kb * 1024)
        };
        let _ = tx.send(ClientMessage::SetGlobalSpeedLimit { max_speed_bps: max });
    });
}

fn apply(
    ui_weak: &slint::Weak<MainWindow>,
    model: &Rc<VecModel<DownloadRow>>,
    order: &mut Vec<String>,
    infos: &mut HashMap<String, DownloadInfo>,
    msg: UiMsg,
) {
    match msg {
        UiMsg::Connected(c) => {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_connected(c);
            }
        }
        UiMsg::Server(ServerMessage::Snapshot { downloads }) => {
            order.clear();
            infos.clear();
            let mut rows = Vec::with_capacity(downloads.len());
            for d in downloads {
                let id = d.id.to_string();
                rows.push(to_row(&d));
                order.push(id.clone());
                infos.insert(id, d);
            }
            model.set_vec(rows);
        }
        UiMsg::Server(ServerMessage::Added { info }) => {
            let id = info.id.to_string();
            if let Some(idx) = order.iter().position(|x| x == &id) {
                infos.insert(id, info.clone());
                model.set_row_data(idx, to_row(&info));
            } else {
                order.push(id.clone());
                model.push(to_row(&info));
                infos.insert(id, info);
            }
        }
        UiMsg::Server(ServerMessage::Progress {
            id,
            downloaded_bytes,
            total_bytes,
            speed_bps,
        }) => {
            let key = id.to_string();
            if let Some(info) = infos.get_mut(&key) {
                info.downloaded_bytes = downloaded_bytes;
                info.total_bytes = total_bytes;
                info.speed_bps = speed_bps;
                if let Some(idx) = order.iter().position(|x| x == &key) {
                    model.set_row_data(idx, to_row(info));
                }
            }
        }
        UiMsg::Server(ServerMessage::StateChanged { id, state, error }) => {
            let key = id.to_string();
            if let Some(info) = infos.get_mut(&key) {
                info.state = state;
                info.error = error;
                if let Some(idx) = order.iter().position(|x| x == &key) {
                    model.set_row_data(idx, to_row(info));
                }
            }
        }
        UiMsg::Server(ServerMessage::Removed { id }) => {
            let key = id.to_string();
            if let Some(idx) = order.iter().position(|x| x == &key) {
                order.remove(idx);
                infos.remove(&key);
                model.remove(idx);
            }
        }
        UiMsg::Server(_) => {}
    }
}

fn to_row(d: &DownloadInfo) -> DownloadRow {
    let pct = match d.total_bytes {
        Some(t) if t > 0 => (d.downloaded_bytes as f64 / t as f64 * 100.0) as f32,
        _ => 0.0,
    };
    let size_text = match d.total_bytes {
        Some(t) => format!("{} / {}", human(d.downloaded_bytes), human(t)),
        None => human(d.downloaded_bytes),
    };
    let speed_text = if matches!(d.state, DownloadState::Downloading) {
        format!("{}/s", human(d.speed_bps))
    } else {
        String::new()
    };
    DownloadRow {
        id: d.id.to_string().into(),
        filename: d.filename.clone().into(),
        state: state_str(d.state).into(),
        pct,
        size_text: size_text.into(),
        speed_text: speed_text.into(),
        priority: match d.priority {
            Priority::Low => "Low",
            Priority::Normal => "Normal",
            Priority::High => "High",
        }
        .into(),
        error: d.error.clone().unwrap_or_default().into(),
        active: matches!(
            d.state,
            DownloadState::Downloading | DownloadState::Preparing | DownloadState::Queued
        ),
    }
}

fn state_str(s: DownloadState) -> &'static str {
    match s {
        DownloadState::Queued => "queued",
        DownloadState::Preparing => "preparing",
        DownloadState::Downloading => "downloading",
        DownloadState::Paused => "paused",
        DownloadState::Completed => "completed",
        DownloadState::Failed => "failed",
        DownloadState::Canceled => "canceled",
    }
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

async fn ws_loop(
    cfg: Config,
    ui_tx: std::sync::mpsc::Sender<UiMsg>,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<ClientMessage>,
) {
    loop {
        ensure_daemon(&cfg).await;
        let url = format!("ws://127.0.0.1:{}/?token={}", cfg.port, cfg.token);
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                let _ = ui_tx.send(UiMsg::Connected(true));
                let (mut write, mut read) = ws.split();
                let _ = write
                    .send(Message::Text(
                        serde_json::to_string(&ClientMessage::List).unwrap(),
                    ))
                    .await;
                loop {
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(cmd) => {
                                    let txt = serde_json::to_string(&cmd).unwrap();
                                    if write.send(Message::Text(txt)).await.is_err() { break; }
                                }
                                None => return, // UI gone
                            }
                        }
                        frame = read.next() => {
                            match frame {
                                Some(Ok(Message::Text(t))) => {
                                    if let Ok(m) = serde_json::from_str::<ServerMessage>(&t) {
                                        let _ = ui_tx.send(UiMsg::Server(m));
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => break,
                                Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                    }
                }
                let _ = ui_tx.send(UiMsg::Connected(false));
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Best-effort: if nothing is listening, launch `magpie run` from next to us.
async fn ensure_daemon(cfg: &Config) {
    if tokio::net::TcpStream::connect(("127.0.0.1", cfg.port))
        .await
        .is_ok()
    {
        return;
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let magpie = dir.join(if cfg!(windows) { "magpie.exe" } else { "magpie" });
            if magpie.exists() {
                let _ = std::process::Command::new(magpie)
                    .arg("run")
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn();
            }
        }
    }
    for _ in 0..30 {
        if tokio::net::TcpStream::connect(("127.0.0.1", cfg.port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
