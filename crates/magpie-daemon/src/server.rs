//! Localhost WebSocket server. Clients (extension, GUI, CLI) present the shared
//! token as `?token=...` during the handshake, then exchange [`ClientMessage`] /
//! [`ServerMessage`] JSON frames.

use std::sync::Arc;

use magpie_core::{Engine, EngineEvent};
use magpie_proto::{ClientMessage, ServerMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn run(engine: Engine, port: u16, token: Arc<String>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!("daemon listening on ws://127.0.0.1:{port}");
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };
        let engine = engine.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, engine, token).await {
                tracing::debug!("connection closed: {e}");
            }
        });
    }
}

fn token_from_query(query: Option<&str>) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some(v) = pair.strip_prefix("token=") {
            return Some(v.to_string());
        }
    }
    None
}

async fn handle_conn(
    stream: TcpStream,
    engine: Engine,
    token: Arc<String>,
) -> anyhow::Result<()> {
    let expected = token.clone();
    let ws = tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, resp: Response| {
        let ok = token_from_query(req.uri().query())
            .map(|t| t == *expected)
            .unwrap_or(false);
        if ok {
            Ok(resp)
        } else {
            let mut err = ErrorResponse::new(Some("invalid or missing token".into()));
            *err.status_mut() = StatusCode::UNAUTHORIZED;
            Err(err)
        }
    })
    .await?;

    let (mut write, mut read) = ws.split();
    let (tx, mut rx) = mpsc::channel::<Message>(256);

    // Writer task: single owner of the sink.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Subscribe before snapshot so no event is missed (clients dedupe by id).
    let mut events = engine.subscribe();
    if let Ok(msg) = to_msg(&ServerMessage::Snapshot {
        downloads: engine.list().await,
    }) {
        let _ = tx.send(msg).await;
    }

    // Event-forwarding task.
    let ev_tx = tx.clone();
    let ev_task = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(ev) => {
                    if let Ok(msg) = to_msg(&map_event(ev)) {
                        if ev_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    });

    // Read / dispatch loop.
    while let Some(frame) = read.next().await {
        let frame = match frame {
            Ok(f) => f,
            Err(_) => break,
        };
        match frame {
            Message::Text(txt) => {
                match serde_json::from_str::<ClientMessage>(&txt) {
                    Ok(cmd) => dispatch(&engine, cmd, &tx).await,
                    Err(e) => {
                        let _ = tx
                            .send(to_msg(&ServerMessage::Error {
                                message: format!("bad message: {e}"),
                            })?)
                            .await;
                    }
                }
            }
            Message::Ping(p) => {
                let _ = tx.send(Message::Pong(p)).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    ev_task.abort();
    drop(tx);
    let _ = writer.await;
    Ok(())
}

async fn dispatch(engine: &Engine, cmd: ClientMessage, tx: &mpsc::Sender<Message>) {
    match cmd {
        ClientMessage::Add { request } => {
            engine.add(request).await;
        }
        ClientMessage::Pause { id } => engine.pause(id).await,
        ClientMessage::Resume { id } => engine.resume(id).await,
        ClientMessage::Cancel { id } => engine.cancel(id).await,
        ClientMessage::Remove { id } => engine.remove(id).await,
        ClientMessage::SetOptions { id, options } => engine.set_options(id, options).await,
        ClientMessage::SetGlobalSpeedLimit { max_speed_bps } => {
            engine.set_global_speed_limit(max_speed_bps).await;
        }
        ClientMessage::List => {
            if let Ok(msg) = to_msg(&ServerMessage::Snapshot {
                downloads: engine.list().await,
            }) {
                let _ = tx.send(msg).await;
            }
        }
        ClientMessage::Ping => {
            if let Ok(msg) = to_msg(&ServerMessage::Pong {
                version: VERSION.to_string(),
            }) {
                let _ = tx.send(msg).await;
            }
        }
    }
}

fn map_event(ev: EngineEvent) -> ServerMessage {
    match ev {
        EngineEvent::Added(info) => ServerMessage::Added { info },
        EngineEvent::Progress {
            id,
            downloaded_bytes,
            total_bytes,
            speed_bps,
        } => ServerMessage::Progress {
            id,
            downloaded_bytes,
            total_bytes,
            speed_bps,
        },
        EngineEvent::StateChanged { id, state, error } => {
            ServerMessage::StateChanged { id, state, error }
        }
        EngineEvent::Removed(id) => ServerMessage::Removed { id },
    }
}

fn to_msg(m: &ServerMessage) -> anyhow::Result<Message> {
    Ok(Message::Text(serde_json::to_string(m)?))
}
