/*
 * Magpie background script (Firefox MV2).
 *
 * Responsibilities:
 *   1. Bootstrap a connection to the Magpie daemon via native messaging
 *      (which also auto-launches the daemon if it isn't running).
 *   2. Keep a WebSocket open to the daemon for forwarding downloads and
 *      receiving live state.
 *   3. Intercept browser downloads, cancel them, collect cookies + referrer,
 *      and hand them to the daemon.
 */

const NATIVE_HOST = "com.magpie.host";
const RECONNECT_MS = 2000;

let conn = null; // { port, token }
let ws = null;
let wsReady = false;
const pending = []; // messages queued while the socket is down
const downloads = new Map(); // id -> DownloadInfo (mirrored from daemon)

// ---------------------------------------------------------------------------
// Request-header capture
//
// downloads.onCreated gives us no request headers, so we snoop the real headers
// the browser sent (via webRequest.onSendHeaders) and correlate them to the
// download by URL. This lets authenticated / token-gated downloads be replayed
// faithfully by the daemon.
// ---------------------------------------------------------------------------

const headerCache = new Map(); // url -> { headers: [{name,value}], ts }
const HEADER_TTL_MS = 60000;
const HEADER_CACHE_MAX = 300;

// Headers the daemon (reqwest) manages itself or that must not be replayed.
const SKIP_HEADERS = new Set([
  "host",
  "connection",
  "content-length",
  "keep-alive",
  "transfer-encoding",
  "te",
  "upgrade",
  "proxy-connection",
  "accept-encoding", // reqwest negotiates + decodes gzip/br itself
  "range",           // the engine does its own byte-range segmenting
  "cookie",          // forwarded separately as request.cookie
  "referer",         // forwarded separately as request.referrer
  "user-agent",      // forwarded separately as request.user_agent
]);

function cacheHeaders(details) {
  if (!details.requestHeaders) return;
  const now = Date.now();
  if (headerCache.size >= HEADER_CACHE_MAX) {
    for (const [k, v] of headerCache) {
      if (now - v.ts > HEADER_TTL_MS) headerCache.delete(k);
    }
    while (headerCache.size >= HEADER_CACHE_MAX) {
      headerCache.delete(headerCache.keys().next().value); // drop oldest
    }
  }
  headerCache.set(details.url, { headers: details.requestHeaders, ts: now });
}

browser.webRequest.onSendHeaders.addListener(
  cacheHeaders,
  {
    urls: ["<all_urls>"],
    types: ["main_frame", "sub_frame", "xmlhttprequest", "other", "media"],
  },
  ["requestHeaders"]
);

// Split captured headers into (replayable headers, cookie, referrer, UA).
function splitCapturedHeaders(item) {
  const entry =
    headerCache.get(item.url) ||
    (item.finalUrl ? headerCache.get(item.finalUrl) : null);

  let cookie = null;
  let referrer = item.referrer || null;
  let userAgent = navigator.userAgent;
  const headers = [];

  if (entry) {
    for (const h of entry.headers) {
      const n = h.name.toLowerCase();
      if (n === "cookie") { cookie = h.value; continue; }
      if (n === "referer") { referrer = h.value; continue; }
      if (n === "user-agent") { userAgent = h.value; continue; }
      if (SKIP_HEADERS.has(n)) continue;
      headers.push([h.name, h.value]);
    }
  }
  return { headers, cookie, referrer, userAgent };
}

// ---------------------------------------------------------------------------
// Daemon connection
// ---------------------------------------------------------------------------

async function bootstrap() {
  // sendNativeMessage spawns the host, which ensures the daemon is up and
  // returns { ok, port, token }. The host process then exits.
  const resp = await browser.runtime.sendNativeMessage(NATIVE_HOST, {
    cmd: "bootstrap",
  });
  if (!resp || !resp.ok) {
    throw new Error("bootstrap failed: " + (resp && resp.error));
  }
  conn = { port: resp.port, token: resp.token };
  await browser.storage.local.set({ conn });
  return conn;
}

async function ensureConn() {
  if (!conn) {
    const stored = await browser.storage.local.get("conn");
    if (stored && stored.conn) conn = stored.conn;
  }
  if (!conn) await bootstrap();
}

function openWs() {
  return new Promise((resolve, reject) => {
    try {
      ws = new WebSocket(`ws://127.0.0.1:${conn.port}/?token=${conn.token}`);
    } catch (e) {
      return reject(e);
    }
    ws.onopen = () => {
      wsReady = true;
      flushPending();
      send({ type: "list" });
      resolve();
    };
    ws.onmessage = (ev) => handleServerMessage(ev.data);
    ws.onclose = () => {
      wsReady = false;
      ws = null;
      setTimeout(connect, RECONNECT_MS);
    };
    ws.onerror = () => {
      try { ws.close(); } catch (e) {}
      reject(new Error("websocket error"));
    };
  });
}

async function connect() {
  try {
    await ensureConn();
    await openWs();
  } catch (e) {
    // The cached conn may be stale (daemon restarted → new token). Re-bootstrap.
    conn = null;
    try {
      await bootstrap();
      await openWs();
    } catch (e2) {
      console.warn("Magpie: cannot reach daemon:", e2);
      setTimeout(connect, RECONNECT_MS);
    }
  }
}

function flushPending() {
  while (pending.length && wsReady) {
    ws.send(JSON.stringify(pending.shift()));
  }
}

function send(msg) {
  if (wsReady && ws) {
    ws.send(JSON.stringify(msg));
  } else {
    pending.push(msg);
    connect();
  }
}

function handleServerMessage(data) {
  let msg;
  try { msg = JSON.parse(data); } catch (e) { return; }
  switch (msg.type) {
    case "snapshot":
      downloads.clear();
      for (const d of msg.downloads) downloads.set(d.id, d);
      break;
    case "added":
      downloads.set(msg.info.id, msg.info);
      break;
    case "progress": {
      const d = downloads.get(msg.id);
      if (d) {
        d.downloaded_bytes = msg.downloaded_bytes;
        d.total_bytes = msg.total_bytes;
        d.speed_bps = msg.speed_bps;
      }
      break;
    }
    case "state_changed": {
      const d = downloads.get(msg.id);
      if (d) { d.state = msg.state; d.error = msg.error; }
      break;
    }
    case "removed":
      downloads.delete(msg.id);
      break;
  }
  updateBadge();
}

function updateBadge() {
  let active = 0;
  for (const d of downloads.values()) {
    if (d.state === "downloading" || d.state === "preparing") active++;
  }
  const text = active > 0 ? String(active) : "";
  try {
    browser.browserAction.setBadgeText({ text });
    browser.browserAction.setBadgeBackgroundColor({ color: "#2b6cb0" });
  } catch (e) {}
}

// ---------------------------------------------------------------------------
// Download interception
// ---------------------------------------------------------------------------

function shouldIntercept(item) {
  if (!item || !item.url) return false;
  return /^https?:\/\//i.test(item.url);
}

async function buildCookieHeader(url) {
  try {
    const cookies = await browser.cookies.getAll({ url });
    return cookies.map((c) => `${c.name}=${c.value}`).join("; ");
  } catch (e) {
    return null;
  }
}

function basename(path) {
  if (!path) return null;
  return path.split(/[\\/]/).pop() || null;
}

async function onDownloadCreated(item) {
  if (!shouldIntercept(item)) return;

  const enabled = (await browser.storage.local.get("enabled"));
  if (enabled && enabled.enabled === false) return; // user turned interception off

  // Take over: cancel the browser's own download and remove its record.
  try { await browser.downloads.cancel(item.id); } catch (e) {}
  try { await browser.downloads.erase({ id: item.id }); } catch (e) {}

  // Replay the browser's real request headers where we captured them; fall back
  // to the cookie jar when no capture matched.
  const cap = splitCapturedHeaders(item);
  const cookie = cap.cookie || (await buildCookieHeader(item.url));

  const request = {
    url: item.url,
    filename: basename(item.filename),
    headers: cap.headers,
    cookie: cookie || null,
    referrer: cap.referrer,
    user_agent: cap.userAgent,
    mime: item.mime || null,
    size_hint: item.fileSize && item.fileSize > 0 ? item.fileSize : null,
    source_page: cap.referrer,
    options: {},
  };

  forwardDownload(request);
}

function forwardDownload(request) {
  if (wsReady && ws) {
    send({ type: "add", request });
  } else {
    // Fallback: relay via the native host (which also launches the daemon).
    browser.runtime
      .sendNativeMessage(NATIVE_HOST, { cmd: "add", request })
      .catch((e) => console.warn("Magpie relay failed:", e));
    // And (re)establish the socket for live updates.
    connect();
  }

  try {
    browser.notifications.create({
      type: "basic",
      iconUrl: browser.runtime.getURL("icons/magpie.svg"),
      title: "Magpie",
      message: "Sent to Magpie: " + (basename(request.filename) || request.url),
    });
  } catch (e) {}
}

// Expose state to the popup.
browser.runtime.onMessage.addListener((msg) => {
  if (msg === "getState") {
    return Promise.resolve({
      connected: wsReady,
      downloads: Array.from(downloads.values()),
    });
  }
  if (msg && msg.cmd && ws && wsReady) {
    send(msg); // popup can forward pause/resume/cancel/set_options
  }
  return false;
});

browser.downloads.onCreated.addListener(onDownloadCreated);
connect();
