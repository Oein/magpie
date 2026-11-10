/*
 * Magpie background service worker (Chrome MV3).
 *
 * Same responsibilities as the Firefox build, adapted to MV3:
 *   - the background is an ephemeral service worker, so we persist the daemon
 *     connection info in chrome.storage and reconnect on each wake;
 *   - a chrome.alarms heartbeat keeps the worker alive / reconnected;
 *   - header capture, download interception and native-messaging bootstrap are
 *     otherwise identical.
 */

const NATIVE_HOST = "com.magpie.host";
const RECONNECT_MS = 2000;

let conn = null; // { port, token }
let ws = null;
let wsReady = false;
const pending = [];
const downloads = new Map();

// --- header capture (observational webRequest is still allowed in MV3) -------

const headerCache = new Map();
const HEADER_TTL_MS = 60000;
const HEADER_CACHE_MAX = 300;

const SKIP_HEADERS = new Set([
  "host", "connection", "content-length", "keep-alive", "transfer-encoding",
  "te", "upgrade", "proxy-connection", "accept-encoding", "range",
  "cookie", "referer", "user-agent",
]);

function cacheHeaders(details) {
  if (!details.requestHeaders) return;
  const now = Date.now();
  if (headerCache.size >= HEADER_CACHE_MAX) {
    for (const [k, v] of headerCache) if (now - v.ts > HEADER_TTL_MS) headerCache.delete(k);
    while (headerCache.size >= HEADER_CACHE_MAX) {
      headerCache.delete(headerCache.keys().next().value);
    }
  }
  headerCache.set(details.url, { headers: details.requestHeaders, ts: now });
}

chrome.webRequest.onSendHeaders.addListener(
  cacheHeaders,
  { urls: ["<all_urls>"], types: ["main_frame", "sub_frame", "xmlhttprequest", "other", "media"] },
  ["requestHeaders"]
);

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

// --- daemon connection -------------------------------------------------------

function sendNative(msg) {
  // Works whether or not this Chrome build returns a promise.
  return new Promise((resolve, reject) => {
    try {
      chrome.runtime.sendNativeMessage(NATIVE_HOST, msg, (resp) => {
        if (chrome.runtime.lastError) reject(new Error(chrome.runtime.lastError.message));
        else resolve(resp);
      });
    } catch (e) {
      reject(e);
    }
  });
}

async function bootstrap() {
  const resp = await sendNative({ cmd: "bootstrap" });
  if (!resp || !resp.ok) throw new Error("bootstrap failed: " + (resp && resp.error));
  conn = { port: resp.port, token: resp.token };
  await chrome.storage.local.set({ conn });
  return conn;
}

async function ensureConn() {
  if (!conn) {
    const stored = await chrome.storage.local.get("conn");
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
  if (wsReady) return;
  try {
    await ensureConn();
    await openWs();
  } catch (e) {
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
  while (pending.length && wsReady) ws.send(JSON.stringify(pending.shift()));
}

function send(msg) {
  if (wsReady && ws) ws.send(JSON.stringify(msg));
  else { pending.push(msg); connect(); }
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
      if (d) { d.downloaded_bytes = msg.downloaded_bytes; d.total_bytes = msg.total_bytes; d.speed_bps = msg.speed_bps; }
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
  for (const d of downloads.values()) if (d.state === "downloading" || d.state === "preparing") active++;
  const text = active > 0 ? String(active) : "";
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color: "#2b6cb0" });
}

// --- download interception ---------------------------------------------------

function shouldIntercept(item) {
  return item && item.url && /^https?:\/\//i.test(item.url);
}

async function buildCookieHeader(url) {
  try {
    const cookies = await chrome.cookies.getAll({ url });
    return cookies.map((c) => `${c.name}=${c.value}`).join("; ");
  } catch (e) {
    return null;
  }
}

function basename(path) {
  return path ? path.split(/[\\/]/).pop() || null : null;
}

async function onDownloadCreated(item) {
  if (!shouldIntercept(item)) return;
  const enabled = await chrome.storage.local.get("enabled");
  if (enabled && enabled.enabled === false) return;

  try { await chrome.downloads.cancel(item.id); } catch (e) {}
  try { await chrome.downloads.erase({ id: item.id }); } catch (e) {}

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
    sendNative({ cmd: "add", request }).catch((e) => console.warn("Magpie relay failed:", e));
    connect();
  }
  chrome.notifications.create({
    type: "basic",
    iconUrl: chrome.runtime.getURL("icons/magpie-128.png"),
    title: "Magpie",
    message: "Sent to Magpie: " + (basename(request.filename) || request.url),
  });
}

// --- popup bridge + lifecycle ------------------------------------------------

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg === "getState") {
    connect();
    sendResponse({ connected: wsReady, downloads: Array.from(downloads.values()) });
    return true;
  }
  if (msg && msg.cmd) {
    send(msg);
    sendResponse({ ok: true });
    return true;
  }
  return false;
});

chrome.downloads.onCreated.addListener(onDownloadCreated);
chrome.runtime.onStartup.addListener(connect);
chrome.runtime.onInstalled.addListener(connect);

// Heartbeat: keep the worker alive / auto-reconnect.
chrome.alarms.create("magpie-keepalive", { periodInMinutes: 0.4 });
chrome.alarms.onAlarm.addListener((a) => {
  if (a.name === "magpie-keepalive") connect();
});

connect();
