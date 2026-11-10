# Magpie

A fast, low-memory, cross-platform download manager (macOS + Linux + Windows),
with a browser extension that forwards downloads — **cookies, referrer and
headers included** — to a background engine, plus **user-scriptable, async
download paths**.

> Internal crate names are still `magpie-*` (the repo's original codename); the
> user-facing product is **Magpie**.

## Why

- **Low memory.** The engine streams every connection in fixed ~64 KiB buffers
  straight to disk via positioned writes, so peak RAM is independent of file
  size. An idle daemon sits around ~18 MiB (debug) and less in release.
- **Fast (aria2-style).** One file is split into N byte-range segments pulled
  over parallel connections, then reassembled in place.
- **Resumable.** Segment progress is persisted to a `<file>.magpiepart` sidecar,
  so downloads survive pause and daemon restarts when the server supports HTTP
  range requests.
- **Priorities + throttling.** Per-download priority drives connection count and
  a weighted share of bandwidth; a global or per-download speed cap is enforced
  by a token bucket (no extra buffering).
- **Dynamic paths.** A user JavaScript function (`resolvePath(info)`, may be
  `async`) decides where each file lands, sandboxed in QuickJS.
- **Auto-launch.** If the daemon is off when a download starts, the browser's
  native-messaging host boots it, then the extension connects over WebSocket.

## Architecture

```
┌─────────────────┐   native messaging (bootstrap/auto-launch)   ┌──────────────┐
│ Firefox / Chrome│ ───────────────────────────────────────────▶ │ magpie native- │
│   extension     │                                               │     host     │
│                 │   ws://127.0.0.1:7675/?token=…  (downloads,   └──────┬───────┘
│                 │ ◀────────────────── live state ─────────────┐        │ launches
└─────────────────┘                                             │        ▼
                                                          ┌─────────────────────┐
  GUI (Slint, planned) ──── same WebSocket protocol ─────▶│   magpie daemon        │
  CLI (magpie add/list) ─────────────────────────────────▶ │   • engine (core)    │
                                                          │   • QuickJS resolver │
                                                          └─────────────────────┘
```

### Crates

| crate          | role |
|----------------|------|
| `magpie-proto`   | Shared JSON wire types (extension / GUI / daemon). |
| `magpie-core`    | Download engine: queue, parallel range segments, throttling, priorities, resume. |
| `magpie-script`  | QuickJS-backed async path resolver (sandboxed). |
| `magpie-daemon`  | `magpie` binary: WebSocket server, CLI, native-messaging host. |
| `magpie-gui`     | `magpie-gui` binary: Slint native GUI (dark theme, live download list). |

## Build

```bash
cargo build --release          # produces target/release/magpie
```

## Try it (CLI)

```bash
magpie run &                          # start the daemon (or let auto-launch do it)
magpie add https://example.com/big.iso
magpie add https://example.com/x.iso --no-wait   # queue and return immediately
magpie list                           # ids are shown abbreviated; commands accept a prefix
magpie pause   <id>
magpie resume  <id>                   # resumes from the .magpiepart sidecar
magpie cancel  <id>                   # stop + delete partial file
magpie remove  <id>                   # drop a finished/failed entry
magpie priority <id> <low|normal|high>
magpie limit   <id> <KB/s|off>        # per-download speed cap
magpie limit-global <KB/s|off>        # global aggregate cap
magpie paths                          # show config + script locations
```

Config lives at (created on first run):

- Linux: `~/.config/magpie/`
- macOS: `~/Library/Application Support/Magpie/`

`path-resolver.js` in that directory controls the destination:

```js
async function resolvePath(info) {
  // info = { url, filename, mime, size, source_page, host, ext }
  if (info.ext === "pdf") return "~/Downloads/papers/" + info.filename;
  if (info.host && info.host.endsWith("github.com")) return "~/code/" + info.filename;
  return info.filename; // relative -> under the default download dir
}
```

## Browser extension

Both builds capture the download's real request headers (`webRequest`),
cookies, referrer and User-Agent and forward them to the daemon, so
authenticated / token-gated downloads are reproduced faithfully.

### Firefox (`extension/firefox`, MV2)

```bash
packaging/install-native-host.sh target/release/magpie   # register native host
```

Then: `about:debugging` → **This Firefox** → **Load Temporary Add-on** → pick
`extension/firefox/manifest.json`.

### Chrome / Chromium (`extension/chrome`, MV3)

1. `chrome://extensions` → enable **Developer mode** → **Load unpacked** →
   select `extension/chrome`.
2. Copy the extension's generated **ID**, then register the native host for it:

   ```bash
   packaging/install-native-host.sh target/release/magpie --chrome-id <THE_ID>
   ```

(The MV3 background is a service worker; a `chrome.alarms` heartbeat keeps the
daemon connection alive and reconnects after the worker sleeps.)

## Auto-start the daemon (idle-resident, minimal RAM)

- Linux: `packaging/systemd/magpie.service`
- macOS: `packaging/launchd/com.magpie.daemon.plist`

## GUI

```bash
cargo build --release          # also builds target/release/magpie-gui
./target/release/magpie-gui    # needs a desktop display (OpenGL)
```

The GUI auto-launches the daemon, shows a live list with progress/speed, and
exposes pause/resume/cancel, per-download priority, and a global speed limit.

## Status

Working end-to-end: multi-connection engine, async path scripts, WebSocket
daemon, full CLI (add/pause/resume/cancel/remove/priority/limit), verified
incremental resume, native-messaging auto-launch, Firefox (MV2) **and** Chrome
(MV3) extensions with full header capture, and the Slint GUI (dark theme, with
the Magpie logo).

### TODO

- [ ] Checksum verification, scheduler, category rules.
- [ ] Signed/packaged extension + app bundles for distribution.
