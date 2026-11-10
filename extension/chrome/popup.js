function human(n) {
  const u = ["B", "KB", "MB", "GB", "TB"];
  let v = n || 0, i = 0;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(1) + u[i];
}

function render(state) {
  const status = document.getElementById("status");
  status.textContent = state.connected ? "connected" : "offline";
  status.className = "status " + (state.connected ? "ok" : "down");

  const list = document.getElementById("list");
  list.innerHTML = "";
  if (!state.downloads.length) {
    list.innerHTML = '<div class="empty">No downloads yet.</div>';
    return;
  }
  state.downloads.sort((a, b) => b.created_at - a.created_at);
  for (const d of state.downloads) {
    const pct = d.total_bytes ? (d.downloaded_bytes / d.total_bytes) * 100 : 0;
    const el = document.createElement("div");
    el.className = "item";
    el.innerHTML = `
      <div class="name" title="${d.filename}">${d.filename}</div>
      <div class="bar"><div class="fill" style="width:${pct.toFixed(1)}%"></div></div>
      <div class="meta">
        <span>${d.state} · ${human(d.downloaded_bytes)}${d.total_bytes ? " / " + human(d.total_bytes) : ""}</span>
        <span>${d.state === "downloading" ? human(d.speed_bps) + "/s" : ""}</span>
      </div>`;
    list.appendChild(el);
  }
}

async function refresh() {
  try {
    const state = await chrome.runtime.sendMessage("getState");
    if (state) render(state);
  } catch (e) {}
}

refresh();
setInterval(refresh, 700);
