const box = document.getElementById("enabled");

async function load() {
  const stored = await chrome.storage.local.get("enabled");
  box.checked = stored.enabled !== false; // default on
}

box.addEventListener("change", () => {
  chrome.storage.local.set({ enabled: box.checked });
});

load();
