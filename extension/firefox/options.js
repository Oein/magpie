const box = document.getElementById("enabled");

async function load() {
  const stored = await browser.storage.local.get("enabled");
  box.checked = stored.enabled !== false; // default on
}

box.addEventListener("change", () => {
  browser.storage.local.set({ enabled: box.checked });
});

load();
