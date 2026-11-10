#!/usr/bin/env bash
#
# Registers the Magpie native-messaging host so the browser can bootstrap /
# auto-launch the daemon. Run once after building:
#
#   packaging/install-native-host.sh [path-to-magpie-binary] [--chrome-id <ID>]
#
# Firefox is registered unconditionally. Chrome/Chromium needs the unpacked
# extension's ID (shown on chrome://extensions with Developer mode on); pass it
# with --chrome-id, or set CHROME_EXT_ID, to also register there.
#
set -euo pipefail

HOST_NAME="com.magpie.host"
FF_EXT_ID="magpie@better-fdm"
CHROME_ID="${CHROME_EXT_ID:-}"

# --- parse args ---
MAGPIE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --chrome-id) CHROME_ID="${2:-}"; shift 2;;
    *) MAGPIE="$1"; shift;;
  esac
done

# 1. Locate the magpie binary.
if [[ -z "$MAGPIE" ]]; then
  for cand in \
    "$(dirname "$0")/../target/release/magpie" \
    "$(dirname "$0")/../target/debug/magpie" \
    "$(command -v magpie || true)"; do
    if [[ -n "$cand" && -x "$cand" ]]; then MAGPIE="$cand"; break; fi
  done
fi
if [[ -z "$MAGPIE" || ! -x "$MAGPIE" ]]; then
  echo "error: could not find the magpie binary. Pass it as an argument." >&2
  exit 1
fi
MAGPIE="$(cd "$(dirname "$MAGPIE")" && pwd)/$(basename "$MAGPIE")"

# 2. OS-specific locations.
case "$(uname -s)" in
  Darwin)
    CFG_DIR="$HOME/Library/Application Support/Magpie"
    FF_HOST_DIR="$HOME/Library/Application Support/Mozilla/NativeMessagingHosts"
    CHROME_HOST_DIRS=(
      "$HOME/Library/Application Support/Google/Chrome/NativeMessagingHosts"
      "$HOME/Library/Application Support/Chromium/NativeMessagingHosts"
    )
    ;;
  Linux)
    CFG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/magpie"
    FF_HOST_DIR="$HOME/.mozilla/native-messaging-hosts"
    CHROME_HOST_DIRS=(
      "${XDG_CONFIG_HOME:-$HOME/.config}/google-chrome/NativeMessagingHosts"
      "${XDG_CONFIG_HOME:-$HOME/.config}/chromium/NativeMessagingHosts"
    )
    ;;
  *)
    echo "unsupported OS" >&2; exit 1;;
esac

mkdir -p "$CFG_DIR" "$FF_HOST_DIR"

# 3. Wrapper so the browser launches `magpie native-host` (the browser passes
#    its own args, so we cannot point the manifest straight at the binary).
WRAPPER="$CFG_DIR/magpie-native-host.sh"
cat > "$WRAPPER" <<EOF
#!/usr/bin/env bash
exec "$MAGPIE" native-host
EOF
chmod +x "$WRAPPER"

# 4. Firefox manifest (keyed by extension id via allowed_extensions).
cat > "$FF_HOST_DIR/$HOST_NAME.json" <<EOF
{
  "name": "$HOST_NAME",
  "description": "Magpie download manager bootstrap",
  "path": "$WRAPPER",
  "type": "stdio",
  "allowed_extensions": ["$FF_EXT_ID"]
}
EOF

echo "Installed native host:"
echo "  magpie   : $MAGPIE"
echo "  wrapper  : $WRAPPER"
echo "  firefox  : $FF_HOST_DIR/$HOST_NAME.json"

# 5. Chrome/Chromium manifest (keyed by origin via allowed_origins).
if [[ -n "$CHROME_ID" ]]; then
  for d in "${CHROME_HOST_DIRS[@]}"; do
    mkdir -p "$d"
    cat > "$d/$HOST_NAME.json" <<EOF
{
  "name": "$HOST_NAME",
  "description": "Magpie download manager bootstrap",
  "path": "$WRAPPER",
  "type": "stdio",
  "allowed_origins": ["chrome-extension://$CHROME_ID/"]
}
EOF
    echo "  chrome   : $d/$HOST_NAME.json"
  done
else
  echo
  echo "Chrome not registered. Load extension/chrome in chrome://extensions"
  echo "(Developer mode -> Load unpacked), copy its ID, then re-run:"
  echo "  $0 \"$MAGPIE\" --chrome-id <THE_ID>"
fi

echo
echo "Firefox: about:debugging -> This Firefox -> Load Temporary Add-on ->"
echo "extension/firefox/manifest.json (or install a signed build)."
