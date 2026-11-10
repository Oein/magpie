#!/usr/bin/env bash
#
# Build universal (arm64 + x86_64) Magpie binaries, assemble Magpie.app, and
# produce dist/Magpie-<version>.dmg. macOS only (uses lipo/iconutil/hdiutil).
#
# Usage: packaging/macos/make-dmg.sh [version]
#
# Requires: Rust toolchain, and `rsvg-convert` (brew install librsvg) for the
# icon. Run from anywhere; paths are resolved relative to the repo root.
#
set -euo pipefail

VER="${1:-0.1.0}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

APP_NAME="Magpie"
BUNDLE_ID="com.magpie.app"
DIST="$ROOT/dist"
APP="$DIST/$APP_NAME.app"

echo "==> Building universal release binaries (v$VER)"
rustup target add aarch64-apple-darwin x86_64-apple-darwin >/dev/null
for target in aarch64-apple-darwin x86_64-apple-darwin; do
  cargo build --release --target "$target" -p magpie-daemon -p magpie-gui
done

rm -rf "$DIST"
mkdir -p "$DIST"
for bin in magpie magpie-gui; do
  lipo -create -output "$DIST/$bin" \
    "target/aarch64-apple-darwin/release/$bin" \
    "target/x86_64-apple-darwin/release/$bin"
done

echo "==> Assembling $APP_NAME.app"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
# Ship both the GUI and the daemon; the GUI launches `magpie` as a sibling.
cp "$DIST/magpie" "$DIST/magpie-gui" "$APP/Contents/MacOS/"
chmod +x "$APP/Contents/MacOS/magpie" "$APP/Contents/MacOS/magpie-gui"

echo "==> Rendering app icon"
ICONSET="$DIST/magpie.iconset"
mkdir -p "$ICONSET"
SVG="$ROOT/crates/magpie-gui/ui/magpie.svg"
gen() { rsvg-convert -w "$1" -h "$1" "$SVG" -o "$ICONSET/$2"; }
gen 16   icon_16x16.png
gen 32   icon_16x16@2x.png
gen 32   icon_32x32.png
gen 64   icon_32x32@2x.png
gen 128  icon_128x128.png
gen 256  icon_128x128@2x.png
gen 256  icon_256x256.png
gen 512  icon_256x256@2x.png
gen 512  icon_512x512.png
gen 1024 icon_512x512@2x.png
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/magpie.icns"

echo "==> Writing Info.plist"
cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>$APP_NAME</string>
  <key>CFBundleDisplayName</key><string>$APP_NAME</string>
  <key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
  <key>CFBundleExecutable</key><string>magpie-gui</string>
  <key>CFBundleIconFile</key><string>magpie</string>
  <key>CFBundleVersion</key><string>$VER</string>
  <key>CFBundleShortVersionString</key><string>$VER</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

echo "==> Building DMG"
STAGE="$DIST/dmg"
rm -rf "$STAGE"
mkdir -p "$STAGE"
cp -R "$APP" "$STAGE/"
ln -s /Applications "$STAGE/Applications"
DMG="$DIST/$APP_NAME-$VER.dmg"
rm -f "$DMG"
hdiutil create -volname "$APP_NAME" -srcfolder "$STAGE" -ov -format UDZO "$DMG"

echo "==> Done: $DMG"
ls -lh "$DMG"
