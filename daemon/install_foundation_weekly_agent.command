#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_NAME="com.quant.foundation_weekly.plist"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS_DIR/$PLIST_NAME"
LOG_DIR="$ROOT_DIR/data/logs"

mkdir -p "$LAUNCH_AGENTS_DIR" "$LOG_DIR"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.quant.foundation_weekly</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>cd "$ROOT_DIR" &amp;&amp; source ~/.zshrc 2&gt;/dev/null || true; python3 daemon/foundation_weekly_worker.py --once --scope AH</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key><integer>0</integer>
    <key>Hour</key><integer>8</integer>
    <key>Minute</key><integer>30</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/foundation_weekly.out.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/foundation_weekly.err.log</string>
</dict>
</plist>
PLIST

chmod 600 "$PLIST_PATH"
launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load "$PLIST_PATH"

echo "Installed launch agent: $PLIST_PATH"
echo "stdout log: $LOG_DIR/foundation_weekly.out.log"
echo "stderr log: $LOG_DIR/foundation_weekly.err.log"
