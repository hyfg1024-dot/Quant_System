#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
DESKTOP_DIR="$HOME/Desktop"
LAUNCHER_PATH="$DESKTOP_DIR/启动Quant_System.command"

if [ ! -f "$ROOT_DIR/requirements.txt" ]; then
  echo "错误：未找到 $ROOT_DIR/requirements.txt"
  echo "请确认你是在完整的 Quant_System 项目根目录中执行本脚本。"
  exit 1
fi

if [ ! -f "$ROOT_DIR/apps/trading/app.py" ]; then
  echo "错误：未找到 $ROOT_DIR/apps/trading/app.py"
  echo "请确认 GitHub 项目下载完整。"
  exit 1
fi

mkdir -p "$DESKTOP_DIR"

cat > "$LAUNCHER_PATH" <<EOF
#!/bin/zsh
set -euo pipefail

ROOT_DIR="$ROOT_DIR"
cd "\$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo "[首次启动] 创建虚拟环境..."
  python3 -m venv .venv
fi

source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

echo "正在启动 Quant_System..."
exec python3 -m streamlit run apps/trading/app.py --server.headless false
EOF

chmod +x "$LAUNCHER_PATH"
xattr -d com.apple.quarantine "$LAUNCHER_PATH" 2>/dev/null || true

echo "已生成桌面一键启动按钮：$LAUNCHER_PATH"
echo "请双击桌面的“启动Quant_System.command”启动程序。"
