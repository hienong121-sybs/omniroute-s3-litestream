#!/bin/bash
# preflight.sh — chạy trước docker compose up -d

set -euo pipefail
ERRORS=0

check_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "❌ MISSING FILE: $path"
    ERRORS=$((ERRORS + 1))
  elif [ ! -s "$path" ]; then
    echo "⚠️  EMPTY FILE:   $path"
    ERRORS=$((ERRORS + 1))
  else
    echo "✅ $path"
  fi
}

check_dir() {
  local path="$1"
  if [ ! -d "$path" ]; then
    echo "❌ MISSING DIR:  $path"
    ERRORS=$((ERRORS + 1))
  else
    echo "✅ $path/"
  fi
}

echo "=== Pre-flight check ==="

# --- Files được mount trong docker-compose.yml ---
check_file ".env"
check_file "./cloudflared/config.yml"
check_file "./cloudflared-credentials.json"
check_file "./litestream/litestream.yml"

# --- Dirs được mount ---
check_dir "./data"
check_dir "./services/elector"
check_dir "./litestream"

# --- Docker socket ---
if [ ! -S /var/run/docker.sock ]; then
  echo "❌ MISSING SOCKET: /var/run/docker.sock"
  ERRORS=$((ERRORS + 1))
else
  echo "✅ /var/run/docker.sock"
fi

# --- CUR_WORK_DIR phải là thư mục thật ---
source .env 2>/dev/null || true
if [ -n "${CUR_WORK_DIR:-}" ] && [ ! -d "$CUR_WORK_DIR" ]; then
  echo "❌ CUR_WORK_DIR không tồn tại: $CUR_WORK_DIR"
  ERRORS=$((ERRORS + 1))
fi

echo ""
if [ "$ERRORS" -gt 0 ]; then
  echo "🚫 $ERRORS lỗi — hãy fix trước khi up!"
  exit 1
fi

# --- Kiểm tra container cũ có mount conflict không ---
echo ""
echo "=== Checking existing containers ==="
for cname in litestream omniroute cloudflared elector; do
  if docker inspect "$cname" &>/dev/null; then
    # Kiểm tra xem mount destination có phải directory không
    MOUNTS=$(docker inspect "$cname" --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}')
    echo "⚠️  Container '$cname' đã tồn tại — cần --force-recreate nếu mount thay đổi"
    echo "$MOUNTS" | sed 's/^/   /'
  fi
done