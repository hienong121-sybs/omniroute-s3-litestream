#!/usr/bin/env bash
# Path: .github/scripts/collect-artifacts.sh
# Mục đích: Thu thập docker runtime artifacts sau mỗi lần deploy
# Dùng chung: Linux runner + WSL2 (Windows runner)
# Gọi từ: thư mục workspace (GITHUB_WORKSPACE hoặc WSL_WORKSPACE)
set -uo pipefail   # không dùng -e để tránh fail khi lệnh docker lỗi nhỏ

OUT="artifacts/docker-runtime"
mkdir -p "$OUT/logs"

echo "=== [collect-artifacts] Collecting to $OUT ==="

# Detect có cần sudo không (WSL2 user thường cần sudo)
DOCKER="docker"
if ! docker info &>/dev/null 2>&1 && sudo docker info &>/dev/null 2>&1; then
  DOCKER="sudo docker"
fi
echo "Using: $DOCKER"

# ── Docker Compose state ──────────────────────────────────────────────
$DOCKER compose ps -a             > "$OUT/compose-ps.txt"       2>&1 || true
$DOCKER compose images            > "$OUT/compose-images.txt"   2>&1 || true
$DOCKER compose logs --no-color   > "$OUT/compose-logs.txt"     2>&1 || true

# ── Docker host state ─────────────────────────────────────────────────
$DOCKER ps -a                     > "$OUT/docker-ps.txt"        2>&1 || true
$DOCKER images                    > "$OUT/docker-images.txt"    2>&1 || true
$DOCKER system df                 > "$OUT/docker-system-df.txt" 2>&1 || true

# ── Per-container inspect + logs ─────────────────────────────────────
CONTAINERS=$($DOCKER compose ps -q 2>/dev/null || true)
if [ -n "$CONTAINERS" ]; then
  for c in $CONTAINERS; do
    $DOCKER inspect "$c" > "$OUT/inspect-${c}.json" 2>&1 || true
    $DOCKER logs    "$c" > "$OUT/logs/${c}.log"      2>&1 || true
  done
fi

# ── App logs ──────────────────────────────────────────────────────────
[ -d logs ] && cp -r logs "$OUT/app-logs" || true

# ── ttyd log (chỉ có trên Windows/WSL2) ──────────────────────────────
[ -f /tmp/ttyd.log ]   && cp /tmp/ttyd.log   "$OUT/ttyd.log"   || true
[ -f /tmp/dockerd.log ] && cp /tmp/dockerd.log "$OUT/dockerd.log" || true

echo "✅ [collect-artifacts] Done → $OUT"
ls -lh "$OUT/"