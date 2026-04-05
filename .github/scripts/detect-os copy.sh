#!/usr/bin/env bash
# Path: .github/scripts/detect-os.sh
# Mục đích: Detect OS của runner, set biến môi trường phù hợp
# Output  : append vào .env (cho docker compose) + $GITHUB_ENV (cho các step sau)
set -euo pipefail

echo "=== [detect-os] Runner OS: $RUNNER_OS ==="

if [ "$RUNNER_OS" = "Windows" ]; then
  CUR_OS="windows"
  # Docker chạy trong WSL2 → dùng Linux socket
  DOCKER_SOCK="/var/run/docker.sock"
  COMPOSE_PROFILES="windows-only"

  # Convert Windows workspace path sang WSL2 mount path
  # Ví dụ: D:\a\repo\repo → /mnt/d/a/repo/repo
  WIN_PATH="${GITHUB_WORKSPACE//\\//}"          # backslash → slash
  DRIVE="${WIN_PATH:0:1}"                        # lấy ký tự drive (D)
  DRIVE_LOWER="${DRIVE,,}"                       # uppercase → lowercase
  PATH_REST="${WIN_PATH:2}"                      # bỏ "D:"
  WSL_WORKSPACE="/mnt/${DRIVE_LOWER}${PATH_REST}"

  echo "WSL_WORKSPACE=$WSL_WORKSPACE" >> "$GITHUB_ENV"
  echo "  → WSL_WORKSPACE=$WSL_WORKSPACE"
else
  CUR_OS="linux"
  DOCKER_SOCK="/var/run/docker.sock"
  COMPOSE_PROFILES="linux-only"
  WSL_WORKSPACE=""
fi

COMPOSE_PROJECT_NAME="$(basename "$GITHUB_WORKSPACE")"
CUR_WORK_DIR="$GITHUB_WORKSPACE"
CUR_WHOAMI="$(whoami)"

# ── Append vào .env (cho docker compose đọc) ─────────────────────────
{
  echo "CUR_OS=$CUR_OS"
  echo "DOCKER_SOCK=$DOCKER_SOCK"
  echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
  echo "COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME"
  echo "CUR_WORK_DIR=$CUR_WORK_DIR"
  echo "CUR_WHOAMI=$CUR_WHOAMI"
} >> .env

# ── Export sang GITHUB_ENV (cho các step tiếp theo dùng $env:VAR) ─────
{
  echo "CUR_OS=$CUR_OS"
  echo "DOCKER_SOCK=$DOCKER_SOCK"
  echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
  echo "COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME"
} >> "$GITHUB_ENV"

echo "✅ [detect-os] CUR_OS=$CUR_OS | PROFILES=$COMPOSE_PROFILES"