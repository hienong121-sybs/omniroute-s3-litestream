#!/usr/bin/env bash
# Path: .github/scripts/detect-os.sh
# Mục đích : Detect OS thực của máy chạy script (không phụ thuộc biến của runner)
# Tương thích: GitHub Actions, Azure Pipelines, self-hosted, local
set -euo pipefail

# ── 1. Detect OS bằng uname (không dùng RUNNER_OS hay agent.os) ───────
UNAME_S="$(uname -s)"
UNAME_R="$(uname -r)"

case "$UNAME_S" in
  Linux*)
    # WSL2 kernel report chứa "microsoft" hoặc "WSL"
    if echo "$UNAME_R" | grep -qi "microsoft\|wsl"; then
      CUR_OS="windows"
      COMPOSE_PROFILES="windows-only"
    else
      CUR_OS="linux"
      COMPOSE_PROFILES="linux-only"
    fi
    DOCKER_SOCK="/var/run/docker.sock"
    ;;
  Darwin*)
    CUR_OS="macos"
    COMPOSE_PROFILES="linux-only"
    DOCKER_SOCK="/var/run/docker.sock"
    ;;
  MINGW* | MSYS* | CYGWIN*)
    # Git Bash / MSYS2 trên Windows (không qua WSL)
    CUR_OS="windows"
    COMPOSE_PROFILES="windows-only"
    DOCKER_SOCK="/var/run/docker.sock"
    ;;
  *)
    CUR_OS="linux"
    COMPOSE_PROFILES="linux-only"
    DOCKER_SOCK="/var/run/docker.sock"
    ;;
esac

echo "=== [detect-os] uname -s: $UNAME_S | uname -r: $UNAME_R ==="
echo "  → CUR_OS=$CUR_OS | COMPOSE_PROFILES=$COMPOSE_PROFILES"

# ── 2. Resolve workspace (fallback chain, không hardcode runner) ───────
# GitHub Actions  : GITHUB_WORKSPACE
# Azure Pipelines : BUILD_SOURCESDIRECTORY
# Self-hosted/local: thư mục hiện tại
CUR_WORK_DIR="${GITHUB_WORKSPACE:-${BUILD_SOURCESDIRECTORY:-$(pwd)}}"
COMPOSE_PROJECT_NAME="$(basename "$CUR_WORK_DIR")"
CUR_WHOAMI="$(whoami)"

echo "  → CUR_WORK_DIR=$CUR_WORK_DIR"

# ── 3. WSL path conversion (chỉ áp dụng khi chạy trong WSL2) ─────────
WSL_WORKSPACE=""
if [ "$CUR_OS" = "windows" ]; then
  if command -v wslpath &>/dev/null; then
    # wslpath có sẵn trong WSL2, chính xác nhất
    WSL_WORKSPACE="$(wslpath -u "$CUR_WORK_DIR" 2>/dev/null || echo "$CUR_WORK_DIR")"
  else
    # Fallback: manual convert  D:\a\repo → /mnt/d/a/repo
    WIN_PATH="${CUR_WORK_DIR//\\//}"
    DRIVE="${WIN_PATH:0:1}"
    DRIVE_LOWER="${DRIVE,,}"
    PATH_REST="${WIN_PATH:2}"
    WSL_WORKSPACE="/mnt/${DRIVE_LOWER}${PATH_REST}"
  fi
  echo "  → WSL_WORKSPACE=$WSL_WORKSPACE"
fi

# ── 4. Ghi vào .env (docker compose đọc) ─────────────────────────────
{
  echo "CUR_OS=$CUR_OS"
  echo "DOCKER_SOCK=$DOCKER_SOCK"
  echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
  echo "COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME"
  echo "CUR_WORK_DIR=$CUR_WORK_DIR"
  echo "CUR_WHOAMI=$CUR_WHOAMI"
  [ -n "$WSL_WORKSPACE" ] && echo "WSL_WORKSPACE=$WSL_WORKSPACE"
} >> .env

# ── 5. Export sang CI env (tự detect CI platform) ────────────────────
# Hỗ trợ đồng thời GitHub Actions, Azure Pipelines, và local/self-hosted.
set_ci_var() {
  local name="$1" value="$2"

  # GitHub Actions: GITHUB_ENV luôn được set
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "${name}=${value}" >> "$GITHUB_ENV"
  fi

  # Azure Pipelines: TF_BUILD=True khi chạy trong pipeline
  if [ -n "${TF_BUILD:-}" ]; then
    echo "##vso[task.setvariable variable=${name}]${value}"
  fi

  # Self-hosted / local: export vào shell hiện tại
  # (có tác dụng nếu script được `source`, hoặc dùng trong cùng process)
  export "${name}=${value}"
}

set_ci_var "CUR_OS"               "$CUR_OS"
set_ci_var "DOCKER_SOCK"          "$DOCKER_SOCK"
set_ci_var "COMPOSE_PROFILES"     "$COMPOSE_PROFILES"
set_ci_var "COMPOSE_PROJECT_NAME" "$COMPOSE_PROJECT_NAME"
[ -n "$WSL_WORKSPACE" ] && set_ci_var "WSL_WORKSPACE" "$WSL_WORKSPACE"

echo "✅ [detect-os] CUR_OS=$CUR_OS | PROFILES=$COMPOSE_PROFILES"