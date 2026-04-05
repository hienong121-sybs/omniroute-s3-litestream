#!/usr/bin/env bash
# Path: .github/scripts/pull-env.sh
# Mục đích: Pull .env và credentials từ Firebase RTDB
# Dùng chung: Linux runner + WSL2 (Windows runner)
set -euo pipefail

echo "=== [pull-env] Installing dotenvrtdb CLI ==="
npm install -g @tltdh61/dotenvrtdb

echo "=== [pull-env] Pulling .env & credentials ==="
mkdir -p services/webssh/.ssh

dotenvrtdb -e .env --pull -eUrl="$RTDB_URL" \
  --writefilebase64=./cloudflared-credentials.json \
  --var=CLOUDFLARED_TUNNEL_CREDENTIALS_BASE64

echo "✅ [pull-env] Done"