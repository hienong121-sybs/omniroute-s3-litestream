#!/usr/bin/env bash
# Path: .github/scripts/setup-linux.sh
# Mục đích: Setup môi trường cho Linux runner
#   - Tạo SSH keypair cho webssh container
#   - Thêm public key vào authorized_keys của runner
#   - Đảm bảo sshd đang chạy
set -euo pipefail

echo "=== [setup-linux] Generating SSH keypair ==="
mkdir -p services/webssh/.ssh

ssh-keygen -t ed25519 -f ./services/webssh/.ssh/id_rsa -N "" -q
echo "✅ SSH keypair generated"

echo "=== [setup-linux] Adding public key to authorized_keys ==="
mkdir -p ~/.ssh && chmod 700 ~/.ssh
cat ./services/webssh/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
echo "✅ Public key added"

echo "=== [setup-linux] Restarting sshd ==="
sudo systemctl restart ssh
sleep 1
sudo systemctl is-active ssh \
  && echo "✅ sshd is running" \
  || { echo "❌ sshd failed to start"; exit 1; }