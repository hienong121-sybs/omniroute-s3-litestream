# Path: .github/scripts/setup-windows.ps1
$ErrorActionPreference = "Stop"

# ── Helper: ghi script ra file tạm trong WSL2 rồi chạy bằng bash ─────
# Tránh hoàn toàn vấn đề escape / here-string khi truyền qua -c
function Invoke-WSLScript {
  param(
    [string]$ScriptContent,
    [string]$Label = "wsl-script"
  )
  # Ghi script vào file tạm trên Windows (WSL mount được qua /mnt/...)
  $tmpFile = [System.IO.Path]::GetTempFileName() + ".sh"
  # Dùng LF line endings, không phải CRLF
  $ScriptContent = $ScriptContent -replace "`r`n", "`n"
  [System.IO.File]::WriteAllText($tmpFile, $ScriptContent, [System.Text.UTF8Encoding]::new($false))

  # Convert path sang WSL path
  $wslTmp = $tmpFile -replace "\\", "/" -replace "^([A-Za-z]):", { "/mnt/$($_.Value[0].ToString().ToLower())" }

  Write-Host "--- [$Label] running $wslTmp ---"
  wsl -d Ubuntu -- bash "$wslTmp"
  $exit = $LASTEXITCODE

  Remove-Item $tmpFile -ErrorAction SilentlyContinue

  if ($exit -ne 0) {
    throw "[$Label] failed (exit $exit)"
  }
}

# ── Lấy WSL_WORKSPACE ────────────────────────────────────────────────
$wslWorkspace = $env:WSL_WORKSPACE
if (-not $wslWorkspace) { throw "WSL_WORKSPACE is not set" }
Write-Host "WSL_WORKSPACE: $wslWorkspace"

# ════════════════════════════════════════════════════════════════════
#  PHẦN 1 — Đảm bảo Ubuntu có sẵn trong WSL2
# ════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=== [1/3] Checking WSL2 Ubuntu ==="

$ubuntuReady = $false
try {
  $probe = wsl -d Ubuntu -- bash -c "echo WSL2_PROBE" 2>&1
  if ("$probe" -match "WSL2_PROBE") { $ubuntuReady = $true }
}
catch { }

if (-not $ubuntuReady) {
  Write-Host "Ubuntu not found — installing..."
  wsl --install -d Ubuntu --no-launch
  if ($LASTEXITCODE -ne 0) { throw "wsl --install failed" }

  Write-Host "Waiting for Ubuntu (up to 90s)..."
  for ($i = 0; $i -lt 18; $i++) {
    Start-Sleep -Seconds 5
    $probe = wsl -d Ubuntu -- bash -c "echo WSL2_PROBE" 2>$null
    if ("$probe" -match "WSL2_PROBE") {
      Write-Host "✅ Ubuntu ready after $(($i+1)*5)s"
      $ubuntuReady = $true; break
    }
    Write-Host "  ... waiting ($( ($i+1)*5 )s)"
  }
  if (-not $ubuntuReady) { throw "Ubuntu did not become ready in 90s" }
}
else {
  Write-Host "✅ Ubuntu already responsive"
}

# ════════════════════════════════════════════════════════════════════
#  PHẦN 2 — Cài Docker Engine bên trong WSL2
# ════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=== [2/3] Installing Docker Engine in WSL2 ==="

Invoke-WSLScript -Label "docker-setup" -ScriptContent @"
#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

if command -v docker &>/dev/null; then
  echo "Docker already installed: \$(docker --version)"
else
  echo "Installing Docker Engine..."
  sudo apt-get update -qq
  curl -fsSL https://get.docker.com | sudo sh
  echo "Docker installed: \$(docker --version)"
fi

# Start dockerd nếu chưa chạy
if sudo docker info &>/dev/null 2>&1; then
  echo "dockerd already running"
else
  echo "Starting dockerd..."
  sudo dockerd > /tmp/dockerd.log 2>&1 &
  for i in \$(seq 1 30); do
    sudo docker info &>/dev/null 2>&1 && break || true
    sleep 1
  done
  if sudo docker info &>/dev/null 2>&1; then
    echo "dockerd is running"
  else
    echo "dockerd failed to start"
    cat /tmp/dockerd.log
    exit 1
  fi
fi

sudo docker info | grep -E "OSType|Server Version"
echo "Docker Engine ready"
"@

Write-Host "✅ Docker Linux engine ready in WSL2"

# ════════════════════════════════════════════════════════════════════
#  PHẦN 3 — Cài + chạy ttyd trong WSL2
# ════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=== [3/3] Installing and starting ttyd in WSL2 ==="

Invoke-WSLScript -Label "ttyd-setup" -ScriptContent @"
#!/usr/bin/env bash
set -euo pipefail

if command -v ttyd &>/dev/null; then
  echo "ttyd already installed"
else
  echo "Installing ttyd..."
  if sudo apt-get install -y ttyd 2>/dev/null; then
    echo "ttyd installed via apt"
  else
    TTYD_VER="1.7.7"
    echo "Downloading ttyd binary v\${TTYD_VER}..."
    sudo curl -fsSL \
      "https://github.com/tsl0922/ttyd/releases/download/\${TTYD_VER}/ttyd.x86_64" \
      -o /usr/local/bin/ttyd
    sudo chmod +x /usr/local/bin/ttyd
    echo "ttyd binary installed"
  fi
fi

# Stop instance cũ
pkill -x ttyd 2>/dev/null && echo "Stopped existing ttyd" || true
sleep 1

echo "Starting ttyd on 0.0.0.0:7681..."
nohup ttyd \
  -W \
  -p 7681 \
  -t fontSize=15 \
  bash \
  > /tmp/ttyd.log 2>&1 &

sleep 2

if pgrep -x ttyd > /dev/null; then
  echo "ttyd running (PID=`$(pgrep -x ttyd))"
else
  echo "ttyd failed to start"
  cat /tmp/ttyd.log
  exit 1
fi

ss -tlnp | grep 7681 && echo "Port 7681 listening" || echo "Port 7681 not detected yet"
"@

# ── Verify từ Windows host ────────────────────────────────────────────
Start-Sleep -Seconds 3
$portCheck = netstat -ano 2>$null | Select-String ":7681"
if ($portCheck) {
  Write-Host "✅ Port 7681 visible from Windows host"
  Write-Host $portCheck
}
else {
  Write-Host "⚠️  Port 7681 not yet visible from Windows host (WSL2 auto-forward may take a moment)"
}

Write-Host ""
Write-Host "✅ [setup-windows] All done"