#!/usr/bin/env node
// ══════════════════════════════════════════════════════════════════════
// services/elector/elector.js
//
// Leader Election Daemon — Firebase RTDB SSE (EventSource)
//
// Logic:
//   • Mỗi instance ghi {timestamp} lên RTDB khi start + heartbeat liên tục
//   • Lắng nghe SSE realtime — khi instances thay đổi, so sánh timestamp
//   • Instance có timestamp MỚI NHẤT = LEADER
//   • Instance cũ hơn = FOLLOWER: chỉ stop services, vẫn sống & heartbeat
//   • Nếu leader biến mất (zombie) → follower mới nhất còn lại lên leader
//   • Zombie cleanup: xóa entry không heartbeat > ZOMBIE_TTL_MS
// ══════════════════════════════════════════════════════════════════════
"use strict";

const https = require("https");
const http = require("http");
const { spawnSync } = require("child_process");
const crypto = require("crypto");

// ──────────────────────────────────────────────────────────────────────
// 1. Config
// ──────────────────────────────────────────────────────────────────────
const RTDB_URL_RAW = process.env.RTDB_URL;
if (!RTDB_URL_RAW) {
  console.error("[elector] RTDB_URL is required");
  process.exit(1);
}

const urlObj = new URL(RTDB_URL_RAW);
const RTDB_BASE = `${urlObj.protocol}//${urlObj.host}`;
const RTDB_QUERY = urlObj.search; // "" hoặc "?auth=TOKEN"

const COMPOSE_PROJECT = (process.env.COMPOSE_PROJECT_NAME || "omniroute-s3-litestream")
  .toLowerCase()
  .replace(/[^a-z0-9-]/g, "-")
  .replace(/^-+|-+$/g, "");

const INSTANCE_ID = (() => {
  const fromEnv = (process.env.INSTANCE_ID || "").trim();
  // Tránh placeholder literal từ CI
  if (fromEnv && fromEnv !== "INSTANCE_ID") return fromEnv;
  return crypto.randomBytes(8).toString("hex");
})();

const LOCK_NODE = `leader-lock-${COMPOSE_PROJECT}/instances`;
const HEARTBEAT_MS = parseInt(process.env.HEARTBEAT_INTERVAL || "10", 10) * 1000;
const ZOMBIE_TTL_MS = parseInt(process.env.LEADER_LOCK_TTL || "30", 10) * 3 * 1000;

const LEADER_SVCS = ["litestream", "omniroute", "cloudflared"];
const FOLLOWER_STOP = ["cloudflared", "omniroute", "litestream"];

// ──────────────────────────────────────────────────────────────────────
// 2. Logging
// ──────────────────────────────────────────────────────────────────────
const ts = () => new Date().toTimeString().slice(0, 8);
const log = (...a) => console.log(`[elector ${ts()}]`, ...a);
const warn = (...a) => console.error(`[elector ${ts()}] ⚠`, ...a);

// ──────────────────────────────────────────────────────────────────────
// 3. RTDB REST helpers
// ──────────────────────────────────────────────────────────────────────
function buildUrl(path) {
  return `${RTDB_BASE}/${path}.json${RTDB_QUERY}`;
}

function rtdbRequest(method, path, body = null, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const url = new URL(buildUrl(path));
    const lib = url.protocol === "https:" ? https : http;
    const opts = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path: url.pathname + url.search,
      method,
      headers: { "Content-Type": "application/json" },
    };
    const req = lib.request(opts, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      reject(new Error("RTDB timeout"));
    });
    req.on("error", reject);
    if (body !== null) req.write(JSON.stringify(body));
    req.end();
  });
}

const rtdbGet = (path) => rtdbRequest("GET", path);
const rtdbPut = (path, body) => rtdbRequest("PUT", path, body);
const rtdbDelete = (path) => rtdbRequest("DELETE", path);

// ──────────────────────────────────────────────────────────────────────
// 4. Instance registry
// ──────────────────────────────────────────────────────────────────────
const makeSelfPayload = () => ({ timestamp: Date.now() });

async function registerSelf() {
  await rtdbPut(`${LOCK_NODE}/${INSTANCE_ID}`, makeSelfPayload());
  log(`📝 Registered: ${INSTANCE_ID}`);
}

async function heartbeatSelf() {
  await rtdbPut(`${LOCK_NODE}/${INSTANCE_ID}`, makeSelfPayload());
}

async function cleanupZombies(instances) {
  const now = Date.now();
  for (const [id, data] of Object.entries(instances)) {
    if (id === INSTANCE_ID) continue;
    const age = now - (data.timestamp || 0);
    if (age > ZOMBIE_TTL_MS) {
      warn(`🧟 Zombie: ${id} (${Math.round(age / 1000)}s) — xóa`);
      await rtdbDelete(`${LOCK_NODE}/${id}`).catch(() => {});
    }
  }
}

// Instance có timestamp LỚN NHẤT = leader (instance start mới nhất)
function electLeader(instances) {
  let leader = null,
    maxTs = -1;
  for (const [id, data] of Object.entries(instances)) {
    if ((data.timestamp || 0) > maxTs) {
      maxTs = data.timestamp;
      leader = id;
    }
  }
  return leader;
}

// ──────────────────────────────────────────────────────────────────────
// 5. Docker helpers
// ──────────────────────────────────────────────────────────────────────
function dockerExec(args, { silent = false } = {}) {
  const r = spawnSync("docker", args, { encoding: "utf8", timeout: 15000 });
  if (!silent && r.stderr && r.status !== 0) process.stderr.write(r.stderr);
  return { ok: r.status === 0, stdout: (r.stdout || "").trim() };
}

function getContainerName(service) {
  const r = dockerExec(
    [
      "ps",
      "-a",
      "--filter",
      `label=com.docker.compose.service=${service}`,
      "--filter",
      `label=com.docker.compose.project=${COMPOSE_PROJECT}`,
      "--format",
      "{{.Names}}",
    ],
    { silent: true },
  );
  return r.stdout.split("\n").filter(Boolean)[0] || null;
}

function isRunning(service) {
  const cname = getContainerName(service);
  if (!cname) return false;
  const r = dockerExec(["inspect", "-f", "{{.State.Running}}", cname], { silent: true });
  return r.stdout === "true";
}

function getHealth(service) {
  const cname = getContainerName(service);
  if (!cname) return "missing";
  const r = dockerExec(["inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}", cname], { silent: true });
  return r.stdout || "unknown";
}

function svcStart(service) {
  const cname = getContainerName(service);
  if (!cname) {
    warn(`Container không tìm thấy: ${service}`);
    return false;
  }
  if (isRunning(service)) {
    log(`${service} đã chạy`);
    return true;
  }
  log(`▶ Starting ${service} (${cname})...`);
  const r = dockerExec(["start", cname]);
  r.ok ? log(`  ✅ ${service} started`) : warn(`  ✖ Failed: ${service}`);
  return r.ok;
}

function svcStop(service, graceSeconds = 10) {
  if (!isRunning(service)) return;
  const cname = getContainerName(service);
  if (!cname) return;
  log(`■ Stopping ${service} (grace=${graceSeconds}s)...`);
  const r = dockerExec(["stop", "-t", String(graceSeconds), cname]);
  r.ok ? log(`  ✅ ${service} stopped`) : warn(`  ✖ Failed stop: ${service}`);
}

function waitHealthy(service, timeoutSec = 180) {
  return new Promise((resolve) => {
    const POLL = 5000;
    let waited = 0;
    const check = () => {
      const h = getHealth(service);
      if (h === "healthy" || h === "no-healthcheck") {
        log(`  ${service}: ${h} ✅`);
        return resolve(true);
      }
      if (h === "unhealthy" || h === "missing") {
        warn(`  ${service}: ${h}`);
        return resolve(false);
      }
      waited += POLL / 1000;
      if (waited >= timeoutSec) {
        warn(`  ${service}: timeout`);
        return resolve(false);
      }
      log(`  ${service}: ${h} — ${waited}/${timeoutSec}s`);
      setTimeout(check, POLL);
    };
    check();
  });
}

// ──────────────────────────────────────────────────────────────────────
// 6. Role transitions
// ──────────────────────────────────────────────────────────────────────
let IS_LEADER = false;
let _transitioning = false;

async function onBecomeLeader() {
  if (IS_LEADER || _transitioning) return;
  _transitioning = true;
  try {
    IS_LEADER = true;
    log("══════════════════════════════════════");
    log(`🎉 LEADER — ${INSTANCE_ID}`);
    log("══════════════════════════════════════");

    svcStart("litestream");
    const ok = await waitHealthy("litestream", 180);
    if (!ok) warn("Litestream chưa healthy — tiếp tục...");

    svcStart("omniroute");
    svcStart("cloudflared");
    log("✅ LEADER mode active");
  } finally {
    _transitioning = false;
  }
}

async function onBecomeFollower(reason = "") {
  if (!IS_LEADER || _transitioning) return;
  _transitioning = true;
  try {
    IS_LEADER = false;
    log("══════════════════════════════════════");
    log(`📡 FOLLOWER — ${INSTANCE_ID}${reason ? ` (${reason})` : ""}`);
    log("══════════════════════════════════════");

    svcStop("cloudflared", 10);
    svcStop("omniroute", 35);
    svcStop("litestream", 15);

    // KHÔNG xóa RTDB entry — vẫn heartbeat, sẵn sàng lên leader
    log("✅ FOLLOWER — hot standby");
  } finally {
    _transitioning = false;
  }
}

function leaderHealthCheck() {
  if (!IS_LEADER) return;
  for (const svc of LEADER_SVCS) {
    if (!isRunning(svc)) {
      warn(`${svc} crashed — restart`);
      svcStart(svc);
    }
  }
}

// ──────────────────────────────────────────────────────────────────────
// 7. Evaluate role từ snapshot instances
// ──────────────────────────────────────────────────────────────────────
let _evaluating = false;

async function evaluateRole(instances) {
  if (_evaluating || !instances || typeof instances !== "object") return;
  _evaluating = true;
  try {
    await cleanupZombies(instances);

    // Đọc lại sau cleanup để có snapshot sạch nhất
    let live = instances;
    try {
      const snap = await rtdbGet(LOCK_NODE);
      if (snap.status === 200 && snap.body && typeof snap.body === "object") {
        live = snap.body;
      }
    } catch {
      /* dùng snapshot cũ */
    }

    const leader = electLeader(live);
    log(`📊 Instances: ${Object.keys(live).length} | Leader: ${leader || "none"}`);

    if (leader === INSTANCE_ID) {
      await onBecomeLeader();
    } else if (IS_LEADER) {
      await onBecomeFollower(`instance mới hơn: ${leader}`);
    } else {
      log(`👥 Follower — leader: ${leader}`);
    }
  } finally {
    _evaluating = false;
  }
}

// ──────────────────────────────────────────────────────────────────────
// 8. SSE listener
// ──────────────────────────────────────────────────────────────────────
let _sseReq = null;
let _sseReconnectTimer = null;

function startSSE() {
  if (_sseReconnectTimer) {
    clearTimeout(_sseReconnectTimer);
    _sseReconnectTimer = null;
  }

  const sseUrl = new URL(buildUrl(LOCK_NODE));
  const lib = sseUrl.protocol === "https:" ? https : http;
  const opts = {
    hostname: sseUrl.hostname,
    port: sseUrl.port || (sseUrl.protocol === "https:" ? 443 : 80),
    path: sseUrl.pathname + sseUrl.search,
    method: "GET",
    headers: { Accept: "text/event-stream", "Cache-Control": "no-cache" },
  };

  log(`🔌 SSE connecting: ${RTDB_BASE}/${LOCK_NODE}`);

  _sseReq = lib.request(opts, (res) => {
    if (res.statusCode !== 200) {
      warn(`SSE HTTP ${res.statusCode} — reconnect 5s`);
      scheduleSSEReconnect(5000);
      return;
    }
    log("✅ SSE connected");

    let buf = "",
      eventName = "";

    res.on("data", (chunk) => {
      buf += chunk.toString();
      const lines = buf.split("\n");
      buf = lines.pop();

      for (const line of lines) {
        const t = line.trim();
        if (!t) {
          eventName = "";
          continue;
        }
        if (t.startsWith("event:")) {
          eventName = t.slice(6).trim();
        } else if (t.startsWith("data:")) {
          handleSSEEvent(eventName || "put", t.slice(5).trim()).catch((e) => warn("SSE handler error:", e.message));
        }
      }
    });

    res.on("end", () => {
      warn("SSE end — reconnect 3s");
      scheduleSSEReconnect(3000);
    });
    res.on("error", (e) => {
      warn("SSE error:", e.message, "3s");
      scheduleSSEReconnect(3000);
    });
  });

  _sseReq.on("error", (e) => {
    warn("SSE req error:", e.message);
    scheduleSSEReconnect(5000);
  });
  _sseReq.end();
}

function scheduleSSEReconnect(ms) {
  if (_sseReq) {
    try {
      _sseReq.destroy();
    } catch {}
    _sseReq = null;
  }
  _sseReconnectTimer = setTimeout(startSSE, ms);
}

async function handleSSEEvent(event, raw) {
  if (event === "cancel") {
    warn("SSE cancel");
    scheduleSSEReconnect(5000);
    return;
  }
  if (event === "auth_revoked") {
    warn("SSE auth_revoked");
    scheduleSSEReconnect(10000);
    return;
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return;
  }

  const instances = parsed?.data;

  // Node bị xóa hoàn toàn → re-register
  if (!instances || typeof instances !== "object") {
    log("SSE: node rỗng — re-register");
    await registerSelf().catch((e) => warn("re-register:", e.message));
    return;
  }

  // Entry của mình bị zombie-cleanup bởi instance khác → re-register
  if (!instances[INSTANCE_ID]) {
    log("SSE: entry mình bị xóa — re-register");
    await registerSelf().catch((e) => warn("re-register:", e.message));
    return;
  }

  await evaluateRole(instances);
}

// ──────────────────────────────────────────────────────────────────────
// 9. Heartbeat loop — fallback khi SSE miss event
// ──────────────────────────────────────────────────────────────────────
async function heartbeatLoop() {
  while (true) {
    await sleep(HEARTBEAT_MS);
    try {
      await heartbeatSelf(); // Luôn update timestamp dù là leader hay follower

      if (IS_LEADER) {
        leaderHealthCheck();
        log(`💚 Heartbeat — LEADER`);
      } else {
        log(`💤 Heartbeat — follower`);
      }

      // Fallback evaluate từ snapshot (bắt SSE miss)
      const snap = await rtdbGet(LOCK_NODE);
      if (snap.status === 200 && snap.body && typeof snap.body === "object") {
        await evaluateRole(snap.body);
      }
    } catch (e) {
      warn("Heartbeat error:", e.message);
    }
  }
}

// ──────────────────────────────────────────────────────────────────────
// 10. Graceful shutdown
// ──────────────────────────────────────────────────────────────────────
let _shuttingDown = false;

async function shutdown(signal) {
  if (_shuttingDown) return;
  _shuttingDown = true;
  log(`🛑 Shutdown (${signal})`);

  if (_sseReq) {
    try {
      _sseReq.destroy();
    } catch {}
  }
  if (_sseReconnectTimer) clearTimeout(_sseReconnectTimer);

  // Dù leader hay follower đều stop services + xóa entry để không zombie
  if (IS_LEADER) {
    svcStop("cloudflared", 5);
    svcStop("omniroute", 10);
    svcStop("litestream", 10);
  }
  await rtdbDelete(`${LOCK_NODE}/${INSTANCE_ID}`).catch(() => {});
  log(`Goodbye — ${INSTANCE_ID}`);
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("uncaughtException", (e) => warn("uncaughtException:", e.message));
process.on("unhandledRejection", (r) => warn("unhandledRejection:", r));

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ──────────────────────────────────────────────────────────────────────
// 11. Main
// ──────────────────────────────────────────────────────────────────────
async function main() {
  log("╔══════════════════════════════════════╗");
  log("║  Leader Elector v2 (Node.js + SSE)   ║");
  log("╠══════════════════════════════════════╣");
  log(`║ Instance   : ${INSTANCE_ID}`);
  log(`║ Project    : ${COMPOSE_PROJECT}`);
  log(`║ RTDB node  : ${LOCK_NODE}`);
  log(`║ Heartbeat  : ${HEARTBEAT_MS / 1000}s`);
  log(`║ Zombie TTL : ${ZOMBIE_TTL_MS / 1000}s`);
  log("╚══════════════════════════════════════╝");

  // Dừng tất cả services — elector kiểm soát hoàn toàn
  log("Init: dừng tất cả managed services...");
  for (const svc of FOLLOWER_STOP) {
    try {
      svcStop(svc, 5);
    } catch {}
  }
  log("Init done");

  // Register mình
  await registerSelf();

  // Đọc snapshot + evaluate lần đầu
  try {
    const snap = await rtdbGet(LOCK_NODE);
    const instances = snap.status === 200 && snap.body && typeof snap.body === "object" ? snap.body : { [INSTANCE_ID]: makeSelfPayload() };
    await evaluateRole(instances);
  } catch (e) {
    warn("Init evaluate error:", e.message, "— assume leader");
    await onBecomeLeader();
  }

  // SSE realtime
  startSSE();

  // Heartbeat (fallback)
  heartbeatLoop().catch((e) => warn("Heartbeat fatal:", e.message));

  log("🚀 Elector running");
}

main().catch((e) => {
  console.error("[elector] Fatal:", e);
  process.exit(1);
});
