
const http = require("http");
const https = require("https");
const fs = require("fs");

const PORT = 8080;
const REPORTER_INTERVAL = parseInt(process.env.REPORTER_INTERVAL || "60", 10) * 1000;
const STATS_INTERVAL = parseInt(process.env.REPORTER_STATS_INTERVAL || "60", 10) * 1000;
const ENDPOINT = process.env.REPORTER_ENDPOINT || "";
const INSTANCE_ID = process.env.HOSTNAME || "unknown";
const XMRIG_API_PORT = 8081;
const XMRIG_API_TOKEN = "xmrig-api-token";

const STALL_THRESHOLD = 10;
const LIVENESS_GRACE_SEC = 300;
let consecutiveZeroHashSamples = 0;

const MAX_BUFFERED_SAMPLES = 10;

process.on("uncaughtException", (err) => {
  console.error("[reporter] UNCAUGHT EXCEPTION:", err.message);
  console.error(err.stack);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("[reporter] UNHANDLED REJECTION at:", promise, "reason:", reason);
});

let cachedStats = {
  hashrate: 0,
  sharesAccepted: 0,
  sharesRejected: 0,
  cpuPercent: 0,
  memoryPercent: 0,
  pool: "",
  uptime: 0,
  connectionStatus: "unknown",
  poolState: "unknown",
  tlsStatus: "unknown",
  lastError: "",
  lastErrorTime: 0,
  lastUpdate: 0,
};

let statsBatchBuffer = [];
let lastStatsLogAt = 0;
let lastSuccessfulPushLogAt = 0;

function stableJitterMs(maxMs) {
  const max = Math.max(0, Math.trunc(maxMs));
  if (max <= 0) return 0;
  let hash = 0;
  for (let i = 0; i < INSTANCE_ID.length; i++) {
    hash = (hash * 31 + INSTANCE_ID.charCodeAt(i)) >>> 0;
  }
  return hash % max;
}

function fetchXmrigApi(path, callback) {
  let settled = false;
  const safeCallback = (err, val) => {
    if (settled) return;
    settled = true;
    callback(err, val);
  };

  const options = {
    hostname: "localhost",
    port: XMRIG_API_PORT,
    path: path,
    method: "GET",
    headers: {
      "Authorization": "Bearer " + XMRIG_API_TOKEN,
      "Accept": "application/json",
    },
    timeout: 2000,
  };

  const req = http.request(options, (res) => {
    let data = "";
    res.on("data", (chunk) => {
      data += chunk;
    });
    res.on("end", () => {
      try {
        const json = JSON.parse(data);
        safeCallback(null, json);
      } catch (e) {
        safeCallback(new Error("JSON parse error: " + e.message), null);
      }
    });
  });

  req.on("error", (err) => {
    safeCallback(err, null);
  });

  req.on("timeout", () => {
    req.destroy();
    safeCallback(new Error("Request timeout"), null);
  });

  req.end();
}

function getCpuUsage(callback) {
  fs.readFile("/proc/stat", "utf8", (err, data) => {
    if (err) {
      callback(0);
      return;
    }
    const line = data.split("\n")[0];
    const parts = line.split(/\s+/).slice(1).map(Number);
    const total = parts.reduce((a, b) => a + b, 0);
    const idle = parts[3];
    const usage = total > 0 ? ((total - idle) / total) * 100 : 0;
    callback(usage);
  });
}

function getMemoryUsage(callback) {
  fs.readFile("/proc/meminfo", "utf8", (err, data) => {
    if (err) {
      callback(0);
      return;
    }
    const totalMatch = data.match(/MemTotal:\s+(\d+)/);
    const availableMatch = data.match(/MemAvailable:\s+(\d+)/);
    if (totalMatch && availableMatch) {
      const total = parseInt(totalMatch[1], 10);
      const available = parseInt(availableMatch[1], 10);
      const used = total - available;
      const percent = (used / total) * 100;
      callback(percent);
    } else {
      callback(0);
    }
  });
}

function readLogTail(path, maxBytes = 51200) {
  try {
    if (!fs.existsSync(path)) {
      return null;
    }
    const stats = fs.statSync(path);
    const start = Math.max(0, stats.size - maxBytes);
    const buffer = Buffer.alloc(stats.size - start);
    const fd = fs.openSync(path, "r");
    fs.readSync(fd, buffer, 0, buffer.length, start);
    fs.closeSync(fd);

    let text = buffer.toString("utf8");
    if (start > 0 && text.indexOf("\n") !== -1) {
      text = text.substring(text.indexOf("\n") + 1);
    }
    return text;
  } catch (err) {
    return null;
  }
}

function parseXmrigLog() {
  const log = readLogTail("/tmp/xmrig.log", 51200);

  if (log === null) {
    return {
      accepted: 0,
      rejected: 0,
      poolState: "unknown",
      tlsStatus: "unknown",
      lastError: "No log file",
      lastErrorTime: 0,
      lastLines: "No log file",
    };
  }

  const lines = log.split("\n");

  let accepted = 0;
  let rejected = 0;
  let poolState = "unknown";
  let tlsStatus = "unknown";
  let lastError = "";
  let lastErrorTime = 0;

  for (const line of lines) {

    if (line.includes("accepted")) {
      const match = line.match(/\((\d+)\/(\d+)\)/);
      if (match) {
        accepted = parseInt(match[1], 10);
        rejected = parseInt(match[2], 10);
      }
    }

    if (line.includes("TLS") || line.includes("tls") || line.includes("SSL") || line.includes("ssl")) {
      tlsStatus = "error";
      if (line.includes("handshake failed") || line.includes("handshake error")) {
        lastError = "TLS handshake failed";
        lastErrorTime = Date.now();
        poolState = "tls_error";
      } else if (line.includes("certificate") || line.includes("verify") || line.includes("self signed")) {
        lastError = "TLS certificate error";
        lastErrorTime = Date.now();
        poolState = "tls_error";
      } else if (line.includes("error")) {
        lastError = line.trim();
        lastErrorTime = Date.now();
        poolState = "tls_error";
      }
    }

    if (line.includes("rejected") && !line.includes("accepted")) {
      poolState = "pool_rejected";
      lastError = line.trim();
      lastErrorTime = Date.now();
    }
    if (line.includes("unauthorized") || line.includes("banned") || line.includes("blocked")) {
      poolState = "pool_rejected";
      lastError = line.trim();
      lastErrorTime = Date.now();
    }

    if (line.includes("connect error") || line.includes("connection refused") || line.includes("Connection refused")) {
      poolState = "disconnected";
      lastError = "Connection refused";
      lastErrorTime = Date.now();
    }
    if (line.includes("DNS") || line.includes("resolve") || line.includes("host not found")) {
      poolState = "disconnected";
      lastError = "DNS resolution failed";
      lastErrorTime = Date.now();
    }
    if (line.includes("timeout") || line.includes("timed out")) {
      poolState = "disconnected";
      lastError = "Connection timeout";
      lastErrorTime = Date.now();
    }
    if (line.includes("read error") || line.includes("write error") || line.includes("socket error")) {
      poolState = "disconnected";
      lastError = line.trim();
      lastErrorTime = Date.now();
    }

    if (line.includes("connected") && line.includes("pool")) {
      poolState = "connected";
      tlsStatus = tlsStatus === "error" ? "error" : "ok";
    }
    if (line.includes("use pool")) {
      poolState = "connected";
    }
    if (line.includes("TLS") && line.includes("enabled")) {
      tlsStatus = "enabled";
    }
  }

  return {
    accepted,
    rejected,
    poolState,
    tlsStatus,
    lastError,
    lastErrorTime,
    lastLines: lines.slice(-20).join("\n"),
  };
}

function updateStats() {
  fetchXmrigApi("/1/summary", (err, summary) => {
    if (err) {
      const logStats = parseXmrigLog();
      console.error("[reporter] XMRig API error:", err.message);

      cachedStats.connectionStatus = "api_unavailable";
      cachedStats.poolState = logStats.poolState !== "unknown" ? logStats.poolState : cachedStats.poolState;
      cachedStats.tlsStatus = logStats.tlsStatus !== "unknown" ? logStats.tlsStatus : cachedStats.tlsStatus;
      if (logStats.lastError) {
        cachedStats.lastError = logStats.lastError;
        cachedStats.lastErrorTime = logStats.lastErrorTime;
      }
      cachedStats.hashrate = 0;
      cachedStats.lastUpdate = Date.now();
      const reporterUptimeSec = process.uptime();
      if (reporterUptimeSec > LIVENESS_GRACE_SEC) {
        consecutiveZeroHashSamples++;
        console.warn(
          `[reporter] api-unreachable sample ${consecutiveZeroHashSamples}/${STALL_THRESHOLD} (reporterUptime=${reporterUptimeSec.toFixed(0)}s, err=${err.message})`,
        );
        if (consecutiveZeroHashSamples >= STALL_THRESHOLD) {
          console.error(
            `[reporter] XMRig API STALLED: unreachable for ${STALL_THRESHOLD} consecutive samples. Exiting to force container restart.`,
          );
          process.exit(2);
        }
      }
      return;
    }

    const hashrate = summary.hashrate?.total?.[0] || 0;
    const sharesGood = summary.results?.shares_good || 0;
    const sharesTotal = summary.results?.shares_total || 0;
    const sharesRejected = sharesTotal - sharesGood;
    const pool = summary.connection?.pool || "";
    const uptime = summary.connection?.uptime || 0;

    if (sharesGood < cachedStats.sharesAccepted * 0.9) {
      console.warn(
        `[reporter] XMRig shares counter reset detected: ${cachedStats.sharesAccepted} -> ${sharesGood}`,
      );
      cachedStats.sharesAccepted = 0;
      cachedStats.sharesRejected = 0;
    }

    let poolState = "unknown";
    let tlsStatus = "unknown";
    let logStats = null;
    if (uptime > 0) {
      poolState = "connected";
      tlsStatus = summary.connection?.tls ? "enabled" : "disabled";
    } else {
      logStats = parseXmrigLog();
      if (logStats.poolState !== "unknown") {
        poolState = logStats.poolState;
        tlsStatus = logStats.tlsStatus;
      } else {
        poolState = "disconnected";
        tlsStatus = "unknown";
      }
    }

    getCpuUsage((cpuPercent) => {
      getMemoryUsage((memoryPercent) => {
        cachedStats = {
          hashrate: hashrate,
          sharesAccepted: sharesGood,
          sharesRejected: sharesRejected,
          cpuPercent: cpuPercent,
          memoryPercent: memoryPercent,
          pool: pool,
          uptime: uptime,
          connectionStatus: uptime > 0 ? "connected" : "disconnected",
          poolState: poolState,
          tlsStatus: tlsStatus,
          lastError: logStats?.lastError || cachedStats.lastError,
          lastErrorTime: logStats?.lastErrorTime || cachedStats.lastErrorTime,
          lastUpdate: Date.now(),
        };
        if (Date.now() - lastStatsLogAt > 300000) {
          lastStatsLogAt = Date.now();
          console.log(
            `[reporter] Stats updated: ${hashrate.toFixed(1)} H/s, CPU: ${cpuPercent.toFixed(1)}%, Shares: ${sharesGood}/${sharesTotal}, Pool: ${poolState}`,
          );
        }

        const reporterUptimeSec = process.uptime();
        if (reporterUptimeSec > LIVENESS_GRACE_SEC && (!hashrate || hashrate <= 0)) {
          consecutiveZeroHashSamples++;
          console.warn(
            `[reporter] zero-hashrate sample ${consecutiveZeroHashSamples}/${STALL_THRESHOLD} (reporterUptime=${reporterUptimeSec.toFixed(0)}s, xmrigConnUptime=${uptime}s, pool=${poolState})`,
          );
          if (consecutiveZeroHashSamples >= STALL_THRESHOLD) {
            console.error(
              `[reporter] XMRig STALLED: hashrate=0 for ${STALL_THRESHOLD} consecutive samples (reporterUptime=${reporterUptimeSec.toFixed(0)}s, xmrigConnUptime=${uptime}s). Exiting to force container restart.`,
            );
            process.exit(2);
          }
        } else if (hashrate > 0) {
          consecutiveZeroHashSamples = 0;
        }
      });
    });
  });
}

function reportMetrics() {
  if (!ENDPOINT) {
    console.error("[reporter] ENDPOINT is empty, skipping push");
    return;
  }

  statsBatchBuffer.push({
    instanceId: INSTANCE_ID,
    hashrate: cachedStats.hashrate,
    sharesAccepted: cachedStats.sharesAccepted,
    sharesRejected: cachedStats.sharesRejected,
    cpuPercent: cachedStats.cpuPercent,
    memoryPercent: cachedStats.memoryPercent,
    pool: cachedStats.pool,
    connectionStatus: cachedStats.connectionStatus,
    poolState: cachedStats.poolState,
    tlsStatus: cachedStats.tlsStatus,
    uptime: cachedStats.uptime,
    lastError: cachedStats.lastError,
    lastErrorTime: cachedStats.lastErrorTime,
    timestamp: Date.now(),
  });

  const data = JSON.stringify({ batch: statsBatchBuffer });
  const batchSize = statsBatchBuffer.length;
  const pendingBuffer = statsBatchBuffer;
  statsBatchBuffer = [];
  let settled = false;

  const requeue = (label) => {
    if (settled) return;
    settled = true;
    console.error(`[reporter] ${label}; requeuing ${pendingBuffer.length} samples`);
    statsBatchBuffer = pendingBuffer.concat(statsBatchBuffer).slice(-MAX_BUFFERED_SAMPLES);
  };

  const requester = ENDPOINT.startsWith("https:") ? https : http;

  const req = requester.request(
    ENDPOINT,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data),
      },
      timeout: 5000,
    },
    (res) => {
      if (res.statusCode === 200) {
        settled = true;
        res.resume();
        if (Date.now() - lastSuccessfulPushLogAt > 600000) {
          lastSuccessfulPushLogAt = Date.now();
          console.log(`[reporter] Metrics push successful (${batchSize} samples)`);
        }
      } else {
        res.resume();
        requeue(`Metrics push failed: ${res.statusCode}`);
      }
    },
  );

  req.on("error", (err) => {
    requeue(`Push error: ${err.message}`);
  });

  req.on("timeout", () => {
    req.destroy();
    requeue(`Push timeout`);
  });

  req.write(data);
  req.end();
}

function readLogFile(path) {
  try {
    if (fs.existsSync(path)) {
      return fs.readFileSync(path, "utf8");
    }
    return "File not found";
  } catch (err) {
    return `Error reading file: ${err.message}`;
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, instance: INSTANCE_ID }));
    return;
  }

  if (req.url === "/stats" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        ...cachedStats,
        stallCounter: consecutiveZeroHashSamples,
        stallThreshold: STALL_THRESHOLD,
      }),
    );
    return;
  }

  if (req.url === "/xmrig-summary" && req.method === "GET") {
    fetchXmrigApi("/1/summary", (err, summary) => {
      if (err) {
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.message, instance: INSTANCE_ID }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(summary));
    });
    return;
  }

  if (req.url === "/logs" && req.method === "GET") {
    const reporterLog = readLogFile("/tmp/reporter.log");
    const xmrigLog = readLogFile("/tmp/xmrig.log");
    const startLog = readLogFile("/tmp/start.log");
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        reporter: reporterLog.substring(0, 5000),
        xmrig: xmrigLog.substring(0, 5000),
        start: startLog.substring(0, 5000),
      }),
    );
    return;
  }

  if (req.url === "/pool-status" && req.method === "GET") {
    const logStats = parseXmrigLog();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        pool: cachedStats.pool,
        connectionStatus: cachedStats.connectionStatus,
        poolState: cachedStats.poolState,
        tlsStatus: cachedStats.tlsStatus,
        uptime: cachedStats.uptime,
        lastError: cachedStats.lastError,
        lastErrorTime: cachedStats.lastErrorTime,
        lastErrorAgo: cachedStats.lastErrorTime > 0 ? Date.now() - cachedStats.lastErrorTime : null,
        logPoolState: logStats.poolState,
        logTlsStatus: logStats.tlsStatus,
        logLastError: logStats.lastError,
        lastUpdate: cachedStats.lastUpdate,
        instance: INSTANCE_ID,
      }),
    );
    return;
  }

  res.writeHead(404);
  res.end("Not Found");
});

server.on("error", (err) => {
  console.error("[reporter] SERVER ERROR:", err.message);
});

server.listen(PORT, () => {
  console.log(`[reporter] Health server listening on :${PORT}`);
  console.log(`[reporter] Endpoints: /health, /stats, /xmrig-summary, /logs, /pool-status`);
});

const statsInitialDelay = Math.min(
  Math.max(1000, STATS_INTERVAL - 1000),
  5000 + stableJitterMs(5000),
);
const reportJitterWindow = Math.max(1, REPORTER_INTERVAL - 10000);
const reportInitialDelay = REPORTER_INTERVAL <= 10000
  ? REPORTER_INTERVAL
  : 10000 + stableJitterMs(reportJitterWindow);

setTimeout(() => {
  updateStats();
  setInterval(updateStats, STATS_INTERVAL);
}, statsInitialDelay);

setTimeout(() => {
  reportMetrics();
  setInterval(reportMetrics, REPORTER_INTERVAL);
}, reportInitialDelay);
