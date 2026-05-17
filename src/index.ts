import { Hono } from "hono";
import { bearerAuth } from "hono/bearer-auth";
import { HTTPException } from "hono/http-exception";
import type { Context } from "hono";
import { connect } from "cloudflare:sockets";
import { MinerCoordinator } from "./coordinator";
import { MiningStatsStore, processHeartbeats } from "./mining-stats";

const DEFAULT_POOL = "pool.supportxmr.com:3333";

function emitLog(level: string, fields: Record<string, unknown>, msg?: string): void {
	const payload = JSON.stringify({
		level,
		time: new Date().toISOString(),
		service: "miner-worker",
		...fields,
		...(msg ? { msg } : {}),
	});
	if(level === "error" || level === "fatal") console.error(payload);
	else if(level === "warn") console.warn(payload);
	else console.log(payload);
}

const log = {
	info: (fields: Record<string, unknown>, msg?: string) => emitLog("info", fields, msg),
	warn: (fields: Record<string, unknown>, msg?: string) => emitLog("warn", fields, msg),
	error: (fields: Record<string, unknown>, msg?: string) => emitLog("error", fields, msg),
};

const D1_READ_BOOKMARK = "first-unconstrained";
const STALE_PRUNE_INTERVAL_MS = 5 * 60_000;
const SLOW_PRUNE_INTERVAL_MS = 60 * 60_000;

let lastRolledUpHour = 0;
let lastStalePruneBucket = -1;
let lastSlowPruneBucket = -1;

function readSession(env: Env): D1Database | D1DatabaseSession {
	const db = env.DB as D1Database & {
		withSession?: (bookmark?: string) => D1DatabaseSession;
	};
	return typeof db.withSession === "function"
		? db.withSession(D1_READ_BOOKMARK)
		: env.DB;
}

async function readStatsStore(env: Env): Promise<MiningStatsStore> {
	await MiningStatsStore.ensureReady(env.DB);
	return new MiningStatsStore(readSession(env));
}

const app = new Hono<{ Bindings: Env }>();

function coloFromRequest(request: Request | null): string | null {
	if(!request) return null;
	const cfRay = request.headers.get("CF-Ray") ?? "";
	const colo = cfRay.split("-")[1];
	return colo ? colo : null;
}

async function coordRpc<T>(
	env: Env,
	request: Request | null,
	path: string,
	method: "GET" | "POST" = "GET",
	body?: unknown,
): Promise<T> {
	const id = env.MINER_COORDINATOR.idFromName("global-coordinator");
	const coordinator = env.MINER_COORDINATOR.get(id);
	const colo = coloFromRequest(request);
	const headers: Record<string, string> = { "Content-Type": "application/json" };
	if(colo) headers["X-Colo"] = colo;
	const init: RequestInit = body === undefined ? { method, headers } : { method, headers, body: JSON.stringify(body) };
	const result = await coordinator.fetch(`http://internal${path}`, init);
	return (await result.json()) as T;
}

app.use("/*", async (c, next) => {

	if(c.req.path === "/health") return next();
	if(c.req.path === "/instances/heartbeat") return next();

	const token = c.env.API_KEY;
	if(typeof token !== "string" || token.length === 0){
		return c.json(
			{
				success: false,
				error: "API_KEY is not configured; set it in wrangler.jsonc#vars or via `wrangler secret put API_KEY`",
			},
			503,
		);
	}
	const auth = bearerAuth({ token });
	return auth(c, next);
});

app.onError((err, c) => {
	log.error(
		{ err: err.message, stack: err.stack, path: c.req.path },
		"worker request error",
	);
	if(err instanceof HTTPException) return err.getResponse();
	return c.json(
		{
			success: false,
			error: err.message,
			timestamp: new Date().toISOString(),
		},
		500,
	);
});

app.get("/health", (c) => {
	c.header("Cloudflare-CDN-Cache-Control", "max-age=60");
	c.header("CDN-Cache-Control", "max-age=60");
	const authReady = typeof c.env.API_KEY === "string" && c.env.API_KEY.length > 0;
	const reporterReady = typeof c.env.REPORTER_ENDPOINT === "string" && c.env.REPORTER_ENDPOINT.length > 0;
	return c.json({
		ok: authReady && reporterReady,
		version: "3.2.0-fleet-watchdog",
		config: {
			authReady,
			reporterReady,
		},
	});
});

app.get("/heartbeat-health", async (c) => {
	const stats = new MiningStatsStore(c.env.DB);
	const testId = `health-check-${Date.now()}`;
	const testHashrate = 999;
	const now = Date.now();
	let row: { hashrate: number } | null = null;
	let insertOk = false;

	try {
		try {
			await stats.recordStats({
				instanceId: testId,
				hashrate: testHashrate,
				timestamp: now,
			});
			insertOk = true;
			row = await c.env.DB.prepare("SELECT hashrate FROM instance_latest WHERE instance_id = ?").bind(testId).first<{ hashrate: number }>();
		}finally{
			if(insertOk){
				try {
					await c.env.DB.prepare("DELETE FROM instance_latest WHERE instance_id = ?").bind(testId).run();
				}catch(delErr){
					log.error(
						{ err: (delErr as Error).message },
						"heartbeat-health cleanup failed",
					);
				}
			}
		}
		const schemaOk = await stats.validateSchema();
		const healthy = !!row && Number(row.hashrate) === testHashrate && schemaOk;

		return c.json({
			healthy,
			schemaOk,
			writeOk: !!row,
			readOk: row ? Number(row.hashrate) === testHashrate : false,
			timestamp: now,
			message: healthy ? "D1 heartbeat pipeline operational" : "D1 heartbeat pipeline failed",
		});
	}catch(err){
		const e = err as Error;
		return c.json(
			{
				healthy: false,
				schemaOk: false,
				writeOk: false,
				readOk: false,
				error: e.message,
				timestamp: Date.now(),
				message: `D1 heartbeat pipeline error: ${e.message}`,
			},
			500,
		);
	}
});

app.get("/status", async (c) => {
	const status = await coordRpc<unknown>(c.env, c.req.raw, "/status");
	c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
	c.header("CDN-Cache-Control", "max-age=15");
	return c.json(status);
});

app.get("/instance-details", async (c) => {
	const details = await coordRpc<unknown>(c.env, c.req.raw, "/instance-details");
	c.header("Cloudflare-CDN-Cache-Control", "max-age=30");
	c.header("CDN-Cache-Control", "max-age=30");
	return c.json(details);
});

app.get("/dark-fleet", async (c) => {
	const result = await coordRpc<unknown>(c.env, c.req.raw, "/dark-fleet");
	c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
	c.header("CDN-Cache-Control", "max-age=15");
	return c.json(result);
});

app.post("/restart-instance", async (c) => {
	const body = (await c.req.json().catch(() => ({}))) as { instanceId?: string };
	if(!body.instanceId){
		return c.json({ success: false, error: "Missing instanceId" }, 400);
	}
	const result = await coordRpc<unknown>(
		c.env,
		c.req.raw,
		"/restart-instance",
		"POST",
		body,
	);
	return c.json(result);
});

app.post("/heal", async (c) => {
	const result = await coordRpc<unknown>(c.env, c.req.raw, "/force-heal", "POST");
	return c.json(result);
});

app.post("/set-pool", async (c) => {
	const body = (await c.req.json().catch(() => ({}))) as { pool?: string };
	if(!body.pool){
		return c.json({ success: false, error: "Missing pool" }, 400);
	}
	const result = await coordRpc<unknown>(c.env, c.req.raw, "/set-pool", "POST", body);
	return c.json(result);
});

app.post("/optimize-pool", async (c) => {
	try {
		const colo = coloFromRequest(c.req.raw) ?? "";
		const optimalPool = DEFAULT_POOL;

		const result = await coordRpc<{ success?: boolean; error?: string }>(
			c.env,
			c.req.raw,
			"/set-pool",
			"POST",
			{ pool: optimalPool },
		);

		if(!result.success){
			log.warn(
				{ colo, optimalPool, coordError: result.error },
				"optimize-pool: coordinator rejected pool",
			);
			return c.json(
				{
					success: false,
					colo: colo || "unknown",
					optimalPool,
					error: result.error ?? "coordinator rejected pool",
					coordinatorResponse: result,
				},
				502,
			);
		}

		return c.json({
			success: true,
			colo: colo || "unknown",
			optimalPool,
			message: `Pool optimized for ${colo || "unknown"}`,
			coordinatorResponse: result,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/pool-probe", async (c) => {
	const explicitHost = c.req.query("host");
	const timeoutMs = clampInt(c.req.query("timeout_ms"), 3000, 250, 10000);

	const targets: string[] = explicitHost ? [explicitHost] : [DEFAULT_POOL];

	const results = await mapLimit(targets, 3, (target) => probePool(target, timeoutMs));

	const allOk = results.every((r) => r.ok);
	c.header("Cloudflare-CDN-Cache-Control", "no-store");
	c.header("CDN-Cache-Control", "no-store");
	return c.json(
		{
			success: true,
			allOk,
			probes: results,
			timestamp: Date.now(),
		},
		allOk ? 200 : 503,
	);
});

interface PoolProbeResult {
	target: string;
	host: string | null;
	port: number | null;
	ok: boolean;
	latencyMs: number | null;
	error: string | null;
}

async function probePool(target: string, timeoutMs: number): Promise<PoolProbeResult> {
	const parsed = parseHostPort(target);
	if(!parsed){
		return {
			target,
			host: null,
			port: null,
			ok: false,
			latencyMs: null,
			error: "invalid host:port format",
		};
	}
	const { host, port } = parsed;
	const start = Date.now();
	let socket: ReturnType<typeof connect> | null = null;
	try {
		socket = connect({ hostname: host, port }, { allowHalfOpen: false });

		await Promise.race([
			socket.opened,
			new Promise<never>((_, reject) =>
				setTimeout(
					() => reject(new Error(`connect timeout after ${timeoutMs}ms`)),
					timeoutMs,
				),
			),
		]);
		const latency = Date.now() - start;
		return { target, host, port, ok: true, latencyMs: latency, error: null };
	}catch(err){
		return {
			target,
			host,
			port,
			ok: false,
			latencyMs: null,
			error: (err as Error).message,
		};
	}finally{

		if(socket){
			try {
				await socket.close();
			}catch{

			}
		}
	}
}

function parseHostPort(input: string): { host: string; port: number } | null {
	const trimmed = (input ?? "").trim();
	if(!trimmed) return null;

	if(/[\s\/?]/.test(trimmed)) return null;
	const lastColon = trimmed.lastIndexOf(":");
	if(lastColon <= 0 || lastColon === trimmed.length - 1) return null;
	const host = trimmed.slice(0, lastColon);
	const port = Number.parseInt(trimmed.slice(lastColon + 1), 10);
	if(!Number.isFinite(port) || port <= 0 || port > 65535) return null;
	if(!/^[A-Za-z0-9.\-]+$/.test(host)) return null;
	return { host, port };
}

function clampInt(raw: string | undefined, fallback: number, min: number, max: number): number {
	if(raw === undefined) return fallback;
	const n = Number.parseInt(raw, 10);
	if(!Number.isFinite(n)) return fallback;
	return Math.min(max, Math.max(min, n));
}

app.post("/instances/heartbeat", async (c) => {
	const raw = await c.req.json().catch(() => null);
	if(raw === null || typeof raw !== "object" || Array.isArray(raw)){
		return c.json({ acknowledged: false, error: "invalid heartbeat body" }, 400);
	}
	const body = raw as { batch?: unknown[]; [key: string]: unknown };

	const payloads: Array<Record<string, unknown>> = Array.isArray(body.batch) && body.batch.length > 0 ? (body.batch as Array<Record<string, unknown>>) : [body];

	const colo = coloFromRequest(c.req.raw);
	await processHeartbeats(c.env, payloads, colo);
	return c.json({ acknowledged: true, batchSize: payloads.length });
});

app.get("/instance/:containerId/xmrig-summary", async (c) => {
	const containerId = c.req.param("containerId");
	if(!/^[a-zA-Z0-9._-]+$/.test(containerId)){
		return c.json({ success: false, error: "Invalid containerId" }, 400);
	}
	try {
		const id = c.env.MINER_CONTAINER.idFromName(containerId);
		const container = c.env.MINER_CONTAINER.get(id);
		const result = await fetchWithTimeout(
			() => container.containerFetch("http://localhost:8080/xmrig-summary"),
			8000,
		);
		if(result.status !== 200){
			return c.json(
				{
					success: false,
					containerId,
					upstreamStatus: result.status,
					error: `Reporter returned ${result.status}`,
				},
				502,
			);
		}
		const summary = await result.json();
		c.header("Cloudflare-CDN-Cache-Control", "max-age=10");
		c.header("CDN-Cache-Control", "max-age=10");
		return c.json({ success: true, containerId, summary });
	}catch(err){
		return c.json(
			{ success: false, containerId, error: (err as Error).message },
			500,
		);
	}
});

app.get("/container-health", async (c) => {
	try {
		const status = await coordRpc<{
			instances: Array<{ status: string; containerId: string }>;
		}>(c.env, c.req.raw, "/status");
		if(!status.instances?.length){
			c.header("Cloudflare-CDN-Cache-Control", "max-age=30");
			c.header("CDN-Cache-Control", "max-age=30");
			return c.json({ success: true, running: false });
		}

		const health = await mapLimit(status.instances, 25, async (inst) => {
			if(inst.status !== "running"){
				return {
					containerId: inst.containerId,
					status: inst.status,
					reporter: false,
					_running: false,
					_ok: false,
				};
			}
			try {
				const id = c.env.MINER_CONTAINER.idFromName(inst.containerId);
				const container = c.env.MINER_CONTAINER.get(id);
				const result = await fetchWithTimeout(
					() => container.containerFetch("http://localhost:8080/health"),
					5000,
				);
				const ok = result.status === 200;
				return {
					containerId: inst.containerId,
					status: "running",
					reporter: ok,
					_running: true,
					_ok: ok,
				};
			}catch(err){
				return {
					containerId: inst.containerId,
					status: "running",
					reporter: false,
					error: (err as Error).message,
					_running: true,
					_ok: false,
				};
			}
		});

		let totalRunning = 0;
		let totalReporterOk = 0;
		for(const h of health){
			if(h._running) totalRunning++;
			if(h._ok) totalReporterOk++;
			delete (h as Partial<typeof h>)._running;
			delete (h as Partial<typeof h>)._ok;
		}

		c.header("Cloudflare-CDN-Cache-Control", "max-age=30");
		c.header("CDN-Cache-Control", "max-age=30");
		return c.json({
			success: true,
			running: true,
			totalInstances: status.instances.length,
			runningInstances: totalRunning,
			healthyReporters: totalReporterOk,
			instances: health,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/container-logs", async (c) => {
	try {
		const status = await coordRpc<{
			instances: Array<{ status: string; containerId: string }>;
		}>(c.env, c.req.raw, "/status");
		if(!status.instances?.length){
			return c.json({ success: true, running: false });
		}

		const candidates = status.instances
			.filter((i) => i.status === "running")
			.slice(0, 5);

		const collected = await mapLimit(candidates, 5, async (inst) => {
			try {
				const id = c.env.MINER_CONTAINER.idFromName(inst.containerId);
				const container = c.env.MINER_CONTAINER.get(id);
				const result = await fetchWithTimeout(
					() => container.containerFetch("http://localhost:8080/logs"),
					5000,
				);
				if(result.status === 200){
					const logs = await result.json();
					return { containerId: inst.containerId, logs };
				}
				return null;
			}catch(err){
				log.error(
					{ container: inst.containerId, err: (err as Error).message },
					"container logs fetch failed",
				);
				return null;
			}
		});
		const allLogs = collected.filter(
			(x): x is { containerId: string; logs: unknown } => x !== null,
		);

		return c.json({
			success: true,
			totalInstances: status.instances.length,
			instanceLogs: allLogs,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/mining-status", async (c) => {
	try {
		if(c.req.query("live") !== "1"){
			try {
				const stats = await readStatsStore(c.env);
				const report = await stats.getLatestStatusReport();
				if(report){
					c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
					c.header("CDN-Cache-Control", "max-age=15");
					return c.json({
						success: true,
						running: report.runningInstances > 0,
						source: "cron",
						reportAgeMs: report.reportAgeMs,
						staleReport: report.staleReport,
						totalInstances: report.totalInstances,
						activeInstances: report.activeInstances,
						runningInstances: report.runningInstances,
						operation: report.operation,
						aggregated: {
							totalHashrate: report.totalHashrate,
							totalSharesAccepted: report.totalSharesLifetime,
							totalSharesRejected: report.totalSharesRejectedLifetime,
							avgCpuPercent: null,
						},
						totals: report,
						instances: [],
					});
				}
			}catch(err){
				log.warn(
					{ err: (err as Error).message },
					"cron report unavailable, falling back live",
				);
			}
		}

		const status = await coordRpc<{
			instances: Array<{ status: string; containerId: string }>;
		}>(c.env, c.req.raw, "/status");
		if(!status.instances?.length){
			return c.json({
				success: true,
				running: false,
				message: "No active mining instances",
			});
		}

		const perInstance = await mapLimit(status.instances, 25, async (inst) => {
			if(inst.status !== "running"){
				return {
					containerId: inst.containerId,
					status: inst.status,
					stats: null,
					_agg: null,
				};
			}
			try {
				const id = c.env.MINER_CONTAINER.idFromName(inst.containerId);
				const container = c.env.MINER_CONTAINER.get(id);
				const result = await fetchWithTimeout(
					() => container.containerFetch("http://localhost:8080/stats"),
					5000,
				);
				if(result.status === 200){
					const s = (await result.json()) as Record<string, unknown>;
					return {
						containerId: inst.containerId,
						status: "running",
						stats: s,
						_agg: s,
					};
				}
				return {
					containerId: inst.containerId,
					status: "running",
					stats: { error: `Reporter returned ${result.status}` },
					_agg: null,
				};
			}catch(err){
				return {
					containerId: inst.containerId,
					status: "running",
					stats: { error: (err as Error).message },
					_agg: null,
				};
			}
		});

		let totalHashrate = 0;
		let totalSharesAccepted = 0;
		let totalSharesRejected = 0;
		let totalCpuPercent = 0;
		let activeInstances = 0;
		const instanceStats: Array<{
			containerId: string;
			status: string;
			stats: unknown;
		}> = [];
		for(const r of perInstance){
			instanceStats.push({
				containerId: r.containerId,
				status: r.status,
				stats: r.stats,
			});
			if(r._agg){
				const agg = r._agg as Record<string, unknown>;
				totalHashrate += Number(agg.hashrate) || 0;
				totalSharesAccepted += Number(agg.sharesAccepted) || 0;
				totalSharesRejected += Number(agg.sharesRejected) || 0;
				totalCpuPercent += Number(agg.cpuPercent) || 0;
				activeInstances++;
			}
		}

		c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
		c.header("CDN-Cache-Control", "max-age=15");
		return c.json({
			success: true,
			running: true,
			totalInstances: status.instances.length,
			activeInstances,
			aggregated: {
				totalHashrate,
				totalSharesAccepted,
				totalSharesRejected,
				avgCpuPercent: activeInstances > 0 ? totalCpuPercent / activeInstances : 0,
			},
			instances: instanceStats,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/mining-history", async (c) => {
	try {
		const limit = Number.parseInt(c.req.query("limit") ?? "100", 10);
		const stats = await readStatsStore(c.env);
		const history = await stats.getHistory(limit);
		c.header("Cloudflare-CDN-Cache-Control", "max-age=60");
		c.header("CDN-Cache-Control", "max-age=60");
		return c.json({ success: true, history });
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/mining-totals", async (c) => {
	try {
		const stats = await readStatsStore(c.env);
		const totals = await stats.getTotals();
		c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
		c.header("CDN-Cache-Control", "max-age=15");
		return c.json({ success: true, totals });
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/quick-status", async (c) => {
	try {
		try {
			const stats = await readStatsStore(c.env);
			const report = await stats.getLatestStatusReport();
			if(report && !report.staleReport){
				return c.json({
					success: true,
					running: report.runningInstances > 0,
					source: "cron",
					reportAgeMs: report.reportAgeMs,
					totalInstances: report.totalInstances,
					runningInstances: report.runningInstances,
					stoppedInstances: report.stoppedInstances,
					totalShares: report.totalShares,
					totalSharesLifetime: report.totalSharesLifetime,
					totalSharesRejected: report.totalSharesRejected,
					totalSharesRejectedLifetime: report.totalSharesRejectedLifetime,
					rejectionRate: report.rejectionRate,
					totalHashrate: report.totalHashrate,
					averageHashrate: report.averageHashrate,
					peakHashrate: report.peakHashrate,
					totalRecords: report.totalRecords,
					cumulativeUptimeSeconds: report.cumulativeUptimeSeconds,
					sharesPerSecond: report.sharesPerSecond,
					activeInstances: report.activeInstances,
					operation: report.operation,
					config: report.config,
				});
			}
		}catch(err){
			log.warn(
				{ err: (err as Error).message },
				"quick-status: cron report unavailable, falling back live",
			);
		}

		const status = await coordRpc<{
			counts?: { total?: number; running?: number };
			operation?: string;
			config?: unknown;
		}>(c.env, c.req.raw, "/status-summary");

		if((status.counts?.total ?? 0) <= 0){
			c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
			c.header("CDN-Cache-Control", "max-age=15");
			return c.json({
				success: true,
				running: false,
				message: "No active mining instances",
			});
		}

		const counts = status.counts ?? {};
		const runningCount = counts.running ?? 0;
		const totalCount = counts.total ?? 0;

		let totals = {
			totalShares: 0,
			totalSharesLifetime: 0,
			totalSharesRejected: 0,
			totalSharesRejectedLifetime: 0,
			rejectionRate: 0,
			totalHashrate: 0,
			averageHashrate: 0,
			peakHashrate: 0,
			totalRecords: 0,
			cumulativeUptimeSeconds: 0,
			activeInstances: 0,
			sharesPerSecond: 0,
		};
		try {
			const stats = await readStatsStore(c.env);
			totals = await stats.getTotals();
		}catch(err){
			log.error(
				{ err: (err as Error).message },
				"quick-status: totals failed",
			);
		}

		c.header("Cloudflare-CDN-Cache-Control", "max-age=15");
		c.header("CDN-Cache-Control", "max-age=15");
		return c.json({
			success: true,
			running: true,
			totalInstances: totalCount,
			runningInstances: runningCount,
			stoppedInstances: totalCount - runningCount,
			totalShares: totals.totalShares,
			totalSharesLifetime: totals.totalSharesLifetime,
			totalSharesRejected: totals.totalSharesRejected,
			totalSharesRejectedLifetime: totals.totalSharesRejectedLifetime,
			rejectionRate: totals.rejectionRate,
			totalHashrate: totals.totalHashrate,
			averageHashrate: totals.averageHashrate,
			peakHashrate: totals.peakHashrate,
			totalRecords: totals.totalRecords,
			cumulativeUptimeSeconds: totals.cumulativeUptimeSeconds,
			sharesPerSecond: totals.sharesPerSecond,
			activeInstances: totals.activeInstances,
			operation: status.operation,
			config: status.config,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.post("/trigger-cron", async (c) => {
	try {
		const result = await runCronStatsCollection(c.env);
		return c.json(result);
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.post("/prune-stats", async (c) => {
	try {
		const stats = new MiningStatsStore(c.env.DB);
		const staleDeleted = await stats.pruneStaleInstances(10);
		const statusDeleted = await stats.pruneStatusReports(168);
		const hourlyDeleted = await stats.pruneHourlyStats(30);
		return c.json({
			success: true,
			staleDeleted,
			statusDeleted,
			hourlyDeleted,
		});
	}catch(err){
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

async function runCronStatsCollection(env: Env): Promise<{
	success: boolean;
	message: string;
	sampled?: number;
	totalHashrate?: number;
	totalShares?: number;
	stalePruned?: number;
	statusPruned?: number;
	hourlyPruned?: number;
	rolledUp?: number;
	report?: unknown;
}> {
	log.info({ time: new Date().toISOString() }, "cron: status report begin");
	const stats = new MiningStatsStore(env.DB);
	const schemaOk = await stats.validateSchema();
	if(!schemaOk){
		log.error({}, "cron: instance_latest schema validation failed; skipping write cycle");
		return { success: false, message: "schema validation failed" };
	}
	let stalePruned = 0;
	let statusPruned = 0;
	let hourlyPruned = 0;
	let rolledUp = 0;

	type CoordStatus = {
		counts?: Record<string, number>;
		targetInstances?: number;
		operation?: string;
		config?: unknown;
	};

	try {
		let status: CoordStatus | null = null;
		try {
			status = await coordRpc<CoordStatus>(env, null, "/status-summary");
		}catch(err){
			log.error(
				{ err: (err as Error).message },
				"cron: coordinator status failed",
			);
		}

		const now = Date.now();
		const report = await stats.writeStatusReport(status ?? undefined);

		const hourFloor = Math.floor((now - 3_600_000) / 3_600_000) * 3_600_000;
		if(hourFloor !== lastRolledUpHour){
			try {
				rolledUp = await stats.rollupHourly(hourFloor);
				lastRolledUpHour = hourFloor;
			}catch(err){
				log.error({ err: (err as Error).message }, "cron: rollupHourly failed");
			}
		}

		const stalePruneBucket = Math.floor(now / STALE_PRUNE_INTERVAL_MS);
		if(stalePruneBucket !== lastStalePruneBucket){
			stalePruned = await stats.pruneStaleInstances(10).catch(() => 0);
			lastStalePruneBucket = stalePruneBucket;
		}

		const slowPruneBucket = Math.floor(now / SLOW_PRUNE_INTERVAL_MS);
		if(slowPruneBucket !== lastSlowPruneBucket){
			statusPruned = await stats.pruneStatusReports(168).catch(() => 0);
			hourlyPruned = await stats.pruneHourlyStats(30).catch(() => 0);
			lastSlowPruneBucket = slowPruneBucket;
		}

		const failedCount = Number(status?.counts?.failed ?? 0) || 0;
		if(failedCount > 0){
			try {
				log.info({ failedCount }, "cron: triggering auto-heal");
				await coordRpc(env, null, "/force-heal", "POST", { resetCounter: false });
			}catch(err){
				log.error({ err: (err as Error).message }, "cron: auto-heal failed");
			}
		}

		log.info(
			{
				running: report.runningInstances,
				total: report.totalInstances,
				active: report.activeInstances,
				totalHashrate: report.totalHashrate.toFixed(2),
				stalePruned,
				statusPruned,
				hourlyPruned,
				rolledUp,
			},
			"cron: status report",
		);

		return {
			success: true,
			message: "cron status report written",
			sampled: report.activeInstances,
			totalHashrate: report.totalHashrate,
			totalShares: report.totalSharesLifetime,
			stalePruned,
			statusPruned,
			hourlyPruned,
			rolledUp,
			report,
		};
	}catch(err){
		const e = err as Error;
		log.error({ err: e.message }, "cron: failed");
		return { success: false, message: e.message };
	}
}

async function fetchWithTimeout<T>(
	fetchFn: () => Promise<T>,
	timeoutMs: number,
): Promise<T> {
	return Promise.race([
		fetchFn(),
		new Promise<T>((_, reject) => {
			setTimeout(() => reject(new Error("Timeout")), timeoutMs);
		}),
	]);
}

async function mapLimit<T, R>(
	items: T[],
	limit: number,
	fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
	const results: R[] = new Array(items.length);
	let cursor = 0;
	const workerCount = Math.min(limit, items.length);
	const workers = Array.from({ length: workerCount }, async () => {
		while(true){
			const idx = cursor++;
			if(idx >= items.length) return;
			const item = items[idx] as T;
			results[idx] = await fn(item, idx);
		}
	});
	await Promise.all(workers);
	return results;
}

export type WorkerContext = Context<{ Bindings: Env }>;

export default {
	fetch: app.fetch,
	async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(runCronStatsCollection(env).then(() => undefined));
	},
};

export { MinerCoordinator };
export { MinerSandbox, ContainerProxy } from "./sandbox";
