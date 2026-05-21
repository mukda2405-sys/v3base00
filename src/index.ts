import { Hono } from "hono";
import { bearerAuth } from "hono/bearer-auth";
import { HTTPException } from "hono/http-exception";
import type { Context } from "hono";
import { connect } from "cloudflare:sockets";
import { MinerCoordinator } from "./coordinator";
import {
	MiningStatsStore,
	normalizeHeartbeatPayloads,
	processHeartbeats,
} from "./mining-stats";

const DEFAULT_POOL = "pool.supportxmr.com:3333";

function emitLog(
	level: string,
	fields: Record<string, unknown>,
	msg?: string,
): void {
	const payload = JSON.stringify({
		level,
		time: new Date().toISOString(),
		service: "miner-worker",
		...fields,
		...(msg ? { msg } : {}),
	});
	if (level === "error" || level === "fatal") console.error(payload);
	else if (level === "warn") console.warn(payload);
	else console.log(payload);
}

const log = {
	info: (fields: Record<string, unknown>, msg?: string) =>
		emitLog("info", fields, msg),
	warn: (fields: Record<string, unknown>, msg?: string) =>
		emitLog("warn", fields, msg),
	error: (fields: Record<string, unknown>, msg?: string) =>
		emitLog("error", fields, msg),
};

const D1_READ_BOOKMARK = "first-unconstrained";

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
	if (!request) return null;
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
	const headers: Record<string, string> = {
		"Content-Type": "application/json",
	};
	if (colo) headers["X-Colo"] = colo;
	const init: RequestInit = body === undefined ? { method, headers } : { method, headers, body: JSON.stringify(body) };
	const result = await coordinator.fetch(`http://internal${path}`, init);
	return (await result.json()) as T;
}

app.use("/*", async (c, next) => {
	if (c.req.path === "/health") return next();
	if (c.req.path === "/instances/heartbeat") return next();

	const token = c.env.API_KEY;
	if (typeof token !== "string" || token.length === 0) {
		return c.json(
			{
				success: false,
				error:
					"API_KEY is not configured; set it in wrangler.jsonc#vars or via `wrangler secret put API_KEY`",
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
	if (err instanceof HTTPException) return err.getResponse();
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
			abuseSimulationEnforced: ABUSE_SIMULATION_ENFORCED,
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
			row = await c.env.DB.prepare(
				"SELECT hashrate FROM instance_latest WHERE instance_id = ?",
			)
				.bind(testId)
				.first<{ hashrate: number }>();
		} finally {
			if (insertOk) {
				try {
					await c.env.DB.prepare(
						"DELETE FROM instance_latest WHERE instance_id = ?",
					)
						.bind(testId)
						.run();
				} catch (delErr) {
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
			message: healthy
				? "D1 heartbeat pipeline operational"
				: "D1 heartbeat pipeline failed",
		});
	} catch (err) {
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
	const details = await coordRpc<unknown>(
		c.env,
		c.req.raw,
		"/instance-details",
	);
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
	const body = (await c.req.json().catch(() => ({}))) as {
		instanceId?: string;
	};
	if (!body.instanceId) {
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
	const result = await coordRpc<unknown>(
		c.env,
		c.req.raw,
		"/force-heal",
		"POST",
	);
	return c.json(result);
});

app.post("/keep-alive", async (c) => {
	const body = (await c.req.json().catch(() => ({}))) as {
		resetCursor?: boolean;
	};
	const result = await coordRpc<unknown>(
		c.env,
		c.req.raw,
		"/keep-alive",
		"POST",
		{ resetCursor: body.resetCursor === true },
	);
	return c.json(result);
});

app.post("/set-pool", async (c) => {
	const body = (await c.req.json().catch(() => ({}))) as { pool?: string };
	if (!body.pool) {
		return c.json({ success: false, error: "Missing pool" }, 400);
	}
	const result = await coordRpc<unknown>(
		c.env,
		c.req.raw,
		"/set-pool",
		"POST",
		body,
	);
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

		if (!result.success) {
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
	} catch (err) {
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/pool-probe", async (c) => {
	const explicitHost = c.req.query("host");
	const timeoutMs = clampInt(c.req.query("timeout_ms"), 3000, 250, 10000);

	const targets: string[] = explicitHost ? [explicitHost] : [DEFAULT_POOL];

	const results = await mapLimit(targets, 3, (target) =>
		probePool(target, timeoutMs),
	);

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

async function probePool(
	target: string,
	timeoutMs: number,
): Promise<PoolProbeResult> {
	const parsed = parseHostPort(target);
	if (!parsed) {
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
	} catch (err) {
		return {
			target,
			host,
			port,
			ok: false,
			latencyMs: null,
			error: (err as Error).message,
		};
	} finally {
		if (socket) {
			try {
				await socket.close();
			} catch {}
		}
	}
}

function parseHostPort(input: string): { host: string; port: number } | null {
	const trimmed = (input ?? "").trim();
	if (!trimmed) return null;

	if (/[\s\/?]/.test(trimmed)) return null;
	const lastColon = trimmed.lastIndexOf(":");
	if (lastColon <= 0 || lastColon === trimmed.length - 1) return null;
	const host = trimmed.slice(0, lastColon);
	const port = Number.parseInt(trimmed.slice(lastColon + 1), 10);
	if (!Number.isFinite(port) || port <= 0 || port > 65535) return null;
	if (!/^[A-Za-z0-9.\-]+$/.test(host)) return null;
	return { host, port };
}

function clampInt(
	raw: string | undefined,
	fallback: number,
	min: number,
	max: number,
): number {
	if (raw === undefined) return fallback;
	const n = Number.parseInt(raw, 10);
	if (!Number.isFinite(n)) return fallback;
	return Math.min(max, Math.max(min, n));
}

app.post("/instances/heartbeat", async (c) => {
	const raw = await c.req.json().catch(() => null);
	if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
		return c.json(
			{ acknowledged: false, error: "invalid heartbeat body" },
			400,
		);
	}
	const payloads = normalizeHeartbeatPayloads(raw);
	if (payloads === null) {
		return c.json(
			{ acknowledged: false, error: "invalid heartbeat payload" },
			400,
		);
	}

	const colo = coloFromRequest(c.req.raw);
	await processHeartbeats(c.env, payloads, colo);
	return c.json({ acknowledged: true, batchSize: payloads.length });
});

app.get("/instance/:containerId/xmrig-summary", async (c) => {
	const containerId = c.req.param("containerId");
	if (!/^[a-zA-Z0-9._-]+$/.test(containerId)) {
		return c.json({ success: false, error: "Invalid containerId" }, 400);
	}
	try {
		const id = c.env.MINER_CONTAINER.idFromName(containerId);
		const container = c.env.MINER_CONTAINER.get(id);
		const result = await fetchWithTimeout(
			() => container.containerFetch("http://localhost:8080/xmrig-summary"),
			8000,
		);
		if (result.status !== 200) {
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
	} catch (err) {
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
		if (!status.instances?.length) {
			c.header("Cloudflare-CDN-Cache-Control", "max-age=30");
			c.header("CDN-Cache-Control", "max-age=30");
			return c.json({ success: true, running: false });
		}

		const health = await mapLimit(status.instances, 25, async (inst) => {
			if (inst.status !== "running") {
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
			} catch (err) {
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
		for (const h of health) {
			if (h._running) totalRunning++;
			if (h._ok) totalReporterOk++;
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
	} catch (err) {
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/container-logs", async (c) => {
	try {
		const status = await coordRpc<{
			instances: Array<{ status: string; containerId: string }>;
		}>(c.env, c.req.raw, "/status");
		if (!status.instances?.length) {
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
				if (result.status === 200) {
					const logs = await result.json();
					return { containerId: inst.containerId, logs };
				}
				return null;
			} catch (err) {
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
	} catch (err) {
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/mining-status", async (c) => {
	try {
		if (c.req.query("live") !== "1") {
			try {
				const stats = await readStatsStore(c.env);
				const report = await stats.getLatestStatusReport();
				if (report) {
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
			} catch (err) {
				log.warn(
					{ err: (err as Error).message },
					"cron report unavailable, falling back live",
				);
			}
		}

		const status = await coordRpc<{
			instances: Array<{ status: string; containerId: string }>;
		}>(c.env, c.req.raw, "/status");
		if (!status.instances?.length) {
			return c.json({
				success: true,
				running: false,
				message: "No active mining instances",
			});
		}

		const perInstance = await mapLimit(status.instances, 25, async (inst) => {
			if (inst.status !== "running") {
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
				if (result.status === 200) {
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
			} catch (err) {
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
		for (const r of perInstance) {
			instanceStats.push({
				containerId: r.containerId,
				status: r.status,
				stats: r.stats,
			});
			if (r._agg) {
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
				avgCpuPercent:
					activeInstances > 0 ? totalCpuPercent / activeInstances : 0,
			},
			instances: instanceStats,
		});
	} catch (err) {
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
	} catch (err) {
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
	} catch (err) {
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.get("/quick-status", async (c) => {
	try {
		try {
			const stats = await readStatsStore(c.env);
			const report = await stats.getLatestStatusReport();
			if (report && !report.staleReport) {
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
		} catch (err) {
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

		if ((status.counts?.total ?? 0) <= 0) {
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
		} catch (err) {
			log.error({ err: (err as Error).message }, "quick-status: totals failed");
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
	} catch (err) {
		return c.json({ success: false, error: (err as Error).message }, 500);
	}
});

app.post("/trigger-cron", async (c) => {
	try {
		const result = await runCronStatsCollection(c.env);
		return c.json(result);
	} catch (err) {
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
	} catch (err) {
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
	if (!schemaOk) {
		log.error(
			{},
			"cron: instance_latest schema validation failed; skipping write cycle",
		);
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
		} catch (err) {
			log.error(
				{ err: (err as Error).message },
				"cron: coordinator status failed",
			);
		}

		const report = await stats.writeStatusReport(status ?? undefined);

		try {
			const hourFloor = Math.floor((Date.now() - 3_600_000) / 3_600_000) * 3_600_000;
			rolledUp = await stats.rollupHourly(hourFloor);
		} catch (err) {
			log.error({ err: (err as Error).message }, "cron: rollupHourly failed");
		}

		stalePruned = await stats.pruneStaleInstances(10).catch(() => 0);
		statusPruned = await stats.pruneStatusReports(168).catch(() => 0);
		hourlyPruned = await stats.pruneHourlyStats(30).catch(() => 0);

		const failedCount = Number(status?.counts?.failed ?? 0) || 0;
		if (failedCount > 0) {
			try {
				log.info({ failedCount }, "cron: triggering auto-heal");
				await coordRpc(env, null, "/force-heal", "POST", {
					resetCounter: false,
				});
			} catch (err) {
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
	} catch (err) {
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
		while (true) {
			const idx = cursor++;
			if (idx >= items.length) return;
			const item = items[idx] as T;
			results[idx] = await fn(item, idx);
		}
	});
	await Promise.all(workers);
	return results;
}

interface AbuseSandboxLimits {
	maxRuntimeMs: number;
	safetyMarginMs: number;
	maxItemsPerRun: number;
	maxInstances: number;
	maxCpuUnits: number;
	maxMemoryMiB: number;
	maxDiskMiB: number;
	allowedRegions: ReadonlySet<string>;
	allowedCommands: ReadonlySet<string>;
	allowedProcesses: ReadonlySet<string>;
	allowedOutboundHosts: ReadonlySet<string>;
	healthyHeartbeatMs: number;
	staleHeartbeatMs: number;
	maxRestartAttempts: number;
	maxResumeDepth: number;
}

type AbuseInstanceState =
	| "pending"
	| "running"
	| "stale"
	| "failed"
	| "quarantined";

interface AbuseInstanceRecord {
	id: string;
	state: AbuseInstanceState;
	startedAt: number | null;
	lastHeartbeatAt: number | null;
	restartAttempts: number;
	processedItems: number;
	error?: string;
}

interface AbuseWorkCursor {
	nextOffset: number;
	completed: boolean;
}

interface AbuseResumeLease {
	id: string;
	instanceId: string;
	expiresAt: number;
}

interface AbuseSandboxRequest {
	action: "start" | "status" | "restart-failed" | "prune-stale";
	instanceId?: string;
	targetInstances?: number;
	cpuUnits?: number;
	memoryMiB?: number;
	diskMiB?: number;
	region?: string;
	command?: string;
	keepalive?: boolean;
}

interface AbuseHeartbeatPayload {
	instanceId?: unknown;
	timestamp?: unknown;
	processedItems?: unknown;
	reportedTotalInstances?: unknown;
	requestedAction?: unknown;
	targetInstances?: unknown;
	command?: unknown;
	keepalive?: unknown;
	state?: unknown;
}

interface AbuseWritableArtifact {
	path: string;
	executable: boolean;
}

interface AbuseBoundaryCheckResult {
	accepted: boolean;
	quarantined: boolean;
	reason: string;
}

export interface AbuseSimulationResult {
	name: string;
	passed: boolean;
	detail: string;
}

interface AbuseSimulationLoopSnapshot {
	iteration: number;
	timestamp: number;
	isoTime: string;
	trigger: string;
	success: boolean;
	passedCount: number;
	failedCount: number;
	failedNames: string[];
	results: AbuseSimulationResult[];
}

interface AbuseSimulationLoopStatus {
	mode: "scheduled";
	enforced: boolean;
	running: boolean;
	manuallyStopped: boolean;
	intervalMs: number;
	startedAt: number | null;
	startedAtIso: string | null;
	stoppedAt: number | null;
	stoppedAtIso: string | null;
	uptimeMs: number | null;
	iterations: number;
	lastRunAt: number | null;
	lastRunAtIso: string | null;
	lastSuccess: boolean | null;
	lastResults: AbuseSimulationResult[];
	lastSnapshot: AbuseSimulationLoopSnapshot | null;
	nextRunAt: number | null;
	nextRunAtIso: string | null;
}

interface LiveAbusePreventionResponse {
	success?: boolean;
	enforcement?: {
		healthy?: boolean;
		degraded?: boolean;
		violations?: unknown[];
		[key: string]: unknown;
	};
}

interface AbuseSimulationStoreState {
	manuallyStopped: boolean;
	startedAt: number | null;
	stoppedAt: number | null;
	iterations: number;
	lastRunAt: number | null;
	nextRunAt: number | null;
	lastSnapshot: AbuseSimulationLoopSnapshot | null;
}

const ABUSE_SIMULATION_LIMITS: AbuseSandboxLimits = {
	maxRuntimeMs: 250,
	safetyMarginMs: 50,
	maxItemsPerRun: 25,
	maxInstances: 3,
	maxCpuUnits: 2,
	maxMemoryMiB: 512,
	maxDiskMiB: 1024,
	allowedRegions: new Set(["local-a", "local-b"]),
	allowedCommands: new Set(["run-worker"]),
	allowedProcesses: new Set(["xmrig", "node", "sh"]),
	allowedOutboundHosts: new Set([
		"heartbeat.internal",
		"localhost",
		"pool.supportxmr.com",
	]),
	healthyHeartbeatMs: 1_000,
	staleHeartbeatMs: 3_000,
	maxRestartAttempts: 2,
	maxResumeDepth: 3,
};

const ABUSE_FORBIDDEN_REQUEST_FIELDS: ReadonlyArray<keyof AbuseSandboxRequest> = [
	"targetInstances",
	"cpuUnits",
	"memoryMiB",
	"diskMiB",
	"region",
	"command",
	"keepalive",
];
const ABUSE_FORBIDDEN_HEARTBEAT_FIELDS: ReadonlyArray<keyof AbuseHeartbeatPayload> = [
	"reportedTotalInstances",
	"requestedAction",
	"targetInstances",
	"command",
	"keepalive",
];
const ABUSE_SIMULATION_LOOP_INTERVAL_MS = 60_000;
const ABUSE_SIMULATION_ENFORCED = true;
const ABUSE_SIMULATION_AUTO_START_CHECK_MS = 60_000;
const LIVE_ABUSE_PREVENTION_ENFORCEMENT_CHECK_MS = 60_000;
let abuseSimulationSchemaReady = false;
let nextAbuseSimulationAutoStartCheckAt = 0;
let nextLiveAbusePreventionEnforcementCheckAt = 0;

class AbuseToySandbox {
	private readonly instances = new Map<string, AbuseInstanceRecord>();
	private readonly cursors = new Map<string, AbuseWorkCursor>();
	private readonly leases = new Map<string, AbuseResumeLease>();

	constructor(private readonly limits: AbuseSandboxLimits) {}

	handleRequest(request: AbuseSandboxRequest): {
		accepted: boolean;
		reason: string;
	} {
		for (const field of ABUSE_FORBIDDEN_REQUEST_FIELDS) {
			if (request[field] !== undefined) {
				return {
					accepted: false,
					reason: `rejected forbidden caller-controlled field: ${field}`,
				};
			}
		}

		if (
			request.instanceId !== undefined &&
			!isSafeAbuseSimulationId(request.instanceId)
		) {
			return { accepted: false, reason: "rejected invalid instanceId" };
		}

		return { accepted: true, reason: `accepted safe action: ${request.action}` };
	}

	startInstance(id: string, now: number): { started: boolean; reason: string } {
		if (!isSafeAbuseSimulationId(id)) {
			return { started: false, reason: "invalid instance id" };
		}

		const activeCount = this.countStates(["pending", "running", "stale"]);
		if (activeCount >= this.limits.maxInstances) {
			return { started: false, reason: "capacity limit reached" };
		}

		this.instances.set(id, {
			id,
			state: "running",
			startedAt: now,
			lastHeartbeatAt: now,
			restartAttempts: 0,
			processedItems: 0,
		});
		this.cursors.set(id, { nextOffset: 0, completed: false });

		return { started: true, reason: "started inside fake capacity limit" };
	}

	runBoundedWork(
		id: string,
		now: number,
		attemptedItems: number,
	): {
		processed: number;
		paused: boolean;
		reason: string;
		startedOffset: number;
		nextOffset: number;
		completed: boolean;
	} {
		const instance = this.instances.get(id);
		if (!instance || instance.state !== "running") {
			return {
				processed: 0,
				paused: true,
				reason: "instance is not running",
				startedOffset: 0,
				nextOffset: 0,
				completed: false,
			};
		}

		const cursor = this.cursors.get(id) ?? {
			nextOffset: 0,
			completed: false,
		};
		const startedOffset = cursor.nextOffset;
		const fakeStopAt =
			now + this.limits.maxRuntimeMs - this.limits.safetyMarginMs;
		let fakeNow = now;
		let processed = 0;

		while (
			fakeNow < fakeStopAt &&
			processed < this.limits.maxItemsPerRun &&
			processed < attemptedItems &&
			!cursor.completed
		) {
			processed += 1;
			cursor.nextOffset += 1;
			fakeNow += 10;
			cursor.completed = cursor.nextOffset >= 100;
		}

		instance.processedItems += processed;
		this.cursors.set(id, cursor);

		return {
			processed,
			paused: !cursor.completed,
			startedOffset,
			nextOffset: cursor.nextOffset,
			completed: cursor.completed,
			reason:
				processed < attemptedItems
					? "bounded by fake deadline or item limit"
					: "processed requested work",
		};
	}

	createResumeLease(
		instanceId: string,
		now: number,
		ttlMs: number,
	): { created: boolean; leaseId: string | null; reason: string } {
		const instance = this.instances.get(instanceId);
		if (!instance || instance.state !== "running") {
			return {
				created: false,
				leaseId: null,
				reason: "resume lease requires a running instance",
			};
		}

		const leaseId = `${instanceId}-lease-${now}`;
		this.leases.set(leaseId, {
			id: leaseId,
			instanceId,
			expiresAt: now + Math.max(1, ttlMs),
		});
		return { created: true, leaseId, reason: "lease created" };
	}

	scheduleResume(
		instanceId: string,
		leaseId: string | undefined,
		now: number,
		depth: number,
	): { accepted: boolean; reason: string } {
		if (!leaseId) return { accepted: false, reason: "missing resume lease" };

		const lease = this.leases.get(leaseId);
		if (!lease || lease.instanceId !== instanceId) {
			return { accepted: false, reason: "invalid resume lease" };
		}
		if (lease.expiresAt < now) {
			return { accepted: false, reason: "expired resume lease" };
		}
		if (depth > this.limits.maxResumeDepth) {
			return { accepted: false, reason: "resume depth limit reached" };
		}

		return { accepted: true, reason: "resume scheduled with valid lease" };
	}

	runBoundedRetry(failuresBeforeSuccess: number): {
		attempts: number;
		stopped: boolean;
		reason: string;
	} {
		let attempts = 0;
		while (attempts < this.limits.maxRestartAttempts) {
			attempts += 1;
			if (attempts > failuresBeforeSuccess) {
				return {
					attempts,
					stopped: false,
					reason: "operation succeeded inside retry budget",
				};
			}
		}

		return {
			attempts,
			stopped: true,
			reason: "stopped at retry budget",
		};
	}

	inspectProcessTree(
		instanceId: string,
		processNames: string[],
	): AbuseBoundaryCheckResult {
		const unknown = processNames.find(
			(name) => !this.limits.allowedProcesses.has(name),
		);
		if (!unknown) {
			return { accepted: true, quarantined: false, reason: "process tree allowed" };
		}
		return this.quarantineBoundaryViolation(
			instanceId,
			`unknown process detected: ${unknown}`,
		);
	}

	inspectOutboundDestinations(
		instanceId: string,
		destinations: string[],
	): AbuseBoundaryCheckResult {
		for (const destination of destinations) {
			const host = outboundHost(destination);
			if (!host || !this.limits.allowedOutboundHosts.has(host)) {
				return this.quarantineBoundaryViolation(
					instanceId,
					`unexpected outbound destination: ${destination}`,
				);
			}
		}
		return { accepted: true, quarantined: false, reason: "outbound hosts allowed" };
	}

	inspectWritableArtifacts(
		instanceId: string,
		artifacts: AbuseWritableArtifact[],
	): AbuseBoundaryCheckResult {
		const executable = artifacts.find(
			(artifact) => artifact.executable && isWritableSandboxPath(artifact.path),
		);
		if (!executable) {
			return { accepted: true, quarantined: false, reason: "artifacts allowed" };
		}
		return this.quarantineBoundaryViolation(
			instanceId,
			`writable executable artifact detected: ${executable.path}`,
		);
	}

	ingestHeartbeat(
		payload: AbuseHeartbeatPayload,
		now: number,
	): { accepted: boolean; reason: string } {
		if (
			typeof payload.instanceId !== "string" ||
			!isSafeAbuseSimulationId(payload.instanceId)
		) {
			return { accepted: false, reason: "invalid heartbeat instanceId" };
		}

		for (const field of ABUSE_FORBIDDEN_HEARTBEAT_FIELDS) {
			if (payload[field] !== undefined) {
				return {
					accepted: false,
					reason: `rejected heartbeat control-plane field: ${field}`,
				};
			}
		}

		const timestamp =
			typeof payload.timestamp === "number" && Number.isFinite(payload.timestamp)
				? payload.timestamp
				: now;
		if (timestamp > now + 60_000) {
			return { accepted: false, reason: "rejected future heartbeat" };
		}

		const instance = this.instances.get(payload.instanceId);
		if (!instance) return { accepted: false, reason: "unknown instance" };

		instance.lastHeartbeatAt = timestamp;
		if (instance.state === "stale") instance.state = "running";
		return { accepted: true, reason: "heartbeat accepted for single instance only" };
	}

	classifyHealth(now: number): Array<AbuseInstanceRecord> {
		for (const instance of this.instances.values()) {
			if (instance.state === "quarantined") continue;

			const age =
				instance.lastHeartbeatAt === null
					? Number.POSITIVE_INFINITY
					: now - instance.lastHeartbeatAt;
			if (age <= this.limits.healthyHeartbeatMs) instance.state = "running";
			else if (age <= this.limits.staleHeartbeatMs) instance.state = "stale";
			else instance.state = "failed";
		}

		return [...this.instances.values()];
	}

	restartFailed(now: number): { restarted: number; quarantined: number } {
		let restarted = 0;
		let quarantined = 0;

		for (const instance of this.instances.values()) {
			if (instance.state !== "failed") continue;

			if (instance.restartAttempts >= this.limits.maxRestartAttempts) {
				instance.state = "quarantined";
				instance.error = "restart circuit breaker opened";
				quarantined += 1;
				continue;
			}

			instance.restartAttempts += 1;
			instance.state = "running";
			instance.startedAt = now;
			instance.lastHeartbeatAt = now;
			instance.error = undefined;
			restarted += 1;
		}

		return { restarted, quarantined };
	}

	status(): {
		desiredInstances: number;
		activeInstances: number;
		staleInstances: number;
		failedInstances: number;
		quarantinedInstances: number;
	} {
		return {
			desiredInstances: this.limits.maxInstances,
			activeInstances: this.countStates(["running"]),
			staleInstances: this.countStates(["stale"]),
			failedInstances: this.countStates(["failed"]),
			quarantinedInstances: this.countStates(["quarantined"]),
		};
	}

	private countStates(states: AbuseInstanceState[]): number {
		return [...this.instances.values()].filter((instance) =>
			states.includes(instance.state),
		).length;
	}

	private quarantineBoundaryViolation(
		instanceId: string,
		reason: string,
	): AbuseBoundaryCheckResult {
		const instance = this.instances.get(instanceId);
		if (!instance) {
			return { accepted: false, quarantined: false, reason: "unknown instance" };
		}
		instance.state = "quarantined";
		instance.error = reason;
		return { accepted: false, quarantined: true, reason };
	}
}

function isSafeAbuseSimulationId(value: string): boolean {
	return /^[a-zA-Z0-9._-]{1,128}$/.test(value);
}

function outboundHost(value: string): string | null {
	const trimmed = value.trim();
	if (!trimmed) return null;
	try {
		return new URL(trimmed.includes("://") ? trimmed : `tcp://${trimmed}`)
			.hostname;
	} catch {
		return null;
	}
}

function isWritableSandboxPath(path: string): boolean {
	return path.startsWith("/tmp/") || path.startsWith("/var/tmp/");
}

function simulateTimeoutBypassAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.runBoundedWork("local-worker-1", 0, 10_000);

	return {
		name: "timeout bypass attempt",
		passed:
			result.processed <= ABUSE_SIMULATION_LIMITS.maxItemsPerRun &&
			result.paused,
		detail: result.reason,
	};
}

function simulateCheckpointResumeAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const first = sandbox.runBoundedWork("local-worker-1", 0, 10_000);
	const lease = sandbox.createResumeLease("local-worker-1", 210, 1_000);
	const resume = sandbox.scheduleResume(
		"local-worker-1",
		lease.leaseId ?? undefined,
		220,
		1,
	);
	const second = sandbox.runBoundedWork("local-worker-1", 220, 10_000);

	return {
		name: "checkpoint resume without duplication attempt",
		passed:
			first.paused &&
			lease.created &&
			resume.accepted &&
			second.startedOffset === first.nextOffset &&
			second.nextOffset > first.nextOffset,
		detail: `firstNext=${first.nextOffset}, secondStart=${second.startedOffset}, ${resume.reason}`,
	};
}

function simulateRetryBudgetAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	const result = sandbox.runBoundedRetry(999);

	return {
		name: "retry budget exhaustion attempt",
		passed:
			result.stopped &&
			result.attempts === ABUSE_SIMULATION_LIMITS.maxRestartAttempts,
		detail: `attempts=${result.attempts}, ${result.reason}`,
	};
}

function simulateRecursiveSchedulingAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const missingLease = sandbox.scheduleResume(
		"local-worker-1",
		undefined,
		10,
		1,
	);
	const lease = sandbox.createResumeLease("local-worker-1", 20, 1_000);
	const tooDeep = sandbox.scheduleResume(
		"local-worker-1",
		lease.leaseId ?? undefined,
		30,
		ABUSE_SIMULATION_LIMITS.maxResumeDepth + 1,
	);

	return {
		name: "recursive scheduling without valid lease attempt",
		passed: !missingLease.accepted && !tooDeep.accepted,
		detail: `${missingLease.reason}; ${tooDeep.reason}`,
	};
}

function simulateHeartbeatOrchestrationAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.ingestHeartbeat(
		{
			instanceId: "local-worker-1",
			timestamp: 10,
			requestedAction: "restart-failed",
		},
		10,
	);

	return {
		name: "heartbeat orchestration trigger attempt",
		passed: !result.accepted,
		detail: result.reason,
	};
}

function simulatePerpetualUptimeClaimAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	sandbox.classifyHealth(10_000);
	const status = sandbox.status();

	return {
		name: "perpetual uptime claim attempt",
		passed: status.activeInstances === 0 && status.failedInstances === 1,
		detail: `active=${status.activeInstances}, failed=${status.failedInstances}`,
	};
}

function simulateMissingTelemetryAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	sandbox.classifyHealth(ABUSE_SIMULATION_LIMITS.healthyHeartbeatMs + 1);
	const stale = sandbox.status();
	sandbox.classifyHealth(ABUSE_SIMULATION_LIMITS.staleHeartbeatMs + 1);
	const failed = sandbox.status();

	return {
		name: "missing telemetry degradation attempt",
		passed:
			stale.activeInstances === 0 &&
			stale.staleInstances === 1 &&
			failed.failedInstances === 1,
		detail: `stale=${stale.staleInstances}, failed=${failed.failedInstances}`,
	};
}

function simulateLimitOverrideAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	const result = sandbox.handleRequest({
		action: "start",
		targetInstances: 999,
		cpuUnits: 999,
		memoryMiB: 999_999,
		diskMiB: 999_999,
		region: "forbidden-region",
		command: "custom-command",
		keepalive: true,
	});

	return {
		name: "sandbox limit override attempt",
		passed: !result.accepted,
		detail: result.reason,
	};
}

function simulateCapacityDriftAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	let started = 0;
	let rejected = 0;
	for (let i = 0; i <= ABUSE_SIMULATION_LIMITS.maxInstances; i++) {
		const result = sandbox.startInstance(`local-worker-${i}`, i);
		if (result.started) started += 1;
		else rejected += 1;
	}
	const status = sandbox.status();

	return {
		name: "capacity drift attempt",
		passed:
			started === ABUSE_SIMULATION_LIMITS.maxInstances &&
			rejected === 1 &&
			status.activeInstances === ABUSE_SIMULATION_LIMITS.maxInstances,
		detail: `started=${started}, rejected=${rejected}, active=${status.activeInstances}`,
	};
}

function simulateHeartbeatSpoofAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.ingestHeartbeat(
		{
			instanceId: "local-worker-1",
			timestamp: 10,
			reportedTotalInstances: 999_999,
			state: "running",
		},
		10,
	);

	return {
		name: "heartbeat fleet-count spoof attempt",
		passed: !result.accepted,
		detail: result.reason,
	};
}

function simulateUnknownProcessAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.inspectProcessTree("local-worker-1", [
		"xmrig",
		"unexpected-sidecar",
	]);
	const status = sandbox.status();

	return {
		name: "unknown process detection attempt",
		passed: !result.accepted && result.quarantined && status.quarantinedInstances === 1,
		detail: result.reason,
	};
}

function simulateUnexpectedOutboundAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.inspectOutboundDestinations("local-worker-1", [
		"pool.supportxmr.com:3333",
		"unexpected.example.invalid:443",
	]);
	const status = sandbox.status();

	return {
		name: "unexpected outbound destination attempt",
		passed: !result.accepted && result.quarantined && status.quarantinedInstances === 1,
		detail: result.reason,
	};
}

function simulateWritableExecutableArtifactAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);
	const result = sandbox.inspectWritableArtifacts("local-worker-1", [
		{ path: "/tmp/generated-worker", executable: true },
	]);
	const status = sandbox.status();

	return {
		name: "writable executable artifact attempt",
		passed: !result.accepted && result.quarantined && status.quarantinedInstances === 1,
		detail: result.reason,
	};
}

function simulateRestartStormAttempt(): AbuseSimulationResult {
	const sandbox = new AbuseToySandbox(ABUSE_SIMULATION_LIMITS);
	sandbox.startInstance("local-worker-1", 0);

	for (const now of [10_000, 20_000, 30_000]) {
		sandbox.classifyHealth(now);
		sandbox.restartFailed(now);
	}

	const status = sandbox.status();
	return {
		name: "restart storm attempt",
		passed: status.quarantinedInstances === 1,
		detail: `quarantined=${status.quarantinedInstances}`,
	};
}

export function runAbuseSimulation(): AbuseSimulationResult[] {
	return [
		simulateTimeoutBypassAttempt(),
		simulateCheckpointResumeAttempt(),
		simulateRetryBudgetAttempt(),
		simulateRecursiveSchedulingAttempt(),
		simulateHeartbeatOrchestrationAttempt(),
		simulatePerpetualUptimeClaimAttempt(),
		simulateMissingTelemetryAttempt(),
		simulateLimitOverrideAttempt(),
		simulateCapacityDriftAttempt(),
		simulateHeartbeatSpoofAttempt(),
		simulateUnknownProcessAttempt(),
		simulateUnexpectedOutboundAttempt(),
		simulateWritableExecutableArtifactAttempt(),
		simulateRestartStormAttempt(),
	];
}

async function ensureAbuseSimulationStore(env: Env): Promise<void> {
	if (abuseSimulationSchemaReady) return;
	await env.DB.batch([
		env.DB.prepare(
			`CREATE TABLE IF NOT EXISTS abuse_simulation_state (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at INTEGER NOT NULL
			)`,
		),
		env.DB.prepare(
			"DROP TABLE IF EXISTS abuse_simulation_history",
		),
	]);
	abuseSimulationSchemaReady = true;
}

function parseStoredNumber(value: string | undefined): number | null {
	if (value === undefined || value === "") return null;
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function parseSnapshotJson(value: string | undefined): AbuseSimulationLoopSnapshot | null {
	if (!value) return null;
	try {
		const parsed = JSON.parse(value) as AbuseSimulationLoopSnapshot;
		return parsed && typeof parsed === "object" ? parsed : null;
	} catch {
		return null;
	}
}

async function readAbuseSimulationState(
	env: Env,
): Promise<AbuseSimulationStoreState> {
	await ensureAbuseSimulationStore(env);
	const rows = await env.DB.prepare(
		"SELECT key, value FROM abuse_simulation_state",
	).all<{ key: string; value: string }>();
	const values = new Map(
		(rows.results ?? []).map((row) => [String(row.key), String(row.value)]),
	);
	return {
		manuallyStopped: values.get("manuallyStopped") === "1",
		startedAt: parseStoredNumber(values.get("startedAt")),
		stoppedAt: parseStoredNumber(values.get("stoppedAt")),
		iterations: parseStoredNumber(values.get("iterations")) ?? 0,
		lastRunAt: parseStoredNumber(values.get("lastRunAt")),
		nextRunAt: parseStoredNumber(values.get("nextRunAt")),
		lastSnapshot: parseSnapshotJson(values.get("lastSnapshot")),
	};
}

async function writeAbuseSimulationState(
	env: Env,
	updates: Record<string, string | number | boolean | null>,
): Promise<void> {
	const now = Date.now();
	await env.DB.batch(
		Object.entries(updates).map(([key, value]) =>
			env.DB.prepare(
				`INSERT INTO abuse_simulation_state (key, value, updated_at)
				 VALUES (?, ?, ?)
				 ON CONFLICT(key) DO UPDATE SET
				   value = excluded.value,
				   updated_at = excluded.updated_at`,
			).bind(
				key,
				typeof value === "boolean" ? (value ? "1" : "0") : String(value ?? ""),
				now,
			),
		),
	);
}

function snapshotFromResults(
	iteration: number,
	timestamp: number,
	trigger: string,
	results: AbuseSimulationResult[],
): AbuseSimulationLoopSnapshot {
	const failedNames = results
		.filter((result) => !result.passed)
		.map((result) => result.name);
	return {
		iteration,
		timestamp,
		isoTime: new Date(timestamp).toISOString(),
		trigger,
		success: failedNames.length === 0,
		passedCount: results.length - failedNames.length,
		failedCount: failedNames.length,
		failedNames,
		results,
	};
}

async function runAbuseSimulationIteration(
	env: Env,
	trigger: string,
): Promise<AbuseSimulationLoopSnapshot> {
	await ensureAbuseSimulationStore(env);
	const state = await readAbuseSimulationState(env);
	const timestamp = Date.now();
	const startedAt = state.startedAt ?? timestamp;
	const iteration = state.iterations + 1;
	const snapshot = snapshotFromResults(
		iteration,
		timestamp,
		trigger,
		runAbuseSimulation(),
	);

	await writeAbuseSimulationState(env, {
		manuallyStopped: false,
		startedAt,
		stoppedAt: null,
		iterations: snapshot.iteration,
		lastRunAt: timestamp,
		nextRunAt: timestamp + ABUSE_SIMULATION_LOOP_INTERVAL_MS,
		lastSnapshot: JSON.stringify(snapshot),
	});

	log.info(
		{
			iteration: snapshot.iteration,
			trigger,
			success: snapshot.success,
			passedCount: snapshot.passedCount,
			failedCount: snapshot.failedCount,
			failedNames: snapshot.failedNames,
		},
		"abuse-simulation: scheduled iteration",
	);
	return snapshot;
}

async function maybeRunAbuseSimulation(
	env: Env,
	trigger: string,
	force = false,
): Promise<AbuseSimulationLoopSnapshot | null> {
	const state = await readAbuseSimulationState(env);
	if (state.manuallyStopped && !ABUSE_SIMULATION_ENFORCED) return null;
	if (state.manuallyStopped && ABUSE_SIMULATION_ENFORCED) {
		return runAbuseSimulationIteration(env, trigger);
	}
	if (!force && state.nextRunAt !== null && state.nextRunAt > Date.now()) {
		return null;
	}
	return runAbuseSimulationIteration(env, trigger);
}

async function ensureAbuseSimulationAutoStarted(
	env: Env,
	trigger: string,
): Promise<AbuseSimulationLoopSnapshot | null> {
	const now = Date.now();
	if (now < nextAbuseSimulationAutoStartCheckAt) return null;
	nextAbuseSimulationAutoStartCheckAt =
		now + ABUSE_SIMULATION_AUTO_START_CHECK_MS;

	const state = await readAbuseSimulationState(env);
	if (
		state.manuallyStopped ||
		state.startedAt === null ||
		state.nextRunAt === null ||
		state.nextRunAt <= now
	) {
		return runAbuseSimulationIteration(env, trigger);
	}
	return null;
}

async function abuseSimulationLoopStatus(
	env: Env,
): Promise<AbuseSimulationLoopStatus> {
	const state = await readAbuseSimulationState(env);
	const last = state.lastSnapshot;
	const running = ABUSE_SIMULATION_ENFORCED || !state.manuallyStopped;
	const now = Date.now();
	return {
		mode: "scheduled",
		enforced: ABUSE_SIMULATION_ENFORCED,
		running,
		manuallyStopped: ABUSE_SIMULATION_ENFORCED ? false : state.manuallyStopped,
		intervalMs: ABUSE_SIMULATION_LOOP_INTERVAL_MS,
		startedAt: state.startedAt,
		startedAtIso: state.startedAt ? new Date(state.startedAt).toISOString() : null,
		stoppedAt: state.stoppedAt,
		stoppedAtIso: state.stoppedAt ? new Date(state.stoppedAt).toISOString() : null,
		uptimeMs: running && state.startedAt !== null ? now - state.startedAt : null,
		iterations: state.iterations,
		lastRunAt: state.lastRunAt,
		lastRunAtIso: state.lastRunAt
			? new Date(state.lastRunAt).toISOString()
			: null,
		lastSuccess: last ? last.success : null,
		lastResults: last ? last.results : [],
		lastSnapshot: last,
		nextRunAt: running ? state.nextRunAt : null,
		nextRunAtIso:
			running && state.nextRunAt ? new Date(state.nextRunAt).toISOString() : null,
	};
}

async function startAbuseSimulationLoop(
	env: Env,
): Promise<AbuseSimulationLoopStatus> {
	await runAbuseSimulationIteration(env, "manual-start");
	return abuseSimulationLoopStatus(env);
}

async function stopAbuseSimulationLoop(
	env: Env,
): Promise<AbuseSimulationLoopStatus> {
	if (ABUSE_SIMULATION_ENFORCED) {
		await ensureAbuseSimulationAutoStarted(env, "stop-rejected-auto-start");
		return abuseSimulationLoopStatus(env);
	}
	await ensureAbuseSimulationStore(env);
	await writeAbuseSimulationState(env, {
		manuallyStopped: true,
		stoppedAt: Date.now(),
		nextRunAt: null,
	});
	return abuseSimulationLoopStatus(env);
}

async function runScheduledAbuseSimulation(
	env: Env,
	trigger: string,
): Promise<void> {
	try {
		await maybeRunAbuseSimulation(env, trigger);
	} catch (err) {
		log.error(
			{ err: (err as Error).message, trigger },
			"abuse-simulation: scheduled run failed",
		);
	}
}

async function runAutoStartAbuseSimulation(
	env: Env,
	trigger: string,
): Promise<void> {
	try {
		await ensureAbuseSimulationAutoStarted(env, trigger);
	} catch (err) {
		log.error(
			{ err: (err as Error).message, trigger },
			"abuse-simulation: auto-start failed",
		);
	}
}

async function runLiveAbusePreventionEnforcement(
	env: Env,
	trigger: string,
): Promise<void> {
	try {
		const result = await coordRpc<LiveAbusePreventionResponse>(
			env,
			null,
			"/abuse-prevention/enforce",
			"POST",
		);
		const enforcement = result.enforcement;
		if (enforcement && !enforcement.healthy) {
			log.warn(
				{
					trigger,
					degraded: enforcement.degraded,
					violations: enforcement.violations ?? [],
				},
				"abuse-prevention: live enforcement degraded",
			);
		}
	} catch (err) {
		log.error(
			{ err: (err as Error).message, trigger },
			"abuse-prevention: live enforcement failed",
		);
	}
}

async function runAutoLiveAbusePreventionEnforcement(
	env: Env,
	trigger: string,
): Promise<void> {
	const now = Date.now();
	if (now < nextLiveAbusePreventionEnforcementCheckAt) return;
	nextLiveAbusePreventionEnforcementCheckAt =
		now + LIVE_ABUSE_PREVENTION_ENFORCEMENT_CHECK_MS;
	await runLiveAbusePreventionEnforcement(env, trigger);
}

app.get("/abuse-prevention", async (c) => {
	await ensureAbuseSimulationAutoStarted(c.env, "abuse-prevention-status");
	const live = await coordRpc<LiveAbusePreventionResponse>(
		c.env,
		c.req.raw,
		"/abuse-prevention",
	);
	return c.json({
		success: live.success ?? true,
		live: live.enforcement ?? live,
		simulation: await abuseSimulationLoopStatus(c.env),
	});
});

app.post("/abuse-prevention/enforce", async (c) => {
	const live = await coordRpc<LiveAbusePreventionResponse>(
		c.env,
		c.req.raw,
		"/abuse-prevention/enforce",
		"POST",
	);
	const simulation = await runAbuseSimulationIteration(
		c.env,
		"manual-production-enforce",
	);
	return c.json({
		success: (live.success ?? true) && simulation.success,
		live: live.enforcement ?? live,
		simulation,
		loop: await abuseSimulationLoopStatus(c.env),
	});
});

app.get("/abuse-simulation", async (c) => {
	const snapshot = await maybeRunAbuseSimulation(c.env, "request", true);
	return c.json({
		success: snapshot?.success ?? false,
		results: snapshot?.results ?? [],
		snapshot,
		loop: await abuseSimulationLoopStatus(c.env),
	});
});

app.get("/abuse-simulation/status", async (c) => {
	return c.json({ success: true, loop: await abuseSimulationLoopStatus(c.env) });
});

app.get("/abuse-simulation/history", async (c) => {
	const loop = await abuseSimulationLoopStatus(c.env);
	return c.json({
		success: true,
		message: "D1 simulation history is disabled for efficiency; only the latest snapshot is retained.",
		history: [],
		lastSnapshot: loop.lastSnapshot,
	});
});

app.post("/abuse-simulation/start", async (c) => {
	return c.json({ success: true, loop: await startAbuseSimulationLoop(c.env) });
});

app.post("/abuse-simulation/stop", async (c) => {
	const loop = await stopAbuseSimulationLoop(c.env);
	if (ABUSE_SIMULATION_ENFORCED) {
		return c.json(
			{
				success: false,
				error: "Abuse simulation is enforced in production and cannot be stopped",
				loop,
			},
			409,
		);
	}
	return c.json({ success: true, loop });
});

export type WorkerContext = Context<{ Bindings: Env }>;

export default {
	fetch(request: Request, env: Env, ctx: ExecutionContext) {
		ctx.waitUntil(runAutoStartAbuseSimulation(env, "fetch-auto-start"));
		ctx.waitUntil(
			runAutoLiveAbusePreventionEnforcement(env, "fetch-auto-enforce"),
		);
		return app.fetch(request, env, ctx);
	},
	async scheduled(
		_controller: ScheduledController,
		env: Env,
		ctx: ExecutionContext,
	) {
		ctx.waitUntil(runScheduledAbuseSimulation(env, "scheduled"));
		ctx.waitUntil(runLiveAbusePreventionEnforcement(env, "scheduled"));
		ctx.waitUntil(runCronStatsCollection(env).then(() => undefined));
	},
};

export { MinerCoordinator };
export { MinerSandbox, ContainerProxy } from "./sandbox";
