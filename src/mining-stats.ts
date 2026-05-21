export interface MiningStatsPayload {
	timestamp: number;
	instanceId?: string;
	hashrate?: number;
	sharesAccepted?: number;
	sharesRejected?: number;
	cpuPercent?: number;
	pool?: string;
	connectionStatus?: string;
}

export interface MiningTotals {
	totalShares: number;
	totalSharesLifetime: number;
	totalSharesRejected: number;
	totalSharesRejectedLifetime: number;
	rejectionRate: number;
	totalHashrate: number;
	averageHashrate: number;
	peakHashrate: number;
	cumulativeUptimeSeconds: number;
	totalRecords: number;
	activeInstances: number;
	sharesPerSecond: number;
}

export interface MiningStatusReport extends MiningTotals {
	timestamp: number;
	totalInstances: number;
	runningInstances: number;
	stoppedInstances: number;
	operation: string;
	config?: unknown;
	staleReport?: boolean;
	reportAgeMs?: number;
}

type D1Like = D1Database | D1DatabaseSession;

const STALE_INSTANCE_MS = 10 * 60 * 1000;
const STATUS_REPORT_RETENTION_HOURS = 168;
const HOURLY_STATS_RETENTION_DAYS = 30;
const STATEMENT_LIMIT = 25;
const MAX_HEARTBEAT_BATCH_SIZE = 100;
const MAX_HEARTBEAT_FUTURE_SKEW_MS = 60_000;
const HEARTBEAT_FLEET_FIELDS = new Set([
	"reportedTotalInstances",
	"reported_total_instances",
	"totalInstances",
	"total_instances",
	"activeInstances",
	"active_instances",
	"runningInstances",
	"running_instances",
	"desiredInstances",
	"desired_instances",
	"maxInstances",
	"max_instances",
]);

let schemaReady = false;

export class MiningStatsStore {
	constructor(private readonly db: D1Like) {}

	static async ensureReady(db: D1Like): Promise<void> {
		await ensureSchema(db);
	}

	async recordStats(payload: MiningStatsPayload): Promise<void> {
		await this.recordStatsBatch([payload]);
	}

	async recordStatsBatch(payloads: MiningStatsPayload[]): Promise<void> {
		if (payloads.length === 0) return;
		await ensureSchema(this.db);

		for (let i = 0; i < payloads.length; i += STATEMENT_LIMIT) {
			const chunk = payloads.slice(i, i + STATEMENT_LIMIT);
			const statements = chunk.map((payload) => {
				const now = Date.now();
				const timestamp = toTimestamp(payload.timestamp, now);
				const sharesAccepted = toNonNegativeInteger(payload.sharesAccepted);
				const sharesRejected = toNonNegativeInteger(payload.sharesRejected);

				return this.db
					.prepare(
						`INSERT INTO instance_latest
						 (instance_id, timestamp, hashrate, shares_accepted, shares_rejected,
						  cpu_percent, pool, connection_status, updated_at, started_at,
						  shares_accepted_lifetime, shares_rejected_lifetime)
						 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						 ON CONFLICT(instance_id) DO UPDATE SET
						   timestamp = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.timestamp ELSE instance_latest.timestamp END,
						   hashrate = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.hashrate ELSE instance_latest.hashrate END,
						   shares_accepted = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.shares_accepted ELSE instance_latest.shares_accepted END,
						   shares_rejected = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.shares_rejected ELSE instance_latest.shares_rejected END,
						   cpu_percent = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.cpu_percent ELSE instance_latest.cpu_percent END,
						   pool = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.pool ELSE instance_latest.pool END,
						   connection_status = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.connection_status ELSE instance_latest.connection_status END,
						   updated_at = CASE WHEN excluded.timestamp >= instance_latest.timestamp THEN excluded.updated_at ELSE instance_latest.updated_at END,
						   started_at = CASE WHEN COALESCE(instance_latest.started_at, 0) > 0 THEN instance_latest.started_at ELSE excluded.started_at END,
						   shares_accepted_lifetime = COALESCE(instance_latest.shares_accepted_lifetime, 0)
						     + CASE WHEN excluded.timestamp >= instance_latest.timestamp AND excluded.shares_accepted >= instance_latest.shares_accepted
						            THEN excluded.shares_accepted - instance_latest.shares_accepted
						            ELSE 0
						       END,
						   shares_rejected_lifetime = COALESCE(instance_latest.shares_rejected_lifetime, 0)
						     + CASE WHEN excluded.timestamp >= instance_latest.timestamp AND excluded.shares_rejected >= instance_latest.shares_rejected
						            THEN excluded.shares_rejected - instance_latest.shares_rejected
						            ELSE 0
						       END`,
					)
					.bind(
						cleanText(payload.instanceId, "unknown", 128),
						timestamp,
						toNonNegativeNumber(payload.hashrate),
						sharesAccepted,
						sharesRejected,
						toNullablePercent(payload.cpuPercent),
						cleanNullableText(payload.pool, 256),
						cleanNullableText(payload.connectionStatus, 64),
						now,
						timestamp,
						sharesAccepted,
						sharesRejected,
					);
			});
			await this.db.batch(statements);
		}
	}

	async getHistory(limit = 100): Promise<unknown[]> {
		await ensureSchema(this.db);
		const safeLimit = Math.min(Math.max(Math.trunc(limit) || 100, 1), 1000);
		const r = await this.db
			.prepare(
				`SELECT
				   timestamp,
				   '__cluster__' AS instance_id,
				   total_hashrate AS hashrate,
				   total_shares_lifetime AS shares_accepted,
				   total_shares_rejected_lifetime AS shares_rejected,
				   active_instances,
				   running_instances,
				   total_instances,
				   average_hashrate,
				   peak_hashrate,
				   rejection_rate,
				   cumulative_uptime_seconds,
				   shares_per_second,
				   operation
				 FROM mining_status_reports
				 ORDER BY timestamp DESC
				 LIMIT ?`,
			)
			.bind(safeLimit)
			.all();
		return r.results ?? [];
	}

	async getTotals(): Promise<MiningTotals> {
		await ensureSchema(this.db);
		const staleCutoff = Date.now() - STALE_INSTANCE_MS;
		const row = await this.db
			.prepare(
				`SELECT
				   COALESCE(SUM(shares_accepted), 0)                                                                            AS totalShares,
				   COALESCE(SUM(shares_rejected), 0)                                                                            AS totalSharesRejected,
				   COALESCE(SUM(shares_accepted_lifetime), 0)                                                                   AS totalSharesLifetime,
				   COALESCE(SUM(shares_rejected_lifetime), 0)                                                                   AS totalSharesRejectedLifetime,
				   COALESCE(SUM(hashrate), 0)                                                                                   AS totalHashrate,
				   COALESCE(AVG(CASE WHEN hashrate > 0 THEN hashrate END), 0)                                                   AS averageHashrate,
				   COALESCE(MAX(hashrate), 0)                                                                                   AS peakHashrate,
				   COALESCE(SUM(CASE WHEN hashrate > 0 AND updated_at > started_at THEN updated_at - started_at ELSE 0 END), 0) AS cumulativeUptimeMs,
				   COUNT(*)                                                                                                     AS totalRecords,
				   COALESCE(SUM(CASE WHEN hashrate > 0 THEN 1 ELSE 0 END), 0)                                                   AS activeInstances
				 FROM instance_latest
				 WHERE updated_at >= ?`,
			)
			.bind(staleCutoff)
			.first<Record<string, unknown>>();

		const num = (k: string): number => Number(row?.[k] ?? 0) || 0;
		const totalSharesLifetime = num("totalSharesLifetime");
		const totalSharesRejectedLifetime = num("totalSharesRejectedLifetime");
		const attempts = totalSharesLifetime + totalSharesRejectedLifetime;
		const cumulativeUptimeSeconds = Math.floor(
			num("cumulativeUptimeMs") / 1000,
		);

		return {
			totalShares: num("totalShares"),
			totalSharesLifetime,
			totalSharesRejected: num("totalSharesRejected"),
			totalSharesRejectedLifetime,
			rejectionRate:
				attempts > 0 ? (totalSharesRejectedLifetime / attempts) * 100 : 0,
			totalHashrate: num("totalHashrate"),
			averageHashrate: num("averageHashrate"),
			peakHashrate: num("peakHashrate"),
			cumulativeUptimeSeconds,
			totalRecords: num("totalRecords"),
			activeInstances: num("activeInstances"),
			sharesPerSecond:
				cumulativeUptimeSeconds > 0
					? totalSharesLifetime / cumulativeUptimeSeconds
					: 0,
		};
	}

	async writeStatusReport(coordinatorStatus?: {
		counts?: Record<string, number>;
		targetInstances?: number;
		operation?: string;
		config?: unknown;
	}): Promise<MiningStatusReport> {
		await ensureSchema(this.db);

		const totals = await this.getTotals();
		const timestamp = Date.now();
		const counts = coordinatorStatus?.counts ?? {};
		const totalInstances = Number(counts.total ?? coordinatorStatus?.targetInstances ?? totals.totalRecords) || 0;
		const runningInstances = Number(counts.running ?? totals.activeInstances) || 0;
		const stoppedInstances = Math.max(0, totalInstances - runningInstances);
		const operation = cleanText(coordinatorStatus?.operation, "unknown", 32);
		const configJson = coordinatorStatus?.config
			? JSON.stringify(coordinatorStatus.config).slice(0, 4096)
			: null;

		await this.db
			.prepare(
				`INSERT INTO mining_status_reports
				 (timestamp, total_instances, running_instances, stopped_instances, active_instances,
				  total_records, total_hashrate, average_hashrate, peak_hashrate, total_shares,
				  total_shares_lifetime, total_shares_rejected, total_shares_rejected_lifetime,
				  rejection_rate, cumulative_uptime_seconds, shares_per_second, operation, config_json)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			)
			.bind(
				timestamp,
				totalInstances,
				runningInstances,
				stoppedInstances,
				totals.activeInstances,
				totals.totalRecords,
				totals.totalHashrate,
				totals.averageHashrate,
				totals.peakHashrate,
				totals.totalShares,
				totals.totalSharesLifetime,
				totals.totalSharesRejected,
				totals.totalSharesRejectedLifetime,
				totals.rejectionRate,
				totals.cumulativeUptimeSeconds,
				totals.sharesPerSecond,
				operation,
				configJson,
			)
			.run();

		return {
			...totals,
			timestamp,
			totalInstances,
			runningInstances,
			stoppedInstances,
			operation,
			config: coordinatorStatus?.config ?? null,
		};
	}

	async getLatestStatusReport(): Promise<MiningStatusReport | null> {
		await ensureSchema(this.db);
		const row = await this.db
			.prepare(
				`SELECT timestamp, total_instances, running_instances, stopped_instances,
				        active_instances, total_records, total_hashrate, average_hashrate,
				        peak_hashrate, total_shares, total_shares_lifetime,
				        total_shares_rejected, total_shares_rejected_lifetime, rejection_rate,
				        cumulative_uptime_seconds, shares_per_second, operation, config_json
				   FROM mining_status_reports
				   ORDER BY timestamp DESC
				   LIMIT 1`,
			)
			.first<Record<string, unknown>>();
		if (!row) return null;
		return mapStatusReport(row);
	}

	async rollupHourly(hour?: number): Promise<number> {
		await ensureSchema(this.db);
		const targetHour = hour ?? Math.floor(Date.now() / 3_600_000) * 3_600_000;
		const nextHour = targetHour + 3_600_000;

		const r = await this.db
			.prepare(
				`INSERT INTO hourly_stats (hour, total_instances, avg_hashrate, peak_hashrate,
				   total_shares_delta, rejected_shares_delta, avg_cpu_percent)
				 SELECT ?,
				        COALESCE(MAX(total_instances), 0),
				        COALESCE(AVG(total_hashrate), 0),
				        COALESCE(MAX(total_hashrate), 0),
				        COALESCE(MAX(total_shares_lifetime) - MIN(total_shares_lifetime), 0),
				        COALESCE(MAX(total_shares_rejected_lifetime) - MIN(total_shares_rejected_lifetime), 0),
				        0
				   FROM mining_status_reports
				  WHERE timestamp >= ? AND timestamp < ?
				 HAVING COUNT(*) > 0
				 ON CONFLICT (hour) DO UPDATE SET
				   total_instances        = excluded.total_instances,
				   avg_hashrate           = excluded.avg_hashrate,
				   peak_hashrate          = excluded.peak_hashrate,
				   total_shares_delta     = excluded.total_shares_delta,
				   rejected_shares_delta  = excluded.rejected_shares_delta,
				   avg_cpu_percent        = excluded.avg_cpu_percent`,
			)
			.bind(targetHour, targetHour, nextHour)
			.run();

		return r.meta?.changes ?? 0;
	}

	async pruneStatusReports(
		retentionHours = STATUS_REPORT_RETENTION_HOURS,
	): Promise<number> {
		await ensureSchema(this.db);
		const cutoff = Date.now() - retentionHours * 3_600_000;
		const r = await this.db
			.prepare("DELETE FROM mining_status_reports WHERE timestamp < ?")
			.bind(cutoff)
			.run();
		return r.meta?.changes ?? 0;
	}

	async pruneStaleInstances(staleMinutes = 10): Promise<number> {
		await ensureSchema(this.db);
		const cutoff = Date.now() - staleMinutes * 60_000;
		const r = await this.db
			.prepare("DELETE FROM instance_latest WHERE updated_at < ?")
			.bind(cutoff)
			.run();
		return r.meta?.changes ?? 0;
	}

	async pruneHourlyStats(
		retentionDays = HOURLY_STATS_RETENTION_DAYS,
	): Promise<number> {
		await ensureSchema(this.db);
		const cutoff = Date.now() - retentionDays * 24 * 3_600_000;
		const r = await this.db
			.prepare("DELETE FROM hourly_stats WHERE hour < ?")
			.bind(cutoff)
			.run();
		return r.meta?.changes ?? 0;
	}

	async getQuickStatus(): Promise<{
		activeInstances: number;
		avgHashrate: number;
		totalShares: number;
		totalSharesRejected: number;
		peakHashrate: number;
	}> {
		const totals = await this.getTotals();
		return {
			activeInstances: totals.activeInstances,
			avgHashrate: totals.averageHashrate,
			totalShares: totals.totalSharesLifetime,
			totalSharesRejected: totals.totalSharesRejectedLifetime,
			peakHashrate: totals.peakHashrate,
		};
	}

	async validateSchema(): Promise<boolean> {
		try {
			await ensureSchema(this.db);
			const expected = [
				"instance_id",
				"timestamp",
				"hashrate",
				"shares_accepted",
				"shares_rejected",
				"cpu_percent",
				"pool",
				"connection_status",
				"updated_at",
				"started_at",
				"shares_accepted_lifetime",
				"shares_rejected_lifetime",
			];
			const r = await this.db
				.prepare("PRAGMA table_info(instance_latest)")
				.all<{ name: string }>();
			const cols = new Set((r.results ?? []).map((row) => row.name));
			return expected.every((c) => cols.has(c));
		} catch {
			return false;
		}
	}
}

async function ensureColumn(
	db: D1Like,
	table: string,
	column: string,
	definition: string,
): Promise<void> {
	try {
		const info = await db
			.prepare(`PRAGMA table_info(${table})`)
			.all<{ name: string }>();
		const present = (info.results ?? []).some((row) => row.name === column);
		if (!present) {
			await db
				.prepare(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
				.run();
		}
	} catch {}
}

async function ensureSchema(db: D1Like): Promise<void> {
	if (schemaReady) return;
	await db.batch([
		db.prepare(
			`CREATE TABLE IF NOT EXISTS instance_latest (
				instance_id              TEXT PRIMARY KEY,
				timestamp                INTEGER NOT NULL,
				hashrate                 REAL NOT NULL DEFAULT 0,
				shares_accepted          INTEGER NOT NULL DEFAULT 0,
				shares_rejected          INTEGER NOT NULL DEFAULT 0,
				cpu_percent              REAL,
				pool                     TEXT,
				connection_status        TEXT,
				updated_at               INTEGER NOT NULL,
				started_at               INTEGER NOT NULL DEFAULT 0,
				shares_accepted_lifetime INTEGER NOT NULL DEFAULT 0,
				shares_rejected_lifetime INTEGER NOT NULL DEFAULT 0
			)`,
		),
		db.prepare(
			"CREATE INDEX IF NOT EXISTS idx_instance_latest_updated_at ON instance_latest(updated_at)",
		),
		db.prepare(
			`CREATE INDEX IF NOT EXISTS idx_instance_latest_active
			   ON instance_latest(updated_at)
			   WHERE hashrate > 0`,
		),
		db.prepare(
			`CREATE TABLE IF NOT EXISTS mining_status_reports (
				id                              INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp                       INTEGER NOT NULL,
				total_instances                 INTEGER NOT NULL DEFAULT 0,
				running_instances               INTEGER NOT NULL DEFAULT 0,
				stopped_instances               INTEGER NOT NULL DEFAULT 0,
				active_instances                INTEGER NOT NULL DEFAULT 0,
				total_records                   INTEGER NOT NULL DEFAULT 0,
				total_hashrate                  REAL NOT NULL DEFAULT 0,
				average_hashrate                REAL NOT NULL DEFAULT 0,
				peak_hashrate                   REAL NOT NULL DEFAULT 0,
				total_shares                    INTEGER NOT NULL DEFAULT 0,
				total_shares_lifetime           INTEGER NOT NULL DEFAULT 0,
				total_shares_rejected           INTEGER NOT NULL DEFAULT 0,
				total_shares_rejected_lifetime  INTEGER NOT NULL DEFAULT 0,
				rejection_rate                  REAL NOT NULL DEFAULT 0,
				cumulative_uptime_seconds       INTEGER NOT NULL DEFAULT 0,
				shares_per_second               REAL NOT NULL DEFAULT 0,
				operation                       TEXT NOT NULL DEFAULT 'unknown',
				config_json                     TEXT
			)`,
		),
		db.prepare(
			"CREATE INDEX IF NOT EXISTS idx_mining_status_reports_timestamp ON mining_status_reports(timestamp DESC)",
		),
		db.prepare(
			`CREATE TABLE IF NOT EXISTS hourly_stats (
				hour                  INTEGER PRIMARY KEY,
				total_instances       INTEGER NOT NULL DEFAULT 0,
				avg_hashrate          REAL NOT NULL DEFAULT 0,
				peak_hashrate         REAL NOT NULL DEFAULT 0,
				total_shares_delta    INTEGER NOT NULL DEFAULT 0,
				rejected_shares_delta INTEGER NOT NULL DEFAULT 0,
				avg_cpu_percent       REAL NOT NULL DEFAULT 0
			)`,
		),
	]);

	await ensureColumn(
		db,
		"instance_latest",
		"started_at",
		"INTEGER NOT NULL DEFAULT 0",
	);
	await ensureColumn(
		db,
		"instance_latest",
		"shares_accepted_lifetime",
		"INTEGER NOT NULL DEFAULT 0",
	);
	await ensureColumn(
		db,
		"instance_latest",
		"shares_rejected_lifetime",
		"INTEGER NOT NULL DEFAULT 0",
	);
	await ensureColumn(
		db,
		"mining_status_reports",
		"total_records",
		"INTEGER NOT NULL DEFAULT 0",
	);
	schemaReady = true;
}

function mapStatusReport(row: Record<string, unknown>): MiningStatusReport {
	let config: unknown = null;
	if (row.config_json) {
		try {
			config = typeof row.config_json === "string" ? JSON.parse(row.config_json) : row.config_json;
		} catch {
			config = null;
		}
	}
	const num = (k: string): number => Number(row[k] ?? 0) || 0;
	const reportAgeMs = Date.now() - num("timestamp");
	return {
		timestamp: num("timestamp"),
		totalInstances: num("total_instances"),
		runningInstances: num("running_instances"),
		stoppedInstances: num("stopped_instances"),
		activeInstances: num("active_instances"),
		totalHashrate: num("total_hashrate"),
		averageHashrate: num("average_hashrate"),
		peakHashrate: num("peak_hashrate"),
		totalShares: num("total_shares"),
		totalSharesLifetime: num("total_shares_lifetime"),
		totalSharesRejected: num("total_shares_rejected"),
		totalSharesRejectedLifetime: num("total_shares_rejected_lifetime"),
		rejectionRate: num("rejection_rate"),
		cumulativeUptimeSeconds: num("cumulative_uptime_seconds"),
		sharesPerSecond: num("shares_per_second"),

		totalRecords: num("total_records") || num("active_instances"),
		operation: (row.operation as string) || "unknown",
		config,
		reportAgeMs,
		staleReport: reportAgeMs > 2 * 60 * 1000,
	};
}

function toTimestamp(value: unknown, fallback: number): number {
	const n = Number(value);
	return Number.isFinite(n) && n > 0 ? Math.trunc(n) : fallback;
}

function toNonNegativeNumber(value: unknown): number {
	const n = Number(value);
	return Number.isFinite(n) && n > 0 ? n : 0;
}

function toNonNegativeInteger(value: unknown): number {
	return Math.trunc(toNonNegativeNumber(value));
}

function toNullablePercent(value: unknown): number | null {
	const n = Number(value);
	if (!Number.isFinite(n)) return null;
	return Math.min(100, Math.max(0, n));
}

function cleanText(value: unknown, fallback: string, maxLength: number): string {
	if (typeof value !== "string" || value.trim() === "") return fallback;
	return value.trim().slice(0, maxLength);
}

function cleanNullableText(value: unknown, maxLength: number): string | null {
	if (typeof value !== "string" || value.trim() === "") return null;
	return value.trim().slice(0, maxLength);
}

export function normalizeHeartbeatPayloads(value: unknown): Array<Record<string, unknown>> | null {
	if (value === null || typeof value !== "object" || Array.isArray(value)) {
		return null;
	}

	const body = value as { batch?: unknown; [key: string]: unknown };
	if (body.batch !== undefined && !Array.isArray(body.batch)) return null;

	const payloads = Array.isArray(body.batch) ? body.batch : [body];
	if (payloads.length === 0 || payloads.length > MAX_HEARTBEAT_BATCH_SIZE) {
		return null;
	}

	const normalized: Array<Record<string, unknown>> = [];
	const now = Date.now();
	for (const payload of payloads) {
		if (
			payload === null ||
			typeof payload !== "object" ||
			Array.isArray(payload)
		) {
			return null;
		}
		const normalizedPayload = normalizeHeartbeatPayload(
			payload as Record<string, unknown>,
			now,
		);
		if (normalizedPayload === null) return null;
		normalized.push(normalizedPayload);
	}

	return normalized;
}

function normalizeHeartbeatPayload(
	payload: Record<string, unknown>,
	now: number,
): Record<string, unknown> | null {
	for (const field of HEARTBEAT_FLEET_FIELDS) {
		if (field in payload) return null;
	}

	const instanceId = readHeartbeatInstanceId(payload);
	if (!instanceId) return null;

	const timestamp = readHeartbeatTimestamp(payload.timestamp, now);
	if (timestamp === null) return null;

	return {
		instanceId,
		timestamp,
		hashrate: toNonNegativeNumber(payload.hashrate),
		sharesAccepted: toNonNegativeInteger(payload.sharesAccepted),
		sharesRejected: toNonNegativeInteger(payload.sharesRejected),
		cpuPercent: toNullablePercent(payload.cpuPercent),
		pool: cleanNullableText(payload.pool, 256),
		connectionStatus: cleanNullableText(payload.connectionStatus, 64),
	};
}

function readHeartbeatInstanceId(
	payload: Record<string, unknown>,
): string | null {
	const value =
		typeof payload.instanceId === "string"
			? payload.instanceId
			: typeof payload.instance_id === "string"
				? payload.instance_id
				: "";
	const trimmed = value.trim();
	return /^[a-zA-Z0-9._-]{1,128}$/.test(trimmed) ? trimmed : null;
}

function readHeartbeatTimestamp(value: unknown, now: number): number | null {
	if (value === undefined || value === null || value === "") return now;
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed <= 0) return null;
	const timestamp = Math.trunc(parsed);
	return timestamp <= now + MAX_HEARTBEAT_FUTURE_SKEW_MS ? timestamp : null;
}

export async function processHeartbeats(
	env: Env,
	payloads: Array<Record<string, unknown>>,
	colo: string | null,
): Promise<void> {
	if (payloads.length === 0) return;

	const stats = new MiningStatsStore(env.DB);
	const dbWrite = stats
		.recordStatsBatch(
			payloads.map((p) => ({
				timestamp: Number(p.timestamp ?? Date.now()),
				instanceId: typeof p.instanceId === "string" ? p.instanceId : "unknown",
				hashrate: heartbeatNumeric(p.hashrate),
				sharesAccepted: heartbeatNumeric(p.sharesAccepted),
				sharesRejected: heartbeatNumeric(p.sharesRejected),
				cpuPercent: heartbeatNumeric(p.cpuPercent),
				pool: typeof p.pool === "string" ? p.pool : undefined,
				connectionStatus:
					typeof p.connectionStatus === "string"
						? p.connectionStatus
						: undefined,
			})),
		)
		.catch((err: Error) => {
			heartbeatLog(
				"error",
				{ err: err.message },
				"heartbeat: D1 batch write failed",
			);
			throw err;
		});

	const latest = payloads[payloads.length - 1] ?? {};
	const coordUpdate = coordinatorHeartbeat(env, colo, latest).catch(
		(err: Error) => {
			heartbeatLog(
				"error",
				{ err: err.message },
				"heartbeat: coordinator update failed",
			);
			throw err;
		},
	);

	try {
		const dataset = env.HEARTBEATS as AnalyticsEngineDataset | undefined;
		if (dataset && typeof dataset.writeDataPoint === "function") {
			for (const p of payloads) {
				dataset.writeDataPoint({
					indexes: [
						typeof p.instanceId === "string"
							? p.instanceId.slice(0, 96)
							: "unknown",
					],
					blobs: [
						typeof p.instanceId === "string"
							? p.instanceId.slice(0, 128)
							: "unknown",
						typeof p.pool === "string" ? p.pool.slice(0, 256) : "",
						typeof p.connectionStatus === "string"
							? p.connectionStatus.slice(0, 64)
							: "",
						colo ?? "",
					],
					doubles: [
						Number(p.hashrate) || 0,
						Number(p.sharesAccepted) || 0,
						Number(p.sharesRejected) || 0,
						Number(p.cpuPercent) || 0,
						Number(p.timestamp) || Date.now(),
					],
				});
			}
		}
	} catch (err) {
		heartbeatLog(
			"warn",
			{ err: (err as Error).message },
			"heartbeat: analytics engine write failed (non-fatal)",
		);
	}

	await Promise.all([dbWrite, coordUpdate]);
}

async function coordinatorHeartbeat(
	env: Env,
	colo: string | null,
	latest: Record<string, unknown>,
): Promise<void> {
	const id = env.MINER_COORDINATOR.idFromName("global-coordinator");
	const coordinator = env.MINER_COORDINATOR.get(id);
	const headers: Record<string, string> = {
		"Content-Type": "application/json",
	};
	if (colo) headers["X-Colo"] = colo;
	await coordinator.fetch("http://internal/heartbeat", {
		method: "POST",
		headers,
		body: JSON.stringify({
			instanceId: latest.instanceId ?? "unknown",
			hashrate: latest.hashrate,
			colo,
			timestamp: latest.timestamp ?? Date.now(),
		}),
	});
}

function heartbeatNumeric(v: unknown): number | undefined {
	if (v === undefined || v === null) return undefined;
	const n = Number(v);
	return Number.isFinite(n) ? n : undefined;
}

function heartbeatLog(
	level: "warn" | "error",
	fields: Record<string, unknown>,
	msg: string,
): void {
	const payload = JSON.stringify({
		level,
		time: new Date().toISOString(),
		service: "miner-heartbeat",
		...fields,
		msg,
	});
	if (level === "error") console.error(payload);
	else console.warn(payload);
}
