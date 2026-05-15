
export interface HeartbeatSnapshot {
	instanceId: string;
	timestamp: number;
	hashrate: number;
	sharesAccepted: number;
	sharesRejected: number;
	sharesAcceptedLifetime: number;
	sharesRejectedLifetime: number;
	cpuPercent: number | null;
	pool: string | null;
	connectionStatus: string | null;
	startedAt: number;
	updatedAt: number;
}

export interface ClusterSnapshot {
	activeInstances: number;
	avgHashrate: number;
	totalHashrate: number;
	totalShares: number;
	uptimeSeconds: number;
}

export interface HourlyBucket {
	hour: number;
	totalInstances: number;
	avgHashrate: number;
	peakHashrate: number;
	totalSharesDelta: number;
	rejectedSharesDelta: number;
}

const STALE_INSTANCE_MS = 10 * 60 * 1000;

export class MetricsStore {
	constructor(private readonly db: D1Database) {}

	async getInstance(instanceId: string): Promise<HeartbeatSnapshot | null> {
		try {
			const row = await this.db
				.prepare(
					`SELECT instance_id, timestamp, hashrate, shares_accepted, shares_rejected,
					        shares_accepted_lifetime, shares_rejected_lifetime, cpu_percent,
					        pool, connection_status, started_at, updated_at
					   FROM instance_latest
					  WHERE instance_id = ?
					  LIMIT 1`,
				)
				.bind(instanceId)
				.first<Record<string, unknown>>();
			return row ? rowToSnapshot(row) : null;
		} catch {
			return null;
		}
	}

	async getActiveInstances(): Promise<HeartbeatSnapshot[]> {
		try {
			const cutoff = Date.now() - STALE_INSTANCE_MS;
			const r = await this.db
				.prepare(
					`SELECT instance_id, timestamp, hashrate, shares_accepted, shares_rejected,
					        shares_accepted_lifetime, shares_rejected_lifetime, cpu_percent,
					        pool, connection_status, started_at, updated_at
					   FROM instance_latest
					  WHERE updated_at >= ?
					  ORDER BY hashrate DESC`,
				)
				.bind(cutoff)
				.all<Record<string, unknown>>();
			return (r.results ?? []).map(rowToSnapshot);
		} catch {
			return [];
		}
	}

	async getAggregateStatus(): Promise<ClusterSnapshot> {
		try {
			const cutoff = Date.now() - STALE_INSTANCE_MS;
			const row = await this.db
				.prepare(
					`SELECT
					   COALESCE(SUM(CASE WHEN hashrate > 0 THEN 1 ELSE 0 END), 0)                                                    AS active_instances,
					   COALESCE(AVG(CASE WHEN hashrate > 0 THEN hashrate END), 0)                                                    AS avg_hashrate,
					   COALESCE(SUM(hashrate), 0)                                                                                    AS total_hashrate,
					   COALESCE(SUM(shares_accepted_lifetime), 0)                                                                    AS total_shares,
					   COALESCE(SUM(CASE WHEN hashrate > 0 AND updated_at > started_at THEN updated_at - started_at ELSE 0 END), 0) AS uptime_ms
					 FROM instance_latest
					 WHERE updated_at >= ?`,
				)
				.bind(cutoff)
				.first<Record<string, unknown>>();

			return {
				activeInstances: num(row, "active_instances"),
				avgHashrate: num(row, "avg_hashrate"),
				totalHashrate: num(row, "total_hashrate"),
				totalShares: num(row, "total_shares"),
				uptimeSeconds: Math.floor(num(row, "uptime_ms") / 1000),
			};
		} catch {
			return {
				activeInstances: 0,
				avgHashrate: 0,
				totalHashrate: 0,
				totalShares: 0,
				uptimeSeconds: 0,
			};
		}
	}

	async getHourly(limitHours = 24): Promise<HourlyBucket[]> {
		const safeLimit = Math.min(Math.max(Math.trunc(limitHours) || 24, 1), 720);
		try {
			const r = await this.db
				.prepare(
					`SELECT hour, total_instances, avg_hashrate, peak_hashrate,
					        total_shares_delta, rejected_shares_delta
					   FROM hourly_stats
					  ORDER BY hour DESC
					  LIMIT ?`,
				)
				.bind(safeLimit)
				.all<Record<string, unknown>>();
			return (r.results ?? []).map((row) => ({
				hour: num(row, "hour"),
				totalInstances: num(row, "total_instances"),
				avgHashrate: num(row, "avg_hashrate"),
				peakHashrate: num(row, "peak_hashrate"),
				totalSharesDelta: num(row, "total_shares_delta"),
				rejectedSharesDelta: num(row, "rejected_shares_delta"),
			}));
		} catch {
			return [];
		}
	}
}

function rowToSnapshot(row: Record<string, unknown>): HeartbeatSnapshot {
	return {
		instanceId: String(row.instance_id ?? "unknown"),
		timestamp: num(row, "timestamp"),
		hashrate: num(row, "hashrate"),
		sharesAccepted: num(row, "shares_accepted"),
		sharesRejected: num(row, "shares_rejected"),
		sharesAcceptedLifetime: num(row, "shares_accepted_lifetime"),
		sharesRejectedLifetime: num(row, "shares_rejected_lifetime"),
		cpuPercent: row.cpu_percent == null ? null : Number(row.cpu_percent),
		pool: (row.pool as string | null) ?? null,
		connectionStatus: (row.connection_status as string | null) ?? null,
		startedAt: num(row, "started_at"),
		updatedAt: num(row, "updated_at"),
	};
}

function num(row: Record<string, unknown> | null | undefined, key: string): number {
	if (!row) return 0;
	const v = Number(row[key] ?? 0);
	return Number.isFinite(v) ? v : 0;
}