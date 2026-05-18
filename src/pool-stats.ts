import { FALLBACK_WALLET } from "./config";

export interface PoolBalanceSnapshot {
	timestamp: number;
	wallet: string;
	poolHashrate: number;
	lastHashAt: number | null;
	validShares: number;
	invalidShares: number;
	amtDueXmr: number;
	amtPaidXmr: number;
	totalBalanceXmr: number;
	networkHashrate: number | null;
	blockRewardXmr: number | null;
}

export interface PoolEarningsRate {
	xmrPerHour: number | null;
	xmrPerDay: number | null;
	windowMs: number | null;
	deltaXmr: number | null;
}

export interface PoolStatsSummary extends PoolBalanceSnapshot {
	ageMs: number;
	stale: boolean;
	actualHour: PoolEarningsRate;
	actualDay: PoolEarningsRate;
	estimatedXmrPerHour: number | null;
	estimatedXmrPerDay: number | null;
}

type D1Like = D1Database | D1DatabaseSession;

const SUPPORTXMR_MINER_API = "https://www.supportxmr.com/api/miner";
const SUPPORTXMR_NETWORK_API = "https://www.supportxmr.com/api/network/stats";
const MONERO_ATOMIC_UNITS = 1_000_000_000_000;
const BLOCK_SECONDS = 120;
const BLOCKS_PER_DAY = 86_400 / BLOCK_SECONDS;
const POOL_STALE_MS = 3 * 60_000;
const SNAPSHOT_RETENTION_MS = 7 * 24 * 60 * 60_000;
const HOUR_MS = 60 * 60_000;
const DAY_MS = 24 * HOUR_MS;

let schemaReady = false;

export class PoolStatsStore {
	constructor(private readonly db: D1Like) {}

	static walletFromEnv(env: Env): string {
		return cleanWallet(env.MINER_WALLET) ?? FALLBACK_WALLET;
	}

	static async ensureReady(db: D1Like): Promise<void> {
		await ensureSchema(db);
	}

	async refresh(env: Env): Promise<PoolBalanceSnapshot> {
		await ensureSchema(this.db);
		const wallet = PoolStatsStore.walletFromEnv(env);
		const [miner, network] = await Promise.all([
			fetchJson(`${SUPPORTXMR_MINER_API}/${encodeURIComponent(wallet)}/stats`, 8000),
			fetchJson(SUPPORTXMR_NETWORK_API, 8000).catch((err: Error) => {
				console.warn(JSON.stringify({ level: "warn", service: "pool-stats", err: err.message, msg: "network stats unavailable" }));
				return {} as Record<string, unknown>;
			}),
		]);

		const timestamp = Date.now();
		const amtDueXmr = normalizeXmr(miner.amtDue);
		const amtPaidXmr = normalizeXmr(miner.amtPaid);
		const difficulty = numeric(network.difficulty);
		const networkHashrate = numeric(network.hash) || (difficulty > 0 ? difficulty / BLOCK_SECONDS : null);
		const blockRewardXmr = network.value === undefined ? null : normalizeXmr(network.value);
		const snapshot: PoolBalanceSnapshot = {
			timestamp,
			wallet,
			poolHashrate: numeric(miner.hash),
			lastHashAt: timestampValue(miner.lastHash),
			validShares: numeric(miner.validShares),
			invalidShares: numeric(miner.invalidShares),
			amtDueXmr,
			amtPaidXmr,
			totalBalanceXmr: amtDueXmr + amtPaidXmr,
			networkHashrate,
			blockRewardXmr,
		};

		await this.db.prepare(`INSERT INTO pool_balance_snapshots (wallet, timestamp, pool_hashrate, last_hash_at, valid_shares, invalid_shares, amt_due_xmr, amt_paid_xmr, total_balance_xmr, network_hashrate, block_reward_xmr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(wallet, timestamp) DO UPDATE SET pool_hashrate = excluded.pool_hashrate, last_hash_at = excluded.last_hash_at, valid_shares = excluded.valid_shares, invalid_shares = excluded.invalid_shares, amt_due_xmr = excluded.amt_due_xmr, amt_paid_xmr = excluded.amt_paid_xmr, total_balance_xmr = excluded.total_balance_xmr, network_hashrate = excluded.network_hashrate, block_reward_xmr = excluded.block_reward_xmr`).bind(snapshot.wallet, snapshot.timestamp, snapshot.poolHashrate, snapshot.lastHashAt, snapshot.validShares, snapshot.invalidShares, snapshot.amtDueXmr, snapshot.amtPaidXmr, snapshot.totalBalanceXmr, snapshot.networkHashrate, snapshot.blockRewardXmr).run();

		await this.pruneOld(timestamp - SNAPSHOT_RETENTION_MS).catch((err: Error) => {
			console.warn(JSON.stringify({ level: "warn", service: "pool-stats", err: err.message, msg: "snapshot prune failed" }));
		});

		return snapshot;
	}

	async getLatest(wallet: string): Promise<PoolBalanceSnapshot | null> {
		await ensureSchema(this.db);
		const row = await this.db.prepare(`SELECT wallet, timestamp, pool_hashrate, last_hash_at, valid_shares, invalid_shares, amt_due_xmr, amt_paid_xmr, total_balance_xmr, network_hashrate, block_reward_xmr FROM pool_balance_snapshots WHERE wallet = ? ORDER BY timestamp DESC LIMIT 1`).bind(wallet).first<Record<string, unknown>>();
		return row ? rowToSnapshot(row) : null;
	}

	async getSummary(wallet: string): Promise<PoolStatsSummary | null> {
		const latest = await this.getLatest(wallet);
		if(!latest) return null;

		const [hourBaseline, dayBaseline] = await Promise.all([
			this.getBaseline(wallet, latest.timestamp - HOUR_MS),
			this.getBaseline(wallet, latest.timestamp - DAY_MS),
		]);
		const estimatedXmrPerDay = estimateXmrPerDay(latest);
		const ageMs = Date.now() - latest.timestamp;

		return {
			...latest,
			ageMs,
			stale: ageMs > POOL_STALE_MS,
			actualHour: calculateRate(latest, hourBaseline),
			actualDay: calculateRate(latest, dayBaseline),
			estimatedXmrPerHour: estimatedXmrPerDay === null ? null : estimatedXmrPerDay / 24,
			estimatedXmrPerDay,
		};
	}

	private async getBaseline(wallet: string, targetTimestamp: number): Promise<PoolBalanceSnapshot | null> {
		let row = await this.db.prepare(`SELECT wallet, timestamp, pool_hashrate, last_hash_at, valid_shares, invalid_shares, amt_due_xmr, amt_paid_xmr, total_balance_xmr, network_hashrate, block_reward_xmr FROM pool_balance_snapshots WHERE wallet = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1`).bind(wallet, targetTimestamp).first<Record<string, unknown>>();
		if(!row){
			row = await this.db.prepare(`SELECT wallet, timestamp, pool_hashrate, last_hash_at, valid_shares, invalid_shares, amt_due_xmr, amt_paid_xmr, total_balance_xmr, network_hashrate, block_reward_xmr FROM pool_balance_snapshots WHERE wallet = ? ORDER BY timestamp ASC LIMIT 1`).bind(wallet).first<Record<string, unknown>>();
		}
		return row ? rowToSnapshot(row) : null;
	}

	private async pruneOld(cutoff: number): Promise<void> {
		await this.db.prepare("DELETE FROM pool_balance_snapshots WHERE timestamp < ?").bind(cutoff).run();
	}
}

export async function refreshPoolStats(env: Env): Promise<PoolBalanceSnapshot> {
	return new PoolStatsStore(env.DB).refresh(env);
}

export async function getPoolStatsSummary(env: Env): Promise<PoolStatsSummary | null> {
	const store = new PoolStatsStore(env.DB);
	return store.getSummary(PoolStatsStore.walletFromEnv(env));
}

async function ensureSchema(db: D1Like): Promise<void> {
	if(schemaReady) return;
	await db.batch([
		db.prepare(`CREATE TABLE IF NOT EXISTS pool_balance_snapshots ( wallet TEXT NOT NULL, timestamp INTEGER NOT NULL, pool_hashrate REAL NOT NULL DEFAULT 0, last_hash_at INTEGER, valid_shares REAL NOT NULL DEFAULT 0, invalid_shares REAL NOT NULL DEFAULT 0, amt_due_xmr REAL NOT NULL DEFAULT 0, amt_paid_xmr REAL NOT NULL DEFAULT 0, total_balance_xmr REAL NOT NULL DEFAULT 0, network_hashrate REAL, block_reward_xmr REAL, PRIMARY KEY (wallet, timestamp) )`),
		db.prepare("CREATE INDEX IF NOT EXISTS idx_pool_balance_snapshots_wallet_timestamp ON pool_balance_snapshots(wallet, timestamp DESC)"),
	]);
	schemaReady = true;
}

async function fetchJson(url: string, timeoutMs: number): Promise<Record<string, unknown>> {
	const response = await Promise.race([
		fetch(url, {
			headers: {
				"Accept": "application/json",
			},
		}),
		new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)),
	]);
	if(!response.ok){
		throw new Error(`SupportXMR request failed: ${response.status}`);
	}
	const json = await response.json();
	if(json === null || typeof json !== "object" || Array.isArray(json)){
		throw new Error("SupportXMR returned invalid JSON");
	}
	return json as Record<string, unknown>;
}

function rowToSnapshot(row: Record<string, unknown>): PoolBalanceSnapshot {
	return {
		wallet: String(row.wallet ?? FALLBACK_WALLET),
		timestamp: numeric(row.timestamp),
		poolHashrate: numeric(row.pool_hashrate),
		lastHashAt: row.last_hash_at == null ? null : numeric(row.last_hash_at),
		validShares: numeric(row.valid_shares),
		invalidShares: numeric(row.invalid_shares),
		amtDueXmr: numeric(row.amt_due_xmr),
		amtPaidXmr: numeric(row.amt_paid_xmr),
		totalBalanceXmr: numeric(row.total_balance_xmr),
		networkHashrate: row.network_hashrate == null ? null : numeric(row.network_hashrate),
		blockRewardXmr: row.block_reward_xmr == null ? null : numeric(row.block_reward_xmr),
	};
}

function calculateRate(latest: PoolBalanceSnapshot, baseline: PoolBalanceSnapshot | null): PoolEarningsRate {
	if(!baseline || baseline.timestamp >= latest.timestamp){
		return { xmrPerHour: null, xmrPerDay: null, windowMs: null, deltaXmr: null };
	}
	const windowMs = latest.timestamp - baseline.timestamp;
	const deltaXmr = Math.max(0, latest.totalBalanceXmr - baseline.totalBalanceXmr);
	const hours = windowMs / HOUR_MS;
	const xmrPerHour = hours > 0 ? deltaXmr / hours : null;
	return {
		xmrPerHour,
		xmrPerDay: xmrPerHour === null ? null : xmrPerHour * 24,
		windowMs,
		deltaXmr,
	};
}

function estimateXmrPerDay(snapshot: PoolBalanceSnapshot): number | null {
	if(snapshot.poolHashrate <= 0 || !snapshot.networkHashrate || snapshot.networkHashrate <= 0 || !snapshot.blockRewardXmr || snapshot.blockRewardXmr <= 0){
		return null;
	}
	return (snapshot.poolHashrate / snapshot.networkHashrate) * BLOCKS_PER_DAY * snapshot.blockRewardXmr;
}

function cleanWallet(raw: string | undefined): string | null {
	if(typeof raw !== "string") return null;
	const trimmed = raw.trim();
	return trimmed === "" ? null : trimmed;
}

function normalizeXmr(value: unknown): number {
	const n = numeric(value);
	if(n <= 0) return 0;
	return n > 1_000_000 ? n / MONERO_ATOMIC_UNITS : n;
}

function timestampValue(value: unknown): number | null {
	const n = numeric(value);
	if(n <= 0) return null;
	return n < 10_000_000_000 ? n * 1000 : n;
}

function numeric(value: unknown): number {
	const n = Number(value ?? 0);
	return Number.isFinite(n) ? n : 0;
}
