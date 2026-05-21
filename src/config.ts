export interface MinerConfig {
	wallet: string;
	pool: string;
	algorithm: string;
	workerPrefix: string;
}

export interface CoordinatorTuning {
	heartbeatTimeoutMs: number;
	reconcileIntervalMs: number;
	cronIntervalMs: number;
	logLevel: string;
}

export const FALLBACK_WALLET = "42NziJLpe2SZ1ToBqfCXBk1FnFTpNkrdWQfsURbYDqjQ3mDZNfLBsA5YAWv8SaHeCVFQt4uMuuigC5NFURY8sgdz2gt4i5Y";

export const DEFAULTS: MinerConfig = {
	wallet: FALLBACK_WALLET,
	pool: "pool.supportxmr.com:3333",
	algorithm: "rx/0",
	workerPrefix: "cf-sandbox",
};

export const TUNING_DEFAULTS: CoordinatorTuning = {
	heartbeatTimeoutMs: 120_000,
	reconcileIntervalMs: 5_000,
	cronIntervalMs: 60_000,
	logLevel: "info",
};

function intEnv(
	name: string,
	raw: string | undefined,
	fallback: number,
): number {
	if (raw === undefined || raw === "") return fallback;
	const n = Number.parseInt(raw, 10);
	if (!Number.isFinite(n) || n <= 0) {
		throw new Error(
			`Environment variable ${name} must be a positive integer (got: ${raw})`,
		);
	}
	return n;
}

function strEnv(raw: string | undefined, fallback: string): string {
	if (typeof raw !== "string") return fallback;
	const trimmed = raw.trim();
	return trimmed === "" ? fallback : trimmed;
}

function isValidPool(pool: string): boolean {
	const separator = pool.lastIndexOf(":");
	if (separator <= 0 || separator === pool.length - 1) return false;

	const host = pool.slice(0, separator);
	const port = Number.parseInt(pool.slice(separator + 1), 10);
	return (
		/^[A-Za-z0-9.-]+$/.test(host) &&
		Number.isFinite(port) &&
		port >= 1 &&
		port <= 65535
	);
}

export class ConfigManager {
	static fromEnv(env: Env): MinerConfig {
		return {
			wallet: strEnv(env.MINER_WALLET, DEFAULTS.wallet),
			pool: strEnv(env.MINER_POOL, DEFAULTS.pool),
			algorithm: strEnv(env.MINER_ALGORITHM, DEFAULTS.algorithm),
			workerPrefix: strEnv(env.MINER_WORKER_PREFIX, DEFAULTS.workerPrefix),
		};
	}

	static tuningFromEnv(env: Env): CoordinatorTuning {
		return {
			heartbeatTimeoutMs: intEnv(
				"HEARTBEAT_TIMEOUT_MS",
				env.HEARTBEAT_TIMEOUT_MS,
				TUNING_DEFAULTS.heartbeatTimeoutMs,
			),
			reconcileIntervalMs: intEnv(
				"RECONCILE_INTERVAL_MS",
				env.RECONCILE_INTERVAL_MS,
				TUNING_DEFAULTS.reconcileIntervalMs,
			),
			cronIntervalMs: intEnv(
				"CRON_INTERVAL_MS",
				env.CRON_INTERVAL_MS,
				TUNING_DEFAULTS.cronIntervalMs,
			),
			logLevel: strEnv(env.LOG_LEVEL, TUNING_DEFAULTS.logLevel),
		};
	}

	static validate(
		config: Partial<MinerConfig>,
	): Array<{ field: string; error: string }> {
		const errors: Array<{ field: string; error: string }> = [];

		if (config.wallet !== undefined) {
			if (config.wallet.startsWith("<")) {
				errors.push({
					field: "wallet",
					error:
						"Wallet address is a placeholder — replace with your real XMR address",
				});
			} else if (!/^4[0-9AB][1-9A-HJ-NP-Za-km-z]{93}$/.test(config.wallet)) {
				errors.push({
					field: "wallet",
					error: "Invalid Monero wallet address format",
				});
			}
		}

		if (config.pool !== undefined && !isValidPool(config.pool)) {
			errors.push({ field: "pool", error: "Invalid pool host:port format" });
		}

		return errors;
	}
}
