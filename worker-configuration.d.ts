import type { Sandbox } from "@cloudflare/sandbox";

export {};

declare global {
	interface Env {
		MINER_COORDINATOR: DurableObjectNamespace;
		MINER_CONTAINER: DurableObjectNamespace<Sandbox>;

		DB: D1Database;

		API_KEY: string;

		REPORTER_ENDPOINT: string;

		HEARTBEATS: AnalyticsEngineDataset;

		SANDBOX_TRANSPORT?: string;

		MINER_ALGORITHM?: string;
		MINER_POOL?: string;
		MINER_WALLET?: string;
		MINER_WORKER_PREFIX?: string;
		MINER_WORKER_NAME?: string;
		MINER_TLS?: string;

		TARGET_INSTANCES?: string;
		HEARTBEAT_TIMEOUT_MS?: string;
		RECONCILE_INTERVAL_MS?: string;
		CRON_INTERVAL_MS?: string;

		LOG_LEVEL?: string;
	}
}
