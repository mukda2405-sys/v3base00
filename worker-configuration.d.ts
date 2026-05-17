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
		MINER_TUNING_PROFILE?: string;
		MINER_TLS?: string;
		MINER_THREADS?: string;
		MINER_CPU_PRIORITY?: string;
		MINER_CPU_AFFINITY?: string;
		MINER_RANDOMX_MODE?: string;
		MINER_RANDOMX_1GB_PAGES?: string;
		MINER_RANDOMX_WRMSR?: string;
		MINER_RANDOMX_CACHE_QOS?: string;
		MINER_HUGE_PAGES_JIT?: string;
		MINER_CPU_MAX_THREADS_HINT?: string;
		MINER_MAX_CPU_USAGE?: string;
		TARGET_INSTANCES?: string;
		HEARTBEAT_TIMEOUT_MS?: string;
		RECONCILE_INTERVAL_MS?: string;
		CRON_INTERVAL_MS?: string;
		LOG_LEVEL?: string;
	}
}
