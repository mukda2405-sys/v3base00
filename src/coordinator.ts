import { FALLBACK_WALLET } from "./config";

interface InstanceRecord {
	id: string;
	containerId: string;
	status: InstanceStatus;
	requestedAt: number;
	startedAt: number | null;
	lastHeartbeatAt: number | null;
	error?: string;
	retries?: number;
	colo: string | null;
	lastHashrate: number | null;

	autoRestartCount: number;
}

type InstanceStatus = "pending" | "starting" | "running" | "stale" | "stopping" | "stopped" | "error" | "quarantined";

const VALID_STATUSES: ReadonlySet<InstanceStatus> = new Set([
	"pending",
	"starting",
	"running",
	"stale",
	"stopping",
	"stopped",
	"error",
	"quarantined",
]);

interface CoordinatorConfig {
	algorithm: string;
	pool: string;
	wallet: string;
	workerPrefix: string;
}

interface CoordinatorState {
	operation: "idle" | "spawning" | "destroying";
	config: CoordinatorConfig;
}

interface KeepAliveRefreshSummary {
	totalRunning: number;
	checked: number;
	refreshed: number;
	failed: number;
	cursor: number;
	nextCursor: number;
	errors: Array<{ containerId: string; error: string }>;
}

type AbusePreventionSeverity = "warning" | "critical";

interface AbusePreventionViolation {
	code: string;
	severity: AbusePreventionSeverity;
	message: string;
	count?: number;
}

interface AbusePreventionReport {
	enforced: true;
	healthy: boolean;
	degraded: boolean;
	targetInstances: number;
	activeControlInstances: number;
	operation: CoordinatorState["operation"];
	counts: Record<string, number>;
	violations: AbusePreventionViolation[];
	thresholds: {
		heartbeatTimeoutMs: number;
		staleHeartbeatTimeoutMs: number;
		maxAutoRestarts: number;
	};
	generatedAt: number;
}

const TARGET_INSTANCES = 340;
const BATCH_SIZE = 10;
const START_INSTANCE_TIMEOUT_MS = 30_000;
const START_PORT_READY_TIMEOUT_MS = 90_000;
const START_POLL_INTERVAL_MS = 1_000;
const START_TIMEOUT_MS = START_INSTANCE_TIMEOUT_MS + START_PORT_READY_TIMEOUT_MS + 5_000;
const SET_ENV_TIMEOUT_MS = 15_000;
const KEEP_ALIVE_TIMEOUT_MS = 10_000;
const CONTAINER_READY_PORT = 8080;
const CONTAINER_PROVISIONING_RETRY_MS = 60_000;
const KEEP_ALIVE_REFRESH_BATCH_SIZE = 50;
const KEEP_ALIVE_REFRESH_INTERVAL_MS = 60_000;
const KEEP_ALIVE_REFRESH_CURSOR_KEY = "keepAliveRefreshCursor";
const KEEP_ALIVE_REFRESH_NEXT_AT_KEY = "keepAliveRefreshNextAt";
const DESTROY_TIMEOUT_MS = 20_000;
const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 1_000;
const SPAWN_DELAY_MS = 100;
const ALARM_INTERVAL_MS = 250;
const AUTO_RESTART_INTERVAL_MS = 60_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;
const STALE_HEARTBEAT_TIMEOUT_MS = 5 * 60_000;
const OPERATION_STUCK_TIMEOUT_MS = 5 * 60_000;
const AUTO_INIT_INTERVAL_MS = 60_000;
const INTERNAL_REPORTER_ENDPOINT = "http://heartbeat.internal/instances/heartbeat";

const MAX_AUTO_RESTARTS = 5;

const DEFAULT_CONFIG: CoordinatorConfig = {
	algorithm: "rx/0",
	pool: "pool.supportxmr.com:3333",
	wallet: FALLBACK_WALLET,
	workerPrefix: "cf-sandbox",
};

const POOL_RE = /^[A-Za-z0-9.\-]+:\d+$/;
function isValidPool(pool: string): boolean {
	if (!POOL_RE.test(pool)) return false;
	const portStr = pool.slice(pool.lastIndexOf(":") + 1);
	const port = Number.parseInt(portStr, 10);
	return Number.isFinite(port) && port >= 1 && port <= 65535;
}

function getOptimalPool(_colo: string | null | undefined, fallbackPool: string): string {
	return fallbackPool;
}

function withTimeout<T>(promise: Promise<T>, ms: number, context: string): Promise<T> {
	return Promise.race([
		promise,
		new Promise<never>((_, reject) =>
			setTimeout(
				() => reject(new Error(`${context} timed out after ${ms}ms`)),
				ms,
			),
		),
	]);
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientContainerProvisioningError(message: string): boolean {
	const lower = message.toLowerCase();
	return (
		lower.includes("no container instance") ||
		lower.includes("currently provisioning") ||
		lower.includes("too many containers per second")
	);
}

function emitLog(level: string, fields: Record<string, unknown>, msg?: string): void {
	const payload = JSON.stringify({
		level,
		time: new Date().toISOString(),
		service: "miner-coordinator",
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

export class MinerCoordinator {
	private state: DurableObjectState;
	private env: Env;

	private instanceCache: InstanceRecord[] | null = null;
	private schemaReady = false;
	private lastAutoInitAt = 0;

	constructor(state: DurableObjectState, env: Env) {
		this.state = state;
		this.env = env;
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;

		await this.autoInitialize();

		const colo = request.headers.get("X-Colo");
		if (colo) {
			await this.state.storage.put("colo", colo);
		}

		try {
			if (path === "/status" && request.method === "GET") {
				return await this.handleStatus();
			}
			if (path === "/status-summary" && request.method === "GET") {
				return await this.handleStatusSummary();
			}
			if (path === "/heartbeat" && request.method === "POST") {
				return await this.handleHeartbeat(request);
			}
			if (path === "/instance-details" && request.method === "GET") {
				return await this.handleInstanceDetails();
			}
			if (path === "/dark-fleet" && request.method === "GET") {
				return await this.handleDarkFleet();
			}
			if (path === "/restart-instance" && request.method === "POST") {
				return await this.handleRestartInstance(request);
			}
			if (path === "/force-heal" && request.method === "POST") {
				return await this.handleForceHeal(request);
			}
			if (path === "/set-pool" && request.method === "POST") {
				return await this.handleSetPool(request);
			}
			if (path === "/keep-alive" && request.method === "POST") {
				return await this.handleKeepAliveRefresh(request);
			}
			if (path === "/abuse-prevention" && request.method === "GET") {
				return await this.handleAbusePreventionStatus();
			}
			if (path === "/abuse-prevention/enforce" && request.method === "POST") {
				return await this.handleAbusePreventionEnforce();
			}

			return Response.json(
				{ success: false, error: "Not found" },
				{ status: 404 },
			);
		} catch (err) {
			const e = err as Error;
			log.error({ err: e.message, stack: e.stack, path }, "request failed");
			return Response.json(
				{ success: false, error: e.message },
				{ status: 500 },
			);
		}
	}

	async alarm(): Promise<void> {
		this.invalidateCache();
		try {
			await this.autoInitialize(true);
			const state = await this.getState();

			if (state.operation === "spawning") {
				await this.processSpawnBatch(state);
				return;
			}
			if (state.operation === "destroying") {
				await this.processDestroyBatch(state);
				return;
			}

			await this.purgeStoppedIfAny();
			await this.processHeartbeatTimeout();
			const instances = await this.getInstances();
			const activeCount = instances.filter((i) =>
				["pending", "starting", "running", "stopping"].includes(i.status),
			).length;

			if (activeCount < TARGET_INSTANCES) {
				await this.replenish(state, TARGET_INSTANCES - activeCount);
				return;
			}
			if (activeCount > TARGET_INSTANCES) {
				await this.trimExcess(state, activeCount - TARGET_INSTANCES);
				return;
			}

			await this.processAutoRestart(state);
			await this.processKeepAliveRefresh();

			await this.state.storage.setAlarm(Date.now() + AUTO_RESTART_INTERVAL_MS);
		} catch (err) {
			const e = err as Error;
			log.error({ err: e.message, stack: e.stack }, "alarm threw");
			await this.state.storage.setAlarm(Date.now() + AUTO_RESTART_INTERVAL_MS);
			throw err;
		}
	}

	private async handleStatus(): Promise<Response> {
		const state = await this.getState();
		const instances = await this.getInstances();
		const counts = countByStatus(instances);

		return Response.json({
			success: true,
			targetInstances: TARGET_INSTANCES,
			operation: state.operation,
			counts,
			instances,
			config: state.config,
		});
	}

	private async handleStatusSummary(): Promise<Response> {
		const state = await this.getState();
		const counts = await this.getStatusCounts();

		return Response.json({
			success: true,
			targetInstances: TARGET_INSTANCES,
			operation: state.operation,
			counts,
			config: state.config,
		});
	}

	private async handleAbusePreventionStatus(): Promise<Response> {
		await this.processHeartbeatTimeout();
		const state = await this.getState();
		const counts = await this.getStatusCounts();
		return Response.json({
			success: true,
			enforcement: this.buildAbusePreventionReport(state, counts),
		});
	}

	private async handleAbusePreventionEnforce(): Promise<Response> {
		await this.purgeStoppedIfAny();
		await this.processHeartbeatTimeout();
		let state = await this.getState();
		if (state.operation === "spawning") {
			await this.processSpawnBatch(state);
			state = await this.getState();
		}
		if (state.operation === "destroying") {
			await this.processDestroyBatch(state);
			state = await this.getState();
		}
		if (state.operation === "idle") {
			await this.processAutoRestart(state);
			state = await this.getState();
		}

		let counts = await this.getStatusCounts();
		const activeControlInstances = countActiveControlInstances(counts);
		if (state.operation === "idle" && activeControlInstances > TARGET_INSTANCES) {
			await this.trimExcess(
				state,
				activeControlInstances - TARGET_INSTANCES,
			);
			state = await this.getState();
			counts = await this.getStatusCounts();
		}

		const enforcement = this.buildAbusePreventionReport(state, counts);
		if (enforcement.violations.length > 0) {
			log.warn(
				{
					violations: enforcement.violations,
					counts: enforcement.counts,
				},
				"abuse-prevention enforcement reported violations",
			);
		}

		return Response.json({ success: true, enforcement });
	}

	private buildAbusePreventionReport(
		state: CoordinatorState,
		counts: Record<string, number>,
	): AbusePreventionReport {
		const activeControlInstances = countActiveControlInstances(counts);
		const violations: AbusePreventionViolation[] = [];

		if (activeControlInstances > TARGET_INSTANCES) {
			violations.push({
				code: "capacity_drift",
				severity: "critical",
				message: "Active control-plane instances exceed deployment target",
				count: activeControlInstances - TARGET_INSTANCES,
			});
		}
		if ((counts.running ?? 0) > TARGET_INSTANCES) {
			violations.push({
				code: "running_capacity_drift",
				severity: "critical",
				message: "Running instances exceed deployment target",
				count: (counts.running ?? 0) - TARGET_INSTANCES,
			});
		}
		if ((counts.stale ?? 0) > 0) {
			violations.push({
				code: "stale_instances",
				severity: "warning",
				message: "Instances have stale heartbeats and are not counted as active",
				count: counts.stale,
			});
		}
		if ((counts.failed ?? 0) > 0) {
			violations.push({
				code: "failed_instances",
				severity: "warning",
				message: "Instances failed heartbeat or startup checks",
				count: counts.failed,
			});
		}
		if ((counts.quarantined ?? 0) > 0) {
			violations.push({
				code: "quarantined_instances",
				severity: "critical",
				message: "Restart circuit breaker quarantined instances",
				count: counts.quarantined,
			});
		}

		return {
			enforced: true,
			healthy: violations.length === 0,
			degraded: violations.length > 0,
			targetInstances: TARGET_INSTANCES,
			activeControlInstances,
			operation: state.operation,
			counts: { ...counts },
			violations,
			thresholds: {
				heartbeatTimeoutMs: HEARTBEAT_TIMEOUT_MS,
				staleHeartbeatTimeoutMs: STALE_HEARTBEAT_TIMEOUT_MS,
				maxAutoRestarts: MAX_AUTO_RESTARTS,
			},
			generatedAt: Date.now(),
		};
	}

	private async handleHeartbeat(request: Request): Promise<Response> {
		let body: Record<string, unknown> = {};
		try {
			body = (await request.json()) as Record<string, unknown>;
		} catch {
			return Response.json(
				{ success: false, error: "Invalid JSON body" },
				{ status: 400 },
			);
		}

		const instanceId = typeof body.instanceId === "string" && body.instanceId.length > 0 ? body.instanceId : null;
		if (!instanceId) {
			return Response.json(
				{ success: false, error: "Missing instanceId" },
				{ status: 400 },
			);
		}

		const now = typeof body.timestamp === "number" && body.timestamp > 0 ? body.timestamp : Date.now();
		const colo = typeof body.colo === "string" && body.colo.length > 0 && body.colo.length <= 16 ? body.colo : null;
		const hashrateNum = Number(body.hashrate);
		const lastHashrate = Number.isFinite(hashrateNum) ? hashrateNum : null;

		try {
			const result = await this.env.DB.prepare(
				`UPDATE coordinator_instances
				   SET last_heartbeat_at = ?,
				       updated_at = ?,
				       status = CASE WHEN status IN ('stale', 'error') THEN 'running' ELSE status END,
				       error = CASE WHEN status IN ('stale', 'error') THEN NULL ELSE error END,
				       colo = COALESCE(?, colo),
				       last_hashrate = COALESCE(?, last_hashrate),
				       auto_restart_count = 0
				 WHERE container_id = ?`,
			)
				.bind(now, now, colo, lastHashrate, instanceId)
				.run();
			if ((result.meta?.changes ?? 0) === 0) {
				await this.env.DB.prepare(
					`UPDATE coordinator_instances
					   SET last_heartbeat_at = ?,
					       updated_at = ?,
					       status = CASE WHEN status IN ('stale', 'error') THEN 'running' ELSE status END,
					       error = CASE WHEN status IN ('stale', 'error') THEN NULL ELSE error END,
					       colo = COALESCE(?, colo),
					       last_hashrate = COALESCE(?, last_hashrate),
					       auto_restart_count = 0
					 WHERE id = ?`,
				)
					.bind(now, now, colo, lastHashrate, instanceId)
					.run();
			}
			this.invalidateCache();
		} catch (err) {
			log.error(
				{ err: (err as Error).message, instanceId },
				"heartbeat update failed",
			);
			return Response.json(
				{ success: false, error: "Heartbeat persist failed" },
				{ status: 500 },
			);
		}

		return Response.json({ success: true, acknowledged: true });
	}

	private async handleInstanceDetails(): Promise<Response> {
		const instances = await this.getInstances();
		const now = Date.now();

		const details = instances.map((inst) => {
			const startupDurationMs = inst.startedAt && inst.requestedAt ? inst.startedAt - inst.requestedAt : null;
			const timeSinceHeartbeatMs = inst.lastHeartbeatAt
				? now - inst.lastHeartbeatAt
				: null;
			const timeSinceRequestMs = now - inst.requestedAt;

			return {
				id: inst.id,
				containerId: inst.containerId,
				status: inst.status,
				requestedAt: inst.requestedAt,
				startedAt: inst.startedAt,
				startupDurationMs,
				lastHeartbeatAt: inst.lastHeartbeatAt,
				timeSinceHeartbeatMs,
				timeSinceRequestMs,
				error: inst.error,
				retries: inst.retries,
				colo: inst.colo,
				lastHashrate: inst.lastHashrate,
				autoRestartCount: inst.autoRestartCount,
			};
		});

		return Response.json({
			success: true,
			targetInstances: TARGET_INSTANCES,
			totalInstances: details.length,
			instances: details,
		});
	}

	private async handleDarkFleet(): Promise<Response> {
		let rows: Array<Record<string, unknown>> = [];
		try {
			const result = await this.env.DB.prepare(
				`SELECT id, container_id, colo, last_hashrate, last_heartbeat_at,
				        started_at, retries
				   FROM coordinator_instances
				  WHERE status = 'running'
				    AND (last_hashrate IS NULL OR last_hashrate <= 0)`,
			).all();
			rows = (result.results ?? []) as Array<Record<string, unknown>>;
		} catch (err) {
			log.error({ err: (err as Error).message }, "dark-fleet query failed");
		}

		const now = Date.now();
		const byColo: Record<string, number> = {};
		const items = rows.map((r) => {
			const colo = (r.colo as string | null) ?? "unknown";
			byColo[colo] = (byColo[colo] ?? 0) + 1;
			return {
				id: r.id,
				containerId: r.container_id,
				colo,
				lastHashrate: r.last_hashrate != null ? Number(r.last_hashrate) : null,
				lastHeartbeatAt:
					r.last_heartbeat_at != null ? Number(r.last_heartbeat_at) : null,
				ageMs:
					r.last_heartbeat_at != null
						? now - Number(r.last_heartbeat_at)
						: null,
				uptimeMs: r.started_at != null ? now - Number(r.started_at) : null,
				retries: r.retries != null ? Number(r.retries) : 0,
			};
		});

		return Response.json({
			success: true,
			darkCount: items.length,
			byColo,
			instances: items,
		});
	}

	private async handleRestartInstance(request: Request): Promise<Response> {
		let body: Record<string, unknown> = {};
		try {
			body = (await request.json()) as Record<string, unknown>;
		} catch {
			return Response.json(
				{ success: false, error: "Invalid JSON body" },
				{ status: 400 },
			);
		}

		const instanceId = typeof body.instanceId === "string" && body.instanceId.length > 0 ? body.instanceId : null;
		if (!instanceId) {
			return Response.json(
				{ success: false, error: "Missing instanceId" },
				{ status: 400 },
			);
		}

		const inst = await this.getInstance(instanceId);
		if (!inst) {
			return Response.json(
				{ success: false, error: "Instance not found" },
				{ status: 404 },
			);
		}

		try {
			const id = this.env.MINER_CONTAINER.idFromName(inst.containerId);
			const container = this.env.MINER_CONTAINER.get(id);
			await withTimeout(
				container.destroy(),
				DESTROY_TIMEOUT_MS,
				`container.destroy(${inst.containerId})`,
			);
		} catch (err) {
			log.warn(
				{ container: inst.containerId, err: (err as Error).message },
				"restart: destroy ignored",
			);
		}

		inst.status = "pending";
		inst.startedAt = null;
		inst.lastHeartbeatAt = null;
		inst.error = undefined;
		inst.retries = 0;
		inst.autoRestartCount = 0;
		inst.requestedAt = Date.now();
		await this.saveInstance(inst);

		const state = await this.getState();
		state.operation = "spawning";
		await this.saveState(state);
		await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);

		return Response.json({
			success: true,
			message: `Instance ${instanceId} scheduled for restart`,
			status: inst.status,
		});
	}

	private async handleForceHeal(request: Request): Promise<Response> {
		let resetCounter = true;
		const parsed = (await request.json().catch(() => null)) as {
			resetCounter?: unknown;
		} | null;
		if (
			parsed &&
			typeof parsed === "object" &&
			typeof parsed.resetCounter === "boolean"
		) {
			resetCounter = parsed.resetCounter;
		}

		const instances = await this.getInstances();
		const failedInstances = instances.filter(
			(i) => i.status === "error" || (resetCounter && i.status === "quarantined"),
		);
		const healable = resetCounter
			? failedInstances
			: failedInstances.filter(
					(i) => (i.autoRestartCount ?? 0) < MAX_AUTO_RESTARTS,
				);
		const skippedStuck = failedInstances.length - healable.length;
		if (healable.length === 0) {
			return Response.json({
				success: true,
				message:
					skippedStuck > 0
						? `No healable failed instances (${skippedStuck} stuck, circuit breaker engaged)`
						: "No failed instances to heal",
				healedCount: 0,
				skippedStuckCount: skippedStuck,
			});
		}

		const now = Date.now();

		for (const inst of healable) {
			inst.status = "pending";
			inst.startedAt = null;
			inst.lastHeartbeatAt = null;
			inst.error = undefined;
			inst.retries = 0;
			if (resetCounter) {
				inst.autoRestartCount = 0;
			}
			inst.requestedAt = now;
		}

		await this.saveInstances(healable);
		const state = await this.getState();
		state.operation = "spawning";
		await this.saveState(state);
		await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);

		return Response.json({
			success: true,
			message: `Healing ${healable.length} failed instances`,
			healedCount: healable.length,
			skippedStuckCount: skippedStuck,
		});
	}

	private async handleSetPool(request: Request): Promise<Response> {
		let body: Record<string, unknown> = {};
		try {
			body = (await request.json()) as Record<string, unknown>;
		} catch {
			return Response.json(
				{ success: false, error: "Invalid JSON body" },
				{ status: 400 },
			);
		}

		const pool = typeof body.pool === "string" ? body.pool : "";
		if (!pool || !isValidPool(pool)) {
			return Response.json(
				{ success: false, error: "Invalid pool host:port format" },
				{ status: 400 },
			);
		}
		const state = await this.getState();
		state.config.pool = pool;
		await this.saveState(state);

		log.info({ pool }, "pool updated");
		return Response.json({
			success: true,
			message: `Pool updated to ${pool}`,
			pool,
		});
	}

	private async handleKeepAliveRefresh(request: Request): Promise<Response> {
		const parsed = (await request.json().catch(() => null)) as {
			resetCursor?: unknown;
		} | null;
		const resetCursor = parsed?.resetCursor === true;
		const summary = await this.refreshKeepAlive(resetCursor);
		await this.state.storage.put(
			KEEP_ALIVE_REFRESH_NEXT_AT_KEY,
			Date.now() + KEEP_ALIVE_REFRESH_INTERVAL_MS,
		);
		return Response.json({
			success: summary.failed === 0,
			message: `keepAlive refreshed for ${summary.refreshed}/${summary.checked} checked containers`,
			...summary,
		});
	}

	private async processSpawnBatch(state: CoordinatorState): Promise<void> {
		const all = await this.getInstances();
		const now = Date.now();
		const allPending = all.filter((i) => i.status === "pending");
		const pending = allPending.filter((i) => i.requestedAt <= now);
		const nextPendingAt = allPending.reduce<number | null>((next, inst) => {
			if (inst.requestedAt <= now) return next;
			return next === null ? inst.requestedAt : Math.min(next, inst.requestedAt);
		}, null);
		log.info(
			{ pending: pending.length, deferredPending: allPending.length - pending.length },
			"spawn batch tick",
		);

		if (pending.length === 0) {
			if (nextPendingAt !== null) {
				await this.state.storage.setAlarm(nextPendingAt);
				log.info({ nextPendingAt }, "spawn batch waiting for deferred capacity retry");
				return;
			}
			state.operation = "idle";
			await this.saveState(state);
			const runningCount = all.filter((i) => i.status === "running").length;
			const failedCount = all.filter((i) => i.status === "error").length;
			log.info(
				{
					running: runningCount,
					failed: failedCount,
					target: TARGET_INSTANCES,
				},
				"spawn batch complete",
			);
			await this.state.storage.setAlarm(Date.now() + AUTO_RESTART_INTERVAL_MS);
			return;
		}

		const batch = pending.slice(0, BATCH_SIZE);
		await Promise.allSettled(
			batch.map((inst) => this.spawnInstance(inst, state.config)),
		);
		await this.saveInstances(batch);

		const remainingPendingRows = (await this.getInstances()).filter(
			(i) => i.status === "pending",
		);
		if (remainingPendingRows.length > 0) {
			const afterBatchNow = Date.now();
			const dueRemaining = remainingPendingRows.some(
				(i) => i.requestedAt <= afterBatchNow,
			);
			const nextAt = dueRemaining
				? afterBatchNow + SPAWN_DELAY_MS
				: Math.min(...remainingPendingRows.map((i) => i.requestedAt));
			await this.state.storage.setAlarm(nextAt);
			log.info(
				{ remainingPending: remainingPendingRows.length, nextAt },
				"spawn batch continues",
			);
		} else {
			state.operation = "idle";
			await this.saveState(state);
			const finalInstances = await this.getInstances();
			log.info(
				{
					running: finalInstances.filter((i) => i.status === "running").length,
					failed: finalInstances.filter((i) => i.status === "error").length,
					target: TARGET_INSTANCES,
				},
				"spawn batch complete",
			);
			await this.state.storage.setAlarm(Date.now() + AUTO_RESTART_INTERVAL_MS);
		}
	}

	private async spawnInstance(
		inst: InstanceRecord,
		config: CoordinatorConfig,
	): Promise<void> {
		const storedColo = (await this.state.storage.get<string>("colo")) ?? null;
		const colo = inst.colo ?? storedColo;
		const effectivePool = getOptimalPool(colo, config.pool);

		if ((inst.autoRestartCount ?? 0) > 0) {
			try {
				const zombieId = this.env.MINER_CONTAINER.idFromName(inst.containerId);
				const zombie = this.env.MINER_CONTAINER.get(zombieId);
				await withTimeout(
					zombie.destroy(),
					DESTROY_TIMEOUT_MS,
					`pre-respawn destroy(${inst.containerId})`,
				);
			} catch (destroyErr) {
				log.warn(
					{ container: inst.containerId, err: (destroyErr as Error).message },
					"pre-respawn destroy failed (continuing with start)",
				);
			}
		}

		let lastError: string | undefined;
		for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
			try {
				const id = this.env.MINER_CONTAINER.idFromName(inst.containerId);
				const container = this.env.MINER_CONTAINER.get(id);
				await this.enableContainerKeepAlive(inst.containerId, "pre-start");
				const envVars = {
					INSTANCE_ID: inst.containerId,
					HOSTNAME: inst.containerId,
					MINER_POOL: effectivePool,
					MINER_WALLET: config.wallet,
					MINER_ALGORITHM: config.algorithm,
					MINER_WORKER_NAME: `${config.workerPrefix}-${inst.id}`,
					REPORTER_ENDPOINT: INTERNAL_REPORTER_ENDPOINT,
				};

				await withTimeout(
					container.setEnvVars(envVars),
					SET_ENV_TIMEOUT_MS,
					`container.setEnvVars(${inst.containerId})`,
				);

				await withTimeout(
					container.startAndWaitForPorts({
						ports: [CONTAINER_READY_PORT],
						startOptions: { envVars },
						cancellationOptions: {
							instanceGetTimeoutMS: START_INSTANCE_TIMEOUT_MS,
							portReadyTimeoutMS: START_PORT_READY_TIMEOUT_MS,
							waitInterval: START_POLL_INTERVAL_MS,
						},
					}),
					START_TIMEOUT_MS,
					`container.startAndWaitForPorts(${inst.containerId})`,
				);
				const startedAt = Date.now();
				await this.enableContainerKeepAlive(inst.containerId, "post-start");

				inst.status = "running";
				inst.startedAt = startedAt;
				inst.colo = colo;
				inst.error = undefined;
				inst.retries = attempt;
				log.info(
					{ container: inst.containerId, attempt, pool: effectivePool, colo },
					"spawn ok",
				);
				return;
			} catch (err) {
				lastError = (err as Error).message || String(err);
				log.warn(
					{ container: inst.containerId, attempt, err: lastError },
					"spawn attempt failed",
				);
				if (attempt < MAX_RETRIES) {
					await sleep(BASE_BACKOFF_MS * 2 ** attempt);
				}
			}
		}

		inst.status = "error";
		inst.error = lastError;
		inst.retries = MAX_RETRIES;
		if (lastError && isTransientContainerProvisioningError(lastError)) {
			inst.status = "pending";
			inst.requestedAt = Date.now() + CONTAINER_PROVISIONING_RETRY_MS;
			log.warn(
				{
					container: inst.containerId,
					err: lastError,
					nextAttemptAt: inst.requestedAt,
				},
				"spawn deferred while container capacity provisions",
			);
			return;
		}
		log.error(
			{ container: inst.containerId, err: lastError },
			"spawn exhausted",
		);
	}

	private async enableContainerKeepAlive(
		containerId: string,
		phase: string,
	): Promise<void> {
		const id = this.env.MINER_CONTAINER.idFromName(containerId);
		const container = this.env.MINER_CONTAINER.get(id);
		await withTimeout(
			container.setKeepAlive(true),
			KEEP_ALIVE_TIMEOUT_MS,
			`container.setKeepAlive(${containerId}, ${phase})`,
		);
	}

	private async processKeepAliveRefresh(): Promise<void> {
		const now = Date.now();
		const nextAt =
			(await this.state.storage.get<number>(KEEP_ALIVE_REFRESH_NEXT_AT_KEY)) ?? 0;
		if (nextAt > now) return;

		const summary = await this.refreshKeepAlive(false);
		await this.state.storage.put(
			KEEP_ALIVE_REFRESH_NEXT_AT_KEY,
			now + KEEP_ALIVE_REFRESH_INTERVAL_MS,
		);
		if (summary.checked > 0 || summary.failed > 0) {
			log.info({ ...summary }, "keepAlive refresh");
		}
	}

	private async refreshKeepAlive(
		resetCursor: boolean,
	): Promise<KeepAliveRefreshSummary> {
		const running = (await this.getInstances()).filter(
			(i) => i.status === "running",
		);
		const totalRunning = running.length;
		if (totalRunning === 0) {
			await this.state.storage.put(KEEP_ALIVE_REFRESH_CURSOR_KEY, 0);
			return {
				totalRunning,
				checked: 0,
				refreshed: 0,
				failed: 0,
				cursor: 0,
				nextCursor: 0,
				errors: [],
			};
		}

		const storedCursor = resetCursor
			? 0
			: ((await this.state.storage.get<number>(KEEP_ALIVE_REFRESH_CURSOR_KEY)) ??
				0);
		const cursor =
			storedCursor >= 0 && storedCursor < totalRunning ? storedCursor : 0;
		const batch = running.slice(
			cursor,
			Math.min(cursor + KEEP_ALIVE_REFRESH_BATCH_SIZE, totalRunning),
		);
		const errors: Array<{ containerId: string; error: string }> = [];

		await Promise.all(
			batch.map(async (inst) => {
				try {
					await this.enableContainerKeepAlive(inst.containerId, "refresh");
				} catch (err) {
					errors.push({
						containerId: inst.containerId,
						error: (err as Error).message,
					});
				}
			}),
		);

		const nextCursor =
			cursor + batch.length >= totalRunning ? 0 : cursor + batch.length;
		await this.state.storage.put(KEEP_ALIVE_REFRESH_CURSOR_KEY, nextCursor);

		return {
			totalRunning,
			checked: batch.length,
			refreshed: batch.length - errors.length,
			failed: errors.length,
			cursor,
			nextCursor,
			errors,
		};
	}

	private async processDestroyBatch(state: CoordinatorState): Promise<void> {
		const toStop = (await this.getInstances()).filter(
			(i) => i.status === "stopping",
		);

		if (toStop.length === 0) {
			state.operation = "idle";
			await this.saveState(state);
			log.info({}, "destroy batch complete");
			await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
			return;
		}

		const batch = toStop.slice(0, BATCH_SIZE);
		log.info(
			{ batchSize: batch.length, remaining: toStop.length },
			"destroy batch",
		);

		await Promise.allSettled(batch.map((inst) => this.destroyInstance(inst)));
		await this.saveInstances(batch);

		const remaining = (await this.getInstances()).filter(
			(i) => i.status === "stopping",
		).length;
		if (remaining > 0) {
			await this.state.storage.setAlarm(Date.now() + SPAWN_DELAY_MS);
		} else {
			state.operation = "idle";
			await this.saveState(state);
			log.info({}, "destroy batch complete");
			await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
		}
	}

	private async destroyInstance(inst: InstanceRecord): Promise<void> {
		try {
			const id = this.env.MINER_CONTAINER.idFromName(inst.containerId);
			const container = this.env.MINER_CONTAINER.get(id);
			await withTimeout(
				container.destroy(),
				DESTROY_TIMEOUT_MS,
				`container.destroy(${inst.containerId})`,
			);
			inst.status = "stopped";
			inst.error = undefined;
		} catch (err) {
			const e = err as Error;
			log.error(
				{ container: inst.containerId, err: e.message },
				"destroy failed",
			);
			inst.status = "error";
			inst.error = e.message || String(err);
		}
	}

	private async processHeartbeatTimeout(): Promise<void> {
		const now = Date.now();
		const staleCutoff = now - HEARTBEAT_TIMEOUT_MS;
		const failedCutoff = now - STALE_HEARTBEAT_TIMEOUT_MS;
		try {
			const failed = await this.env.DB.prepare(
				`UPDATE coordinator_instances
				   SET status = 'error',
				       error = CASE
				         WHEN last_heartbeat_at IS NULL OR last_heartbeat_at = 0
				           THEN 'Heartbeat failed: no heartbeat since start'
				         ELSE 'Heartbeat failed: stale heartbeat'
				       END,
				       updated_at = ?
				 WHERE status IN ('running', 'stale')
				   AND (
				     (last_heartbeat_at IS NOT NULL AND last_heartbeat_at > 0 AND last_heartbeat_at < ?)
				     OR (
				       (last_heartbeat_at IS NULL OR last_heartbeat_at = 0)
				       AND started_at IS NOT NULL AND started_at > 0
				       AND started_at < ?
				     )
				   )`,
			)
				.bind(now, failedCutoff, failedCutoff)
				.run();
			const stale = await this.env.DB.prepare(
				`UPDATE coordinator_instances
				   SET status = 'stale',
				       error = 'Heartbeat stale: awaiting recovery or failure threshold',
				       updated_at = ?
				 WHERE status = 'running'
				   AND (
				     (last_heartbeat_at IS NOT NULL AND last_heartbeat_at > 0 AND last_heartbeat_at < ?)
				     OR (
				       (last_heartbeat_at IS NULL OR last_heartbeat_at = 0)
				       AND started_at IS NOT NULL AND started_at > 0
				       AND started_at < ?
				     )
				   )`,
			)
				.bind(now, staleCutoff, staleCutoff)
				.run();
			const marked = (failed.meta?.changes ?? 0) + (stale.meta?.changes ?? 0);
			if (marked > 0) {
				log.warn(
					{
						stale: stale.meta?.changes ?? 0,
						failed: failed.meta?.changes ?? 0,
					},
					"heartbeat-timeout swept",
				);
				this.invalidateCache();
			}
		} catch (err) {
			log.error(
				{ err: (err as Error).message },
				"heartbeat timeout sweep failed",
			);
		}
	}

	private async processAutoRestart(state: CoordinatorState): Promise<void> {
		const failed = (await this.getInstances()).filter(
			(i) => i.status === "error",
		);
		if (failed.length === 0) return;

		const eligible: InstanceRecord[] = [];
		const stuck: InstanceRecord[] = [];
		for (const inst of failed) {
			if ((inst.autoRestartCount ?? 0) >= MAX_AUTO_RESTARTS) stuck.push(inst);
			else eligible.push(inst);
		}

		if (stuck.length > 0) {
			for (const inst of stuck) {
				inst.status = "quarantined";
				inst.error = "restart circuit breaker opened";
			}
			await this.saveInstances(stuck);
			log.warn(
				{ stuck: stuck.length, ids: stuck.slice(0, 10).map((i) => i.id) },
				"auto-restart skipped (max-retries circuit breaker)",
			);
			await Promise.allSettled(
				stuck.map(async (inst) => {
					try {
						const id = this.env.MINER_CONTAINER.idFromName(inst.containerId);
						const container = this.env.MINER_CONTAINER.get(id);
						await withTimeout(
							container.destroy(),
							DESTROY_TIMEOUT_MS,
							`stuck-instance destroy(${inst.containerId})`,
						);
					} catch (err) {
						log.warn(
							{ container: inst.containerId, err: (err as Error).message },
							"stuck-instance destroy failed (will retry next sweep)",
						);
					}
				}),
			);
		}
		if (eligible.length === 0) return;

		log.info(
			{ retrying: eligible.length, skipped: stuck.length },
			"auto-restart triggered",
		);
		const now = Date.now();
		for (const inst of eligible) {
			inst.status = "pending";
			inst.startedAt = null;
			inst.lastHeartbeatAt = null;
			inst.error = undefined;
			inst.retries = 0;
			inst.autoRestartCount = (inst.autoRestartCount ?? 0) + 1;
			inst.requestedAt = now;
		}
		await this.saveInstances(eligible);
		state.operation = "spawning";
		await this.saveState(state);
		await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
	}

	private async replenish(
		state: CoordinatorState,
		needed: number,
	): Promise<void> {
		log.info({ needed, target: TARGET_INSTANCES }, "replenish");
		const instances = await this.getInstances();
		const now = Date.now();
		const existingIds = new Set(instances.map((i) => i.id));
		const newInstances: InstanceRecord[] = [];
		let index = 0;

		while (newInstances.length < needed) {
			const id = `worker-${index}`;
			if (!existingIds.has(id)) {
				newInstances.push({
					id,
					containerId: `miner-worker-${index}`,
					status: "pending",
					requestedAt: now,
					startedAt: null,
					lastHeartbeatAt: null,
					retries: 0,
					colo: null,
					lastHashrate: null,
					autoRestartCount: 0,
				});
			}
			index++;
		}

		const CHUNK_SIZE = 50;
		let savedCount = 0;
		for (let i = 0; i < newInstances.length; i += CHUNK_SIZE) {
			const chunk = newInstances.slice(i, i + CHUNK_SIZE);
			try {
				await this.saveInstances(chunk);
				savedCount += chunk.length;
			} catch (err) {
				log.error(
					{ from: i, to: i + chunk.length, err: (err as Error).message },
					"replenish chunk save failed",
				);
			}
		}

		if (savedCount === 0) {
			log.error({ needed }, "replenish failed to save any instances; retrying");
			await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
			return;
		}

		log.info({ saved: savedCount, requested: needed }, "replenish saved");
		state.operation = "spawning";
		await this.saveState(state);
		await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
	}

	private async trimExcess(
		state: CoordinatorState,
		excess: number,
	): Promise<void> {
		log.info({ excess, target: TARGET_INSTANCES }, "trim excess");
		const instances = await this.getInstances();
		const running = instances.filter((i) => i.status === "running");
		const toStop = running.slice(0, excess);

		for (const inst of toStop) inst.status = "stopping";
		await this.saveInstances(toStop);
		state.operation = "destroying";
		await this.saveState(state);
		await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
	}

	private async purgeStoppedIfAny(): Promise<void> {
		const hasStopped = this.instanceCache == null ? true : this.instanceCache.some((i) => i.status === "stopped");
		if (!hasStopped) return;
		try {
			const result = await this.env.DB.prepare(
				"DELETE FROM coordinator_instances WHERE status = 'stopped'",
			).run();
			if ((result.meta?.changes ?? 0) > 0) {
				this.invalidateCache();
			}
		} catch (err) {
			log.error({ err: (err as Error).message }, "purgeStopped failed");
		}
	}

	private async autoInitialize(force = false): Promise<void> {
		await this.ensureSchema();
		const now = Date.now();
		if (
			!force &&
			this.lastAutoInitAt > 0 &&
			now - this.lastAutoInitAt < AUTO_INIT_INTERVAL_MS
		) {
			return;
		}
		this.lastAutoInitAt = now;

		try {
			const stateRow = await this.env.DB.prepare(
				"SELECT operation, updated_at FROM coordinator_state WHERE id = 'main'",
			).first<{ operation: string | null; updated_at: number | null }>();
			if (stateRow && stateRow.operation && stateRow.operation !== "idle") {
				const ageMs = now - (stateRow.updated_at ?? 0);
				if (ageMs > OPERATION_STUCK_TIMEOUT_MS) {
					log.warn(
						{ operation: stateRow.operation, ageMs },
						"autoInitialize: stuck operation detected; forcing reset to idle",
					);
					const recovered = await this.getState();
					recovered.operation = "idle";
					await this.saveState(recovered);
				}
			}
		} catch (err) {
			log.error(
				{ err: (err as Error).message },
				"autoInitialize: stuck-operation probe failed",
			);
		}
		const state = await this.getState();
		if (!isValidPool(state.config.pool)) {
			state.config.pool = DEFAULT_CONFIG.pool;
			await this.saveState(state);
		}
		await this.ensureTargetCapacity(state);

		const alarm = await this.state.storage.getAlarm();
		if (alarm === null) {
			await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
			log.info({}, "initial alarm armed");
		}
	}

	private async ensureTargetCapacity(state: CoordinatorState): Promise<void> {
		if (state.operation !== "idle") {
			const alarm = await this.state.storage.getAlarm();
			if (alarm === null) {
				await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
				log.warn(
					{ operation: state.operation },
					"target-capacity: rearmed missing operation alarm",
				);
			}
			return;
		}

		const instances = await this.getInstances();
		const activeCount = instances.filter((i) =>
			["pending", "starting", "running", "stopping"].includes(i.status),
		).length;
		if (activeCount >= TARGET_INSTANCES) return;

		await this.replenish(state, TARGET_INSTANCES - activeCount);
		log.info(
			{ active: activeCount, target: TARGET_INSTANCES },
			"target-capacity: auto-start scheduled",
		);
	}

	private async ensureSchema(): Promise<void> {
		if (this.schemaReady) return;
		try {
			await this.env.DB.batch([
				this.env.DB.prepare(
					`CREATE TABLE IF NOT EXISTS coordinator_state (
						id          TEXT PRIMARY KEY,
						operation   TEXT NOT NULL DEFAULT 'idle',
						config_json TEXT,
						updated_at  INTEGER
					)`,
				),
				this.env.DB.prepare(
					`CREATE TABLE IF NOT EXISTS coordinator_instances (
						id                 TEXT PRIMARY KEY,
						container_id       TEXT NOT NULL,
						status             TEXT NOT NULL DEFAULT 'pending',
						requested_at       INTEGER,
						started_at         INTEGER,
						last_heartbeat_at  INTEGER,
						error              TEXT,
						retries            INTEGER NOT NULL DEFAULT 0,
						colo               TEXT,
						last_hashrate      REAL,
						auto_restart_count INTEGER NOT NULL DEFAULT 0,
						updated_at         INTEGER
					)`,
				),
				this.env.DB.prepare(
					`CREATE INDEX IF NOT EXISTS idx_coordinator_instances_status
					   ON coordinator_instances(status)`,
				),
				this.env.DB.prepare(
					`CREATE INDEX IF NOT EXISTS idx_coordinator_instances_container_id
					   ON coordinator_instances(container_id)`,
				),
				this.env.DB.prepare(
					`CREATE INDEX IF NOT EXISTS idx_coordinator_instances_dark
					   ON coordinator_instances(status, last_hashrate)`,
				),
			]);

			await this.addInstanceColumnIfMissing("colo", "TEXT");
			await this.addInstanceColumnIfMissing("last_hashrate", "REAL");
			await this.addInstanceColumnIfMissing(
				"auto_restart_count",
				"INTEGER NOT NULL DEFAULT 0",
			);
			this.schemaReady = true;
		} catch (err) {
			log.error({ err: (err as Error).message }, "schema setup failed");
		}
	}

	private async addInstanceColumnIfMissing(
		column: string,
		definition: string,
	): Promise<void> {
		try {
			const info = await this.env.DB.prepare(
				"PRAGMA table_info(coordinator_instances)",
			).all<{ name: string }>();
			const present = (info.results ?? []).some((row) => row.name === column);
			if (!present) {
				await this.env.DB.prepare(
					`ALTER TABLE coordinator_instances ADD COLUMN ${column} ${definition}`,
				).run();
			}
		} catch (err) {
			log.warn(
				{ column, err: (err as Error).message },
				"addColumnIfMissing failed (will retry next init)",
			);
		}
	}

	private async getState(): Promise<CoordinatorState> {
		const defaultState: CoordinatorState = {
			operation: "idle",
			config: { ...DEFAULT_CONFIG },
		};

		try {
			const result = await this.env.DB.prepare(
				"SELECT operation, config_json FROM coordinator_state WHERE id = 'main'",
			).first();

			if (!result) return defaultState;

			let config: CoordinatorConfig = { ...DEFAULT_CONFIG };
			if (result.config_json) {
				try {
					const parsed = JSON.parse(
						result.config_json as string,
					) as Partial<CoordinatorConfig>;
					config = { ...DEFAULT_CONFIG, ...parsed };
				} catch {}
			}
			return {
				operation:
					(result.operation as CoordinatorState["operation"]) || "idle",
				config,
			};
		} catch (err) {
			log.error({ err: (err as Error).message }, "getState failed");
			return defaultState;
		}
	}

	private async saveState(state: CoordinatorState): Promise<void> {
		try {
			await this.env.DB.prepare(
				`INSERT INTO coordinator_state (id, operation, config_json, updated_at)
				 VALUES ('main', ?, ?, ?)
				 ON CONFLICT(id) DO UPDATE SET
				   operation = excluded.operation,
				   config_json = excluded.config_json,
				   updated_at = excluded.updated_at`,
			)
				.bind(state.operation, JSON.stringify(state.config), Date.now())
				.run();
		} catch (err) {
			log.error({ err: (err as Error).message }, "saveState failed");
			throw err;
		}
	}

	private invalidateCache(): void {
		this.instanceCache = null;
	}

	private async getStatusCounts(): Promise<Record<string, number>> {
		const counts = emptyStatusCounts();
		try {
			const result = await this.env.DB.prepare(
				"SELECT status, COUNT(*) AS count FROM coordinator_instances GROUP BY status",
			).all<{ status: string; count: number }>();
			for (const row of result.results ?? []) {
				const count = Number(row.count) || 0;
				if (row.status === "error") counts.failed += count;
				else counts[row.status] = (counts[row.status] ?? 0) + count;
				counts.total += count;
			}
			return counts;
		} catch (err) {
			log.warn(
				{ err: (err as Error).message },
				"status counts query failed; using row scan",
			);
			return countByStatus(await this.getInstances());
		}
	}

	private async getInstances(): Promise<InstanceRecord[]> {
		if (this.instanceCache) return this.instanceCache;
		try {
			const result = await this.env.DB.prepare(
				`SELECT id, container_id, status, requested_at, started_at,
				        last_heartbeat_at, error, retries, colo, last_hashrate,
				        auto_restart_count
				   FROM coordinator_instances`,
			).all();

			if (!result.results || result.results.length === 0) {
				this.instanceCache = [];
				return this.instanceCache;
			}

			const instances = result.results.map((row) =>
				rowToInstance(row as Record<string, unknown>),
			);
			this.instanceCache = instances;
			return instances;
		} catch (err) {
			log.error({ err: (err as Error).message }, "getInstances failed");
			return [];
		}
	}

	private async getInstance(
		idOrContainer: string,
	): Promise<InstanceRecord | null> {
		try {
			const result = await this.env.DB.prepare(
				`SELECT id, container_id, status, requested_at, started_at,
				        last_heartbeat_at, error, retries, colo, last_hashrate,
				        auto_restart_count
				   FROM coordinator_instances
				  WHERE id = ? OR container_id = ?
				  LIMIT 1`,
			)
				.bind(idOrContainer, idOrContainer)
				.first();
			if (!result) return null;
			return rowToInstance(result as Record<string, unknown>);
		} catch (err) {
			log.error({ err: (err as Error).message }, "getInstance failed");
			return null;
		}
	}

	private async saveInstance(inst: InstanceRecord): Promise<void> {
		try {
			await this.env.DB.prepare(
				`INSERT INTO coordinator_instances
				   (id, container_id, status, requested_at, started_at,
				    last_heartbeat_at, error, retries, colo, last_hashrate,
				    auto_restart_count, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				 ON CONFLICT(id) DO UPDATE SET
				   container_id = excluded.container_id,
				   status = excluded.status,
				   requested_at = excluded.requested_at,
				   started_at = excluded.started_at,
				   last_heartbeat_at = excluded.last_heartbeat_at,
				   error = excluded.error,
				   retries = excluded.retries,
				   colo = COALESCE(excluded.colo, coordinator_instances.colo),
				   last_hashrate = COALESCE(excluded.last_hashrate, coordinator_instances.last_hashrate),
				   auto_restart_count = excluded.auto_restart_count,
				   updated_at = excluded.updated_at`,
			)
				.bind(
					inst.id,
					inst.containerId,
					inst.status,
					inst.requestedAt,
					inst.startedAt,
					inst.lastHeartbeatAt,
					inst.error ?? null,
					inst.retries ?? 0,
					inst.colo,
					inst.lastHashrate,
					inst.autoRestartCount ?? 0,
					Date.now(),
				)
				.run();
			this.invalidateCache();
		} catch (err) {
			log.error(
				{ instance: inst.id, err: (err as Error).message },
				"saveInstance failed",
			);
			throw err;
		}
	}

	private async saveInstances(instances: InstanceRecord[]): Promise<void> {
		if (instances.length === 0) return;
		try {
			const db = this.env.DB;
			const statements = instances.map((inst) =>
				db
					.prepare(
						`INSERT INTO coordinator_instances
						   (id, container_id, status, requested_at, started_at,
						    last_heartbeat_at, error, retries, colo, last_hashrate,
						    auto_restart_count, updated_at)
						 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						 ON CONFLICT(id) DO UPDATE SET
						   container_id = excluded.container_id,
						   status = excluded.status,
						   requested_at = excluded.requested_at,
						   started_at = excluded.started_at,
						   last_heartbeat_at = excluded.last_heartbeat_at,
						   error = excluded.error,
						   retries = excluded.retries,
						   colo = COALESCE(excluded.colo, coordinator_instances.colo),
						   last_hashrate = COALESCE(excluded.last_hashrate, coordinator_instances.last_hashrate),
						   auto_restart_count = excluded.auto_restart_count,
						   updated_at = excluded.updated_at`,
					)
					.bind(
						inst.id,
						inst.containerId,
						inst.status,
						inst.requestedAt,
						inst.startedAt,
						inst.lastHeartbeatAt,
						inst.error ?? null,
						inst.retries ?? 0,
						inst.colo,
						inst.lastHashrate,
						inst.autoRestartCount ?? 0,
						Date.now(),
					),
			);
			await db.batch(statements);
			this.invalidateCache();
		} catch (err) {
			log.error(
				{ count: instances.length, err: (err as Error).message },
				"saveInstances batch failed; falling back to per-row",
			);
			for (const inst of instances) {
				try {
					await this.saveInstance(inst);
				} catch (e) {
					log.error(
						{ instance: inst.id, err: (e as Error).message },
						"saveInstance fallback failed",
					);
				}
			}
		}
	}
}

function rowToInstance(row: Record<string, unknown>): InstanceRecord {
	const rawStatus = String(row.status);
	const status: InstanceStatus = VALID_STATUSES.has(rawStatus as InstanceStatus)
		? (rawStatus as InstanceStatus)
		: "error";
	return {
		id: String(row.id),
		containerId: String(row.container_id),
		status,
		requestedAt: Number(row.requested_at) || 0,
		startedAt: row.started_at != null ? Number(row.started_at) : null,
		lastHeartbeatAt:
			row.last_heartbeat_at != null ? Number(row.last_heartbeat_at) : null,
		error: (row.error as string) || undefined,
		retries: row.retries != null ? Number(row.retries) : 0,
		colo: (row.colo as string | null) ?? null,
		lastHashrate: row.last_hashrate != null ? Number(row.last_hashrate) : null,
		autoRestartCount:
			row.auto_restart_count != null ? Number(row.auto_restart_count) : 0,
	};
}

function countByStatus(instances: InstanceRecord[]): Record<string, number> {
	const counts = emptyStatusCounts();
	counts.total = instances.length;
	for (const instance of instances) {
		if (instance.status === "error") counts.failed++;
		else counts[instance.status] = (counts[instance.status] ?? 0) + 1;
	}
	return counts;
}

function countActiveControlInstances(counts: Record<string, number>): number {
	return (
		(counts.pending ?? 0) +
		(counts.starting ?? 0) +
		(counts.running ?? 0) +
		(counts.stopping ?? 0)
	);
}

function emptyStatusCounts(): Record<string, number> {
	return {
		pending: 0,
		starting: 0,
		running: 0,
		stale: 0,
		stopping: 0,
		stopped: 0,
		failed: 0,
		quarantined: 0,
		total: 0,
	};
}
