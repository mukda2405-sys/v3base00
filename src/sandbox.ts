import { Sandbox, ContainerProxy } from "@cloudflare/sandbox";
import { defaultSandboxEnvVars, INTERNAL_HEARTBEAT_HOST } from "./config";
import { processHeartbeats } from "./mining-stats";

export { ContainerProxy };

export class MinerSandbox extends Sandbox {
	enableInternet = true;
	interceptHttps = false;
	defaultPort = 8080;
	requiredPorts = [8080];
	envVars = defaultSandboxEnvVars();

	override async onStart() {
		console.log(
			JSON.stringify({
				level: "info",
				time: new Date().toISOString(),
				service: "miner-sandbox",
				event: "started",
				id: this.ctx.id.toString(),
			}),
		);
	}

	override async onStop() {
		console.log(
			JSON.stringify({
				level: "info",
				time: new Date().toISOString(),
				service: "miner-sandbox",
				event: "stopped",
				id: this.ctx.id.toString(),
			}),
		);
	}

	override async onError(error: unknown) {
		console.error(
			JSON.stringify({
				level: "error",
				time: new Date().toISOString(),
				service: "miner-sandbox",
				event: "error",
				id: this.ctx.id.toString(),
				err: String(error),
			}),
		);
	}
}

(MinerSandbox as unknown as {
	outboundByHost: Record<string, OutboundHandler>;
}).outboundByHost = {
	[INTERNAL_HEARTBEAT_HOST]: heartbeatOutboundHandler,
};

type OutboundHandler = (
	request: Request,
	env: Env,
	ctx: { containerId?: string } & Record<string, unknown>,
) => Promise<Response>;

async function heartbeatOutboundHandler(
	request: Request,
	env: Env,
	_ctx: { containerId?: string } & Record<string, unknown>,
): Promise<Response> {
	const url = new URL(request.url);
	let expectedHost: string | null = null;
	if(env.REPORTER_ENDPOINT){
		try {
			expectedHost = new URL(env.REPORTER_ENDPOINT).host;
		}catch{
		}
	}

	const hostAllowed = url.host === INTERNAL_HEARTBEAT_HOST || (expectedHost !== null && url.host === expectedHost);
	if(request.method !== "POST" || url.pathname !== "/instances/heartbeat" || !hostAllowed){
		return fetch(request);
	}

	let raw: unknown;
	try {
		raw = await request.json();
	}catch{
		return Response.json(
			{ acknowledged: false, error: "invalid heartbeat body" },
			{ status: 400 },
		);
	}
	if(raw === null || typeof raw !== "object" || Array.isArray(raw)){
		return Response.json(
			{ acknowledged: false, error: "invalid heartbeat body" },
			{ status: 400 },
		);
	}
	const body = raw as { batch?: unknown[]; [key: string]: unknown };

	const payloads: Array<Record<string, unknown>> = Array.isArray(body.batch) && body.batch.length > 0 ? (body.batch as Array<Record<string, unknown>>) : [body as Record<string, unknown>];

	const cfRay = request.headers.get("CF-Ray") ?? "";
	const colo = cfRay.split("-")[1] ?? null;

	try {
		await processHeartbeats(env, payloads, colo);
		return Response.json({ acknowledged: true, batchSize: payloads.length });
	}catch(err){

		return Response.json(
			{
				acknowledged: false,
				error: (err as Error).message,
			},
			{ status: 500 },
		);
	}
}
