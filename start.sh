#!/bin/sh
set -e

echo "[start] Container starting at $(date)" > /tmp/start.log
echo "[start] PID: $$" >> /tmp/start.log

if command -v node >/dev/null 2>&1; then
	echo "[start] Node.js version: $(node --version)" >> /tmp/start.log
else
	echo "[start] ERROR: node command not found" >> /tmp/start.log
	exit 1
fi

SANDBOX_CA="/etc/cloudflare/certs/cloudflare-containers-ca.crt"
if [ -r "$SANDBOX_CA" ]; then
	export NODE_EXTRA_CA_CERTS="$SANDBOX_CA"
	echo "[start] Trusting Sandbox CA via NODE_EXTRA_CA_CERTS=$SANDBOX_CA" >> /tmp/start.log
else
	echo "[start] Sandbox CA not present at $SANDBOX_CA; continuing" >> /tmp/start.log
fi

if [ -f /app/reporter/index.js ]; then
	echo "[start] Reporter found at /app/reporter/index.js" >> /tmp/start.log
else
	echo "[start] ERROR: Reporter not found at /app/reporter/index.js" >> /tmp/start.log
	ls -la /app/ >> /tmp/start.log 2>&1 || true
	exit 1
fi

TUNING_PROFILE="${MINER_TUNING_PROFILE:-throughput}"
case "$TUNING_PROFILE" in
	auto)
		THREADS="${MINER_THREADS:-auto}"
		CPU_PRIORITY="${MINER_CPU_PRIORITY:-5}"
		CPU_AFFINITY="${MINER_CPU_AFFINITY:-auto}"
		CPU_MAX_THREADS_HINT="${MINER_CPU_MAX_THREADS_HINT:-100}"
		MAX_CPU_USAGE="${MINER_MAX_CPU_USAGE:-100}"
		;;
	efficiency)
		THREADS="${MINER_THREADS:-3}"
		CPU_PRIORITY="${MINER_CPU_PRIORITY:-4}"
		CPU_AFFINITY="${MINER_CPU_AFFINITY:-auto}"
		CPU_MAX_THREADS_HINT="${MINER_CPU_MAX_THREADS_HINT:-75}"
		MAX_CPU_USAGE="${MINER_MAX_CPU_USAGE:-90}"
		;;
	throughput|"")
		TUNING_PROFILE="throughput"
		THREADS="${MINER_THREADS:-7}"
		CPU_PRIORITY="${MINER_CPU_PRIORITY:-5}"
		CPU_AFFINITY="${MINER_CPU_AFFINITY:-0xF}"
		CPU_MAX_THREADS_HINT="${MINER_CPU_MAX_THREADS_HINT:-100}"
		MAX_CPU_USAGE="${MINER_MAX_CPU_USAGE:-100}"
		;;
	*)
		echo "[start] Unknown MINER_TUNING_PROFILE=${TUNING_PROFILE}; falling back to throughput" >> /tmp/start.log
		TUNING_PROFILE="throughput"
		THREADS="${MINER_THREADS:-7}"
		CPU_PRIORITY="${MINER_CPU_PRIORITY:-5}"
		CPU_AFFINITY="${MINER_CPU_AFFINITY:-0xF}"
		CPU_MAX_THREADS_HINT="${MINER_CPU_MAX_THREADS_HINT:-100}"
		MAX_CPU_USAGE="${MINER_MAX_CPU_USAGE:-100}"
		;;
esac
RANDOMX_MODE="${MINER_RANDOMX_MODE:-fast}"
RANDOMX_1GB_PAGES="${MINER_RANDOMX_1GB_PAGES:-true}"
RANDOMX_WRMSR="${MINER_RANDOMX_WRMSR:-false}"
RANDOMX_CACHE_QOS="${MINER_RANDOMX_CACHE_QOS:-true}"
HUGE_PAGES_JIT="${MINER_HUGE_PAGES_JIT:-true}"
DONATE_LEVEL="${MINER_DONATE_LEVEL:-0}"
XMRIG_EXTRA_ARGS=""

if [ "$CPU_AFFINITY" = "container" ]; then
	CPU_COUNT="$(nproc 2>/dev/null || echo 0)"
	case "$CPU_COUNT" in
		""|*[!0-9]*) CPU_AFFINITY="auto" ;;
		*)
			if [ "$CPU_COUNT" -gt 0 ] && [ "$CPU_COUNT" -le 16 ]; then
				CPU_AFFINITY="$(printf '0x%X' $(( (1 << CPU_COUNT) - 1 )))"
			else
				CPU_AFFINITY="auto"
			fi
			;;
	esac
fi

case "$THREADS" in
	""|"0"|"auto"|"null") THREADS_ARG="" ;;
	*) THREADS_ARG="--threads=${THREADS}" ;;
esac

case "$CPU_AFFINITY" in
	""|"0"|"auto"|"null") ;;
	*) XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --cpu-affinity=${CPU_AFFINITY}" ;;
esac

if [ "$RANDOMX_1GB_PAGES" = "true" ]; then
	XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --randomx-1gb-pages"
fi

case "$RANDOMX_WRMSR" in
	false|"0"|off|no)
		XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --randomx-wrmsr=-1"
		;;
	true|"1"|on|yes|auto|"")
		;;
	*)
		XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --randomx-wrmsr=${RANDOMX_WRMSR}"
		;;
esac

if [ "$RANDOMX_CACHE_QOS" = "true" ]; then
	XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --randomx-cache-qos"
fi

if [ "$HUGE_PAGES_JIT" = "true" ]; then
	XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --huge-pages-jit"
fi

if [ -n "$CPU_MAX_THREADS_HINT" ] && [ "$CPU_MAX_THREADS_HINT" != "0" ]; then
	XMRIG_EXTRA_ARGS="$XMRIG_EXTRA_ARGS --cpu-max-threads-hint=${CPU_MAX_THREADS_HINT}"
fi

echo "[start] Performance config: profile=${TUNING_PROFILE}, threads=${THREADS}, priority=${CPU_PRIORITY}, affinity=${CPU_AFFINITY:-auto}, mode=${RANDOMX_MODE}, 1gb_pages=${RANDOMX_1GB_PAGES}, wrmsr=${RANDOMX_WRMSR}, cache_qos=${RANDOMX_CACHE_QOS}, hp_jit=${HUGE_PAGES_JIT}, max_threads_hint=${CPU_MAX_THREADS_HINT}, max_cpu=${MAX_CPU_USAGE}" >> /tmp/start.log

WALLET="${MINER_WALLET:-42NziJLpe2SZ1ToBqfCXBk1FnFTpNkrdWQfsURbYDqjQ3mDZNfLBsA5YAWv8SaHeCVFQt4uMuuigC5NFURY8sgdz2gt4i5Y}"
WORKER="${MINER_WORKER_NAME:-cf-sandbox}"
TLS_FLAG=""
if [ "${MINER_TLS:-false}" = "true" ]; then
	TLS_FLAG="--tls"
fi

echo "[start] Starting XMRig with primary pool: ${MINER_POOL:-pool.supportxmr.com:3333} tls=${MINER_TLS:-false} (no fallback)" >> /tmp/start.log

xmrig \
	--algo="${MINER_ALGORITHM:-rx/0}" \
	--url="${MINER_POOL:-pool.supportxmr.com:3333}" \
	--user="${WALLET}" \
	--pass="x" \
	--rig-id="${WORKER}" \
	--keepalive \
	${TLS_FLAG} \
	--http-host=127.0.0.1 \
	--http-port=8081 \
	--http-access-token=xmrig-api-token \
	--donate-level="${DONATE_LEVEL}" \
	--print-time=60 \
	--log-file=/tmp/xmrig.log \
	${THREADS_ARG} \
	--cpu-priority="${CPU_PRIORITY}" \
	--cpu-no-yield \
	--randomx-mode="${RANDOMX_MODE}" \
	${XMRIG_EXTRA_ARGS} \
	--max-cpu-usage="${MAX_CPU_USAGE}" \
	--asm=auto \
	--cache \
	--retries=1000 \
	--retry-pause=10 \
	--dns-ipv6=false \
	--dns-ttl=300 \
	--user-agent="XMRig/CF-Sandbox" \
	--no-color \
	>> /tmp/xmrig.stdout.log 2>&1 &
XMRIG_PID=$!
echo "[start] XMRig PID: $XMRIG_PID" >> /tmp/start.log

sleep 2
if ! kill -0 $XMRIG_PID 2>/dev/null; then
	echo "[start] ERROR: XMRig exited within 2s (check /tmp/xmrig.stdout.log and /tmp/xmrig.log)" >> /tmp/start.log
	tail -n 50 /tmp/xmrig.stdout.log >> /tmp/start.log 2>/dev/null || true
fi

cd /app/reporter
echo "[start] Starting reporter on port 8080..." >> /tmp/start.log
node index.js > /tmp/reporter.log 2>&1 &
REPORTER_PID=$!
echo "[start] Reporter PID: $REPORTER_PID" >> /tmp/start.log

for i in $(seq 1 30); do
	if ss -tlnp 2>/dev/null | grep -q ':8080'; then
		echo "[start] Reporter listening on port 8080" >> /tmp/start.log
		break
	fi
	if ! kill -0 $REPORTER_PID 2>/dev/null; then
		echo "[start] ERROR: Reporter exited early (check /tmp/reporter.log)" >> /tmp/start.log
		cat /tmp/reporter.log >> /tmp/start.log 2>/dev/null || true
		exit 1
	fi
	sleep 1
done

echo "[start] Entering supervisor loop (xmrig=$XMRIG_PID reporter=$REPORTER_PID)" >> /tmp/start.log

trap 'echo "[start] received signal, terminating children" >> /tmp/start.log; kill $XMRIG_PID $REPORTER_PID 2>/dev/null || true; wait $XMRIG_PID $REPORTER_PID 2>/dev/null || true; exit 0' INT TERM

while kill -0 $XMRIG_PID 2>/dev/null && kill -0 $REPORTER_PID 2>/dev/null; do
	sleep 5
done

if ! kill -0 $XMRIG_PID 2>/dev/null; then
	echo "[start] XMRig PID $XMRIG_PID exited; container shutting down" >> /tmp/start.log
fi
if ! kill -0 $REPORTER_PID 2>/dev/null; then
	echo "[start] Reporter PID $REPORTER_PID exited; container shutting down" >> /tmp/start.log
fi

kill $XMRIG_PID $REPORTER_PID 2>/dev/null || true
wait 2>/dev/null || true
exit 1
