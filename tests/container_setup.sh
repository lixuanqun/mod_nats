#!/bin/bash
# mod_nats self-test container setup: build deps + libnats + test tooling.
# Runs INSIDE the signalwire CI base container (debian bookworm).
set -ex

# drop the token-gated signalwire apt repo baked into the CI image
grep -rl 'freeswitch.signalwire.com' /etc/apt/sources.list.d/ /etc/apt/sources.list 2>/dev/null | xargs -r rm -f || true

apt-get update -qq
apt-get install -y -qq --no-install-recommends cmake ca-certificates python3 python3-pip

# libnats via ghproxy mirror (CN-friendly)
rm -rf /tmp/nats.c
git clone --depth 1 https://ghproxy.net/https://github.com/nats-io/nats.c /tmp/nats.c
cmake -S /tmp/nats.c -B /tmp/nats.c/build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DNATS_BUILD_WITH_TLS=OFF \
  -DNATS_BUILD_STREAMING=OFF \
  -DNATS_BUILD_EXAMPLES=OFF
cmake --build /tmp/nats.c/build --parallel "$(nproc)"
cmake --install /tmp/nats.c/build
ldconfig
pkg-config --modversion nats

# nats-server binary
NATS_SRV_VER=v2.10.24
curl -fsSL -o /tmp/nats-server.tgz "https://ghproxy.net/https://github.com/nats-io/nats-server/releases/download/${NATS_SRV_VER}/nats-server-${NATS_SRV_VER}-linux-amd64.tar.gz" \
  || curl -fsSL -o /tmp/nats-server.tgz "https://ghproxy.net/https://github.com/nats-io/nats-server/releases/download/v2.11.6/nats-server-v2.11.6-linux-amd64.tar.gz"
tar -xzf /tmp/nats-server.tgz -C /tmp
cp /tmp/nats-server-*/nats-server /usr/local/bin/
nats-server --version

# python test driver deps (tsinghua mirror)
pip3 install --break-system-packages -q -i https://pypi.tuna.tsinghua.edu.cn/simple nats-py
python3 -c 'import nats; print("nats-py ok")'
