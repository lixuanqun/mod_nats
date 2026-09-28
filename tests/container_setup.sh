#!/bin/bash
# mod_nats self-test container setup: build deps + libnats + nats-server + test tooling.
# Runs INSIDE a debian:bookworm-slim container.
set -ex

# NOTE: run this container with --network host. Plain-HTTP (port 80) egress
# gets selectively reset on some networks; HTTPS mirrors on 443 work.
rm -f /etc/apt/apt.conf.d/95proxy

# HTTPS mirror without cert verify (ca-certificates not installed yet);
# switched back after ca-certificates is present.
printf 'Acquire::https::Verify-Peer "false";\nAcquire::https::Verify-Host "false";\n' > /etc/apt/apt.conf.d/99noverify
cat > /etc/apt/sources.list.d/debian.sources <<'EOF'
Types: deb
URIs: https://mirrors.ustc.edu.cn/debian
Suites: bookworm bookworm-updates
Components: main
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg
EOF

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
  build-essential autoconf automake libtool pkg-config cmake ca-certificates \
  python3 python3-pip nasm yasm \
  libssl-dev libcurl4-openssl-dev libpcre2-dev libspeexdsp-dev libedit-dev \
  libncurses-dev libtiff-dev libldns-dev libsndfile1-dev libopus-dev \
  libvpx-dev libyaml-dev libasound2-dev libavformat-dev libjpeg-dev \
  zlib1g-dev uuid-dev libdb-dev libglib2.0-dev libmpg123-dev libsqlite3-dev

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
pkg-config --modversion libnats

# nats-server binary
NATS_SRV_VER=v2.10.24
curl -fsSL -o /tmp/nats-server.tgz "https://ghproxy.net/https://github.com/nats-io/nats-server/releases/download/${NATS_SRV_VER}/nats-server-${NATS_SRV_VER}-linux-amd64.tar.gz" \
  || curl -fsSL -o /tmp/nats-server.tgz "https://ghproxy.net/https://github.com/nats-io/nats-server/releases/download/v2.11.6/nats-server-v2.11.6-linux-amd64.tar.gz"
tar -xzf /tmp/nats-server.tgz -C /tmp
cp /tmp/nats-server-*/nats-server /usr/local/bin/
nats-server --version

# ca-certificates is in now - drop the insecure apt override
rm -f /etc/apt/apt.conf.d/99noverify

# python test driver deps (tsinghua mirror)
pip3 install --break-system-packages -q -i https://pypi.tuna.tsinghua.edu.cn/simple nats-py
python3 -c 'import nats; print("nats-py ok")'
