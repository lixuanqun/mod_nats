#!/bin/bash
# One-shot build of FS + mod_nats inside the dev container.
# Encodes every lesson learned from the local toolchain fight:
#  - HTTPS USTC mirror + no-cert-verify (port 80 gets selectively reset)
#  - python3 symlink + ConfigParser shim (vendored apr buildconf is py2-era)
#  - CRLF strip (sources come from a Windows checkout)
#  - mod_verto/mod_signalwire disabled (avoid libks dependency tree)
#  - built-sources generated serially before parallel core build
set -ex

# ---------- 1. apt + tooling ----------
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
  build-essential autoconf automake libtool libtool-bin pkg-config cmake ca-certificates \
  python3 python3-pip nasm yasm git curl unzip gdb \
  libssl-dev libcurl4-openssl-dev libpcre2-dev libspeex-dev libspeexdsp-dev libedit-dev \
  libncurses-dev libtiff-dev libldns-dev libsndfile1-dev libopus-dev \
  libvpx-dev libyaml-dev libasound2-dev libavformat-dev libjpeg-dev \
  zlib1g-dev uuid-dev libdb-dev libglib2.0-dev libmpg123-dev libsqlite3-dev libpng-dev
rm -f /etc/apt/apt.conf.d/99noverify
ln -sf /usr/bin/python3 /usr/local/bin/python
printf 'from configparser import *\n' > /usr/lib/python3/dist-packages/ConfigParser.py

# ---------- 2. libnats + nats-server + nats-py ----------
if ! pkg-config --atleast-version=2.0 libnats 2>/dev/null; then
  rm -rf /tmp/nats.c
  git clone --depth 1 https://ghproxy.net/https://github.com/nats-io/nats.c /tmp/nats.c \
    || git clone --depth 1 https://ghfast.top/https://github.com/nats-io/nats.c /tmp/nats.c
  cmake -S /tmp/nats.c -B /tmp/nats.c/build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_LIBDIR=lib -DNATS_BUILD_WITH_TLS=OFF -DNATS_BUILD_STREAMING=OFF -DNATS_BUILD_EXAMPLES=OFF
  cmake --build /tmp/nats.c/build --parallel "$(nproc)"
  cmake --install /tmp/nats.c/build
  ldconfig
fi
if [ ! -x /usr/local/bin/nats-server ]; then
  NATS_SRV_VER=v2.10.24
  curl -fsSL -o /tmp/ns.tgz "https://ghproxy.net/https://github.com/nats-io/nats-server/releases/download/${NATS_SRV_VER}/nats-server-${NATS_SRV_VER}-linux-amd64.tar.gz" \
    || curl -fsSL -o /tmp/ns.tgz "https://ghfast.top/https://github.com/nats-io/nats-server/releases/download/${NATS_SRV_VER}/nats-server-${NATS_SRV_VER}-linux-amd64.tar.gz"
  tar -xzf /tmp/ns.tgz -C /tmp && cp /tmp/nats-server-*/nats-server /usr/local/bin/
fi
pip3 install --break-system-packages -q -i https://pypi.tuna.tsinghua.edu.cn/simple nats-py

# ---------- 3. CRLF strip ----------
grep -rlI $'\r' /freeswitch --exclude-dir=.git 2>/dev/null | xargs -r sed -i 's/\r$//'

# ---------- 4. sofia-sip + spandsp (FS master needs newer than debian) ----------
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
if ! pkg-config --atleast-version=1.13.17 sofia-sip-ua 2>/dev/null; then
  rm -rf /tmp/sofia-sip && tar -xf /deps1.tar -C /tmp
  cd /tmp/sofia-sip && ./bootstrap.sh && ./configure --prefix=/usr/local && make -j"$(nproc)" && make install && ldconfig
fi
if ! pkg-config --atleast-version=3.1.1 spandsp 2>/dev/null; then
  rm -rf /tmp/spandsp && tar -xf /deps2.tar -C /tmp
  cd /tmp/spandsp && ./bootstrap.sh && ./configure --prefix=/usr/local && make -j"$(nproc)" && make install && ldconfig
fi

# ---------- 5. FS configure ----------
cd /freeswitch
sed -i -e 's|^#event_handlers/mod_nats$|event_handlers/mod_nats|' \
       -e 's|^endpoints/mod_verto$|#endpoints/mod_verto|' \
       -e 's|^\(.*mod_signalwire\)$|#\1|' build/modules.conf.in
./bootstrap.sh
PKG_CONFIG_PATH=/usr/local/lib/pkgconfig ./configure

# ---------- 6. build ----------
make -j1 src/include/switch_version.h src/mod/modules.inc
make -j"$(nproc)" libfreeswitch.la
make -j"$(nproc)" freeswitch fs_cli
make -j"$(nproc)" mod_commands mod_console mod_dptools mod_dialplan_xml mod_loopback mod_event_socket mod_nats

rm -rf /tmp/fsmod && mkdir -p /tmp/fsmod
find src/mod -name '*.so' -exec cp {} /tmp/fsmod/ \;
ls -la /tmp/fsmod/
echo BUILD_ALL_DONE
