#!/bin/bash
# mod_nats native WSL (Ubuntu 24.04) build: real Linux, no container.
# Assets come from /mnt/d/git_repo tarballs; work happens in ~/modnats (ext4).
set -ex
W=~/modnats
mkdir -p $W && cd $W

# ---------- 0. sources ----------
[ -d freeswitch/src/mod/event_handlers/mod_nats ] || {
  mkdir -p freeswitch && tar -xf /mnt/d/git_repo/fs_src.tar -C $W
  grep -rlI $'\r' freeswitch --exclude-dir=.git 2>/dev/null | xargs -r sed -i 's/\r$//'
}
grep -q MOD_NATS_TLS freeswitch/src/mod/event_handlers/mod_nats/nats_proto.c \
  || { echo "STALE fs_src.tar"; exit 9; }

# ---------- 1. apt deps (Ubuntu noble) ----------
export DEBIAN_FRONTEND=noninteractive
PKGS="build-essential autoconf automake libtool libtool-bin pkg-config cmake \
ca-certificates python3 python3-pip python-is-python3 nasm yasm git curl \
libssl-dev libcurl4-openssl-dev libpcre2-dev libspeex-dev libspeexdsp-dev \
libedit-dev libncurses-dev libtiff-dev libldns-dev libsndfile1-dev libopus-dev \
libvpx-dev libyaml-dev libasound2-dev libavformat-dev libjpeg-dev zlib1g-dev \
uuid-dev libdb-dev libglib2.0-dev libmpg123-dev libsqlite3-dev libpng-dev"
# CN-friendly apt mirror (Ubuntu 24.04 deb822 format)
if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then
  sed -i 's|http://archive.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g; s|http://security.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g' /etc/apt/sources.list.d/ubuntu.sources
fi
apt-get update -qq
for p in $PKGS; do
  dpkg -s $p >/dev/null 2>&1 || apt-get install -y -qq --no-install-recommends $p || echo "SKIP $p"
done

# ---------- 2. libnats + nats-server ----------
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
if ! pkg-config --atleast-version=2.0 libnats 2>/dev/null; then
  rm -rf /tmp/nats.c && tar -xf /mnt/d/git_repo/nats.c.tar -C /tmp
  cmake -S /tmp/nats.c -B /tmp/nats.c/build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_LIBDIR=lib -DNATS_BUILD_WITH_TLS=OFF -DNATS_BUILD_STREAMING=OFF -DNATS_BUILD_EXAMPLES=OFF
  cmake --build /tmp/nats.c/build --parallel "$(nproc)"
  cmake --install /tmp/nats.c/build && ldconfig
fi
[ -x /usr/local/bin/nats-server ] || {
  tar -xzf /mnt/d/git_repo/nats-server.tar.gz -C /tmp
  cp /tmp/nats-server-*/nats-server /usr/local/bin/ && chmod +x /usr/local/bin/nats-server
}

# ---------- 3. sofia-sip + spandsp ----------
if ! pkg-config --atleast-version=1.13.17 sofia-sip-ua 2>/dev/null; then
  rm -rf /tmp/sofia-sip && tar -xf /mnt/d/git_repo/deps1.tar -C /tmp
  grep -rlI $'
' /tmp/sofia-sip 2>/dev/null | xargs -r sed -i 's/
$//'
  cd /tmp/sofia-sip && ./bootstrap.sh && ./configure --prefix=/usr/local && make -j"$(nproc)" && make install && ldconfig
fi
if ! pkg-config --atleast-version=3.1.1 spandsp 2>/dev/null; then
  rm -rf /tmp/spandsp && tar -xf /mnt/d/git_repo/deps2.tar -C /tmp
  grep -rlI $'
' /tmp/spandsp 2>/dev/null | xargs -r sed -i 's/
$//'
  cd /tmp/spandsp && ./bootstrap.sh && ./configure --prefix=/usr/local && make -j"$(nproc)" && make install && ldconfig
fi

# ---------- 4. FreeSWITCH ----------
cd $W/freeswitch
sed -i -e 's|^#event_handlers/mod_nats$|event_handlers/mod_nats|' \
       -e 's|^endpoints/mod_verto$|#endpoints/mod_verto|' \
       -e 's|^\(.*mod_signalwire\)$|#\1|' build/modules.conf.in
[ -f configure ] || ./bootstrap.sh
[ -f Makefile ] || PKG_CONFIG_PATH=$PKG_CONFIG_PATH ./configure
make -j1 src/include/switch_version.h src/mod/modules.inc
make -j"$(nproc)" libfreeswitch.la
make -j"$(nproc)" freeswitch fs_cli
make mod_commands mod_console mod_dptools mod_dialplan_xml mod_loopback mod_event_socket mod_nats
rm -rf /root/modnats/fsmod && mkdir -p /root/modnats/fsmod
find src/mod -name '*.so' -exec cp {} /root/modnats/fsmod/ \;
ls /root/modnats/fsmod/ | tr '\n' ' '; echo
echo BUILD_WSL_DONE
