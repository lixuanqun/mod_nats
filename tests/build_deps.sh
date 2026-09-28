#!/bin/bash
# Build the newer-than-debian FreeSWITCH core deps: sofia-sip and spandsp.
set -ex
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

if ! pkg-config --atleast-version=1.13.17 sofia-sip-ua; then
  rm -rf /tmp/sofia-sip
  git clone --depth 1 https://ghproxy.net/https://github.com/freeswitch/sofia-sip /tmp/sofia-sip
  cd /tmp/sofia-sip
  ./bootstrap.sh
  ./configure --prefix=/usr/local
  make -j"$(nproc)"
  make install
  ldconfig
fi

if ! pkg-config --atleast-version=3.1.1 spandsp; then
  rm -rf /tmp/spandsp
  git clone --depth 1 https://ghproxy.net/https://github.com/freeswitch/spandsp /tmp/spandsp
  cd /tmp/spandsp
  ./bootstrap.sh
  ./configure --prefix=/usr/local
  make -j"$(nproc)"
  make install
  ldconfig
fi

pkg-config --modversion sofia-sip-ua spandsp
