#!/bin/bash
# Build FreeSWITCH core + the modules needed by the mod_nats self-test.
# Runs inside the dev container. Source tree expected at /freeswitch.
set -ex

cd /freeswitch
sed -i 's|^#event_handlers/mod_nats$|event_handlers/mod_nats|' build/modules.conf.in
grep -n 'mod_nats' build/modules.conf.in

if [ ! -f configure ]; then
  ./bootstrap.sh -j
fi

export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
if [ ! -f Makefile ]; then
  ./configure
fi

make -j"$(nproc)" mod_commands mod_console mod_dptools mod_loopback mod_event_socket mod_nats

# collect module binaries into one dir for runtime
rm -rf /tmp/fsmod && mkdir -p /tmp/fsmod
find src/mod -name '*.so' -exec cp {} /tmp/fsmod/ \;
ls -la /tmp/fsmod/
