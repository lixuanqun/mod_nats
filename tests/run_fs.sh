#!/bin/bash
# Runtime for mod_nats self-test: nats-server + minimal FreeSWITCH.
set -ex

# collect module binaries
rm -rf /tmp/fsmod && mkdir -p /tmp/fsmod
find /freeswitch/src/mod -name '*.so' -exec cp {} /tmp/fsmod/ \;
ls /tmp/fsmod/

mkdir -p /etc/fs-test /tmp/fslog /tmp/fsdb

# nats-server on 4222 (idempotent)
if ! pgrep -x nats-server >/dev/null; then
  nohup nats-server -p 4222 > /tmp/nats-server.log 2>&1 &
  sleep 1
fi
pgrep -x nats-server

# (re)start FreeSWITCH
pkill -x freeswitch 2>/dev/null || true
sleep 1
export LD_LIBRARY_PATH=/freeswitch/.libs:/usr/local/lib
nohup /freeswitch/freeswitch -nf -nonat \
  -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /tmp/fsmod \
  > /tmp/fs-console.log 2>&1 &
sleep 4
pgrep -x freeswitch && echo "FS RUNNING"
tail -20 /tmp/fs-console.log
