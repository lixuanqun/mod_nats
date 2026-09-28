#!/bin/bash
# sync fixed sources into podman container, force rebuild, verify BOTH ways.
# Lesson encoded here: `make | tail` eats the exit code (use pipefail), and a
# validation string that survives across versions proves nothing - assert on
# strings added AND removed by the current change.
set -e -o pipefail
SRC=/mnt/d/git_repo/freeswitch/src/mod/event_handlers/mod_nats
for f in mod_nats.c mod_nats.h nats_conn.c nats_proto.c nats_methods.c nats_events.c; do
  podman cp "$SRC/$f" modnats:/freeswitch/src/mod/event_handlers/mod_nats/$f
done
echo "--- source check (expect present=1 absent=0) ---"
podman exec modnats grep -c 'accept timeout' /freeswitch/src/mod/event_handlers/mod_nats/nats_events.c
podman exec modnats grep -c 'on_disconnected' /freeswitch/src/mod/event_handlers/mod_nats/nats_conn.c || true
echo "--- rebuild ---"
podman exec modnats bash -c 'touch /freeswitch/src/mod/event_handlers/mod_nats/*.c; cd /freeswitch && PKG_CONFIG_PATH=/usr/local/lib/pkgconfig make mod_nats > /tmp/rb.log 2>&1 || { tail -20 /tmp/rb.log; exit 1; }'
echo "--- binary check: new string PRESENT (1) and old string ABSENT (0) ---"
podman exec modnats bash -c 'strings /freeswitch/src/mod/event_handlers/mod_nats/.libs/mod_nats.so | grep -c "accept timeout"'
podman exec modnats bash -c 'strings /freeswitch/src/mod/event_handlers/mod_nats/.libs/mod_nats.so | grep -c "auto-reconnect in progress" || true'
podman exec modnats cp /freeswitch/src/mod/event_handlers/mod_nats/.libs/mod_nats.so /tmp/fsmod/mod_nats.so
podman exec modnats bash -c 'strings /tmp/fsmod/mod_nats.so | grep -c "accept timeout"'
echo SYNC_BUILD_OK
