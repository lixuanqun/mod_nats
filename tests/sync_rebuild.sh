#!/bin/bash
# sync fixed sources into podman container, force rebuild, verify
set -e
SRC=/mnt/d/git_repo/freeswitch/src/mod/event_handlers/mod_nats
for f in mod_nats.c mod_nats.h nats_conn.c nats_proto.c nats_methods.c nats_events.c; do
  podman cp "$SRC/$f" modnats:/freeswitch/src/mod/event_handlers/mod_nats/$f
done
echo "--- source check (expect 1s) ---"
podman exec modnats grep -c 'replying 503' /freeswitch/src/mod/event_handlers/mod_nats/nats_conn.c
podman exec modnats grep -c 'join(&join_status' /freeswitch/src/mod/event_handlers/mod_nats/mod_nats.c
echo "--- rebuild ---"
podman exec modnats bash -c 'touch /freeswitch/src/mod/event_handlers/mod_nats/*.c; cd /freeswitch && PKG_CONFIG_PATH=/usr/local/lib/pkgconfig make mod_nats 2>&1 | tail -2'
echo "--- binary check (expect 1) ---"
podman exec modnats bash -c 'strings /freeswitch/src/mod/event_handlers/mod_nats/.libs/mod_nats.so | grep -c "replying 503"'
podman exec modnats cp /freeswitch/src/mod/event_handlers/mod_nats/.libs/mod_nats.so /tmp/fsmod/mod_nats.so
podman exec modnats bash -c 'strings /tmp/fsmod/mod_nats.so | grep -c "replying 503"'
echo SYNC_BUILD_OK
