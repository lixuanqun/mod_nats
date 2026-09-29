# Build mod_nats (and mod_loopback, for Dial tests) on top of a local
# FreeSWITCH image that already has headers and libfreeswitch.
# Override the base with: FS_IMAGE=your-fs-image docker compose build fs
ARG FS_IMAGE=aicc-fs:local-verify
FROM ${FS_IMAGE} AS build

ARG NATS_C_URL=https://github.com/nats-io/nats.c/archive/refs/tags/v3.11.0.tar.gz
ARG NATS_C_MIRROR=https://ghproxy.net/https://github.com/nats-io/nats.c/archive/refs/tags/v3.11.0.tar.gz
ARG LOOPBACK_URL=https://raw.githubusercontent.com/signalwire/freeswitch/v1.10.12/src/mod/endpoints/mod_loopback/mod_loopback.c
ARG LOOPBACK_MIRROR=https://ghproxy.net/https://raw.githubusercontent.com/signalwire/freeswitch/v1.10.12/src/mod/endpoints/mod_loopback/mod_loopback.c

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/nats-src \
    && (curl -fsSL --retry 2 -o /tmp/nats.c.tar.gz "$NATS_C_URL" \
        || curl -fsSL --retry 2 -o /tmp/nats.c.tar.gz "$NATS_C_MIRROR") \
    && tar -xzf /tmp/nats.c.tar.gz -C /tmp \
    && cp /tmp/nats.c-3.11.0/src/*.c /tmp/nats.c-3.11.0/src/*.h /tmp/nats-src/ \
    && cp -a /tmp/nats.c-3.11.0/src/glib /tmp/nats.c-3.11.0/src/unix /tmp/nats.c-3.11.0/src/include /tmp/nats-src/ \
    && gcc -shared -fPIC -O2 -D_GNU_SOURCE -DLINUX -D_REENTRANT \
        -I/tmp/nats-src -I/tmp/nats-src/include -I/tmp/nats-src/unix -I/tmp/nats-src/glib \
        /tmp/nats-src/*.c /tmp/nats-src/glib/*.c /tmp/nats-src/unix/*.c \
        -lpthread -Wl,-soname,libnats.so.3 \
        -o /usr/local/lib/libnats.so.3 \
    && ln -sf libnats.so.3 /usr/local/lib/libnats.so \
    && mkdir -p /usr/local/include/nats \
    && cp /tmp/nats-src/nats.h /tmp/nats-src/status.h /tmp/nats-src/version.h /usr/local/include/nats/

COPY mod_nats.c mod_nats.h nats_conn.c nats_proto.c nats_methods.c nats_events.c /src/mod_nats/

RUN mkdir -p /src/loopback /out \
    && (curl -fsSL --retry 2 -o /src/loopback/mod_loopback.c "$LOOPBACK_URL" \
        || curl -fsSL --retry 2 -o /src/loopback/mod_loopback.c "$LOOPBACK_MIRROR") \
    && gcc -shared -fPIC -O2 -Wall -Wno-unused-parameter -Wno-unused-function \
        -I/usr/local/freeswitch/include/freeswitch -I/usr/local/include \
        /src/mod_nats/mod_nats.c /src/mod_nats/nats_conn.c /src/mod_nats/nats_proto.c \
        /src/mod_nats/nats_methods.c /src/mod_nats/nats_events.c \
        -L/usr/local/freeswitch/lib -L/usr/local/lib -Wl,-rpath,/usr/local/lib \
        -lfreeswitch -lnats -lpthread \
        -o /out/mod_nats.so \
    && gcc -shared -fPIC -O2 -Wall -Wno-unused-parameter -Wno-unused-function \
        -I/usr/local/freeswitch/include/freeswitch \
        /src/loopback/mod_loopback.c \
        -L/usr/local/freeswitch/lib -Wl,-rpath,/usr/local/freeswitch/lib \
        -lfreeswitch -lm -lpthread \
        -o /out/mod_loopback.so

ARG FS_IMAGE=aicc-fs:local-verify
FROM ${FS_IMAGE}
COPY --from=build /usr/local/lib/libnats.so* /usr/local/lib/
COPY --from=build /out/mod_nats.so /usr/local/freeswitch/lib/freeswitch/mod/mod_nats.so
COPY --from=build /out/mod_loopback.so /usr/local/freeswitch/lib/freeswitch/mod/mod_loopback.so
COPY examples/docker-lab/nats.conf.xml /etc/freeswitch/autoload_configs/nats.conf.xml
COPY examples/docker-lab/entrypoint-wrap.sh /entrypoint-wrap.sh
RUN chmod 755 /entrypoint-wrap.sh && ldconfig
ENV LD_LIBRARY_PATH=/usr/local/lib
ENTRYPOINT ["/entrypoint-wrap.sh"]
