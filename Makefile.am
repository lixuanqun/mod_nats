include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_nats

# Standalone-friendly: resolve libnats via pkg-config directly, so the module
# drops into any FreeSWITCH source tree without configure.ac modifications.
# nats.c installs libnats.pc (older releases used nats.pc - keep fallback).
NATS_PC = $(shell pkg-config --exists libnats && echo libnats || echo nats)
NATS_CFLAGS = $(shell pkg-config --cflags $(NATS_PC) 2>/dev/null)
NATS_LIBS = $(shell pkg-config --libs $(NATS_PC) 2>/dev/null || echo -lnats)

mod_LTLIBRARIES = mod_nats.la
mod_nats_la_SOURCES  = mod_nats.c nats_conn.c nats_proto.c nats_methods.c nats_events.c
mod_nats_la_CFLAGS   = $(AM_CFLAGS) $(NATS_CFLAGS)
mod_nats_la_LIBADD   = $(switch_builddir)/libfreeswitch.la
mod_nats_la_LDFLAGS  = -avoid-version -module -no-undefined -shared $(NATS_LIBS) $(SWITCH_AM_LDFLAGS)
