include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_nats

# Standalone-friendly: resolve libnats via pkg-config directly, so the module
# drops into any FreeSWITCH source tree without configure.ac modifications.
NATS_PC := nats
NATS_CFLAGS := $(shell pkg-config --cflags $(NATS_PC))
NATS_LIBS := $(shell pkg-config --libs $(NATS_PC))

mod_LTLIBRARIES = mod_nats.la
mod_nats_la_SOURCES  = mod_nats.c nats_conn.c nats_proto.c nats_methods.c nats_events.c
mod_nats_la_CFLAGS   = $(AM_CFLAGS) $(NATS_CFLAGS)
mod_nats_la_LIBADD   = $(switch_builddir)/libfreeswitch.la
mod_nats_la_LDFLAGS  = -avoid-version -module -no-undefined -shared $(NATS_LIBS) $(SWITCH_AM_LDFLAGS)
