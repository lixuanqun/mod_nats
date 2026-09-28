include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_nats

if HAVE_NATS

mod_LTLIBRARIES = mod_nats.la
mod_nats_la_SOURCES  = mod_nats.c nats_conn.c nats_proto.c nats_methods.c nats_events.c
mod_nats_la_CFLAGS   = $(AM_CFLAGS) $(NATS_CFLAGS)
mod_nats_la_LIBADD   = $(switch_builddir)/libfreeswitch.la
mod_nats_la_LDFLAGS  = -avoid-version -module -no-undefined -shared $(NATS_LIBS) $(SWITCH_AM_LDFLAGS)

else
install: error
all: error
error:
	$(error You must install libnats (https://github.com/nats-io/nats.c) to build this module)
endif
