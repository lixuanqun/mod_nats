#!/bin/bash
set -euo pipefail
CONF=/etc/freeswitch/autoload_configs/modules.conf.xml
if ! grep -q 'module="mod_nats"' "$CONF"; then
  tmp="${CONF}.wrap"
  sed 's|</modules>|    <load module="mod_loopback"/>\n    <load module="mod_nats"/>\n</modules>|' "$CONF" > "$tmp"
  cat "$tmp" > "$CONF"
  rm -f "$tmp"
fi
export LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec /docker-entrypoint.sh "$@"
