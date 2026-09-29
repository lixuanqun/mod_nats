#!/bin/bash
# standalone selftest: nats + fs + full selftest output (container main proc)
mkdir -p /tmp/fslog /tmp/fsdb
pgrep -x nats-server >/dev/null || { setsid /usr/local/bin/nats-server -p 4222 >/tmp/nats.log 2>&1 </dev/null & sleep 1; }
pkill -x freeswitch 2>/dev/null; sleep 1
rm -f /tmp/fslog/freeswitch.pid
setsid /freeswitch/freeswitch -nf -nonat -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /tmp/fsmod >/tmp/fs.log 2>&1 </dev/null &
for i in $(seq 1 30); do
  sleep 1
  python3 - <<'EOF' && break
import socket
try:
    s = socket.create_connection(("127.0.0.1", 8021), timeout=2)
    s.settimeout(2)
    ok = b"auth/request" in s.recv(64)
    s.close()
    exit(0 if ok else 1)
except Exception:
    exit(1)
EOF
done
sleep 3
python3 /opt/tests/selftest.py
echo SELFDONE
