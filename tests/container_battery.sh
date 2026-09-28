#!/bin/bash
# Full battery (final v2): python-ESL checks only - fs_cli excluded entirely.
# Runs as the container's main process (no exec-session teardown issues).
set -ex
ulimit -c unlimited
ulimit -c unlimited
FS=/freeswitch
export LD_LIBRARY_PATH=$FS/.libs:/usr/local/lib

mkdir -p /etc/fs-test /tmp/fslog /tmp/fsdb
cp /opt/tests/fs-minimal/freeswitch.xml /etc/fs-test/
sed -i -e 's/\r$//' \
  -e 's|<load module="mod_dptools"/>|<load module="mod_dptools"/>\n        <load module="mod_dialplan_xml"/>|' \
  -e 's|<action application="park"/>|<action application="answer"/><action application="park"/>|' \
  /etc/fs-test/freeswitch.xml

start_nats() {
  pgrep -x nats-server >/dev/null && return 0
  setsid /usr/local/bin/nats-server -p 4222 > /tmp/nats.log 2>&1 < /dev/null &
  sleep 1
}

# readiness: ESL banner via raw python
esl_ready() {
  python3 - <<'EOF'
import socket
try:
    s = socket.create_connection(("127.0.0.1", 8021), timeout=2)
    s.settimeout(2)
    d = s.recv(64)
    s.close()
    exit(0 if b"auth/request" in d else 1)
except Exception:
    exit(1)
EOF
}

api() { python3 /opt/tests/eslc.py "$1" "$2" -q; }

start_fs() {
  pkill -x freeswitch 2>/dev/null || true
  sleep 1
  rm -f /tmp/fslog/freeswitch.pid
  setsid $FS/freeswitch -nf -nonat -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /tmp/fsmod \
    > /tmp/fs-console.log 2>&1 < /dev/null &
  for i in $(seq 1 30); do
    sleep 1
    if esl_ready; then return 0; fi
  done
  return 1
}

start_nats

echo "=== functional selftest ==="
start_fs
sleep 2
python3 /opt/tests/selftest.py 2>&1 | tail -6
pkill -x freeswitch || true

echo "=== reload stress: 5 rounds x 3 reloads ==="
PASS=0; FAIL=0
for r in 1 2 3 4 5; do
  start_nats
  if start_fs; then
    ok=1
    for i in 1 2 3; do
      pkill -f "reload" >/dev/null 2>&1 || true
      api "reload mod_nats" "+OK" || true
      sleep 1
      api "nats status" "conn_state" || { ok=0; echo "round $r reload $i: DEAD"; break; }
    done
    if [ $ok -eq 1 ]; then PASS=$((PASS+1)); echo "round $r: OK"; else FAIL=$((FAIL+1)); fi
  else
    echo "round $r: STARTUP FAILED"; FAIL=$((FAIL+1))
  fi
  pkill -x freeswitch 2>/dev/null || true
done

echo "=== control: pure FS (no mod_nats) 3 rounds ==="
sed -i 's|<load module="mod_nats"/>|<!--mod_nats off-->|' /etc/fs-test/freeswitch.xml
CPASS=0
for r in 1 2 3; do
  if start_fs; then
    cok=1
    for i in 1 2 3; do
      api "reload mod_loopback" "+OK" || true
      sleep 1
      api "status" "uptime" || { cok=0; echo "control $r reload $i: DEAD"; break; }
    done
    if [ $cok -eq 1 ]; then CPASS=$((CPASS+1)); echo "control $r: OK"; fi
  fi
  pkill -x freeswitch 2>/dev/null || true
done
sed -i 's|<!--mod_nats off-->|<load module="mod_nats"/>|' /etc/fs-test/freeswitch.xml

echo "=================================="
echo "mod_nats stress: $PASS/5 passed"
echo "pure-FS control: $CPASS/3 passed"
echo "=================================="
echo BATTERY_DONE
