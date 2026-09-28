#!/bin/bash
# Control-group verification: pure FS (NO mod_nats), reload mod_loopback.
# Liveness by process existence (pgrep), NOT by ESL timing - eslc.py's 0.5s
# idle read may false-negative while FS is busy reloading a module.
set -x
FS=/freeswitch
export LD_LIBRARY_PATH=$FS/.libs:/usr/local/lib
ulimit -c unlimited
rm -f /root/core.* /tmp/fslog/freeswitch.pid

mkdir -p /etc/fs-test /tmp/fslog /tmp/fsdb
cp /opt/tests/fs-minimal/freeswitch.xml /etc/fs-test/
sed -i -e 's/\r$//' \
  -e 's|<load module="mod_dptools"/>|<load module="mod_dptools"/>\n        <load module="mod_dialplan_xml"/>|' \
  -e 's|<action application="park"/>|<action application="answer"/><action application="park"/>|' \
  -e 's|<load module="mod_nats"/>|<!--mod_nats off-->|' \
  /etc/fs-test/freeswitch.xml

setsid $FS/freeswitch -nf -nonat -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /tmp/fsmod \
  > /tmp/fs-control.log 2>&1 < /dev/null &

# wait until fully up: ESL banner
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

echo "=== before reload ==="
pgrep -x freeswitch >/dev/null && echo ALIVE || echo DEAD

python3 /opt/tests/eslc.py "reload mod_loopback" "+OK" || echo "ESLC_RELOAD_TIMEOUT_OR_ERR"

echo "=== t+1s ===";  sleep 1;  pgrep -x freeswitch >/dev/null && echo ALIVE || echo DEAD
echo "=== t+3s ===";  sleep 2;  pgrep -x freeswitch >/dev/null && echo ALIVE || echo DEAD
echo "=== t+8s ===";  sleep 5;  pgrep -x freeswitch >/dev/null && echo ALIVE || echo DEAD

echo "=== status via ESL (generous 5s) ==="
python3 /opt/tests/eslc.py "status" "uptime" && echo ESL_OK || echo ESL_FAIL

echo "=== cores? ==="
ls -la /root/core.* 2>/dev/null || echo NO_CORE
echo CONTROL_VERDICT_DONE
