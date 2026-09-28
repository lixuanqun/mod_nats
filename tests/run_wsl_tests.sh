#!/bin/bash
# WSL runtime + full stability battery for mod_nats.
# Usage: run_wsl_tests.sh [rounds]   (default 5)
set -ex
W=~/modnats
FS=$W/freeswitch
ROUNDS=${1:-5}
export LD_LIBRARY_PATH=$FS/.libs:/usr/local/lib
CLI="$FS/fs_cli -p 8021"

pip3 install --break-system-packages -q -i https://pypi.tuna.tsinghua.edu.cn/simple nats-py 2>/dev/null || true

mkdir -p /etc/fs-test
cp $W/tests/fs-minimal/freeswitch.xml /etc/fs-test/
sed -i -e 's/\r$//' \
  -e 's|<load module="mod_dptools"/>|<load module="mod_dptools"/>\n        <load module="mod_dialplan_xml"/>|' \
  -e 's|<action application="park"/>|<action application="answer"/><action application="park"/>|' \
  /etc/fs-test/freeswitch.xml

start_fs() {
  pkill -x freeswitch 2>/dev/null || true
  sleep 1
  rm -f /tmp/fslog/freeswitch.pid
  nohup $FS/freeswitch -nf -nonat -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /root/modnats/fsmod \
    > /tmp/fs-console.log 2>&1 &
  for i in $(seq 1 20); do
    sleep 1
    if $CLI -x 'nats status' 2>/dev/null | grep -q conn_state; then return 0; fi
  done
  return 1
}

mkdir -p /tmp/fslog /tmp/fsdb

# ---------- nats-server ----------
if ! pgrep -x nats-server >/dev/null; then
  nohup /usr/local/bin/nats-server -p 4222 > /tmp/nats.log 2>&1 &
  sleep 1
fi

# ---------- 1. functional selftest ----------
start_fs
sleep 2
python3 $W/tests/selftest.py 2>&1 | tail -8
pkill -x freeswitch || true

# ---------- 2. stability: N rounds x (start + 3 reloads) ----------
PASS=0; FAIL=0
for r in $(seq 1 $ROUNDS); do
  if start_fs; then
    ok=1
    for i in 1 2 3; do
      $CLI -x 'reload mod_nats' >/dev/null 2>&1 || true
      sleep 1
      $CLI -x 'nats status' 2>/dev/null | grep -q conn_state || { ok=0; echo "round $r reload $i: DEAD"; break; }
    done
    if [ $ok -eq 1 ]; then PASS=$((PASS+1)); echo "round $r: startup + 3x reload OK"; else FAIL=$((FAIL+1)); fi
  else
    echo "round $r: STARTUP FAILED"; FAIL=$((FAIL+1))
  fi
  pkill -x freeswitch 2>/dev/null || true
done

# ---------- 3. control: pure FS (no mod_nats) reload stress ----------
sed -i 's|<load module="mod_nats"/>|<!--load module="mod_nats"/-->|' /etc/fs-test/freeswitch.xml
CPASS=0
for r in 1 2 3; do
  if start_fs; then
    cok=1
    for i in 1 2 3; do
      $CLI -x 'reload mod_loopback' >/dev/null 2>&1 || true
      sleep 1
      $CLI -x 'status' 2>/dev/null | grep -q uptime || { cok=0; echo "control round $r reload $i: DEAD"; break; }
    done
    [ $cok -eq 1 ] && CPASS=$((CPASS+1))
  fi
  pkill -x freeswitch 2>/dev/null || true
done
sed -i 's|<!--load module="mod_nats"/>|<load module="mod_nats"/>|' /etc/fs-test/freeswitch.xml

echo "=================================="
echo "mod_nats rounds:   $PASS/$ROUNDS passed"
echo "pure-FS control:   $CPASS/3 passed"
echo "=================================="
