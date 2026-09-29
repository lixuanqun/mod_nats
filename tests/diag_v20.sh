#!/bin/bash
# live diagnosis for v0.2 regressions: metrics heartbeat + header echo
cp /opt/tests/fs-minimal/freeswitch.xml /etc/fs-test/
sed -i -e 's/$//' -e 's|<load module="mod_dptools"/>|<load module="mod_dptools"/>
        <load module="mod_dialplan_xml"/>|' -e 's|<action application="park"/>|<action application="answer"/><action application="park"/>|' /etc/fs-test/freeswitch.xml
mkdir -p /tmp/fslog /tmp/fsdb
pgrep -x nats-server >/dev/null || { setsid /usr/local/bin/nats-server -p 4222 >/tmp/nats.log 2>&1 </dev/null & sleep 1; }
pkill -x freeswitch 2>/dev/null; sleep 1
rm -f /tmp/fslog/freeswitch.pid
echo "--- config has metrics-interval: $(grep -c metrics-interval /etc/fs-test/freeswitch.xml) ---"
setsid /freeswitch/freeswitch -nf -nonat -conf /etc/fs-test -log /tmp/fslog -db /tmp/fsdb -mod /tmp/fsmod >/tmp/fs.log 2>&1 </dev/null &
for i in $(seq 1 30); do
  sleep 1
  python3 - <<'EOF' && break
import socket
try:
    s = socket.create_connection(("127.0.0.1", 8021), timeout=2); s.settimeout(2)
    ok = b"auth/request" in s.recv(64); s.close(); exit(0 if ok else 1)
except Exception: exit(1)
EOF
done
sleep 2
echo "--- nats status ---"
python3 /opt/tests/eslc.py "nats status" | grep -E "metrics|conn_state"
echo "--- raw metrics sub (4s) ---"
python3 - <<'EOF'
import asyncio, json
import nats

async def main():
    nc = await nats.connect("nats://127.0.0.1:4222")
    got = []
    async def cb(m):
        got.append(m.data[:150])
    sub = await nc.subscribe("nats.fs.metrics", cb=cb)
    await nc.flush()
    await asyncio.sleep(4)
    await sub.unsubscribe()
    print("metrics msgs:", len(got))
    for g in got[:2]:
        print("  ", g)
    # header echo probe
    try:
        r = await nc.request("nats.fs.node.test-node-01",
                             json.dumps({"jsonrpc":"2.0","id":"h1","method":"fs.node.hello","params":{}}).encode(),
                             timeout=5, headers={"X-Request-Id": "diag-77"})
        print("reply headers:", dict(r.headers) if r.headers else None)
        print("reply code ok:", b'"code"' in r.data)
    except Exception as e:
        print("request err:", e)
    await nc.drain()

asyncio.run(main())
EOF
echo DIAG_DONE
