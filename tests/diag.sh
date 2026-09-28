#!/bin/bash
# mod_nats WSL ESL diagnostic - runs inside Ubuntu WSL as root
P=$(pgrep -x freeswitch | head -1)
echo "FS_PID=$P"
echo "=== networking mode ==="
wslinfo --networking-mode 2>/dev/null || echo "wslinfo unavailable"

echo "=== fd->socket mapping ==="
python3 - "$P" <<'EOF'
import os, sys, re
pid = sys.argv[1]
inodes = {}
for fd in ("6", "9"):
    try:
        link = os.readlink(f"/proc/{pid}/fd/{fd}")
        m = re.search(r"\[(\d+)\]", link)
        if m:
            inodes[fd] = m.group(1)
    except Exception as e:
        inodes[fd] = f"err:{e}"
rows = {}
for f in ("/proc/net/tcp", "/proc/net/tcp6"):
    try:
        for line in open(f).readlines()[1:]:
            p = line.split()
            rows[p[9]] = (f.split('/')[-1], p[1], p[3])
    except Exception:
        pass
for fd, ino in inodes.items():
    r = rows.get(str(ino), ("?", "?", "?"))
    print(f"fd{fd} inode={ino} table={r[0]} local={r[1]} state={r[2]}")
EOF

echo "=== all listeners ==="
ss -tlnp 2>/dev/null | grep -E "8021|freeswitch"

echo "=== ESL banner tests ==="
for addr in 127.0.0.1 localhost $(hostname -I | awk '{print $1}'); do
  echo -n "[$addr] "
  timeout 2 bash -c "exec 3<>/dev/tcp/$addr/8021; head -c 40 <&3" 2>/dev/null | tr -d '\r\n'
  echo " (rc=$?)"
done

echo "=== accept thread check ==="
for t in /proc/$P/task/*; do
  w=$(cat $t/wchan 2>/dev/null)
  [ "$w" = "inet_csk_accept" ] && echo "accept-wait tid=$(basename $t)"
done
echo DIAG_DONE
