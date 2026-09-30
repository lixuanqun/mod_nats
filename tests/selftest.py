#!/usr/bin/env python3
"""
mod_nats full self-test: every XNode.* method, error codes, and every event
type. Drives FreeSWITCH channels via the NATS bus only (no ESL involved).

Usage: python3 selftest.py   (expects FS + nats-server already running)
"""
import asyncio
import json
import os
import sys
import time
import uuid as uuidlib

NATS_URL = os.environ.get("MODNATS_URL", "nats://127.0.0.1:4222")
# park destination: an extension that answers and stays up. fs-minimal maps
# 1000 to answer+park; the docker-lab image maps 1000 to a LiveKit bridge, so
# point MODNATS_DEST at its echo extension (9196) there.
DEST = os.environ.get("MODNATS_DEST", "loopback/1000")
PREFIX = "nats.fs."
NODE = "test-node-01"
CTRL = "test-ctrl-01"
NODE_SUBJECT = f"{PREFIX}node.{NODE}"
CTRL_SUBJECT = f"{PREFIX}ctrl.{CTRL}"

results = []          # (name, ok, detail)
metrics_msgs = []     # Event.Metrics heartbeats
events = []           # every notification seen on nats.fs.>
ctrl_msgs = []        # messages on the controller mailbox


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


def find_event(method, **conds):
    """first event matching method + field conditions"""
    for subj, msg in events:
        if msg.get("method") != method:
            continue
        p = msg.get("params", {})
        if all(str(p.get(k)) == str(v) for k, v in conds.items()):
            return subj, msg
    return None


class Client:
    def __init__(self, nc):
        self.nc = nc
        self.rid = 0

    async def req(self, method, params=None, expect=None, timeout=8):
        self.rid += 1
        rid = f"t{self.rid}"
        p = dict(params or {})
        # xctrl SDKs stamp ctrl_uuid on every request; owner-gated methods
        # 419 without it since v0.3
        p.setdefault("ctrl_uuid", CTRL)
        env = {"jsonrpc": "2.0", "id": rid, "method": method, "params": p}
        try:
            reply = await self.nc.request(NODE_SUBJECT, json.dumps(env).encode(), timeout=timeout)
            out = json.loads(reply.data.decode())
        except Exception as e:
            check(f"{method} request", False, f"no reply: {e}")
            return None
        return out

    async def req_code(self, method, params, expect_code, name=None):
        out = await self.req(method, params)
        if out is None:
            return None
        code = out.get("result", {}).get("code")
        check(name or f"{method} -> {expect_code}", code == expect_code,
              f"code={code} msg={out.get('result', {}).get('message')}")
        return out


async def originate_parked(cli, tag):
    """create a parked channel via XNode.NativeAPI originate; returns uuid"""
    u = str(uuidlib.uuid4())
    out = await cli.req("XNode.NativeAPI", {
        "cmd": "originate",
        "args": f"{{origination_uuid={u},ignore_early_media=true}}{DEST} &park",
    })
    if out is None:
        return None
    data = out.get("result", {}).get("data", "")
    ok = isinstance(data, str) and "+OK" in data
    check(f"originate parked channel ({tag})", ok, data[:80] if isinstance(data, str) else data)
    return u if ok else None


async def wait_for(pred, timeout=5.0, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(step)
    return False


async def main():
    import nats

    nc = await nats.connect(NATS_URL)

    async def collector(msg):
        try:
            data = json.loads(msg.data.decode())
        except Exception:
            return
        if msg.subject.startswith(f"{PREFIX}event."):
            events.append((msg.subject, data))
        if msg.subject == CTRL_SUBJECT:
            ctrl_msgs.append((msg.subject, data))
        if msg.subject == f"{PREFIX}metrics":
            metrics_msgs.append(data)

    await nc.subscribe(f"{PREFIX}event.>", cb=collector)
    await nc.subscribe(CTRL_SUBJECT, cb=collector)
    await nc.subscribe(f"{PREFIX}metrics", cb=collector)
    cli = Client(nc)
    await asyncio.sleep(0.3)

    print("== metrics heartbeat ==")
    await asyncio.sleep(2.5)
    ok = len(metrics_msgs) >= 1 and "sessions" in metrics_msgs[0].get("params", {})
    m2 = metrics_msgs[1].get("params", {}) if len(metrics_msgs) > 1 else {}
    check("Metrics carry system fields (cpu/mem)",
          "cpu_count" in m2 and "mem_total_mb" in m2,
          f"keys={sorted(m2.keys())[:14] if m2 else None}")
    if ok and m2:
        ok = True
    check("Event.Metrics heartbeat received with fields", ok,
          f"count={len(metrics_msgs)} sample={str(metrics_msgs[0])[:120] if metrics_msgs else None}")

    print("== v0.2 protocol: hello / dual namespace / header echo ==")
    out = await cli.req("fs.node.hello", {})
    if out:
        caps = out.get("result", {}).get("data", {})
        cl = caps.get("capabilities", [])
        check("fs.node.hello -> 200 with capabilities", out.get("result", {}).get("code") == 200 and "fs.channel.answer" in cl and "XNode.Answer" in cl,
              f"count={len(cl)} compat={caps.get('compat_xcc')}")

    out = await cli.req("fs.node.status", {})
    check("canonical fs.node.status -> 200", out is not None and out.get("result", {}).get("code") == 200,
          f"code={out.get('result', {}).get('code') if out else None}")
    out = await cli.req("XNode.JStatus", {})
    check("alias XNode.JStatus -> 200 (compat on)", out is not None and out.get("result", {}).get("code") == 200,
          f"code={out.get('result', {}).get('code') if out else None}")

    # X-Request-Id NATS header echoed on the reply
    try:
        rid = "selftest-rid-42"
        reply = await nc.request(NODE_SUBJECT, json.dumps({"jsonrpc": "2.0", "id": "hdr1", "method": "fs.node.hello", "params": {}}).encode(),
                                 timeout=5, headers={"X-Request-Id": rid})
        got = (reply.headers or {}).get("X-Request-Id")
        check("X-Request-Id header echoed", got == rid, f"got={got}")
    except Exception as e:
        check("X-Request-Id header echoed", False, f"ex={e}")

    print("== basic node methods ==")
    out = await cli.req_code("XNode.JStatus", {}, 200, "XNode.JStatus -> 200")
    if out:
        d = out.get("result", {}).get("data", {})
        check("JStatus fields", isinstance(d, dict) and "sessions" in d and "version" in d,
              f"sessions={d.get('sessions')} version={d.get('version')}")

    out = await cli.req_code("XNode.NativeAPI", {"cmd": "status"}, 200, "XNode.NativeAPI(status) -> 200")
    if out:
        check("NativeAPI status data", "session" in str(out.get("result", {}).get("data", "")), "")

    out = await cli.req_code("XNode.NativeJSAPI", {"cmd": "status"}, 200, "XNode.NativeJSAPI(status) -> 200")

    print("== channel A lifecycle ==")
    ua = await originate_parked(cli, "A")
    if not ua:
        await finish(1)
    await asyncio.sleep(0.5)
    check("Event.Channel START(A)", find_event("Event.Channel", uuid=ua, state="START") is not None, ua[:12])

    await cli.req_code("XNode.Accept", {"uuid": ua, "ctrl_uuid": CTRL}, 200, "XNode.Accept(A) -> 200")
    out = await cli.req_code("XNode.Accept", {"uuid": ua, "ctrl_uuid": "other-ctrl"}, 419,
                             "XNode.Accept(A,2nd ctrl) -> 419")

    out = await cli.req_code("fs.channel.getstate", {"uuid": ua}, 200, "fs.channel.getstate(A) -> 200 (canonical)")
    await cli.req_code("XNode.GetState", {"uuid": ua}, 200, "XNode.GetState(A) -> 200 (alias)")
    await cli.req_code("XNode.SetVar", {"uuid": ua, "data": {"test_var": "hello-nats"}}, 200,
                       "XNode.SetVar(A,test_var) -> 200")
    out = await cli.req_code("XNode.GetVar", {"uuid": ua, "data": ["test_var"]}, 200, "XNode.GetVar(A) -> 200")
    if out:
        val = out.get("result", {}).get("data", {}).get("test_var")
        check("GetVar value roundtrip", val == "hello-nats", f"test_var={val}")
    out = await cli.req_code("XNode.GetChannelData", {"uuid": ua}, 200, "XNode.GetChannelData(A) -> 200")
    if out:
        check("GetChannelData uuid", out.get("result", {}).get("data", {}).get("uuid") == ua, "")

    # force state transitions via the escape hatch
    await cli.req_code("XNode.NativeApp", {"uuid": ua, "cmd": "ring_ready"}, 200,
                       "XNode.NativeApp(A,ring_ready) -> 200")
    print(f"  info: RINGING(A) observed: {find_event('Event.Channel', uuid=ua, state='RINGING') is not None} (loopback: optional)")
    await cli.req_code("XNode.NativeApp", {"uuid": ua, "cmd": "pre_answer"}, 200,
                       "XNode.NativeApp(A,pre_answer) -> 200")
    print(f"  info: MEDIA(A) observed: {find_event('Event.Channel', uuid=ua, state='MEDIA') is not None} (loopback: optional)")

    await cli.req_code("XNode.Answer", {"uuid": ua}, 200, "XNode.Answer(A) -> 200")
    check("Event ANSWERED(A)", await wait_for(lambda: find_event("Event.Channel", uuid=ua, state="ANSWERED")), "")

    tone = "tone_stream://%(2000,0,440,480)"
    await cli.req_code("XNode.Play", {"uuid": ua, "media": {"type": "FILE", "file": tone}}, 200,
                       "XNode.Play(A,tone) -> 200")
    await asyncio.sleep(0.5)
    await cli.req_code("XNode.Stop", {"uuid": ua}, 200, "XNode.Stop(A) -> 200")
    await cli.req_code("XNode.Broadcast", {"uuid": ua, "option": "ALEG", "file": tone}, 200,
                       "XNode.Broadcast(A) -> 200")
    await asyncio.sleep(0.5)
    await cli.req_code("XNode.Stop", {"uuid": ua}, 200, "XNode.Stop(A) after broadcast -> 200")

    print("== DTMF detect + record ==")
    # loopback send_dtmf lands on the PEER channel's queue, and SWITCH_EVENT_
    # DTMF fires on dequeue - so run mod_dptools' read on the peer leg, then
    # send digits from A. '#' terminates read immediately. The peer is the
    # loopback's dialplan leg: A is the first channel of this run, so the
    # only other START event at this point belongs to it (peer_uuid only
    # appears on bridged channels).
    peer = None
    for _, msg in events:
        p = msg.get("params", {})
        if msg.get("method") == "Event.Channel" and p.get("state") == "START" and p.get("uuid") != ua:
            peer = p.get("uuid")
            break
    check("A peer_uuid discovered", peer is not None, str(peer)[:12])
    if peer:
        await cli.req_code("XNode.Accept", {"uuid": peer}, 200, "XNode.Accept(peer) -> 200")
        await cli.req_code("XNode.NativeApp", {"uuid": peer, "cmd": "read",
                                                "args": "1 4 silence_stream://200 nats_digits 8000 #"}, 200,
                           "XNode.NativeApp(peer,read) -> 200")
        await asyncio.sleep(0.3)
        await cli.req_code("XNode.NativeAPI", {"cmd": "uuid_send_dtmf", "args": f"{ua} 12#"}, 200,
                           "uuid_send_dtmf(A,'12#') -> 200")
        check("Event.Detected dtmf=1", await wait_for(
            lambda: find_event("Event.Detected", uuid=peer, dtmf="1") is not None), "")
        check("Event.Detected dtmf=2", await wait_for(
            lambda: find_event("Event.Detected", uuid=peer, dtmf="2") is not None), "")
        check("Event.Detected routed to ctrl mailbox", any(
            m.get("method") == "Event.Detected" and m.get("params", {}).get("uuid") == peer for _, m in ctrl_msgs), "")
    rec = "/tmp/modnats-selftest.wav"
    await cli.req_code("XNode.Record", {"uuid": ua, "action": "RECORD", "file": rec}, 200,
                       "XNode.Record(A,RECORD) -> 200")
    await asyncio.sleep(1)
    await cli.req_code("XNode.Record", {"uuid": ua, "action": "STOP", "file": rec}, 200,
                       "XNode.Record(A,STOP) -> 200")
    await cli.req_code("XNode.Record", {"uuid": ua, "action": "RECORD", "file": "/tmp/../etc/x.wav"}, 400,
                       "XNode.Record path traversal -> 400")

    print("== bridge A<->B ==")
    ub = await originate_parked(cli, "B")
    if ub:
        # both legs must be owned by this ctrl before Bridge; the originate
        # above may have eaten most of A's lease, so touch it too
        await cli.req_code("XNode.Touch", {"uuid": ua}, 200, "XNode.Touch(A, renew) -> 200")
        await cli.req_code("XNode.Accept", {"uuid": ub}, 200, "XNode.Accept(B) -> 200")
        await cli.req_code("XNode.Answer", {"uuid": ub}, 200, "XNode.Answer(B) -> 200")
        await cli.req_code("XNode.Bridge", {"uuid": ua, "peer_uuid": ub}, 200, "XNode.Bridge(A,B) -> 200")
        check("Event BRIDGE", await wait_for(lambda: find_event("Event.Channel", uuid=ua, state="BRIDGE")
                                             or find_event("Event.Channel", uuid=ub, state="BRIDGE")), "")

        await cli.req_code("XNode.Hangup", {"uuid": ua, "cause": "NORMAL_CLEARING"}, 200,
                           "XNode.Hangup(A) -> 200")
        check("Event UNBRIDGE (either leg)", await wait_for(lambda: find_event("Event.Channel", state="UNBRIDGE")), "")
        check("Event DESTROY(A)", await wait_for(lambda: find_event("Event.Channel", uuid=ua, state="DESTROY")), "")
        check("Event CDR(A)", await wait_for(lambda: find_event("Event.CDR", uuid=ua)), "")
        out = await cli.req("XNode.Hangup", {"uuid": ub})
        check("XNode.Hangup(B) [ok if 200/400/404: the unbridge may tear B down first]",
              out is not None and out.get("result", {}).get("code") in (200, 400, 404),
              f"code={out.get('result', {}).get('code') if out else None}")
        await asyncio.sleep(0.5)
        check("Event CDR(B)", find_event("Event.CDR", uuid=ub) is not None, "")

    print("== XNode.Dial (async originate) ==")
    uc = str(uuidlib.uuid4())
    out = await cli.req("XNode.Dial", {
        "ctrl_uuid": CTRL,
        "destination": {"call_params": [{
            "uuid": uc,
            "dial_string": DEST,
            "cid_number": "10000210",
            "cid_name": "SelfTest",
        }]},
    })
    check("XNode.Dial -> accepted", out is not None and out.get("result", {}).get("code") in (200, 202),
          f"result={str(out)[:200] if out else None}")
    if out:
        r = out.get("result", {})
        check("Dial returns 202 + job_uuid", r.get("code") == 202 and r.get("job_uuid"),
              f"code={r.get('code')} job_uuid={str(r.get('job_uuid'))[:12]}")
        check("Event START(C)", await wait_for(lambda: find_event("Event.Channel", uuid=uc, state="START"), 10), uc[:12])
        check("Event.Result on ctrl mailbox", await wait_for(
            lambda: any(m.get("method") == "Event.Result" and m.get("params", {}).get("job_uuid") == r.get("job_uuid")
                        for _, m in ctrl_msgs), 10), "")
        for _, m in ctrl_msgs:
            if m.get("method") == "Event.Result" and m.get("params", {}).get("job_uuid") == r.get("job_uuid"):
                print("    Event.Result params:", json.dumps(m.get("params"))[:300])
        out = await cli.req("XNode.Hangup", {"uuid": uc})
        check("XNode.Hangup(C) [ok if 200 or 404]",
              out is not None and out.get("result", {}).get("code") in (200, 404),
              f"code={out.get('result', {}).get('code') if out else None}")

    print("== owner lease (owner-lease-ttl=4 in the test config) ==")
    ud = await originate_parked(cli, "D")
    if ud:
        await cli.req_code("XNode.Accept", {"uuid": ud}, 200, "XNode.Accept(D) -> 200")
        out = await cli.req_code("XNode.Touch", {"uuid": ud}, 200, "XNode.Touch(D) -> 200 (renews lease)")
        if out:
            check("Touch reports lease_ttl", out.get("result", {}).get("lease_ttl") == 4,
                  f"lease_ttl={out.get('result', {}).get('lease_ttl')}")
        # no further owner requests: the lease must elapse and release D
        check("Event.OwnerLost(D) on event.ownerlost", await wait_for(
            lambda: find_event("Event.OwnerLost", uuid=ud, ctrl_uuid=CTRL) is not None, 15), "ttl=4s")
        out = await cli.req("fs.channel.getstate", {"uuid": ud})
        check("old owner after expiry -> 400 (unclaimed)",
              out is not None and out.get("result", {}).get("code") == 400,
              f"code={out.get('result', {}).get('code') if out else None} msg={out.get('result', {}).get('message') if out else None}")
        await cli.req_code("XNode.Accept", {"uuid": ud, "ctrl_uuid": "standby-ctrl"}, 200,
                           "standby controller re-Accept(D) -> 200")
        await cli.req_code("XNode.Touch", {"uuid": ud, "ctrl_uuid": "standby-ctrl"}, 200,
                           "standby XNode.Touch(D) -> 200")
        await cli.req_code("XNode.Hangup", {"uuid": ud, "ctrl_uuid": "standby-ctrl"}, 200,
                           "standby XNode.Hangup(D) -> 200")

    print("== ctrl mailbox routing ==")
    check("A events routed to ctrl mailbox", any(
        m.get("method") == "Event.Channel" and m.get("params", {}).get("uuid") == ua for _, m in ctrl_msgs), "")

    print("== error paths ==")
    bogus = str(uuidlib.uuid4())
    # v0.3 semantics: the ownership gate fires before the session lookup,
    # so an unknown uuid reports 400 "channel not accepted", not 404
    await cli.req_code("XNode.GetState", {"uuid": bogus}, 400, "bogus uuid -> 400 (not accepted)")
    await cli.req_code("XNode.Answer", {}, 400, "missing uuid -> 400")
    out = await cli.req("XNode.NoSuchMethod", {})
    if out:
        check("unknown method -> 501", out.get("result", {}).get("code") == 501, str(out.get("result")))

    await finish(0)


async def finish(code):
    print("\n================ SUMMARY ================")
    passed = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL':4}  {name}")
    print(f"\n{passed}/{len(results)} checks passed")
    # observed event states
    states = sorted({m.get("params", {}).get("state") for _, m in events if m.get("method") == "Event.Channel"})
    methods = sorted({m.get("method") for _, m in events})
    print(f"observed Event.Channel states: {states}")
    print(f"observed notification methods: {methods}")
    fails = [n for n, ok, _ in results if not ok]
    sys.exit(0 if not fails and code == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
