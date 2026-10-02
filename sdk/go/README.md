# modnats-go

Go client for the [mod_nats](../../README.md) control plane of FreeSWITCH
(wire protocol 2.5.0). One `Client` talks to one FreeSWITCH node over NATS
with JSON-RPC 2.0; media never flows through NATS.

```go
package main

import (
    "context"
    "fmt"
    "time"

    modnats "github.com/lixuanqun/mod_nats/sdk/go"
)

func main() {
    cli, err := modnats.Dial("nats://127.0.0.1:4222", "fs-node-01",
        modnats.WithCtrlUUID("my-controller"),
        modnats.WithAutoIdempotency(), // safe retries for non-idempotent calls
    )
    if err != nil {
        panic(err)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // outbound call: 202 + job_uuid now, final outcome via Event.Result
    res, err := cli.Dial(ctx, "my-call-uuid", "user/1001", "1001", "Demo", 60, nil)
    if err != nil {
        panic(err)
    }
    fmt.Println("dial ack:", res.Code, res.JobUUID)

    // answer an inbound channel after taking ownership
    if _, err := cli.Accept(ctx, "channel-uuid"); err != nil {
        panic(err)
    }
    if _, err := cli.Answer(ctx, "channel-uuid"); err != nil {
        panic(err)
    }
}
```

## Events

```go
cli.SubscribeEvents(func(e *modnats.Event) {
    switch e.Method {
    case modnats.EventChannel:
        ce, _ := e.Parse()
        fmt.Println(ce.(*modnats.ChannelEvent).UUID, ce.(*modnats.ChannelEvent).State)
    case modnats.EventOwnerLost: // lease elapsed: standby can re-Accept
    case modnats.EventDetected:  // DTMF (type=dtmf) or ASR results (type=asr)
    case modnats.EventCDR, modnats.EventResult:
    }
})
cli.SubscribeMailbox(func(e *modnats.Event) { /* owned channel traffic */ })
```

## Notes

- **Prefix**: default `nats.fs.`; set `cn.xswitch.` for drop-in interop
  with the official xctrl SDKs (`WithPrefix`). `WithStyle(StyleCanonical)`
  sends the canonical `fs.*` method names instead of the `XNode.*` aliases.
- **Ownership**: `Accept` returns 419 for channels owned by another
  controller; re-Accept by the same controller is idempotent and renews
  the owner lease. Call `Touch` on idle-but-owned channels when
  `owner-lease-ttl` is armed on the node.
- **Retries**: `WithAutoIdempotency` stamps every request with a fresh
  `idempotency_key`; a retry after a lost reply replays the stored 2xx
  result (`Result.IdempotentReplay == true`) instead of executing twice.
  Supply your own stable key per logical operation if you prefer.
- **Dial**: pass a client-chosen `uuid`; it becomes the `origination_uuid`,
  so a repeated Dial with the same uuid can never create a second call.
  The final outcome arrives as `Event.Result` on the mailbox.
- Result codes: 200 OK, 202 accepted, 400 refused, 404 no channel, 419
  owned elsewhere, 480 dial failed, 503 queue full — see `Result.Error`.
