// quickstart: place a call through mod_nats using the Go SDK, watch its
// events, answer and hang it up. Run inside a network that can reach the
// node's NATS server:
//
//	MODNATS_URL=nats://nats:4222 MODNATS_NODE=test-node-01 go run .
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	modnats "github.com/lixuanqun/mod_nats/sdk/go"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	url := env("MODNATS_URL", "nats://127.0.0.1:4222")
	node := env("MODNATS_NODE", "test-node-01")
	dest := env("MODNATS_DEST", "loopback/9001")

	cli, err := modnats.Dial(url, node,
		modnats.WithCtrlUUID("go-quickstart"),
		modnats.WithAutoIdempotency(),
	)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, status, err := cli.Status(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("node %s up, sessions %.0f\n", status.NodeUUID, status.Sessions)

	events := make(chan *modnats.Event, 128)
	if _, err := cli.SubscribeEvents(func(e *modnats.Event) { events <- e }); err != nil {
		panic(err)
	}
	if _, err := cli.SubscribeMailbox(func(e *modnats.Event) { events <- e }); err != nil {
		panic(err)
	}

	callUUID := fmt.Sprintf("go-quickstart-%d", time.Now().UnixNano())
	res, err := cli.Dial(ctx, callUUID, dest, "10000210", "GoSDK", 60, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("dial -> %d job=%s\n", res.Code, res.JobUUID)

	if _, err := cli.Accept(ctx, callUUID); err != nil {
		panic(err)
	}
	if _, err := cli.Answer(ctx, callUUID); err != nil {
		panic(err)
	}
	if _, err := cli.Play(ctx, callUUID, "tone_stream://%(1000,0,440,480)"); err != nil {
		panic(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	printed := map[string]bool{}
	answered := false
	for time.Now().Before(deadline) && !answered {
		select {
		case e := <-events:
			v, err := e.Parse()
			if err != nil {
				continue
			}
			switch ev := v.(type) {
			case *modnats.ChannelEvent:
				key := ev.UUID + "/" + ev.State
				if !printed[key] {
					printed[key] = true
					fmt.Printf("channel %s %s\n", ev.UUID, ev.State)
				}
				if ev.UUID == callUUID && ev.State == modnats.StateAnswered {
					answered = true
				}
			case *modnats.CDR:
				if ev.UUID == callUUID {
					fmt.Printf("cdr: cause=%s billsec=%s\n", ev.HangupCause, ev.Billsec)
				}
			}
		case <-time.After(time.Second):
			if _, err := cli.Touch(ctx, callUUID); err != nil {
				fmt.Println("touch:", err) // 400 once the call has ended
			}
		}
	}

	if _, err := cli.Hangup(ctx, callUUID, "NORMAL_CLEARING"); err != nil {
		fmt.Println("hangup:", err)
	}
	time.Sleep(1500 * time.Millisecond)
	fmt.Println("quickstart done")
}
