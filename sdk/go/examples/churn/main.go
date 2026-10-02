// churn: call-path load generator for mod_nats, built on the Go SDK.
//
// Each worker loops: Dial -> wait ANSWERED -> Play -> Hangup -> wait DESTROY,
// recording dial-to-answered latency. Reports CPS, success count and a
// latency histogram at the end.
//
//	MODNATS_URL=nats://nats:4222 MODNATS_NODE=test-node-01 \
//	MODNATS_DEST=loopback/9001 WORKERS=4 CALLS=200 go run .
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	modnats "github.com/lixuanqun/mod_nats/sdk/go"
)

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type stats struct {
	mu          sync.Mutex
	failed      int
	latenciesMs []float64
}

func percentile(s []float64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	c := append([]float64(nil), s...)
	sort.Float64s(c)
	i := int(float64(len(c)-1) * p)
	return c[i]
}

func main() {
	url := env("MODNATS_URL", "nats://127.0.0.1:4222")
	node := env("MODNATS_NODE", "test-node-01")
	dest := env("MODNATS_DEST", "loopback/9001")
	workers := envInt("WORKERS", 4)
	calls := envInt("CALLS", 100)

	cli, err := modnats.Dial(url, node,
		modnats.WithCtrlUUID("go-churn"),
		modnats.WithAutoIdempotency(),
	)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	events := make(chan *modnats.Event, 4096)
	if _, err := cli.SubscribeEvents(func(e *modnats.Event) { events <- e }); err != nil {
		panic(err)
	}
	if _, err := cli.SubscribeMailbox(func(e *modnats.Event) { events <- e }); err != nil {
		panic(err)
	}

	// answered[uuid] closes when the call reaches ANSWERED; the worker
	// waits on it right after Dial.
	answered := map[string]chan struct{}{}
	var amu sync.Mutex
	waitAnswered := func(uuid string) chan struct{} {
		amu.Lock()
		defer amu.Unlock()
		ch, ok := answered[uuid]
		if !ok {
			ch = make(chan struct{})
			answered[uuid] = ch
		}
		return ch
	}
	signalAnswered := func(uuid string) {
		amu.Lock()
		defer amu.Unlock()
		if ch, ok := answered[uuid]; ok {
			close(ch)
			delete(answered, uuid)
		}
	}
	destroyed := map[string]chan struct{}{}
	var dmu sync.Mutex
	waitDestroyed := func(uuid string) chan struct{} {
		dmu.Lock()
		defer dmu.Unlock()
		ch, ok := destroyed[uuid]
		if !ok {
			ch = make(chan struct{})
			destroyed[uuid] = ch
		}
		return ch
	}
	signalDestroyed := func(uuid string) {
		dmu.Lock()
		defer dmu.Unlock()
		if ch, ok := destroyed[uuid]; ok {
			close(ch)
			delete(destroyed, uuid)
		}
	}

	done := make(chan struct{})
	go func() {
		for e := range events {
			v, err := e.Parse()
			if err != nil {
				continue
			}
			switch ev := v.(type) {
			case *modnats.ChannelEvent:
				switch ev.State {
				case modnats.StateAnswered:
					signalAnswered(ev.UUID)
				case modnats.StateDestroy:
					signalDestroyed(ev.UUID)
				}
			}
		}
		close(done)
	}()

	st := &stats{}
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < calls/workers; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				uuid := fmt.Sprintf("churn-%d-%d-%d", w, i, time.Now().UnixNano())
				t0 := time.Now()
				if _, err := cli.Dial(ctx, uuid, dest, "10000210", "churn", 15, nil); err != nil {
					st.mu.Lock()
					st.failed++
					st.mu.Unlock()
					cancel()
					continue
				}
				select {
				case <-waitAnswered(uuid):
					st.mu.Lock()
					st.latenciesMs = append(st.latenciesMs, float64(time.Since(t0).Microseconds())/1000.0)
					st.mu.Unlock()
				case <-time.After(15 * time.Second):
					st.mu.Lock()
					st.failed++
					st.mu.Unlock()
				}
				_, _ = cli.Play(ctx, uuid, "tone_stream://%(100,0,600)")
				_, _ = cli.Hangup(ctx, uuid, "NORMAL_CLEARING")
				select {
				case <-waitDestroyed(uuid):
				case <-time.After(10 * time.Second):
				}
				cancel()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	st.mu.Lock()
	lat := st.latenciesMs
	failed := st.failed
	st.mu.Unlock()
	fmt.Printf("\ncalls=%d ok(answered)=%d failed=%d wall=%s cps=%.1f\n",
		calls, len(lat), failed, elapsed.Round(time.Millisecond),
		float64(len(lat))/elapsed.Seconds())
	fmt.Printf("dial->answered ms: p50=%.1f p95=%.1f p99=%.1f max=%.1f\n",
		percentile(lat, 0.50), percentile(lat, 0.95), percentile(lat, 0.99),
		percentile(lat, 1.0))
}
