package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL    = "nats://nats:4222"
	nodeSubj   = "nats.fs.node.fs-lab-1"
	ctrlSubj   = "nats.fs.ctrl.lab-ctrl"
	ctrlUUID   = "lab-ctrl"
	unownedUUID = "11111111-1111-4111-8111-111111111111"
	statsPath  = "/data/stats.json"
	reportPath = "/data/report.json"
)

type stats struct {
	Total    int            `json:"total"`
	ByMethod map[string]int `json:"by_method"`
	ByState  map[string]int `json:"by_state"`
	NodeUp   int            `json:"node_up"`
	CDR      int            `json:"cdr"`
	Result   int            `json:"result"`
	Metrics  int            `json:"metrics"`
	Updated  string         `json:"updated"`
}

type testCase struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

type latency struct {
	N   int     `json:"n"`
	OK  int     `json:"ok"`
	Err int     `json:"err"`
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
	RPS float64 `json:"rps"`
}

type report struct {
	Started       string         `json:"started"`
	Finished      string         `json:"finished"`
	Functional    []testCase     `json:"functional"`
	Passed        int            `json:"passed"`
	Failed        int            `json:"failed"`
	JStatusLoad   latency        `json:"jstatus_load"`
	CallLoad      latency        `json:"call_load"`
	ConsumerStart stats          `json:"consumer_before_calls"`
	ConsumerEnd   stats          `json:"consumer_after_calls"`
	JSCdr         uint64         `json:"js_cdr_messages"`
	JSMetrics     uint64         `json:"js_metrics_messages"`
	Notes         []string       `json:"notes"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lab consume|drive")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "consume":
		if err := consume(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "drive":
		if err := drive(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(2)
	}
}

func connect() (*nats.Conn, error) {
	return nats.Connect(natsURL,
		nats.Name("modnats-lab"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
}

func consume() error {
	nc, err := connect()
	if err != nil {
		return err
	}
	defer nc.Close()
	if js, err := nc.JetStream(); err == nil {
		ensureStream(js, "FS_CDR", "nats.fs.event.cdr")
		ensureStream(js, "FS_METRICS", "nats.fs.metrics")
		fmt.Println("jetstream streams ready")
	} else {
		fmt.Println("jetstream unavailable:", err)
	}
	var mu sync.Mutex
	st := stats{ByMethod: map[string]int{}, ByState: map[string]int{}}
	_, err = nc.Subscribe("nats.fs.>", func(m *nats.Msg) {
		var env struct {
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(m.Data, &env)
		mu.Lock()
		defer mu.Unlock()
		st.Total++
		if env.Method != "" {
			st.ByMethod[env.Method]++
		}
		switch env.Method {
		case "Event.NodeUp":
			st.NodeUp++
		case "Event.CDR":
			st.CDR++
		case "Event.Result":
			st.Result++
		case "Event.Metrics":
			st.Metrics++
		case "Event.Channel":
			if s, ok := env.Params["state"].(string); ok {
				st.ByState[s]++
			}
		}
	})
	if err != nil {
		return err
	}
	if err = nc.Flush(); err != nil {
		return err
	}
	fmt.Println("consumer subscribed nats.fs.>")
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		mu.Lock()
		st.Updated = time.Now().UTC().Format(time.RFC3339)
		snap := st
		snap.ByMethod = copyMap(st.ByMethod)
		snap.ByState = copyMap(st.ByState)
		mu.Unlock()
		writeJSON(statsPath, snap)
	}
	return nil
}

func copyMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

func drive() error {
	nc, err := connect()
	if err != nil {
		return err
	}
	defer nc.Close()
	rep := report{Started: time.Now().UTC().Format(time.RFC3339), Notes: []string{}}
	js, err := nc.JetStream()
	if err != nil {
		rep.Notes = append(rep.Notes, "jetstream context: "+err.Error())
	} else {
		ensureStream(js, "FS_CDR", "nats.fs.event.cdr")
		ensureStream(js, "FS_METRICS", "nats.fs.metrics")
	}

	waitReady(nc, &rep)
	rep.Functional = append(rep.Functional, functional(nc)...)

	before := readStats()
	rep.ConsumerStart = before
	rep.JStatusLoad = loadJStatus(nc, 15*time.Second, 32)
	rep.CallLoad = loadCalls(nc, 24, 4)
	time.Sleep(2 * time.Second)
	rep.ConsumerEnd = readStats()
	if js != nil {
		if info, err := js.StreamInfo("FS_CDR"); err == nil {
			rep.JSCdr = info.State.Msgs
		}
		if info, err := js.StreamInfo("FS_METRICS"); err == nil {
			rep.JSMetrics = info.State.Msgs
		}
	}
	for _, c := range rep.Functional {
		if c.Pass {
			rep.Passed++
		} else {
			rep.Failed++
		}
	}
	rep.Finished = time.Now().UTC().Format(time.RFC3339)
	writeJSON(reportPath, rep)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func ensureStream(js nats.JetStreamContext, name, subject string) {
	_, err := js.StreamInfo(name)
	if err == nil {
		return
	}
	_, _ = js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subject}, Storage: nats.FileStorage})
}

func waitReady(nc *nats.Conn, rep *report) {
	deadline := time.Now().Add(90 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		_, _, err := rpc(nc, `{"jsonrpc":"2.0","id":"ready","method":"XNode.JStatus","params":{}}`, 3*time.Second)
		if err == nil {
			return
		}
		last = err
		time.Sleep(time.Second)
	}
	if last != nil {
		rep.Notes = append(rep.Notes, "node not ready: "+last.Error())
	}
}

func rpc(nc *nats.Conn, body string, timeout time.Duration) ([]byte, time.Duration, error) {
	start := time.Now()
	msg, err := nc.Request(nodeSubj, []byte(body), timeout)
	return msgData(msg), time.Since(start), err
}

func msgData(msg *nats.Msg) []byte {
	if msg == nil {
		return nil
	}
	return msg.Data
}

func resultCode(raw []byte) (int, map[string]any) {
	var env struct {
		ID     any            `json:"id"`
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Result == nil {
		return -1, nil
	}
	code := -1
	switch v := env.Result["code"].(type) {
	case float64:
		code = int(v)
	}
	return code, env.Result
}

func functional(nc *nats.Conn) []testCase {
	var cases []testCase
	add := func(name string, pass bool, detail string) {
		cases = append(cases, testCase{Name: name, Pass: pass, Detail: detail})
		mark := "FAIL"
		if pass {
			mark = "PASS"
		}
		fmt.Printf("[%s] %s %s\n", mark, name, detail)
	}

	raw, _, err := rpc(nc, `{"jsonrpc":"2.0","id":42,"method":"XNode.JStatus","params":{}}`, 3*time.Second)
	code, result := resultCode(raw)
	idNum := false
	var env struct {
		ID json.RawMessage `json:"id"`
	}
	_ = json.Unmarshal(raw, &env)
	idNum = len(env.ID) > 0 && env.ID[0] != '"'
	sessionsOK := false
	if result != nil {
		if data, ok := result["data"].(map[string]any); ok {
			_, sessionsOK = data["sessions"].(float64)
		}
	}
	add("JStatus 与数字 id", err == nil && code == 200 && idNum && sessionsOK, fmt.Sprintf("code=%d id=%s err=%v", code, string(env.ID), err))

	raw, _, err = rpc(nc, `{"jsonrpc":"2.0","id":"nope","method":"XNode.NoSuch","params":{}}`, 3*time.Second)
	code, _ = resultCode(raw)
	add("未知方法 501", err == nil && code == 501, fmt.Sprintf("code=%d err=%v", code, err))

	raw, _, err = rpc(nc, `{"jsonrpc":"2.0","id":"bad","method":"XNode.Hangup","params":{"ctrl_uuid":"lab-ctrl"}}`, 3*time.Second)
	code, _ = resultCode(raw)
	add("缺少 uuid 返回 400", err == nil && code == 400, fmt.Sprintf("code=%d err=%v", code, err))

	raw, _, err = rpc(nc, `{"jsonrpc":"2.0","id":"miss","method":"XNode.Accept","params":{"ctrl_uuid":"lab-ctrl","uuid":"00000000-0000-4000-8000-000000000000"}}`, 3*time.Second)
	code, _ = resultCode(raw)
	add("Accept 不存在的通道 404", err == nil && code == 404, fmt.Sprintf("code=%d err=%v", code, err))

	raw, _, err = rpc(nc, `{"jsonrpc":"2.0","id":"api","method":"XNode.NativeAPI","params":{"cmd":"status"}}`, 3*time.Second)
	code, result = resultCode(raw)
	add("NativeAPI 默认关闭", err == nil && code == 403, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	big := make([]byte, 1024*1024+64)
	for i := range big {
		big[i] = 'A'
	}
	copy(big, []byte(`{"jsonrpc":"2.0","id":"big","method":"XNode.JStatus","params":{}}`))
	raw, _, err = rpc(nc, string(big), 5*time.Second)
	code, _ = resultCode(raw)
	add("超过 1MiB 的请求被拒绝", err == nil && code == 400, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	raw, _, err = rpc(nc, `{"not json`, 3*time.Second)
	code, _ = resultCode(raw)
	add("非法 JSON 返回 400", err == nil && code == 400, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	raw, _, err = rpc(nc, `{"jsonrpc":"2.0","id":"forbid","method":"XNode.Dial","params":{"ctrl_uuid":"lab-ctrl","destination":{"call_params":[{"dial_string":"{execute_on_answer=hangup}loopback/park"}]}}}`, 3*time.Second)
	code, _ = resultCode(raw)
	add("Dial 拒绝 execute_on_*", err == nil && code == 400, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	cases = append(cases, channelFlow(nc)...)
	return cases
}

func channelFlow(nc *nats.Conn) []testCase {
	var cases []testCase
	add := func(name string, pass bool, detail string) {
		cases = append(cases, testCase{Name: name, Pass: pass, Detail: detail})
		mark := "FAIL"
		if pass {
			mark = "PASS"
		}
		fmt.Printf("[%s] %s %s\n", mark, name, detail)
	}

	var code int
	var raw []byte
	var err error
	owned := false
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"pre","method":"XNode.Hangup","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, ctrlUUID, unownedUUID), 3*time.Second)
		code, _ = resultCode(raw)
		if err == nil && code == 400 {
			owned = true
			break
		}
		if err == nil && code != 404 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	add("未 Accept 时控制被拒绝", owned && code == 400, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))
	if !owned {
		add("Accept / GetState / 变量 / 放音 / NativeApp", false, "没有等到未接管通道，后续通道用例跳过")
		return append(cases, dialFlow(nc)...)
	}

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"acc","method":"XNode.Accept","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("Accept 取得控制权", err == nil && code == 200, fmt.Sprintf("code=%d err=%v", code, err))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"acc2","method":"XNode.Accept","params":{"ctrl_uuid":"other-ctrl","uuid":"%s"}}`, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("再次 Accept 返回 419", err == nil && code == 419, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"obs","method":"fs.channel.observe","params":{"ctrl_uuid":"observer-1","uuid":"%s"}}`, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("Observe 不抢控制权", err == nil && code == 200, fmt.Sprintf("code=%d err=%v", code, err))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"st","method":"XNode.GetState","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, result := resultCode(raw)
	answer := ""
	if result != nil {
		if s, ok := result["answer_state"].(string); ok {
			answer = s
		}
	}
	add("GetState.answer_state 是呼叫状态", err == nil && code == 200 && answer != "" && len(answer) >= 2 && answer[:2] != "CS", fmt.Sprintf("code=%d answer_state=%s", code, answer))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"set","method":"XNode.SetVar","params":{"ctrl_uuid":"%s","uuid":"%s","data":{"nats_lab":"ok"}}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	setOK := err == nil && code == 200
	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"get","method":"XNode.GetVar","params":{"ctrl_uuid":"%s","uuid":"%s","data":["nats_lab"]}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, result = resultCode(raw)
	got := ""
	if result != nil {
		if data, ok := result["data"].(map[string]any); ok {
			got, _ = data["nats_lab"].(string)
		}
	}
	add("SetVar / GetVar", setOK && err == nil && code == 200 && got == "ok", fmt.Sprintf("value=%s code=%d", got, code))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"play","method":"XNode.Play","params":{"ctrl_uuid":"%s","uuid":"%s","media":{"file":"tone_stream://%%(200,0,440)"}}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("Play 排队成功", err == nil && code == 200, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"app","method":"XNode.NativeApp","params":{"ctrl_uuid":"%s","uuid":"%s","cmd":"playback","args":"tone_stream://%%(200,0,440)"}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("NativeApp 异步入队", err == nil && code == 200, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))

	raw, _, err = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"hu","method":"XNode.Hangup","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, ctrlUUID, unownedUUID), 3*time.Second)
	code, _ = resultCode(raw)
	add("Hangup 已接管通道", err == nil && code == 200, fmt.Sprintf("code=%d err=%v", code, err))

	return append(cases, dialFlow(nc)...)
}

func dialFlow(nc *nats.Conn) []testCase {
	var cases []testCase
	add := func(name string, pass bool, detail string) {
		cases = append(cases, testCase{Name: name, Pass: pass, Detail: detail})
		mark := "FAIL"
		if pass {
			mark = "PASS"
		}
		fmt.Printf("[%s] %s %s\n", mark, name, detail)
	}
	sub, err := nc.SubscribeSync(ctrlSubj)
	if err != nil {
		add("Dial 全链路", false, err.Error())
		return cases
	}
	defer sub.Unsubscribe()
	_ = nc.Flush()

	a := "22222222-2222-4222-8222-222222222222"
	b := "33333333-3333-4333-8333-333333333333"
	okA, detailA := dialOne(nc, sub, "dial-a", a)
	add("Dial A 回 202 且 Event.Result 成功", okA, detailA)
	okB, detailB := dialOne(nc, sub, "dial-b", b)
	add("Dial B 回 202 且 Event.Result 成功", okB, detailB)
	if okA && okB {
		raw, _, err := rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"br","method":"XNode.Bridge","params":{"ctrl_uuid":"%s","uuid":"%s","peer_uuid":"%s"}}`, ctrlUUID, a, b), 5*time.Second)
		code, _ := resultCode(raw)
		add("Bridge 两条已接管通道", err == nil && code == 200, fmt.Sprintf("code=%d err=%v body=%s", code, err, clip(raw)))
	} else {
		add("Bridge 两条已接管通道", false, "拨号未成功")
	}
	for _, id := range []string{a, b} {
		_, _, _ = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"clr","method":"XNode.Hangup","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, ctrlUUID, id), 3*time.Second)
	}
	deadline := time.Now().Add(8 * time.Second)
	sawCDR := false
	for time.Now().Before(deadline) {
		st := readStats()
		if st.CDR > 0 {
			sawCDR = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	add("消费端收到 Event.CDR", sawCDR, fmt.Sprintf("consumer cdr=%d", readStats().CDR))
	return cases
}

func dialOne(nc *nats.Conn, sub *nats.Subscription, id, uuid string) (bool, string) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"%s","method":"XNode.Dial","params":{"ctrl_uuid":"%s","timeout":20,"destination":{"call_params":[{"dial_string":"loopback/park","cid_number":"1000","cid_name":"lab","uuid":"%s"}]}}}`, id, ctrlUUID, uuid)
	raw, _, err := rpc(nc, body, 5*time.Second)
	code, result := resultCode(raw)
	if err != nil || code != 202 {
		return false, fmt.Sprintf("ack code=%d err=%v body=%s", code, err, clip(raw))
	}
	job := ""
	if result != nil {
		job, _ = result["job_uuid"].(string)
	}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			return false, "no Event.Result: " + err.Error()
		}
		var env struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(msg.Data, &env) != nil || env.Method != "Event.Result" || env.ID != id {
			continue
		}
		rc := -1
		if v, ok := env.Params["code"].(float64); ok {
			rc = int(v)
		}
		got, _ := env.Params["uuid"].(string)
		return rc == 200 && got == uuid, fmt.Sprintf("job=%s result_code=%d uuid=%s", job, rc, got)
	}
	return false, "timeout waiting Event.Result"
}

func loadJStatus(nc *nats.Conn, dur time.Duration, inflight int) latency {
	var mu sync.Mutex
	var samples []float64
	var okN, errN int
	sem := make(chan struct{}, inflight)
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)
	start := time.Now()
	for time.Now().Before(deadline) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_, lat, err := rpc(nc, `{"jsonrpc":"2.0","id":"load","method":"XNode.JStatus","params":{}}`, 3*time.Second)
			mu.Lock()
			if err != nil {
				errN++
			} else {
				okN++
				samples = append(samples, float64(lat.Microseconds())/1000)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	out := summarize(samples, okN, errN)
	if elapsed > 0 {
		out.RPS = float64(okN) / elapsed
	}
	fmt.Printf("[LOAD] JStatus n=%d ok=%d err=%d p50=%.2f p99=%.2f rps=%.0f\n", out.N, out.OK, out.Err, out.P50, out.P99, out.RPS)
	return out
}

func loadCalls(nc *nats.Conn, total, inflight int) latency {
	h := &resultHub{wait: map[string]chan map[string]any{}}
	sub, err := nc.Subscribe("nats.fs.ctrl.lab-ctrl", func(m *nats.Msg) {
		var env struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if json.Unmarshal(m.Data, &env) != nil || env.Method != "Event.Result" {
			return
		}
		h.deliver(env.ID, env.Params)
	})
	if err != nil {
		return latency{}
	}
	defer sub.Unsubscribe()
	_ = nc.Flush()
	var mu sync.Mutex
	var samples []float64
	var okN, errN int
	sem := make(chan struct{}, inflight)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < total; i++ {
		sem <- struct{}{}
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			uuid := fmt.Sprintf("44444444-4444-4444-8444-%012d", idx)
			id := fmt.Sprintf("load-%d", idx)
			ch := h.register(id)
			defer h.remove(id)
			t0 := time.Now()
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"%s","method":"XNode.Dial","params":{"ctrl_uuid":"%s","timeout":20,"destination":{"call_params":[{"dial_string":"loopback/park","cid_number":"1000","cid_name":"lab","uuid":"%s"}]}}}`, id, ctrlUUID, uuid)
			raw, _, err := rpc(nc, body, 5*time.Second)
			code, _ := resultCode(raw)
			ok := false
			if err == nil && code == 202 {
				select {
				case params := <-ch:
					rc := -1
					if v, isNum := params["code"].(float64); isNum {
						rc = int(v)
					}
					got, _ := params["uuid"].(string)
					ok = rc == 200 && got == uuid
				case <-time.After(25 * time.Second):
				}
			}
			_, _, _ = rpc(nc, fmt.Sprintf(`{"jsonrpc":"2.0","id":"lh-%d","method":"XNode.Hangup","params":{"ctrl_uuid":"%s","uuid":"%s"}}`, idx, ctrlUUID, uuid), 3*time.Second)
			mu.Lock()
			if ok {
				okN++
				samples = append(samples, float64(time.Since(t0).Microseconds())/1000)
			} else {
				errN++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	out := summarize(samples, okN, errN)
	elapsed := time.Since(start).Seconds()
	if elapsed > 0 {
		out.RPS = float64(okN) / elapsed
	}
	fmt.Printf("[LOAD] calls n=%d ok=%d err=%d p50=%.0f p99=%.0f cps=%.2f\n", out.N, out.OK, out.Err, out.P50, out.P99, out.RPS)
	return out
}

type resultHub struct {
	mu   sync.Mutex
	wait map[string]chan map[string]any
}

func (h *resultHub) register(id string) chan map[string]any {
	ch := make(chan map[string]any, 1)
	h.mu.Lock()
	h.wait[id] = ch
	h.mu.Unlock()
	return ch
}

func (h *resultHub) remove(id string) {
	h.mu.Lock()
	delete(h.wait, id)
	h.mu.Unlock()
}

func (h *resultHub) deliver(id string, params map[string]any) {
	h.mu.Lock()
	ch := h.wait[id]
	h.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- params:
	default:
	}
}

func summarize(samples []float64, okN, errN int) latency {
	sort.Float64s(samples)
	out := latency{N: okN + errN, OK: okN, Err: errN}
	if len(samples) == 0 {
		return out
	}
	out.P50 = pct(samples, 0.50)
	out.P95 = pct(samples, 0.95)
	out.P99 = pct(samples, 0.99)
	out.Max = samples[len(samples)-1]
	return out
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func readStats() stats {
	b, err := os.ReadFile(statsPath)
	if err != nil {
		return stats{}
	}
	var st stats
	_ = json.Unmarshal(b, &st)
	return st
}

func clip(b []byte) string {
	if len(b) > 180 {
		return string(b[:180])
	}
	return string(b)
}
