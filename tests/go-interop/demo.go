package main

// Minimal third-party controller for mod_nats: plain nats.go speaking the
// XCC-compatible JSON-RPC protocol. Exercises JStatus, Dial, Answer, event
// consumption and CDR - the same flow a Java/Go service would use.
//
// go mod init godemo && go get github.com/nats-io/nats.go && go run demo.go

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	url     = nats.DefaultURL
	prefix  = "nats.fs."
	node    = "test-node-01"
	ctrl    = "go-ctrl-demo"
	subject = prefix + "node." + node
)

type envelope struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResult struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	NodeUUID string `json:"node_uuid"`
	JobUUID  string `json:"job_uuid,omitempty"`
	UUID     string `json:"uuid,omitempty"`
}

var (
	nc    *nats.Conn
	mu    sync.Mutex
	reqID int
	events []string
	cdrs   int
	fail   int
)

func check(name string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		fail++
	}
	fmt.Printf("  %-4s %-40s %s\n", status, name, detail)
}

func call(method string, params map[string]interface{}) *rpcResult {
	mu.Lock()
	reqID++
	id := fmt.Sprintf("go-%d", reqID)
	mu.Unlock()
	env := envelope{Jsonrpc: "2.0", ID: id, Method: method, Params: mustJSON(params)}
	msg, err := nc.Request(subject, mustJSON(env), 8*time.Second)
	if err != nil {
		return &rpcResult{Code: -1, Message: err.Error()}
	}
	var resp envelope
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return &rpcResult{Code: -2, Message: err.Error()}
	}
	var r rpcResult
	json.Unmarshal(resp.Result, &r)
	return &r
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func main() {
	var err error
	nc, err = nats.Connect(url)
	if err != nil {
		fmt.Println("connect fail:", err)
		os.Exit(1)
	}
	defer nc.Close()

	type chanEvent struct{ uuid, state string }
	var chanEvents []chanEvent
	nc.Subscribe(prefix+"event.channel.>", func(m *nats.Msg) {
		var e envelope
		json.Unmarshal(m.Data, &e)
		var p struct {
			State string `json:"state"`
			UUID  string `json:"uuid"`
		}
		json.Unmarshal(e.Params, &p)
		mu.Lock()
		chanEvents = append(chanEvents, chanEvent{p.UUID, p.State})
		events = append(events, p.UUID[:8]+" "+p.State)
		mu.Unlock()
	})
	nc.Subscribe(prefix+"event.cdr", func(m *nats.Msg) {
		mu.Lock()
		cdrs++
		mu.Unlock()
	})
	nc.Subscribe(prefix+"ctrl."+ctrl, func(m *nats.Msg) {})
	nc.Flush()

	fmt.Println("== Go controller interop ==")

	r := call("XNode.JStatus", map[string]interface{}{})
	check("XNode.JStatus", r.Code == 200, fmt.Sprintf("code=%d node=%s", r.Code, r.NodeUUID))

	dial := call("XNode.Dial", map[string]interface{}{
		"ctrl_uuid": ctrl,
		"destination": map[string]interface{}{
			"call_params": []map[string]interface{}{{
				"dial_string": "loopback/1000",
				"cid_number":  "10000210",
				"cid_name":    "GoDemo",
			}},
		},
	})
	check("XNode.Dial accepted", dial.Code == 202, fmt.Sprintf("code=%d job=%s", dial.Code, dial.JobUUID))

	// wait for the first START after our Dial, use its FULL uuid
	uuid := ""
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, ce := range chanEvents {
			if ce.state == "START" {
				uuid = ce.uuid
				break
			}
		}
		mu.Unlock()
		if uuid != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dial.UUID != "" {
		uuid = dial.UUID
	}
	if uuid == "" {
		check("channel discovered from events", false, "no START seen")
	} else {
		check("channel discovered from events", true, uuid)
		r = call("XNode.Accept", map[string]interface{}{"uuid": uuid, "ctrl_uuid": ctrl})
		// 419 here is CORRECT: the dialing controller implicitly owns the
		// b-leg since v0.1.3, so an explicit re-Accept reports conflict
		check("XNode.Accept (419 = implicit ownership)", r.Code == 200 || r.Code == 419, fmt.Sprintf("code=%d", r.Code))
		r = call("XNode.Answer", map[string]interface{}{"uuid": uuid})
		check("XNode.Answer", r.Code == 200, fmt.Sprintf("code=%d", r.Code))
		r = call("XNode.Play", map[string]interface{}{
			"uuid": uuid, "media": map[string]interface{}{"type": "FILE", "file": "tone_stream://%(1000,0,440)"},
		})
		check("XNode.Play tone", r.Code == 200, fmt.Sprintf("code=%d", r.Code))
		time.Sleep(1 * time.Second)
		r = call("XNode.Hangup", map[string]interface{}{"uuid": uuid, "cause": "NORMAL_CLEARING"})
		check("XNode.Hangup", r.Code == 200, fmt.Sprintf("code=%d", r.Code))
	}

	time.Sleep(2 * time.Second)
	mu.Lock()
	states := events
	gotCDR := cdrs
	mu.Unlock()
	fmt.Printf("  events observed: %v\n", states)
	check("channel events received", len(states) >= 2, fmt.Sprintf("%d events", len(states)))
	check("Event.CDR received", gotCDR >= 1, fmt.Sprintf("%d cdrs", gotCDR))

	if fail > 0 {
		fmt.Printf("RESULT: FAIL (%d)\n", fail)
		os.Exit(1)
	}
	fmt.Println("RESULT: ALL PASS")
}
