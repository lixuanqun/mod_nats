package modnats

import (
	"encoding/json"
	"testing"
)

func TestResultError(t *testing.T) {
	r := &Result{Code: 200, Message: "OK"}
	if err := r.Error(); err != nil {
		t.Fatalf("200 must not error: %v", err)
	}
	r = &Result{Code: 202, Message: "accepted"}
	if err := r.Error(); err != nil {
		t.Fatalf("202 must not error: %v", err)
	}
	r = &Result{Code: 419, Message: "channel controlled by another controller"}
	if err := r.Error(); err == nil {
		t.Fatal("419 must error")
	}
}

func TestMethodName(t *testing.T) {
	c := New(nil, "n1")
	if got := c.methodName("fs.channel.answer", "XNode.Answer"); got != "XNode.Answer" {
		t.Fatalf("default style must be XNode.*, got %s", got)
	}
	WithStyle(StyleCanonical)(c)
	if got := c.methodName("fs.channel.answer", "XNode.Answer"); got != "fs.channel.answer" {
		t.Fatalf("canonical style must be fs.*, got %s", got)
	}
	if got := c.methodName("fs.channel.observe", ""); got != "fs.channel.observe" {
		t.Fatalf("canonical fallback, got %s", got)
	}
}

func TestSubjects(t *testing.T) {
	c := New(nil, "node-1", WithPrefix("nats.fs."), WithCtrlUUID("ctrl-1"))
	if c.NodeSubject() != "nats.fs.node.node-1" {
		t.Fatalf("node subject: %s", c.NodeSubject())
	}
	if c.CtrlSubject() != "nats.fs.ctrl.ctrl-1" {
		t.Fatalf("ctrl subject: %s", c.CtrlSubject())
	}
}

func TestParseChannelEvent(t *testing.T) {
	raw := []byte(`{"node_uuid":"n1","uuid":"u1","state":"ANSWERED","caller_id_number":"1000","test_var":"hello"}`)
	e := &Event{Method: EventChannel, Params: raw}
	v, err := e.Parse()
	if err != nil {
		t.Fatal(err)
	}
	ce, ok := v.(*ChannelEvent)
	if !ok {
		t.Fatalf("type: %T", v)
	}
	if ce.UUID != "u1" || ce.State != StateAnswered || ce.CallerIDNumber != "1000" {
		t.Fatalf("fields: %+v", ce)
	}
	if ce.Extra["test_var"] != "hello" {
		t.Fatalf("extra: %v", ce.Extra)
	}
}

func TestParseDetected(t *testing.T) {
	raw := []byte(`{"node_uuid":"n1","uuid":"u1","type":"asr","speech_type":"detected-speech","speech":{"text":"hi","confidence":0.9}}`)
	e := &Event{Method: EventDetected, Params: raw}
	v, err := e.Parse()
	if err != nil {
		t.Fatal(err)
	}
	d := v.(*Detected)
	if d.Type != "asr" || d.SpeechType != "detected-speech" {
		t.Fatalf("fields: %+v", d)
	}
	var sp map[string]interface{}
	if err := json.Unmarshal(d.Speech, &sp); err != nil || sp["text"] != "hi" {
		t.Fatalf("speech: %s err=%v", d.Speech, err)
	}
}

func TestParseOwnerLost(t *testing.T) {
	raw := []byte(`{"node_uuid":"n1","uuid":"u1","ctrl_uuid":"c1","timestamp":123}`)
	e := &Event{Method: EventOwnerLost, Params: raw}
	v, err := e.Parse()
	if err != nil {
		t.Fatal(err)
	}
	o := v.(*OwnerLost)
	if o.CtrlUUID != "c1" || o.UUID != "u1" || o.Timestamp != 123 {
		t.Fatalf("fields: %+v", o)
	}
}

func TestParseUnknownIsRaw(t *testing.T) {
	e := &Event{Method: "Event.NativeEvent", Params: []byte(`{"event":{"a":"b"}}`)}
	v, err := e.Parse()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v.(*map[string]interface{}); !ok {
		t.Fatalf("raw map expected: %T", v)
	}
}
