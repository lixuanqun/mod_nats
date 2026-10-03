package modnats

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

// Event types published by mod_nats.
const (
	EventChannel   = "Event.Channel"
	EventCDR       = "Event.CDR"
	EventDetected  = "Event.Detected"
	EventMetrics   = "Event.Metrics"
	EventNodeUp    = "Event.NodeUp"
	EventOwnerLost = "Event.OwnerLost"
	EventResult    = "Event.Result"
	EventNative    = "Event.NativeEvent"
)

// Channel states carried by Event.Channel params.state.
const (
	StateStart    = "START"
	StateRinging  = "RINGING"
	StateMedia    = "MEDIA"
	StateAnswered = "ANSWERED"
	StateBridge   = "BRIDGE"
	StateUnbridge = "UNBRIDGE"
	StateDestroy  = "DESTROY"
)

// Event is the notification envelope: {"jsonrpc":"2.0","method":...,
// "params":{...}}. Decode Params into the typed structs below or into a
// map for forward compatibility.
type Event struct {
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Subject string          `json:"-"`
}

// ChannelEvent is Event.Channel: a call state transition. All fields are
// strings on the wire: the module copies them straight from FS event
// headers, so Timestamp keeps its string form (FS microsecond stamp).
type ChannelEvent struct {
	NodeUUID          string `json:"node_uuid"`
	UUID              string `json:"uuid"`
	State             string `json:"state"`
	Direction         string `json:"direction,omitempty"`
	CallerIDName      string `json:"caller_id_name,omitempty"`
	CallerIDNumber    string `json:"caller_id_number,omitempty"`
	DestinationNumber string `json:"destination_number,omitempty"`
	NetworkAddr       string `json:"network_addr,omitempty"`
	Context           string `json:"context,omitempty"`
	PeerUUID          string `json:"peer_uuid,omitempty"`
	HangupCause       string `json:"hangup_cause,omitempty"`
	Timestamp         string `json:"timestamp,omitempty"`
	// Extra holds the channel-params whitelist fields and any vars
	// submitted at Accept time.
	Extra map[string]string `json:"-"`
	Raw   json.RawMessage   `json:"-"`
}

// CDR is Event.CDR: the hangup record (core NATS, or JetStream when
// js-cdr is enabled and the stream exists).
type CDR struct {
	NodeUUID          string `json:"node_uuid"`
	UUID              string `json:"uuid"`
	CallerIDName      string `json:"caller_id_name,omitempty"`
	CallerIDNumber    string `json:"caller_id_number,omitempty"`
	DestinationNumber string `json:"destination_number,omitempty"`
	Direction         string `json:"direction,omitempty"`
	Context           string `json:"context,omitempty"`
	HangupCause       string `json:"hangup_cause,omitempty"`
	Duration          string `json:"duration,omitempty"`
	Billsec           string `json:"billsec,omitempty"`
	StartStamp        string `json:"start_stamp,omitempty"`
	AnswerStamp       string `json:"answer_stamp,omitempty"`
	EndStamp          string `json:"end_stamp,omitempty"`
}

// Detected is Event.Detected: DTMF (type=dtmf) or ASR results (type=asr).
type Detected struct {
	NodeUUID   string          `json:"node_uuid"`
	UUID       string          `json:"uuid"`
	Type       string          `json:"type"` // dtmf | asr
	DTMF       string          `json:"dtmf,omitempty"`
	Duration   string          `json:"duration,omitempty"`
	SpeechType string          `json:"speech_type,omitempty"` // detected-speech | begin-speaking | ...
	Speech     json.RawMessage `json:"speech,omitempty"`      // engine JSON result for asr
	Text       string          `json:"text,omitempty"`        // non-JSON engine body
}

// OwnerLost is Event.OwnerLost: the lease elapsed, the channel is
// unclaimed again and a standby controller can re-Accept.
type OwnerLost struct {
	NodeUUID string `json:"node_uuid"`
	UUID     string `json:"uuid"`
	CtrlUUID string `json:"ctrl_uuid"`
	// Timestamp is epoch milliseconds.
	Timestamp int64 `json:"timestamp,omitempty"`
}

// NodeStatus is Event.Metrics / XNode.JStatus data / Event.NodeUp payload.
type NodeStatus struct {
	SystemStatus  string  `json:"systemStatus,omitempty"`
	Uptime        float64 `json:"uptime,omitempty"`
	Version       string  `json:"version,omitempty"`
	Sessions      float64 `json:"sessions,omitempty"`
	SessionsPeak  float64 `json:"sessions_peak,omitempty"`
	SessionsMax   float64 `json:"sessions_max,omitempty"`
	SPS           float64 `json:"sps,omitempty"`
	SPSPeak       float64 `json:"sps_peak,omitempty"`
	NodeUUID      string  `json:"node_uuid,omitempty"`
	ProtoVersion  string  `json:"proto_version,omitempty"`
	SubjectPrefix string  `json:"subject_prefix,omitempty"`
}

// Parse decodes the event params into one of the typed structs. Supported
// methods: Event.Channel (*ChannelEvent), Event.CDR (*CDR), Event.Detected
// (*Detected), Event.OwnerLost (*OwnerLost), Event.Metrics / Event.NodeUp
// (*NodeStatus). Anything else returns the raw params.
func (e *Event) Parse() (interface{}, error) {
	switch e.Method {
	case EventChannel:
		var ce ChannelEvent
		if err := json.Unmarshal(e.Params, &ce); err != nil {
			return nil, err
		}
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(e.Params, &raw)
		ce.Raw = e.Params
		ce.Extra = map[string]string{}
		for k, v := range raw {
			var s string
			if err := json.Unmarshal(v, &s); err == nil {
				ce.Extra[k] = s
			}
		}
		return &ce, nil
	case EventCDR:
		var c CDR
		return &c, json.Unmarshal(e.Params, &c)
	case EventDetected:
		var d Detected
		return &d, json.Unmarshal(e.Params, &d)
	case EventOwnerLost:
		var o OwnerLost
		return &o, json.Unmarshal(e.Params, &o)
	case EventMetrics, EventNodeUp:
		var n NodeStatus
		return &n, json.Unmarshal(e.Params, &n)
	default:
		var raw map[string]interface{}
		return &raw, json.Unmarshal(e.Params, &raw)
	}
}

// SubscribeEvents receives every event the node publishes (NodeUp, channel
// events, CDR, metrics, native events) on {prefix}event.>.
func (c *Client) SubscribeEvents(handler func(*Event)) (*nats.Subscription, error) {
	return c.nc.Subscribe(c.prefix+"event.>", func(m *nats.Msg) {
		var env Event
		if err := json.Unmarshal(m.Data, &env); err != nil {
			return
		}
		env.Subject = m.Subject
		handler(&env)
	})
}

// SubscribeMailbox receives this controller's mailbox traffic: channel
// events for owned/observed channels and Event.Result dial outcomes. If
// the client has no ctrl_uuid the mailbox subject is not addressable and
// this returns an error.
func (c *Client) SubscribeMailbox(handler func(*Event)) (*nats.Subscription, error) {
	if c.ctrlUUID == "" {
		return nil, ErrNoCtrlUUID
	}
	return c.nc.Subscribe(c.CtrlSubject(), func(m *nats.Msg) {
		var env Event
		if err := json.Unmarshal(m.Data, &env); err != nil {
			return
		}
		env.Subject = m.Subject
		handler(&env)
	})
}

// WaitOwned waits for the first Event.Channel of uuid with state in
// states, delivered on either subscription. Utility for tests and simple
// controllers; production code usually keeps its own event loop.
func (c *Client) WaitOwned(uuid string, states ...string) (*Event, error) {
	ch := make(chan *Event, 64)
	sub1, err := c.SubscribeEvents(func(e *Event) {
		select {
		case ch <- e:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer sub1.Unsubscribe()
	var sub2 *nats.Subscription
	if c.ctrlUUID != "" {
		sub2, err = c.SubscribeMailbox(func(e *Event) {
			select {
			case ch <- e:
			default:
			}
		})
		if err != nil {
			return nil, err
		}
		defer sub2.Unsubscribe()
	}
	want := map[string]bool{}
	for _, s := range states {
		want[s] = true
	}
	for {
		e := <-ch
		if e.Method != EventChannel {
			continue
		}
		var ce ChannelEvent
		if err := json.Unmarshal(e.Params, &ce); err != nil {
			continue
		}
		if ce.UUID == uuid && want[ce.State] {
			return e, nil
		}
	}
}
