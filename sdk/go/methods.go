package modnats

import (
	"context"
	"errors"
)

// ErrNoCtrlUUID is returned when a mailbox operation needs WithCtrlUUID.
var ErrNoCtrlUUID = errors.New("modnats: ctrl_uuid not set (WithCtrlUUID)")

// Hello returns the node capabilities (methods, proto version, prefix).
func (c *Client) Hello(ctx context.Context) (*Result, *NodeStatus, error) {
	var data NodeStatus
	res, err := c.request("fs.node.hello", nil, &data)
	return res, &data, err
}

// Status returns node status (sessions, sps, version, system metrics).
func (c *Client) Status(ctx context.Context) (*Result, *NodeStatus, error) {
	var data NodeStatus
	res, err := c.request(c.methodName("fs.node.status", "XNode.JStatus"), nil, &data)
	return res, &data, err
}

// Accept takes ownership of a channel. Re-Accept by the same controller is
// idempotent and renews the lease; another controller gets 419.
func (c *Client) Accept(ctx context.Context, uuid string, channelParams ...string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid}
	if len(channelParams) > 0 {
		p["channel_params"] = channelParams
	}
	return c.request(c.methodName("fs.channel.accept", "XNode.Accept"), p, nil)
}

// Touch renews the ownership lease without any other action.
func (c *Client) Touch(ctx context.Context, uuid string) (*Result, error) {
	return c.request(c.methodName("fs.channel.touch", "XNode.Touch"), map[string]interface{}{"uuid": uuid}, nil)
}

// Observe subscribes ctrl to the channel mailbox without taking ownership.
func (c *Client) Observe(ctx context.Context, uuid string) (*Result, error) {
	return c.request("fs.channel.observe", map[string]interface{}{"uuid": uuid}, nil)
}

// Unobserve removes the mailbox subscription.
func (c *Client) Unobserve(ctx context.Context, uuid string) (*Result, error) {
	return c.request("fs.channel.unobserve", map[string]interface{}{"uuid": uuid}, nil)
}

// Answer answers the channel.
func (c *Client) Answer(ctx context.Context, uuid string) (*Result, error) {
	return c.request(c.methodName("fs.channel.answer", "XNode.Answer"), map[string]interface{}{"uuid": uuid}, nil)
}

// Hangup hangs the channel up with an optional cause name.
func (c *Client) Hangup(ctx context.Context, uuid, cause string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid}
	if cause != "" {
		p["cause"] = cause
	}
	return c.request(c.methodName("fs.channel.hangup", "XNode.Hangup"), p, nil)
}

// Play plays a file (or tone_stream / any FS media string).
func (c *Client) Play(ctx context.Context, uuid, file string) (*Result, error) {
	return c.request(c.methodName("fs.channel.play", "XNode.Play"),
		map[string]interface{}{"uuid": uuid, "media": map[string]string{"file": file}}, nil)
}

// Stop cancels running playback.
func (c *Client) Stop(ctx context.Context, uuid string) (*Result, error) {
	return c.request(c.methodName("fs.channel.stop", "XNode.Stop"), map[string]interface{}{"uuid": uuid}, nil)
}

// Broadcast plays a file without interrupting the current application.
func (c *Client) Broadcast(ctx context.Context, uuid, file string) (*Result, error) {
	return c.request(c.methodName("fs.channel.broadcast", "XNode.Broadcast"),
		map[string]interface{}{"uuid": uuid, "file": file}, nil)
}

// Bridge bridges two channels owned by this controller.
func (c *Client) Bridge(ctx context.Context, uuid, peerUUID string) (*Result, error) {
	return c.request(c.methodName("fs.channel.bridge", "XNode.Bridge"),
		map[string]interface{}{"uuid": uuid, "peer_uuid": peerUUID}, nil)
}

// Record actions.
const (
	RecordStart  = "RECORD"
	RecordStop   = "STOP"
	RecordPause  = "PAUSE"
	RecordResume = "RESUME"
	RecordMask   = "MASK"
	RecordUnmask = "UNMASK"
)

// Record controls a session recording. file is the recording key: STOP and
// friends must repeat the same path used at start.
func (c *Client) Record(ctx context.Context, uuid, action, file string, limitSec int) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid, "action": action, "file": file}
	if limitSec > 0 {
		p["limit"] = limitSec
	}
	return c.request(c.methodName("fs.channel.record", "XNode.Record"), p, nil)
}

// DetectSpeech starts background ASR (action "" means START). The engine
// is any loaded switch_asr_interface module (pocketsphinx, unimrcp:profile,
// test...). Results arrive as Event.Detected with Type=="asr".
func (c *Client) DetectSpeech(ctx context.Context, uuid, action, engine, grammar string, engineParams map[string]string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid}
	if action != "" {
		p["action"] = action
	}
	if engine != "" {
		p["engine"] = engine
	}
	if grammar != "" {
		p["grammar"] = grammar
	}
	if len(engineParams) > 0 {
		p["params"] = engineParams
	}
	return c.request(c.methodName("fs.channel.detectspeech", "XNode.DetectSpeech"), p, nil)
}

// Dial places an outbound call. The result is the 202 ack carrying
// job_uuid; the final outcome arrives as Event.Result on the mailbox. A
// client-chosen uuid makes retries safe on its own; the module reserves it
// as origination_uuid before dialing. With WithAutoIdempotency a repeated
// Dial replays the original 202.
func (c *Client) Dial(ctx context.Context, uuid, dialString, cidNumber, cidName string, timeoutSec int, globalParams map[string]string) (*Result, error) {
	cp := map[string]interface{}{"dial_string": dialString}
	if uuid != "" {
		cp["uuid"] = uuid
	}
	if cidNumber != "" {
		cp["cid_number"] = cidNumber
	}
	if cidName != "" {
		cp["cid_name"] = cidName
	}
	p := map[string]interface{}{"destination": map[string]interface{}{"call_params": []interface{}{cp}}}
	if timeoutSec > 0 {
		p["timeout"] = timeoutSec
	}
	if len(globalParams) > 0 {
		p["global_params"] = globalParams
	}
	return c.request(c.methodName("fs.channel.dial", "XNode.Dial"), p, nil)
}

// Transfer blind-transfers the channel to a dialplan extension.
func (c *Client) Transfer(ctx context.Context, uuid, dest, dialplan, dialContext string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid, "dest": dest}
	if dialplan != "" {
		p["dialplan"] = dialplan
	}
	if dialContext != "" {
		p["context"] = dialContext
	}
	return c.request(c.methodName("fs.channel.transfer", "XNode.Transfer"), p, nil)
}

// Hold holds (action "" or "HOLD") or unholds ("UNHOLD") the channel.
func (c *Client) Hold(ctx context.Context, uuid, action string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid}
	if action != "" {
		p["action"] = action
	}
	return c.request(c.methodName("fs.channel.hold", "XNode.Hold"), p, nil)
}

// Mute mutes (mute=true) or unmutes (mute=false) media; level is
// read|write|both.
func (c *Client) Mute(ctx context.Context, uuid string, mute bool, level string) (*Result, error) {
	return c.request(c.methodName("fs.channel.mute", "XNode.Mute"),
		map[string]interface{}{"uuid": uuid, "mute": mute, "level": level}, nil)
}

// ThreeWay brings bUUID into the call; the current partner is held
// silently and remembered for UnBridge2.
func (c *Client) ThreeWay(ctx context.Context, uuid, bUUID string) (*Result, error) {
	return c.request(c.methodName("fs.channel.threeway", "XNode.ThreeWay"),
		map[string]interface{}{"uuid": uuid, "b_uuid": bUUID}, nil)
}

// UnBridge2 splits the call: both legs are transferred to dest (typically
// an answer+park extension) and the peer threeway set aside is unheld.
func (c *Client) UnBridge2(ctx context.Context, uuid, bUUID, dest, dialContext string) (*Result, error) {
	p := map[string]interface{}{"uuid": uuid, "b_uuid": bUUID, "dest": dest}
	if dialContext != "" {
		p["context"] = dialContext
	}
	return c.request(c.methodName("fs.channel.unbridge2", "XNode.UnBridge2"), p, nil)
}

// SetVar writes channel variables.
func (c *Client) SetVar(ctx context.Context, uuid string, data map[string]string) (*Result, error) {
	return c.request(c.methodName("fs.channel.setvar", "XNode.SetVar"),
		map[string]interface{}{"uuid": uuid, "data": data}, nil)
}

// GetVar reads channel variables; empty keys returns the default set.
func (c *Client) GetVar(ctx context.Context, uuid string, keys ...string) (*Result, map[string]string, error) {
	var data struct {
		Data map[string]string `json:"data"`
	}
	p := map[string]interface{}{"uuid": uuid}
	if len(keys) > 0 {
		p["data"] = keys
	}
	res, err := c.request(c.methodName("fs.channel.getvar", "XNode.GetVar"), p, &data)
	return res, data.Data, err
}

// GetState returns the channel state and answer state.
func (c *Client) GetState(ctx context.Context, uuid string) (*Result, string, string, error) {
	var data struct {
		State      string `json:"state"`
		AnswerState string `json:"answer_state"`
	}
	res, err := c.request(c.methodName("fs.channel.getstate", "XNode.GetState"), map[string]interface{}{"uuid": uuid}, &data)
	return res, data.State, data.AnswerState, err
}

// GetChannelData returns the standard channel data fields.
func (c *Client) GetChannelData(ctx context.Context, uuid string) (*Result, map[string]interface{}, error) {
	var data struct {
		Data map[string]interface{} `json:"data"`
	}
	res, err := c.request(c.methodName("fs.channel.data", "XNode.GetChannelData"), map[string]interface{}{"uuid": uuid}, &data)
	return res, data.Data, err
}

// NativeApp queues a dialplan application on the channel session. The 200
// means queued, not finished.
func (c *Client) NativeApp(ctx context.Context, uuid, cmd, args string) (*Result, error) {
	return c.request(c.methodName("fs.native.app", "XNode.NativeApp"),
		map[string]interface{}{"uuid": uuid, "cmd": cmd, "args": args}, nil)
}
