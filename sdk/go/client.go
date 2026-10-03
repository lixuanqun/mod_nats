// Package modnats is a Go client for the mod_nats control plane of
// FreeSWITCH (protocol 2.5.0). One Client talks to one FreeSWITCH node
// over NATS using JSON-RPC 2.0; XNode.* aliases are sent by default and
// can be switched to the canonical fs.* names.
//
// Media never flows through NATS: the SDK sends control requests and
// receives events, call results and CDRs.
package modnats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultSubjectPrefix matches the module default. Set prefix "cn.xswitch."
// to interoperate with the official xctrl SDKs.
const DefaultSubjectPrefix = "nats.fs."

// request is the JSON-RPC 2.0 envelope sent to {prefix}node.{node_uuid}.
type request struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      string      `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

// Result is the mod_nats result object every reply and Event.Result carries.
type Result struct {
	Code             int             `json:"code"`
	Message          string          `json:"message"`
	NodeUUID         string          `json:"node_uuid"`
	JobUUID          string          `json:"job_uuid,omitempty"`
	UUID             string          `json:"uuid,omitempty"`
	RequestID        string          `json:"request_id,omitempty"`
	IdempotentReplay bool            `json:"idempotent_replay,omitempty"`
	Extra            json.RawMessage `json:"-"`
}

// Error turns a non-2xx result into a Go error. Code semantics:
// 200 OK, 202 accepted, 400 refused, 404 no channel, 419 owned by
// another controller, 480 dial failed, 503 queue full.
func (r *Result) Error() error {
	if r.Code == 200 || r.Code == 202 {
		return nil
	}
	return fmt.Errorf("modnats: code=%d %s", r.Code, r.Message)
}

// Client is a per-node control connection. Create one with New or Dial.
type Client struct {
	nc          *nats.Conn
	nodeUUID    string
	prefix      string
	ctrlUUID    string
	timeout     time.Duration
	methodStyle MethodStyle
	autoIdem    bool
}

// MethodStyle selects the wire method name: XCC aliases (default, matches
// the xctrl SDKs) or the canonical fs.* names.
type MethodStyle int

const (
	// StyleXNode sends XNode.* aliases (compat-xcc must be on, default).
	StyleXNode MethodStyle = iota
	// StyleCanonical sends the fs.* canonical names.
	StyleCanonical
)

// Option configures a Client.
type Option func(*Client)

// WithPrefix overrides the subject prefix (default nats.fs.).
func WithPrefix(p string) Option { return func(c *Client) { c.prefix = p } }

// WithCtrlUUID sets the controller id stamped on every request. Required
// for anything beyond status/hello; owner-gated methods fail without it.
func WithCtrlUUID(id string) Option { return func(c *Client) { c.ctrlUUID = id } }

// WithTimeout sets the per-request NATS timeout (default 8s).
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithStyle switches the wire method names (default StyleXNode).
func WithStyle(s MethodStyle) Option { return func(c *Client) { c.methodStyle = s } }

// WithAutoIdempotency generates a random idempotency_key for every request,
// making client-side retries safe for non-idempotent methods (Play, Dial...).
func WithAutoIdempotency() Option { return func(c *Client) { c.autoIdem = true } }

// New wraps an existing NATS connection for node nodeUUID.
func New(nc *nats.Conn, nodeUUID string, opts ...Option) *Client {
	c := &Client{
		nc:       nc,
		nodeUUID: nodeUUID,
		prefix:   DefaultSubjectPrefix,
		timeout:  8 * time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Dial connects to url and returns a Client for node nodeUUID.
func Dial(url, nodeUUID string, opts ...Option) (*Client, error) {
	nc, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, err
	}
	return New(nc, nodeUUID, opts...), nil
}

// Close drains the underlying NATS connection when the SDK created it.
func (c *Client) Close() { c.nc.Close() }

// NodeSubject is the request entry point of the node.
func (c *Client) NodeSubject() string { return c.prefix + "node." + c.nodeUUID }

// CtrlSubject is this controller's mailbox (events, results).
func (c *Client) CtrlSubject() string { return c.prefix + "ctrl." + c.ctrlUUID }

func (c *Client) methodName(canonical, xnode string) string {
	if c.methodStyle == StyleCanonical || xnode == "" {
		return canonical
	}
	return xnode
}

func randToken() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Request sends a JSON-RPC request and decodes the result envelope. extra
// (optional) receives method-specific fields merged into the result (e.g.
// GetVar's "data"). The ctrl_uuid is stamped automatically; with
// WithAutoIdempotency a fresh idempotency_key is added unless params
// already carries one.
func (c *Client) Request(ctx context.Context, method string, params map[string]interface{}, extra interface{}) (*Result, error) {
	if params == nil {
		params = map[string]interface{}{}
	}
	if _, ok := params["ctrl_uuid"]; !ok && c.ctrlUUID != "" {
		params["ctrl_uuid"] = c.ctrlUUID
	}
	if _, ok := params["idempotency_key"]; !ok && c.autoIdem {
		params["idempotency_key"] = randToken()
	}
	env := request{JSONRPC: "2.0", ID: "go-" + randToken(), Method: method, Params: params}
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	msg, err := c.nc.RequestWithContext(ctx, c.NodeSubject(), payload)
	if err != nil {
		return nil, err
	}
	var reply struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      string          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, fmt.Errorf("modnats: bad reply envelope: %w", err)
	}
	if len(reply.Error) > 0 {
		return nil, fmt.Errorf("modnats: rpc error: %s", string(reply.Error))
	}
	res := &Result{}
	if err := json.Unmarshal(reply.Result, res); err != nil {
		return nil, fmt.Errorf("modnats: bad result: %w", err)
	}
	res.Extra = reply.Result
	if extra != nil {
		if err := json.Unmarshal(reply.Result, extra); err != nil {
			return res, fmt.Errorf("modnats: result decode: %w", err)
		}
	}
	return res, nil
}

// request runs Request with the caller context merged with the client
// timeout. Methods pass their ctx through; nil falls back to Background.
func (c *Client) request(ctx context.Context, method string, params map[string]interface{}, extra interface{}) (*Result, error) {
	ctx, cancel := c.requestCtx(ctx)
	defer cancel()
	return c.Request(ctx, method, params, extra)
}

// requestCtx merges the caller context with the client timeout.
func (c *Client) requestCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, c.timeout)
}
