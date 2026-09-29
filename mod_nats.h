/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2026, All Rights Reserved.
 *
 * mod_nats: NATS message bus integration for FreeSWITCH.
 *
 * Implements an XCC-compatible (xctrl.proto) JSON-RPC control surface over
 * NATS: call control methods, channel events and CDR publishing. Replaces
 * ESL as the primary integration path for external controllers while
 * mod_event_socket stays untouched for ops tooling (fs_cli).
 */
#ifndef MOD_NATS_H
#define MOD_NATS_H

#include <switch.h>
#include <switch_cJSON.h>
#include <nats/nats.h>

#define MOD_NATS_NAME "mod_nats"
/* XCC wire protocol version implemented by this module */
#define MOD_NATS_PROTO_VERSION "2.0.0"

#define MOD_NATS_PREFIX_MAX 64
#define MOD_NATS_URLS_MAX 1024
#define MOD_NATS_DEFAULT_PREFIX "nats.fs."
/* Set prefix to "cn.xswitch." to interop with the official xctrl SDK
 * (its subject namespace is hardcoded, see README). */
#define MOD_NATS_XSWITCH_PREFIX "cn.xswitch."

/* Default capacities; all overridable from nats.conf.xml */
#define MOD_NATS_DEFAULT_PUB_QLEN 8192
#define MOD_NATS_DEFAULT_REQ_QLEN 2048
#define MOD_NATS_DEFAULT_WORKERS 4
#define MOD_NATS_DEFAULT_PUB_WAIT_MS 1000

typedef enum {
	MN_CONN_DOWN = 0,
	MN_CONN_CONNECTING,
	MN_CONN_UP
} mod_nats_conn_state_t;

/* One pending publish: fully serialized payload + destination subject */
typedef struct mod_nats_pub_s {
	char *subject;
	char *reply;					/* optional inbox for request-reply */
	char *payload;
	char *request_id;				/* optional X-Request-Id NATS header echo */
} mod_nats_pub_t;

/* One inbound request popped from the subscription callback */
typedef struct mod_nats_req_s {
	char *subject;
	char *reply;
	char *payload;
	char *request_id;				/* X-Request-Id NATS header, may be NULL */
} mod_nats_req_t;

/* Per-request context handed to every XNode.* method: lets async methods
 * (XNode.Dial) correlate the Event.Result with the original rpc id. */
typedef struct mod_nats_req_ctx_s {
	char rpc_id[128];				/* "" when the request is a notification */
	char ctrl_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char request_id[128];			/* X-Request-Id header value, "" when absent */
} mod_nats_req_ctx_t;

/* Per-channel controller binding, created by XNode.Accept */
typedef struct mod_nats_chan_s {
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char ctrl_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char *params_csv;				/* per-channel channel_params whitelist */
} mod_nats_chan_t;

struct mod_nats_globals_s {
	switch_memory_pool_t *pool;
	switch_bool_t running;

	/* configuration */
	char urls[MOD_NATS_URLS_MAX];
	char subject_prefix[MOD_NATS_PREFIX_MAX];
	char node_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char *user;
	char *password;
	char *credentials;
	switch_bool_t tls_verify;
	char *cdr_subject;				/* default "<prefix>cdr" */
	switch_bool_t enable_events;
	switch_bool_t enable_cdr;
	switch_bool_t publish_native_events;
	int metrics_interval;			/* heartbeat period in sec, 0=off */
	char *channel_params;			/* global channel var whitelist, comma separated */
	int pub_qlen;
	int req_qlen;
	int workers;

	/* runtime */
	mod_nats_conn_state_t conn_state;
	natsConnection *nc;
	natsSubscription *sub_node;
	switch_mutex_t *mutex;
	switch_mutex_t *chan_mutex;
	switch_hash_t *chan_hash;		/* uuid -> mod_nats_chan_t */
	int accept_timeout_sec;		/* unclaimed inbound hangup timer, 0=off */
	switch_bool_t compat_xcc;		/* route XNode.* aliases (default on) */
	switch_queue_t *pub_queue;
	switch_queue_t *req_queue;
	switch_thread_t *pub_thread;
	switch_thread_t *metrics_thread;
	switch_thread_t *req_threads[16];
	int req_thread_count;

	/* stats (atomic-ish under mutex) */
	uint64_t msgs_in;
	uint64_t msgs_out;
	uint64_t metrics_out;
	uint64_t msgs_dropped;
	uint64_t pub_errors;
	uint64_t events_out;
	switch_time_t started;
};

typedef struct mod_nats_globals_s mod_nats_globals_t;
extern mod_nats_globals_t mod_nats_globals;

typedef struct mod_nats_method_s {
	const char *name;				/* canonical name, e.g. "fs.channel.answer" */
	const char *xcc_alias;			/* compat alias, routed when compat-xcc is on */
	switch_status_t (*fn)(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *result);
	switch_bool_t needs_channel;	/* params.uuid must resolve to a session */
} mod_nats_method_t;
extern const mod_nats_method_t mod_nats_methods[];

/* nats_conn.c */
switch_status_t mod_nats_publish_enqueue_hdr(const char *subject, const char *reply, const char *payload, const char *request_id);
switch_status_t mod_nats_conn_start(void);
void mod_nats_conn_stop(void);
switch_status_t mod_nats_publish_enqueue(const char *subject, const char *reply, const char *payload);
const char *mod_nats_conn_state_name(void);

/* nats_proto.c */
void mod_nats_proto_handle_request(mod_nats_req_t *req);
void mod_nats_proto_send_reply(const char *reply, const char *rpc_id, cJSON *result);
void mod_nats_proto_send_reply_hdr(const char *reply, const char *rpc_id, const char *rpc_id_header, cJSON *result);
void mod_nats_proto_send_error(const char *reply, const char *rpc_id, int code, const char *message);
const char *mod_nats_subject_node(void);
const char *mod_nats_subject_ctrl(const char *ctrl_uuid);
const char *mod_nats_subject_event(const char *event_name);
const char *mod_nats_subject_cdr(void);
const char *mod_nats_subject_metrics(void);

/* nats_methods.c */
cJSON *mod_nats_methods_node_status(void);
switch_status_t mod_nats_methods_register_channel(const char *uuid, const char *ctrl_uuid, const char *params_csv);
void mod_nats_methods_unregister_channel(const char *uuid);
const char *mod_nats_methods_channel_ctrl(const char *uuid);
const char *mod_nats_methods_channel_params(const char *uuid);

/* nats_events.c */
cJSON *mod_nats_events_capabilities(void);
void mod_nats_events_publish_nodeup(void);
switch_status_t mod_nats_events_start(void);
switch_status_t mod_nats_metrics_start(void);
void mod_nats_metrics_stop(void);
void mod_nats_events_stop(void);
void mod_nats_events_send_result(const char *ctrl_uuid, const char *rpc_id, cJSON *result);
void mod_nats_event_fill_channel_params(switch_event_t *event, cJSON *params, const char *uuid);

/* mod_nats.c */
switch_status_t mod_nats_config_reload(void);

#endif /* MOD_NATS_H */
