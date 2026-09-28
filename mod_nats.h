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
#define MOD_NATS_PROTO_VERSION "1.0.0"

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
} mod_nats_pub_t;

/* One inbound request popped from the subscription callback */
typedef struct mod_nats_req_s {
	char *subject;
	char *reply;
	char *payload;
} mod_nats_req_t;

/* Per-channel controller binding, created by XNode.Accept */
typedef struct mod_nats_chan_s {
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char ctrl_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	switch_hash_index_t *params;	/* reserved: per-channel channel_params */
} mod_nats_chan_t;

/* bgapi job -> ctrl correlation, used to route Event.Result */
typedef struct mod_nats_job_s {
	char job_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char ctrl_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char rpc_id[128];
} mod_nats_job_t;

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
	switch_hash_t *job_hash;		/* job-uuid -> mod_nats_job_t */
	switch_queue_t *pub_queue;
	switch_queue_t *req_queue;
	switch_thread_t *pub_thread;
	switch_thread_t *req_threads[16];
	int req_thread_count;

	/* stats (atomic-ish under mutex) */
	uint64_t msgs_in;
	uint64_t msgs_out;
	uint64_t msgs_dropped;
	uint64_t pub_errors;
	uint64_t events_out;
	switch_time_t started;
};

typedef struct mod_nats_globals_s mod_nats_globals_t;
extern mod_nats_globals_t mod_nats_globals;

typedef struct mod_nats_method_s {
	const char *name;				/* e.g. "XNode.Answer" */
	switch_status_t (*fn)(cJSON *params, cJSON *result);
	switch_bool_t needs_channel;	/* params.uuid must resolve to a session */
} mod_nats_method_t;
extern const mod_nats_method_t mod_nats_methods[];

/* nats_conn.c */
switch_status_t mod_nats_conn_start(void);
void mod_nats_conn_stop(void);
switch_status_t mod_nats_publish_enqueue(const char *subject, const char *reply, const char *payload);
const char *mod_nats_conn_state_name(void);

/* nats_proto.c */
void mod_nats_proto_handle_request(mod_nats_req_t *req);
void mod_nats_proto_send_reply(const char *reply, const char *rpc_id, cJSON *result);
void mod_nats_proto_send_error(const char *reply, const char *rpc_id, int code, const char *message);
const char *mod_nats_subject_node(void);
const char *mod_nats_subject_ctrl(const char *ctrl_uuid);
const char *mod_nats_subject_event(const char *event_name);
const char *mod_nats_subject_cdr(void);

/* nats_methods.c */
switch_status_t mod_nats_methods_register_channel(const char *uuid, const char *ctrl_uuid);
void mod_nats_methods_unregister_channel(const char *uuid);
const char *mod_nats_methods_channel_ctrl(const char *uuid);
switch_status_t mod_nats_methods_track_job(const char *job_uuid, const char *ctrl_uuid, const char *rpc_id);
void mod_nats_methods_untrack_job(const char *job_uuid);
const char *mod_nats_methods_job_ctrl(const char *job_uuid, char *rpc_id, size_t rpc_id_len);

/* nats_events.c */
switch_status_t mod_nats_events_start(void);
void mod_nats_events_stop(void);
void mod_nats_events_send_result(const char *ctrl_uuid, const char *rpc_id, cJSON *result);
void mod_nats_event_fill_channel_params(switch_event_t *event, cJSON *params);

/* mod_nats.c */
switch_status_t mod_nats_config_reload(void);

#endif /* MOD_NATS_H */
