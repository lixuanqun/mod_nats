/*
 * mod_nats event layer: FreeSWITCH events -> XCC Event.Channel / Event.CDR /
 * Event.Result on the bus.
 *
 * Routing:
 *  - Event.Channel goes to the broadcast subject "<prefix>event.channel.<STATE>"
 *    and, once a controller has run XNode.Accept, additionally to that
 *    controller's mailbox subject "<prefix>ctrl.<ctrl_uuid>".
 *  - Event.CDR goes to the dedicated CDR subject (JetStream stream material).
 *  - XNode.Dial outcomes are delivered to the controller mailbox directly
 *    as Event.Result (correlated by the original rpc id).
 *
 * The event handler runs on an FS core thread: it only serializes and
 * enqueues (bounded, non-blocking) - never does socket I/O here.
 */
#include "mod_nats.h"
#include <ctype.h>

typedef struct state_map_s {
	switch_event_types_t id;
	const char *xcc_state;
} state_map_t;

static state_map_t STATE_MAP[] = {
	{SWITCH_EVENT_CHANNEL_CREATE, "START"},
	{SWITCH_EVENT_CHANNEL_PROGRESS, "RINGING"},
	{SWITCH_EVENT_CHANNEL_PROGRESS_MEDIA, "MEDIA"},
	{SWITCH_EVENT_CHANNEL_ANSWER, "ANSWERED"},
	{SWITCH_EVENT_CHANNEL_BRIDGE, "BRIDGE"},
	{SWITCH_EVENT_CHANNEL_UNBRIDGE, "UNBRIDGE"},
	{SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE, "DESTROY"},
	{SWITCH_EVENT_ALL, NULL}
};

/* event header name -> XCC ChannelEvent field */
static const char *CHAN_FIELD[][2] = {
	{"Unique-ID", "uuid"},
	{"Caller-Direction", "direction"},
	{"Caller-Caller-ID-Name", "caller_id_name"},
	{"Caller-Caller-ID-Number", "caller_id_number"},
	{"Caller-Destination-Number", "destination_number"},
	{"Caller-Network-Addr", "network_addr"},
	{"Caller-Context", "context"},
	{"Other-Side-Unique-ID", "peer_uuid"},
	{"Hangup-Cause", "hangup_cause"},
	{"Event-Date-Timestamp", "timestamp"},
	{NULL, NULL}
};

void mod_nats_event_fill_channel_params(switch_event_t *event, cJSON *params, const char *uuid)
{
	int i;

	for (i = 0; CHAN_FIELD[i][0]; i++) {
		const char *val = switch_event_get_header(event, CHAN_FIELD[i][0]);
		if (!zstr(val)) {
			cJSON_AddStringToObject(params, CHAN_FIELD[i][1], val);
		}
	}

	/* channel var whitelist: global config + per-channel (Accept) overlay */
	{
		const char *per_ch = mod_nats_methods_channel_params(uuid);
		char merged[4096];
		char *argv[96];
		int argc, i;
		char *list;

		snprintf(merged, sizeof(merged), "%s%s%s",
				 switch_str_nil(mod_nats_globals.channel_params),
				 (!zstr(mod_nats_globals.channel_params) && !zstr(per_ch)) ? "," : "",
				 switch_str_nil(per_ch));
		if (!zstr(merged) && (list = strdup(merged)) != NULL) {
			argc = switch_separate_string(list, ',', argv, (int) (sizeof(argv) / sizeof(argv[0])));
			for (i = 0; i < argc; i++) {
				char header[256];
				const char *val;
				if (zstr(argv[i])) {
					continue;
				}
				snprintf(header, sizeof(header), "variable_%s", argv[i]);
				val = switch_event_get_header(event, header);
				if (zstr(val)) {
					val = switch_event_get_header(event, argv[i]);
				}
				if (!zstr(val)) {
					cJSON_AddStringToObject(params, argv[i], val);
				}
			}
			free(list);
		}
	}
}

static void publish_notification(const char *subject, const char *method, cJSON *params)
{
	cJSON *env = cJSON_CreateObject();
	char *payload;

	cJSON_AddStringToObject(env, "jsonrpc", "2.0");
	cJSON_AddStringToObject(env, "method", method);
	cJSON_AddItemToObject(env, "params", params);

	payload = cJSON_PrintUnformatted(env);
	if (payload) {
		mod_nats_publish_enqueue(subject, NULL, payload);
		free(payload);
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.events_out++;
		switch_mutex_unlock(mod_nats_globals.mutex);
	}
	cJSON_Delete(env);
}

void mod_nats_events_send_result(const char *ctrl_uuid, const char *rpc_id, cJSON *result)
{
	cJSON *env;
	char *payload;

	if (zstr(ctrl_uuid) || !result) {
		if (result) cJSON_Delete(result);
		return;
	}

	env = cJSON_CreateObject();
	cJSON_AddStringToObject(env, "jsonrpc", "2.0");
	if (!zstr(rpc_id)) {
		cJSON_AddStringToObject(env, "id", rpc_id);
	}
	cJSON_AddStringToObject(env, "method", "Event.Result");
	cJSON_AddItemToObject(env, "params", result);

	payload = cJSON_PrintUnformatted(env);
	if (payload) {
		mod_nats_publish_enqueue(mod_nats_subject_ctrl(ctrl_uuid), NULL, payload);
		free(payload);
	}
	cJSON_Delete(env);
}

/* one-shot: hang up an inbound channel nobody Accepted within the timeout */
static void accept_timeout_task(switch_scheduler_task_t *task)
{
	char *uuid = (char *) task->cmd_arg;

	if (!zstr(uuid) && zstr(mod_nats_methods_channel_ctrl(uuid))) {
		switch_core_session_t *s = switch_core_session_locate(uuid);
		if (s) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
							  MOD_NATS_NAME " accept timeout, hanging up %s", uuid);
			switch_channel_hangup(switch_core_session_get_channel(s), SWITCH_CAUSE_NO_ANSWER);
			switch_core_session_rwunlock(s);
		}
	}
	switch_safe_free(uuid);
}

static void handle_channel_event(switch_event_t *event)
{
	state_map_t *sm;
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	cJSON *params;

	if (zstr(uuid)) {
		return;
	}

	for (sm = STATE_MAP; sm->xcc_state; sm++) {
		if (sm->id == event->event_id) {
			break;
		}
	}
	if (!sm->xcc_state) {
		return;
	}

	if (event->event_id == SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE) {
		/* channel is gone: release the controller binding */
		mod_nats_methods_unregister_channel(uuid);
	} else if (event->event_id == SWITCH_EVENT_CHANNEL_CREATE && mod_nats_globals.accept_timeout_sec > 0) {
		const char *dir = switch_event_get_header(event, "Caller-Direction");
		if (!zstr(dir) && !strcasecmp(dir, "inbound") && zstr(mod_nats_methods_channel_ctrl(uuid))) {
			time_t when = switch_epoch_time_now(NULL) + mod_nats_globals.accept_timeout_sec;
			switch_scheduler_add_task(when, accept_timeout_task, "mod_nats_accept_timeout",
									   uuid, 0, strdup(uuid), SSHF_NONE);
		}
	}

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "state", sm->xcc_state);
	mod_nats_event_fill_channel_params(event, params, uuid);

	{
		char subject[MOD_NATS_PREFIX_MAX + 64];
		snprintf(subject, sizeof(subject), "%sevent.channel.%s", mod_nats_globals.subject_prefix, sm->xcc_state);
		publish_notification(subject, "Event.Channel", cJSON_Duplicate(params, 1));
	}

	{
		const char *ctrl = mod_nats_methods_channel_ctrl(uuid);
		if (!zstr(ctrl)) {
			publish_notification(mod_nats_subject_ctrl(ctrl), "Event.Channel", params);
		} else {
			cJSON_Delete(params);
		}
	}
}

/* v0.1: FS core no longer fires a dedicated CDR event; synthesize one from
 * CHANNEL_HANGUP_COMPLETE headers. Full CDRs belong to JetStream consumers
 * via mod_cdr-style modules later. */
static void handle_cdr_event(switch_event_t *event)
{
	cJSON *params, *cdr;
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	static const char *keys[][2] = {
		{"Unique-ID", "uuid"},
		{"Caller-Caller-ID-Name", "caller_id_name"},
		{"Caller-Caller-ID-Number", "caller_id_number"},
		{"Caller-Destination-Number", "destination_number"},
		{"Caller-Direction", "direction"},
		{"Caller-Context", "context"},
		{"Hangup-Cause", "hangup_cause"},
		{"variable_duration", "duration"},
		{"variable_billsec", "billsec"},
		{"variable_start_stamp", "start_stamp"},
		{"variable_answer_stamp", "answer_stamp"},
		{"variable_end_stamp", "end_stamp"},
		{NULL, NULL}
	};
	int i;

	if (!mod_nats_globals.enable_cdr) {
		return;
	}

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "uuid", switch_str_nil(uuid));

	cdr = cJSON_CreateObject();
	for (i = 0; keys[i][0]; i++) {
		const char *val = switch_event_get_header(event, keys[i][0]);
		if (!zstr(val)) {
			cJSON_AddStringToObject(cdr, keys[i][1], val);
		}
	}
	cJSON_AddItemToObject(params, "cdr", cdr);

	publish_notification(mod_nats_subject_cdr(), "Event.CDR", params);
}

static void handle_native_event(switch_event_t *event)
{
	cJSON *params;
	const char *ename = switch_event_name(event->event_id);
	char lower[128] = "";
	int i;

	if (!mod_nats_globals.publish_native_events || zstr(ename)) {
		return;
	}
	for (i = 0; ename[i] && i < (int) sizeof(lower) - 1; i++) {
		lower[i] = (char) tolower((unsigned char) ename[i]);
	}
	lower[i] = '\0';

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	{
		cJSON *ev = cJSON_CreateObject();
		switch_event_header_t *hp;
		for (hp = event->headers; hp; hp = hp->next) {
			cJSON_AddStringToObject(ev, hp->name, hp->value);
		}
		cJSON_AddItemToObject(params, "event", ev);
	}

	publish_notification(mod_nats_subject_event(lower), "Event.NativeEvent", params);
}

static void event_handler(switch_event_t *event)
{
	if (!mod_nats_globals.running || !mod_nats_globals.enable_events) {
		return;
	}

	switch (event->event_id) {
	case SWITCH_EVENT_CHANNEL_CREATE:
	case SWITCH_EVENT_CHANNEL_PROGRESS:
	case SWITCH_EVENT_CHANNEL_PROGRESS_MEDIA:
	case SWITCH_EVENT_CHANNEL_ANSWER:
	case SWITCH_EVENT_CHANNEL_BRIDGE:
	case SWITCH_EVENT_CHANNEL_UNBRIDGE:
	case SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE:
	case SWITCH_EVENT_CHANNEL_DESTROY:
		if (event->event_id != SWITCH_EVENT_CHANNEL_DESTROY) {
			handle_channel_event(event);
			if (event->event_id == SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE) {
				handle_cdr_event(event);
			}
		}
		break;
	case SWITCH_EVENT_CUSTOM:
	case SWITCH_EVENT_ALL:
		handle_native_event(event);
		break;
	default:
		handle_native_event(event);
		break;
	}
}

/* metrics heartbeat: publishes JStatus-equivalent node status on
 * <prefix>metrics every metrics_interval seconds so registries/monitors
 * can consume push-style instead of polling XNode.JStatus. */
static void *SWITCH_THREAD_FUNC metrics_thread(switch_thread_t *t, void *data)
{
	int tick = 0;

	while (mod_nats_globals.running) {
		if (mod_nats_globals.metrics_interval > 0 && ++tick >= mod_nats_globals.metrics_interval) {
			tick = 0;
			publish_notification(mod_nats_subject_metrics(), "Event.Metrics", mod_nats_methods_node_status());
			switch_mutex_lock(mod_nats_globals.mutex);
			mod_nats_globals.metrics_out++;
			switch_mutex_unlock(mod_nats_globals.mutex);
		}
		/* 1s granularity so shutdown joins never wait a full period */
	switch_sleep(1000000);
	}
	return NULL;
}

switch_status_t mod_nats_metrics_start(void)
{
	switch_threadattr_t *thd_attr;

	if (mod_nats_globals.metrics_interval <= 0) {
		return SWITCH_STATUS_SUCCESS;
	}
	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	/* joinable: the shutdown path joins this thread */
	return switch_thread_create(&mod_nats_globals.metrics_thread, thd_attr, metrics_thread, NULL, mod_nats_globals.pool);
}

void mod_nats_metrics_stop(void)
{
	switch_status_t st;
	if (mod_nats_globals.metrics_thread) {
		switch_thread_join(&st, mod_nats_globals.metrics_thread);
		mod_nats_globals.metrics_thread = NULL;
	}
}

/* Event.NodeUp: announce this node and its capabilities; published at
 * module load (and every reload) so registries can track node liveness
 * together with the {prefix}metrics heartbeat. */
void mod_nats_events_publish_nodeup(void)
{
	cJSON *params = mod_nats_events_capabilities();
	publish_notification(mod_nats_subject_event("nodeup"), "Event.NodeUp", params);
}

switch_status_t mod_nats_events_start(void)
{
	if (switch_event_bind(MOD_NATS_NAME, SWITCH_EVENT_ALL, SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_GENERR;
	}
	return SWITCH_STATUS_SUCCESS;
}

void mod_nats_events_stop(void)
{
	switch_event_unbind_callback(event_handler);
}
