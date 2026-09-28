/*
 * mod_nats event layer: FreeSWITCH events -> XCC Event.Channel / Event.CDR /
 * Event.Result on the bus.
 *
 * Routing:
 *  - Event.Channel goes to the broadcast subject "<prefix>event.channel.<STATE>"
 *    and, once a controller has run XNode.Accept, additionally to that
 *    controller's mailbox subject "<prefix>ctrl.<ctrl_uuid>".
 *  - Event.CDR goes to the dedicated CDR subject (JetStream stream material).
 *  - BACKGROUND_JOB results for tracked jobs (XNode.Dial) come back to the
 *    requesting controller as Event.Result.
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

void mod_nats_event_fill_channel_params(switch_event_t *event, cJSON *params)
{
	int i;

	for (i = 0; CHAN_FIELD[i][0]; i++) {
		const char *val = switch_event_get_header(event, CHAN_FIELD[i][0]);
		if (!zstr(val)) {
			cJSON_AddStringToObject(params, CHAN_FIELD[i][1], val);
		}
	}

	/* user-configured extra channel vars (whitelist) */
	if (!zstr(mod_nats_globals.channel_params)) {
		char *argv[64];
		int argc, i;
		char *list = strdup(mod_nats_globals.channel_params);
		if (list) {
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
	}

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "state", sm->xcc_state);
	mod_nats_event_fill_channel_params(event, params);

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

static void handle_background_job(switch_event_t *event)
{
	const char *job_uuid = switch_event_get_header(event, "Job-UUID");
	char rpc_id[128] = "";
	const char *ctrl;
	cJSON *result;
	int code = 200;

	if (zstr(job_uuid)) {
		return;
	}
	if (!(ctrl = mod_nats_methods_job_ctrl(job_uuid, rpc_id, sizeof(rpc_id)))) {
		return;
	}

	if (event->body && strstr(event->body, "-ERR")) {
		code = 500;
	}

	result = cJSON_CreateObject();
	cJSON_AddNumberToObject(result, "code", code);
	cJSON_AddStringToObject(result, "message", code == 200 ? "OK" : "background job failed");
	cJSON_AddStringToObject(result, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(result, "job_uuid", job_uuid);
	if (event->body) {
		cJSON_AddStringToObject(result, "data", event->body);
	}

	mod_nats_events_send_result(ctrl, rpc_id, result);
	mod_nats_methods_untrack_job(job_uuid);
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
	case SWITCH_EVENT_BACKGROUND_JOB:
		handle_background_job(event);
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
