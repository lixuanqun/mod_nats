/*
 * mod_nats event layer: FreeSWITCH events -> XCC Event.Channel / Event.CDR /
 * Event.Result on the bus.
 *
 * Routing (event-routing):
 *  - broadcast: every channel event on "<prefix>event.channel.<STATE>" only.
 *  - mailbox:   claimed channels go to the owner mailbox and to each
 *               fs.channel.observe mailbox; unclaimed calls stay public.
 *  - both:      public subject plus those mailboxes (module default).
 *  DESTROY is published before the binding is dropped, so the owner still
 *  receives the terminal event.
 *  - Event.CDR goes to the dedicated CDR subject (JetStream stream material).
 *  - XNode.Dial outcomes are delivered to the controller mailbox directly
 *    as Event.Result (correlated by the original rpc id).
 *
 * The FS event callback copies only the headers a channel event needs, then
 * enqueues. A serializer thread prints each document once and fans the string
 * out. Native events (except LOG) are still fully duplicated.
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
		char per_ch[2048];
		char global_csv[2048];
		char merged[4096];
		char *argv[96];
		int argc, i;
		char *list;

		global_csv[0] = '\0';
		if (mod_nats_globals.mutex) {
			switch_mutex_lock(mod_nats_globals.mutex);
		}
		if (!zstr(mod_nats_globals.channel_params)) {
			switch_copy_string(global_csv, mod_nats_globals.channel_params, sizeof(global_csv));
		}
		if (mod_nats_globals.mutex) {
			switch_mutex_unlock(mod_nats_globals.mutex);
		}
		mod_nats_methods_channel_params_copy(uuid, per_ch, sizeof(per_ch));
		snprintf(merged, sizeof(merged), "%s%s%s",
				 global_csv,
				 (!zstr(global_csv) && !zstr(per_ch)) ? "," : "",
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
		if (mod_nats_publish_enqueue(subject, NULL, payload) == SWITCH_STATUS_SUCCESS) {
			switch_mutex_lock(mod_nats_globals.mutex);
			mod_nats_globals.events_out++;
			switch_mutex_unlock(mod_nats_globals.mutex);
		}
		free(payload);
	}
	cJSON_Delete(env);
}

void mod_nats_events_send_result(const char *ctrl_uuid, const char *rpc_id, int rpc_id_is_number, cJSON *result)
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
		if (rpc_id_is_number) {
			cJSON_AddNumberToObject(env, "id", atof(rpc_id));
		} else {
			cJSON_AddStringToObject(env, "id", rpc_id);
		}
	}
	cJSON_AddStringToObject(env, "method", "Event.Result");
	cJSON_AddItemToObject(env, "params", result);

	payload = cJSON_PrintUnformatted(env);
	if (payload) {
		/* Control result: reply queue, so a JetStream CDR ack cannot delay it. */
		mod_nats_publish_enqueue_hdr(mod_nats_subject_ctrl(ctrl_uuid), NULL, payload, NULL);
		free(payload);
	}
	cJSON_Delete(env);
}

/* Event.OwnerLost: the owner lease elapsed and the channel is unclaimed
 * again. Delivered to the old owner's mailbox and to event.ownerlost so a
 * standby controller can re-Accept. */
static cJSON *ownerlost_params(const char *uuid, const char *ctrl_uuid)
{
	cJSON *params = cJSON_CreateObject();

	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "uuid", uuid);
	cJSON_AddStringToObject(params, "ctrl_uuid", ctrl_uuid);
	cJSON_AddNumberToObject(params, "timestamp", (double) (switch_time_now() / 1000));
	return params;
}

void mod_nats_events_publish_ownerlost(const char *uuid, const char *ctrl_uuid)
{
	if (zstr(uuid) || zstr(ctrl_uuid)) {
		return;
	}
	publish_notification(mod_nats_subject_ctrl(ctrl_uuid), "Event.OwnerLost", ownerlost_params(uuid, ctrl_uuid));
	publish_notification(mod_nats_subject_event("ownerlost"), "Event.OwnerLost", ownerlost_params(uuid, ctrl_uuid));
}

/* Serialize one notification and fan the string out to every audience
 * subject. Channel events and detected input share this path. */
static void publish_fanout(const char *method, cJSON *params, char subjects[][MOD_NATS_PREFIX_MAX + SWITCH_UUID_FORMATTED_LENGTH + 32], int nsub)
{
	cJSON *env = cJSON_CreateObject();
	char *payload;
	int i;

	cJSON_AddStringToObject(env, "jsonrpc", "2.0");
	cJSON_AddStringToObject(env, "method", method);
	cJSON_AddItemToObject(env, "params", params);

	payload = cJSON_PrintUnformatted(env);
	if (payload) {
		for (i = 0; i < nsub; i++) {
			if (mod_nats_publish_enqueue(subjects[i], NULL, payload) == SWITCH_STATUS_SUCCESS) {
				switch_mutex_lock(mod_nats_globals.mutex);
				mod_nats_globals.events_out++;
				switch_mutex_unlock(mod_nats_globals.mutex);
			}
		}
		free(payload);
	}
	cJSON_Delete(env);
}

static void copy_subject(char *dst, size_t dstlen, const char *src)
{
	if (dst && dstlen) {
		switch_copy_string(dst, switch_str_nil(src), dstlen);
	}
}

static void handle_channel_event(switch_event_t *event)
{
	state_map_t *sm;
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	cJSON *params;
	char owner[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char obs[MOD_NATS_MAX_OBSERVERS][SWITCH_UUID_FORMATTED_LENGTH + 1];
	char subjects[MOD_NATS_MAX_OBSERVERS + 2][MOD_NATS_PREFIX_MAX + SWITCH_UUID_FORMATTED_LENGTH + 32];
	int nobs;
	int nsub = 0;
	int i;
	int route;

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

	if (event->event_id == SWITCH_EVENT_CHANNEL_CREATE) {
		const char *dir = switch_event_get_header(event, "Caller-Direction");
		if (!zstr(dir) && !strcasecmp(dir, "inbound")) {
			mod_nats_methods_arm_accept_timeout(uuid);
		}
	}

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "state", sm->xcc_state);
	mod_nats_event_fill_channel_params(event, params, uuid);

	route = mod_nats_globals.event_routing;
	nobs = mod_nats_methods_copy_audience(uuid, owner, sizeof(owner), obs, MOD_NATS_MAX_OBSERVERS);
	/* subject_ctrl uses a thread-local buffer; copy each subject before the next call. */
	if (route != EVENT_ROUTE_MAILBOX || zstr(owner)) {
		snprintf(subjects[nsub], sizeof(subjects[0]), "%sevent.channel.%s", mod_nats_globals.subject_prefix, sm->xcc_state);
		nsub++;
	}
	if (route != EVENT_ROUTE_BROADCAST) {
		if (!zstr(owner)) {
			copy_subject(subjects[nsub], sizeof(subjects[0]), mod_nats_subject_ctrl(owner));
			nsub++;
		}
		for (i = 0; i < nobs; i++) {
			copy_subject(subjects[nsub], sizeof(subjects[0]), mod_nats_subject_ctrl(obs[i]));
			nsub++;
		}
	}
	if (nsub) {
		publish_fanout("Event.Channel", params, subjects, nsub);
	} else {
		cJSON_Delete(params);
	}

	if (event->event_id == SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE) {
		mod_nats_methods_unregister_channel(uuid);
	}
}

/* DTMF arrives as Event.Detected on the owner/observer mailboxes and, per
 * event-routing, the public event.detected subject. Unclaimed channels only
 * publish publicly (same rule as channel events). */
static void handle_dtmf_event(switch_event_t *event)
{
	const char *uuid = switch_event_get_header(event, "Unique-ID");
	const char *digit = switch_event_get_header(event, "DTMF-Digit");
	const char *duration = switch_event_get_header(event, "DTMF-Duration");
	cJSON *params;
	char owner[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char obs[MOD_NATS_MAX_OBSERVERS][SWITCH_UUID_FORMATTED_LENGTH + 1];
	char subjects[MOD_NATS_MAX_OBSERVERS + 2][MOD_NATS_PREFIX_MAX + SWITCH_UUID_FORMATTED_LENGTH + 32];
	int nobs;
	int nsub = 0;
	int i;
	int route;

	if (zstr(uuid) || zstr(digit)) {
		return;
	}

	params = cJSON_CreateObject();
	cJSON_AddStringToObject(params, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(params, "uuid", uuid);
	cJSON_AddStringToObject(params, "type", "dtmf");
	cJSON_AddStringToObject(params, "dtmf", digit);
	if (!zstr(duration)) {
		cJSON_AddStringToObject(params, "duration", duration);
	}

	route = mod_nats_globals.event_routing;
	nobs = mod_nats_methods_copy_audience(uuid, owner, sizeof(owner), obs, MOD_NATS_MAX_OBSERVERS);
	if (route != EVENT_ROUTE_MAILBOX || zstr(owner)) {
		snprintf(subjects[nsub], sizeof(subjects[0]), "%sevent.detected", mod_nats_globals.subject_prefix);
		nsub++;
	}
	if (route != EVENT_ROUTE_BROADCAST) {
		if (!zstr(owner)) {
			copy_subject(subjects[nsub], sizeof(subjects[0]), mod_nats_subject_ctrl(owner));
			nsub++;
		}
		for (i = 0; i < nobs; i++) {
			copy_subject(subjects[nsub], sizeof(subjects[0]), mod_nats_subject_ctrl(obs[i]));
			nsub++;
		}
	}
	if (nsub) {
		publish_fanout("Event.Detected", params, subjects, nsub);
	} else {
		cJSON_Delete(params);
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

static void dispatch_event(switch_event_t *event)
{
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
	case SWITCH_EVENT_DTMF:
		handle_dtmf_event(event);
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

/* FS event thread: copy the headers we will publish, then enqueue.
 * LOG is dropped here so a publish failure cannot feed itself. */
static int is_channel_event(switch_event_types_t id)
{
	switch (id) {
	case SWITCH_EVENT_CHANNEL_CREATE:
	case SWITCH_EVENT_CHANNEL_PROGRESS:
	case SWITCH_EVENT_CHANNEL_PROGRESS_MEDIA:
	case SWITCH_EVENT_CHANNEL_ANSWER:
	case SWITCH_EVENT_CHANNEL_BRIDGE:
	case SWITCH_EVENT_CHANNEL_UNBRIDGE:
	case SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE:
	case SWITCH_EVENT_DTMF:
		return 1;
	default:
		return 0;
	}
}

static void copy_header(switch_event_t *dst, switch_event_t *src, const char *name)
{
	const char *val = switch_event_get_header(src, name);

	if (!zstr(val)) {
		switch_event_add_header_string(dst, SWITCH_STACK_BOTTOM, name, val);
	}
}

static void copy_whitelist(switch_event_t *dst, switch_event_t *src, const char *csv)
{
	char *list;
	char *argv[96];
	int argc;
	int i;

	if (zstr(csv) || !(list = strdup(csv))) {
		return;
	}
	argc = switch_separate_string(list, ',', argv, (int) (sizeof(argv) / sizeof(argv[0])));
	for (i = 0; i < argc; i++) {
		char header[256];

		if (zstr(argv[i])) {
			continue;
		}
		copy_header(dst, src, argv[i]);
		snprintf(header, sizeof(header), "variable_%s", argv[i]);
		copy_header(dst, src, header);
	}
	free(list);
}

static switch_event_t *slim_channel_event(switch_event_t *event)
{
	switch_event_t *dup = NULL;
	char global_csv[2048];
	char per_ch[2048];
	const char *uuid;
	int i;
	static const char *cdr_keys[] = {
		"variable_duration",
		"variable_billsec",
		"variable_start_stamp",
		"variable_answer_stamp",
		"variable_end_stamp",
		NULL
	};

	if (switch_event_create(&dup, event->event_id) != SWITCH_STATUS_SUCCESS) {
		return NULL;
	}
	for (i = 0; CHAN_FIELD[i][0]; i++) {
		copy_header(dup, event, CHAN_FIELD[i][0]);
	}
	if (event->event_id == SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE) {
		for (i = 0; cdr_keys[i]; i++) {
			copy_header(dup, event, cdr_keys[i]);
		}
	}
	if (event->event_id == SWITCH_EVENT_DTMF) {
		copy_header(dup, event, "DTMF-Digit");
		copy_header(dup, event, "DTMF-Duration");
	}
	uuid = switch_event_get_header(event, "Unique-ID");
	global_csv[0] = '\0';
	if (mod_nats_globals.mutex) {
		switch_mutex_lock(mod_nats_globals.mutex);
		if (!zstr(mod_nats_globals.channel_params)) {
			switch_copy_string(global_csv, mod_nats_globals.channel_params, sizeof(global_csv));
		}
		switch_mutex_unlock(mod_nats_globals.mutex);
	}
	per_ch[0] = '\0';
	if (!zstr(uuid)) {
		mod_nats_methods_channel_params_copy(uuid, per_ch, sizeof(per_ch));
	}
	copy_whitelist(dup, event, global_csv);
	copy_whitelist(dup, event, per_ch);
	return dup;
}

static void event_handler(switch_event_t *event)
{
	switch_event_t *dup = NULL;

	if (!mod_nats_globals.running || !mod_nats_globals.enable_events || !event) {
		return;
	}
	if (event->event_id == SWITCH_EVENT_LOG) {
		return;
	}
	if (is_channel_event(event->event_id)) {
		dup = slim_channel_event(event);
	} else if (!mod_nats_globals.publish_native_events || switch_event_dup(&dup, event) != SWITCH_STATUS_SUCCESS) {
		return;
	}
	if (!dup) {
		return;
	}
	if (!mod_nats_globals.event_queue ||
		switch_queue_trypush(mod_nats_globals.event_queue, dup) != SWITCH_STATUS_SUCCESS) {
		switch_event_destroy(&dup);
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
	}
}

static void *SWITCH_THREAD_FUNC event_thread(switch_thread_t *t, void *data)
{
	while (!mod_nats_globals.event_stop) {
		void *pop = NULL;
		switch_event_t *event;
		switch_status_t st = switch_queue_pop_timeout(mod_nats_globals.event_queue, &pop, 200000);

		event = (switch_event_t *) pop;
		if (event) {
			dispatch_event(event);
			switch_event_destroy(&event);
		}
		if (mod_nats_globals.event_stop) {
			break;
		}
		if (st != SWITCH_STATUS_SUCCESS) {
			continue;
		}
	}

	{
		void *pop = NULL;
		while (switch_queue_trypop(mod_nats_globals.event_queue, &pop) == SWITCH_STATUS_SUCCESS && pop) {
			switch_event_t *event = (switch_event_t *) pop;
			dispatch_event(event);
			switch_event_destroy(&event);
		}
	}
	return NULL;
}

static const switch_event_types_t MOD_NATS_CHANNEL_EVENTS[] = {
	SWITCH_EVENT_CHANNEL_CREATE,
	SWITCH_EVENT_CHANNEL_PROGRESS,
	SWITCH_EVENT_CHANNEL_PROGRESS_MEDIA,
	SWITCH_EVENT_CHANNEL_ANSWER,
	SWITCH_EVENT_CHANNEL_BRIDGE,
	SWITCH_EVENT_CHANNEL_UNBRIDGE,
	SWITCH_EVENT_CHANNEL_HANGUP_COMPLETE,
	SWITCH_EVENT_DTMF
};

static switch_status_t bind_events(void)
{
	unsigned int i;

	if (!mod_nats_globals.enable_events) {
		return SWITCH_STATUS_SUCCESS;
	}
	if (mod_nats_globals.publish_native_events) {
		return switch_event_bind(MOD_NATS_NAME, SWITCH_EVENT_ALL, SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL);
	}
	for (i = 0; i < (sizeof(MOD_NATS_CHANNEL_EVENTS) / sizeof(MOD_NATS_CHANNEL_EVENTS[0])); i++) {
		if (switch_event_bind(MOD_NATS_NAME, MOD_NATS_CHANNEL_EVENTS[i], SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL) != SWITCH_STATUS_SUCCESS) {
			switch_event_unbind_callback(event_handler);
			return SWITCH_STATUS_GENERR;
		}
	}
	return SWITCH_STATUS_SUCCESS;
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
	switch_threadattr_t *thd_attr;
	switch_status_t status;

	if (!mod_nats_globals.event_queue) {
		return SWITCH_STATUS_GENERR;
	}
	mod_nats_globals.event_stop = SWITCH_FALSE;
	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	status = switch_thread_create(&mod_nats_globals.event_thread, thd_attr, event_thread, NULL, mod_nats_globals.pool);
	if (status != SWITCH_STATUS_SUCCESS) {
		return status;
	}
	if ((status = bind_events()) != SWITCH_STATUS_SUCCESS) {
		mod_nats_events_shutdown();
	}
	return status;
}

void mod_nats_events_unbind(void)
{
	switch_event_unbind_callback(event_handler);
}

void mod_nats_events_shutdown(void)
{
	switch_status_t st;

	mod_nats_globals.event_stop = SWITCH_TRUE;
	if (mod_nats_globals.event_queue) {
		switch_queue_interrupt_all(mod_nats_globals.event_queue);
	}
	if (mod_nats_globals.event_thread) {
		switch_thread_join(&st, mod_nats_globals.event_thread);
		mod_nats_globals.event_thread = NULL;
	}
}

void mod_nats_events_stop(void)
{
	mod_nats_events_unbind();
	mod_nats_events_shutdown();
}

void mod_nats_events_rebind(void)
{
	mod_nats_events_unbind();
	if (!mod_nats_globals.running) {
		return;
	}
	if (bind_events() != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " event rebind failed\n");
	}
}
