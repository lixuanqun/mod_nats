/*
 * mod_nats method layer: XNode.* call control methods.
 *
 * Conventions:
 *  - Channel methods receive params.uuid and return:
 *      SWITCH_STATUS_SUCCESS -> code 200
 *      SWITCH_STATUS_NOTFOUND -> code 404 (no such channel)
 *      SWITCH_STATUS_FALSE -> code 400 (refused)
 *      SWITCH_STATUS_NOTIMPL -> code 501
 *  - Methods may add fields (e.g. code 202, job_uuid) into the `extra`
 *    result object; they override the defaults set by the protocol layer.
 */
#include "mod_nats.h"

/* ---------------------------------------------------------------------- */
/* channel <-> controller bindings (XNode.Accept)                         */

switch_status_t mod_nats_methods_register_channel(const char *uuid, const char *ctrl_uuid, const char *params_csv)
{
	mod_nats_chan_t *chan;

	if (zstr(uuid) || zstr(ctrl_uuid)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	if (switch_core_hash_find(mod_nats_globals.chan_hash, uuid)) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_FALSE;	/* already owned: caller gets 400/419 */
	}
	chan = (mod_nats_chan_t *) switch_core_alloc(mod_nats_globals.pool, sizeof(*chan));
	switch_copy_string(chan->uuid, uuid, sizeof(chan->uuid));
	switch_copy_string(chan->ctrl_uuid, ctrl_uuid, sizeof(chan->ctrl_uuid));
	if (!zstr(params_csv)) {
		chan->params_csv = switch_core_strdup(mod_nats_globals.pool, params_csv);
	}
	switch_core_hash_insert(mod_nats_globals.chan_hash, uuid, chan);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);

	return SWITCH_STATUS_SUCCESS;
}

/* pool-owned csv of per-channel channel_params; valid until module unload */
const char *mod_nats_methods_channel_params(const char *uuid)
{
	mod_nats_chan_t *chan;
	const char *csv = NULL;

	if (zstr(uuid)) return NULL;
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	if ((chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid))) {
		csv = chan->params_csv;
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return csv;
}

void mod_nats_methods_unregister_channel(const char *uuid)
{
	if (zstr(uuid)) return;
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	switch_core_hash_delete(mod_nats_globals.chan_hash, uuid);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

const char *mod_nats_methods_channel_ctrl(const char *uuid)
{
	mod_nats_chan_t *chan;
	const char *ctrl = NULL;

	if (zstr(uuid)) return NULL;
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	if ((chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid))) {
		ctrl = chan->ctrl_uuid;
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return ctrl;
}

/* ---------------------------------------------------------------------- */
/* helpers                                                                */

static switch_core_session_t *session_from_params(cJSON *params, char *uuid_out, size_t uuid_len)
{
	cJSON *juuid = cJSON_GetObjectItem(params, "uuid");

	if (uuid_out && uuid_len) {
		uuid_out[0] = '\0';
	}
	if (!juuid || !cJSON_IsString(juuid) || zstr(juuid->valuestring)) {
		return NULL;
	}
	if (uuid_out && uuid_len) {
		switch_copy_string(uuid_out, juuid->valuestring, uuid_len);
	}
	return switch_core_session_locate(juuid->valuestring);
}

/* run a dialplan application on a channel without a session thread of ours */
static switch_status_t exec_app(switch_core_session_t *session, const char *app, const char *arg)
{
	switch_application_interface_t *app_interface = switch_loadable_module_get_application_interface(app);

	if (!app_interface) {
		return SWITCH_STATUS_NOTIMPL;
	}
	switch_core_session_exec(session, app_interface, arg);
	UNPROTECT_INTERFACE(app_interface);
	return SWITCH_STATUS_SUCCESS;
}

/* ---------------------------------------------------------------------- */
/* XNode.* methods                                                        */

static switch_status_t mn_accept(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_status_t st;
	cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];

	if (!jctrl || !cJSON_IsString(jctrl) || zstr(jctrl->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, uuid, sizeof(uuid)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	switch_core_session_rwunlock(session);

	{
		/* XCC: callers may subscribe extra channel variables at takeover time */
		char csv[2048] = "";
		size_t l;
		cJSON *jp = cJSON_GetObjectItem(params, "channel_params");
		if (jp && cJSON_IsArray(jp)) {
			cJSON *it;
			cJSON_ArrayForEach(it, jp) {
				if (cJSON_IsString(it) && !zstr(it->valuestring)) {
					l = strlen(csv);
					snprintf(csv + l, sizeof(csv) - l, "%s%s", l ? "," : "", it->valuestring);
				}
			}
		}
		st = mod_nats_methods_register_channel(uuid, jctrl->valuestring, csv);
	}
	if (st != SWITCH_STATUS_SUCCESS) {
		/* someone else took it first: XCC uses 419 for conflicts */
		cJSON_AddNumberToObject(extra, "code", 419);
		cJSON_AddStringToObject(extra, "message", "channel already controlled by another controller");
		return SWITCH_STATUS_FALSE;
	}
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_answer(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;

	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	switch_channel_answer(channel);
	switch_core_session_rwunlock(session);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_hangup(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;
	switch_call_cause_t cause = SWITCH_CAUSE_NORMAL_CLEARING;
	cJSON *jcause = cJSON_GetObjectItem(params, "cause");

	if (jcause && cJSON_IsString(jcause) && !zstr(jcause->valuestring)) {
		cause = switch_channel_str2cause(jcause->valuestring);
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	switch_channel_hangup(channel, cause);
	switch_core_session_rwunlock(session);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_play(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_status_t st;
	cJSON *media = cJSON_GetObjectItem(params, "media");
	const char *file = NULL;

	if (media) {
		cJSON *jfile = cJSON_GetObjectItem(media, "file");
		if (jfile && cJSON_IsString(jfile)) file = jfile->valuestring;
	}
	if (zstr(file)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	st = switch_ivr_broadcast(switch_core_session_get_uuid(session), file, SMF_NONE);
	switch_core_session_rwunlock(session);
	return st;
}

static switch_status_t mn_stop(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_status_t st;

	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	/* "break" is the dialplan app that cancels running playback/loops */
	st = switch_ivr_broadcast(switch_core_session_get_uuid(session), "break", SMF_NONE);
	switch_core_session_rwunlock(session);
	return st;
}

static switch_status_t mn_broadcast(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_status_t st;
	cJSON *jfile = cJSON_GetObjectItem(params, "file");
	const char *file = (jfile && cJSON_IsString(jfile)) ? jfile->valuestring : NULL;

	if (zstr(file)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	st = switch_ivr_broadcast(switch_core_session_get_uuid(session), file, SMF_NONE);
	switch_core_session_rwunlock(session);
	return st;
}

static switch_status_t bridge_two(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *a = NULL, *b = NULL;
	cJSON *jpeer = cJSON_GetObjectItem(params, "peer_uuid");
	switch_status_t st;

	if (!jpeer || !cJSON_IsString(jpeer) || zstr(jpeer->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(a = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	if (!(b = switch_core_session_locate(jpeer->valuestring))) {
		switch_core_session_rwunlock(a);
		return SWITCH_STATUS_NOTFOUND;
	}
	st = switch_ivr_uuid_bridge(switch_core_session_get_uuid(a), switch_core_session_get_uuid(b));
	switch_core_session_rwunlock(b);
	switch_core_session_rwunlock(a);
	return st;
}

static switch_status_t mn_setvar(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;
	cJSON *data = cJSON_GetObjectItem(params, "data");
	cJSON *item;

	if (!data || !cJSON_IsObject(data)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	cJSON_ArrayForEach(item, data) {
		if (item->string && cJSON_IsString(item)) {
			switch_channel_set_variable(channel, item->string, item->valuestring);
		}
	}
	switch_core_session_rwunlock(session);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_getvar(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;
	cJSON *data = cJSON_GetObjectItem(params, "data");
	cJSON *out = NULL, *item;

	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	out = cJSON_CreateObject();

	if (data && cJSON_IsArray(data)) {
		cJSON_ArrayForEach(item, data) {
			if (cJSON_IsString(item) && !zstr(item->valuestring)) {
				const char *val = switch_channel_get_variable(channel, item->valuestring);
				cJSON_AddStringToObject(out, item->valuestring, switch_str_nil(val));
			}
		}
	} else if (data && cJSON_IsObject(data)) {
		cJSON_ArrayForEach(item, data) {
			if (item->string) {
				const char *val = switch_channel_get_variable(channel, item->string);
				cJSON_AddStringToObject(out, item->string, switch_str_nil(val));
			}
		}
	} else {
		/* no keys: return the whitelisted set */
		const char *keys[] = { "state", "direction", "uuid", "caller_id_name", "caller_id_number",
			"destination_number", "read_codec", "write_codec", NULL
		};
		int i;
		for (i = 0; keys[i]; i++) {
			const char *val = switch_channel_get_variable(channel, keys[i]);
			cJSON_AddStringToObject(out, keys[i], switch_str_nil(val));
		}
	}
	switch_core_session_rwunlock(session);

	cJSON_AddItemToObject(extra, "data", out);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_getstate(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;

	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	if (extra) {
		cJSON_AddStringToObject(extra, "state", switch_channel_state_name(switch_channel_get_state(channel)));
		cJSON_AddStringToObject(extra, "answer_state", switch_channel_state_name(switch_channel_get_running_state(channel)));
	}
	switch_core_session_rwunlock(session);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_getchandata(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;
	cJSON *out;
	static const char *keys[] = {
		"uuid", "direction", "state", "caller_id_name", "caller_id_number",
		"destination_number", "network_addr", "context", "read_codec", "write_codec",
		"created_epoch", "answered_epoch", "hangup_epoch", "hangup_cause", NULL
	};
	int i;

	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	out = cJSON_CreateObject();
	for (i = 0; keys[i]; i++) {
		const char *val = switch_channel_get_variable(channel, keys[i]);
		cJSON_AddStringToObject(out, keys[i], switch_str_nil(val));
	}
	switch_core_session_rwunlock(session);

	cJSON_AddItemToObject(extra, "data", out);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_nativeapp(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jcmd = cJSON_GetObjectItem(params, "cmd");
	cJSON *jargs = cJSON_GetObjectItem(params, "args");
	switch_status_t st;

	if (!jcmd || !cJSON_IsString(jcmd) || zstr(jcmd->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	st = exec_app(session, jcmd->valuestring, (jargs && cJSON_IsString(jargs)) ? jargs->valuestring : NULL);
	switch_core_session_rwunlock(session);
	return st;
}

static switch_status_t mn_nativeapi(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *jcmd = cJSON_GetObjectItem(params, "cmd");
	cJSON *jargs = cJSON_GetObjectItem(params, "args");
	switch_stream_handle_t stream = { 0 };
	char *cmd, *arg;

	if (!jcmd || !cJSON_IsString(jcmd) || zstr(jcmd->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	cmd = jcmd->valuestring;
	arg = (jargs && cJSON_IsString(jargs) && !zstr(jargs->valuestring)) ? jargs->valuestring : NULL;

	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute(cmd, arg, NULL, &stream);
	if (extra) {
		cJSON_AddStringToObject(extra, "data", stream.data ? (char *) stream.data : "");
	}
	switch_safe_free(stream.data);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_nativejsapi(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *jcmd = cJSON_GetObjectItem(params, "cmd");
	cJSON *jparams = cJSON_GetObjectItem(params, "data");
	switch_stream_handle_t stream = { 0 };
	char *cmd, *arg = NULL;

	if (!jcmd || !cJSON_IsString(jcmd) || zstr(jcmd->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	cmd = jcmd->valuestring;
	if (jparams) {
		arg = cJSON_PrintUnformatted(jparams);
	}

	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute(cmd, arg, NULL, &stream);
	if (extra) {
		/* passthrough text MVP: parse as JSON when the API returned json, else raw */
		cJSON *jres = stream.data ? cJSON_Parse((char *) stream.data) : NULL;
		if (jres) {
			cJSON_AddItemToObject(extra, "data", jres);
		} else {
			cJSON_AddStringToObject(extra, "data", stream.data ? (char *) stream.data : "");
		}
	}
	switch_safe_free(arg);
	switch_safe_free(stream.data);
	return SWITCH_STATUS_SUCCESS;
}

/* ---------------------------------------------------------------------- */
/* system metrics (Linux /proc based; other platforms omit the fields)   */

#ifdef __linux__
#include <sys/sysinfo.h>

typedef struct sys_cpu_sample_s {
	unsigned long long total;
	unsigned long long idle;
	int valid;
} sys_cpu_sample_t;

static sys_cpu_sample_t last_cpu_sample = { 0, 0, 0 };

static int read_cpu_sample(sys_cpu_sample_t *out)
{
	char buf[1024];
	FILE *f = fopen("/proc/stat", "r");
	unsigned long long v[10] = { 0 };
	int i, n = 0;

	if (!f) return 0;
	if (!fgets(buf, sizeof(buf), f)) { fclose(f); return 0; }
	fclose(f);
	if (strncmp(buf, "cpu ", 4)) return 0;
	{
		char *p = buf + 4, *end;
		for (i = 0; i < 10; i++) {
			v[i] = strtoull(p, &end, 10);
			if (end == p) break;
			n++; p = end;
		}
	}
	if (n < 5) return 0;
	out->total = 0;
	for (i = 0; i < n; i++) out->total += v[i];
	out->idle = v[3] + ((n > 4) ? v[4] : 0);	/* idle + iowait */
	out->valid = 1;
	return 1;
}

static void add_system_metrics(cJSON *data)
{
	struct sysinfo si;
	unsigned long long total_kb = 0, avail_kb = 0;
	char key[64];
	unsigned long long val;
	FILE *f;
	double la[3] = { 0, 0, 0 };
	int cpus = (int) sysconf(_SC_NPROCESSORS_ONLN);

	/* cpu utilization: delta between /proc/stat samples */
	{
		sys_cpu_sample_t now;
		if (read_cpu_sample(&now)) {
			if (last_cpu_sample.valid && now.total > last_cpu_sample.total) {
				unsigned long long dt = now.total - last_cpu_sample.total;
				unsigned long long di = now.idle - last_cpu_sample.idle;
				if ((long long) di < 0) di = 0;
				cJSON_AddNumberToObject(data, "cpu_percent",
								   (double) (dt - di) * 100.0 / (double) dt);
			}
			last_cpu_sample = now;
		}
	}
	cJSON_AddNumberToObject(data, "cpu_count", (double) (cpus > 0 ? cpus : 1));
	if (getloadavg(la, 3) == 3) {
		cJSON_AddNumberToObject(data, "load_1m", la[0]);
		cJSON_AddNumberToObject(data, "load_5m", la[1]);
		cJSON_AddNumberToObject(data, "load_15m", la[2]);
	}

	/* memory: /proc/meminfo */
	if ((f = fopen("/proc/meminfo", "r")) != NULL) {
		while (fscanf(f, "%63s %llu", key, &val) == 2) {
			if (!strncmp(key, "MemTotal:", 9)) total_kb = val;
			else if (!strncmp(key, "MemAvailable:", 13)) avail_kb = val;
		}
		fclose(f);
	}
	if (total_kb > 0) {
		cJSON_AddNumberToObject(data, "mem_total_mb", (double) (total_kb / 1024));
		cJSON_AddNumberToObject(data, "mem_available_mb", (double) (avail_kb / 1024));
		if (total_kb >= avail_kb) {
			cJSON_AddNumberToObject(data, "mem_used_percent",
							   (double) (total_kb - avail_kb) * 100.0 / (double) total_kb);
		}
	}

	/* this process RSS */
	if ((f = fopen("/proc/self/statm", "r")) != NULL) {
		unsigned long long rss_pages = 0;
		if (fscanf(f, "%*s %llu", &rss_pages) == 1) {
			cJSON_AddNumberToObject(data, "process_rss_mb",
						   (double) (rss_pages * (unsigned long long) sysconf(_SC_PAGESIZE) / 1048576ULL));
		}
		fclose(f);
	}
	(void) si;
}
#endif /* __linux__ */

/* node status payload shared by XNode.JStatus and the metrics heartbeat */
cJSON *mod_nats_methods_node_status(void)
{
	int sessions_peak = 0, sps = 0, sps_peak = 0;
	cJSON *data;

	switch_core_session_ctl(SCSC_SESSIONS_PEAK, &sessions_peak);
	switch_core_session_ctl(SCSC_SPS, &sps);
	switch_core_session_ctl(SCSC_SPS_PEAK, &sps_peak);

	data = cJSON_CreateObject();
	cJSON_AddStringToObject(data, "systemStatus", switch_core_ready() ? "READY" : "NOT READY");
	cJSON_AddNumberToObject(data, "uptime", (double) (switch_core_uptime() / 1000000));
	cJSON_AddStringToObject(data, "version", switch_version_full());
	cJSON_AddNumberToObject(data, "sessions", (double) switch_core_session_count());
	cJSON_AddNumberToObject(data, "sessions_peak", (double) sessions_peak);
	cJSON_AddNumberToObject(data, "sps", (double) sps);
	cJSON_AddNumberToObject(data, "sps_peak", (double) sps_peak);
#ifdef __linux__
	add_system_metrics(data);
#endif
	cJSON_AddStringToObject(data, "node_uuid", mod_nats_globals.node_uuid);
	return data;
}

static switch_status_t mn_jstatus(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}
	cJSON_AddItemToObject(extra, "data", mod_nats_methods_node_status());
	return SWITCH_STATUS_SUCCESS;
}

/* XNode.Dial: direct originate via switch_ivr_originate - dial_string is
 * passed as data, never concatenated into an api command line, so it cannot
 * inject commands. Replies 202 immediately; the outcome reaches the ctrl
 * mailbox as Event.Result carrying the original rpc id. The dialing
 * controller owns the new b-leg from the start (implicit Accept). */
static switch_status_t mn_dial(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *dest = cJSON_GetObjectItem(params, "destination");
	cJSON *call_params, *first, *gparams = NULL;
	switch_event_t *ovars = NULL;
	switch_core_session_t *bleg = NULL;
	switch_call_cause_t cause = SWITCH_CAUSE_NONE;
	const char *dial_string = NULL, *cid_name = NULL, *cid_number = NULL;
	char job_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char uuid_b[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";
	int timeout = 60;
	cJSON *res;

	if (zstr(ctx->ctrl_uuid) || !dest) {
		return SWITCH_STATUS_FALSE;
	}
	call_params = cJSON_GetObjectItem(dest, "call_params");
	if (!call_params || !cJSON_IsArray(call_params) || cJSON_GetArraySize(call_params) < 1) {
		return SWITCH_STATUS_FALSE;
	}
	first = cJSON_GetArrayItem(call_params, 0);
	{
		cJSON *jdial = cJSON_GetObjectItem(first, "dial_string");
		cJSON *jcn = cJSON_GetObjectItem(first, "cid_number");
		cJSON *jcm = cJSON_GetObjectItem(first, "cid_name");
		if (!jdial || !cJSON_IsString(jdial) || zstr(jdial->valuestring)) {
			return SWITCH_STATUS_FALSE;
		}
		dial_string = jdial->valuestring;
		cid_name = (jcm && cJSON_IsString(jcm) && !zstr(jcm->valuestring)) ? jcm->valuestring : NULL;
		cid_number = (jcn && cJSON_IsString(jcn) && !zstr(jcn->valuestring)) ? jcn->valuestring : NULL;
		gparams = cJSON_GetObjectItem(dest, "global_params");
	}
	{
		cJSON *jt = cJSON_GetObjectItem(params, "timeout");
		if (jt && cJSON_IsNumber(jt) && jt->valueint >= 5 && jt->valueint <= 3600) {
			timeout = jt->valueint;
		}
	}

	switch_uuid_str(job_uuid, sizeof(job_uuid));
	cJSON_AddNumberToObject(extra, "code", 202);
	cJSON_AddStringToObject(extra, "message", "accepted");
	cJSON_AddStringToObject(extra, "job_uuid", job_uuid);

	switch_event_create(&ovars, SWITCH_EVENT_REQUEST_PARAMS);
	{
		cJSON *juuid2 = cJSON_GetObjectItem(first, "uuid");
		if (juuid2 && cJSON_IsString(juuid2) && !zstr(juuid2->valuestring)) {
			switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, "origination_uuid", "%s", juuid2->valuestring);
		}
		if (gparams && cJSON_IsObject(gparams)) {
			cJSON *item;
			cJSON_ArrayForEach(item, gparams) {
				if (item->string && cJSON_IsString(item)) {
					switch_event_add_header(ovars, SWITCH_STACK_BOTTOM, item->string, "%s", item->valuestring);
				}
			}
		}
	}

	if (switch_ivr_originate(NULL, &bleg, &cause, dial_string, (uint32_t) timeout, NULL,
							 cid_name, cid_number, NULL, ovars, SOF_NONE, NULL, NULL) == SWITCH_STATUS_SUCCESS && bleg) {
		switch_copy_string(uuid_b, switch_core_session_get_uuid(bleg), sizeof(uuid_b));
		/* the dialing controller owns the b-leg immediately */
		mod_nats_methods_register_channel(uuid_b, ctx->ctrl_uuid, NULL);
		switch_ivr_park_session(bleg);
		switch_core_session_rwunlock(bleg);

		res = cJSON_CreateObject();
		cJSON_AddNumberToObject(res, "code", 200);
		cJSON_AddStringToObject(res, "message", "OK");
		cJSON_AddStringToObject(res, "node_uuid", mod_nats_globals.node_uuid);
		cJSON_AddStringToObject(res, "job_uuid", job_uuid);
		cJSON_AddStringToObject(res, "uuid", uuid_b);
	} else {
		res = cJSON_CreateObject();
		cJSON_AddNumberToObject(res, "code", 480);
		cJSON_AddStringToObject(res, "message", switch_channel_cause2str(cause));
		cJSON_AddStringToObject(res, "node_uuid", mod_nats_globals.node_uuid);
		cJSON_AddStringToObject(res, "job_uuid", job_uuid);
	}
	switch_event_destroy(&ovars);

	/* async outcome to the ctrl mailbox, correlated by the original rpc id */
	mod_nats_events_send_result(ctx->ctrl_uuid, ctx->rpc_id, res);
	return SWITCH_STATUS_SUCCESS;
}

/* Method table: keep in sync with xctrl.proto service XNode */
const mod_nats_method_t mod_nats_methods[] = {
	{"XNode.Accept", mn_accept, SWITCH_TRUE},
	{"XNode.Answer", mn_answer, SWITCH_TRUE},
	{"XNode.Hangup", mn_hangup, SWITCH_TRUE},
	{"XNode.Play", mn_play, SWITCH_TRUE},
	{"XNode.Stop", mn_stop, SWITCH_TRUE},
	{"XNode.Broadcast", mn_broadcast, SWITCH_TRUE},
	{"XNode.Bridge", bridge_two, SWITCH_TRUE},
	{"XNode.ChannelBridge", bridge_two, SWITCH_TRUE},
	{"XNode.SetVar", mn_setvar, SWITCH_TRUE},
	{"XNode.GetVar", mn_getvar, SWITCH_TRUE},
	{"XNode.GetState", mn_getstate, SWITCH_TRUE},
	{"XNode.GetChannelData", mn_getchandata, SWITCH_TRUE},
	{"XNode.NativeApp", mn_nativeapp, SWITCH_TRUE},
	{"XNode.NativeAPI", mn_nativeapi, SWITCH_FALSE},
	{"XNode.NativeJSAPI", mn_nativejsapi, SWITCH_FALSE},
	{"XNode.JStatus", mn_jstatus, SWITCH_FALSE},
	{"XNode.Dial", mn_dial, SWITCH_FALSE},
	{NULL, NULL, SWITCH_FALSE}
};
