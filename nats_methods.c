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

switch_status_t mod_nats_methods_register_channel(const char *uuid, const char *ctrl_uuid)
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
	switch_core_hash_insert(mod_nats_globals.chan_hash, uuid, chan);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);

	return SWITCH_STATUS_SUCCESS;
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
/* bgapi job tracking: lets BACKGROUND_JOB events be routed back to the   */
/* requesting controller as Event.Result                                  */

switch_status_t mod_nats_methods_track_job(const char *job_uuid, const char *ctrl_uuid, const char *rpc_id)
{
	mod_nats_job_t *job;

	if (zstr(job_uuid) || zstr(ctrl_uuid)) {
		return SWITCH_STATUS_FALSE;
	}
	job = (mod_nats_job_t *) switch_core_alloc(mod_nats_globals.pool, sizeof(*job));
	switch_copy_string(job->job_uuid, job_uuid, sizeof(job->job_uuid));
	switch_copy_string(job->ctrl_uuid, ctrl_uuid, sizeof(job->ctrl_uuid));
	switch_copy_string(job->rpc_id, switch_str_nil(rpc_id), sizeof(job->rpc_id));

	switch_mutex_lock(mod_nats_globals.chan_mutex);	/* same lock domain: low churn hashes */
	switch_core_hash_insert(mod_nats_globals.job_hash, job_uuid, job);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return SWITCH_STATUS_SUCCESS;
}

void mod_nats_methods_untrack_job(const char *job_uuid)
{
	if (zstr(job_uuid)) return;
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	switch_core_hash_delete(mod_nats_globals.job_hash, job_uuid);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

const char *mod_nats_methods_job_ctrl(const char *job_uuid, char *rpc_id, size_t rpc_id_len)
{
	mod_nats_job_t *job;
	const char *ctrl = NULL;

	if (zstr(job_uuid)) return NULL;
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	if ((job = (mod_nats_job_t *) switch_core_hash_find(mod_nats_globals.job_hash, job_uuid))) {
		ctrl = job->ctrl_uuid;
		if (rpc_id && rpc_id_len) {
			switch_copy_string(rpc_id, job->rpc_id, rpc_id_len);
		}
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

static switch_status_t mn_accept(cJSON *params, cJSON *extra)
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

	st = mod_nats_methods_register_channel(uuid, jctrl->valuestring);
	if (st != SWITCH_STATUS_SUCCESS) {
		/* someone else took it first: XCC uses 419 for conflicts */
		cJSON_AddNumberToObject(extra, "code", 419);
		cJSON_AddStringToObject(extra, "message", "channel already controlled by another controller");
		return SWITCH_STATUS_FALSE;
	}
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t mn_answer(cJSON *params, cJSON *extra)
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

static switch_status_t mn_hangup(cJSON *params, cJSON *extra)
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

static switch_status_t mn_play(cJSON *params, cJSON *extra)
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

static switch_status_t mn_stop(cJSON *params, cJSON *extra)
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

static switch_status_t mn_broadcast(cJSON *params, cJSON *extra)
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

static switch_status_t bridge_two(cJSON *params, cJSON *extra)
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

static switch_status_t mn_setvar(cJSON *params, cJSON *extra)
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

static switch_status_t mn_getvar(cJSON *params, cJSON *extra)
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

static switch_status_t mn_getstate(cJSON *params, cJSON *extra)
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

static switch_status_t mn_getchandata(cJSON *params, cJSON *extra)
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

static switch_status_t mn_nativeapp(cJSON *params, cJSON *extra)
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

static switch_status_t mn_nativeapi(cJSON *params, cJSON *extra)
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

static switch_status_t mn_nativejsapi(cJSON *params, cJSON *extra)
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

static switch_status_t mn_jstatus(cJSON *params, cJSON *extra)
{
	int sessions_peak = 0, sps = 0, sps_peak = 0;
	cJSON *data;

	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}
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
	cJSON_AddStringToObject(data, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddItemToObject(extra, "data", data);
	return SWITCH_STATUS_SUCCESS;
}

/* XNode.Dial: async originate via bgapi. Replies 202 + job_uuid; the
 * BACKGROUND_JOB result is routed to the ctrl mailbox as Event.Result. */
static switch_status_t mn_dial(cJSON *params, cJSON *extra)
{
	cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
	cJSON *dest = cJSON_GetObjectItem(params, "destination");
	cJSON *call_params, *first;
	cJSON *gparams = dest ? cJSON_GetObjectItem(dest, "global_params") : NULL;
	switch_stream_handle_t stream = { 0 };
	char vars[2048] = "";
	char dial_cmd[3072];
	char *job_uuid = NULL, *p;

	if (!jctrl || !cJSON_IsString(jctrl) || zstr(jctrl->valuestring) || !dest) {
		return SWITCH_STATUS_FALSE;
	}
	call_params = cJSON_GetObjectItem(dest, "call_params");
	if (!call_params || !cJSON_IsArray(call_params) || cJSON_GetArraySize(call_params) < 1) {
		return SWITCH_STATUS_FALSE;
	}
	first = cJSON_GetArrayItem(call_params, 0);
	{
		cJSON *jdial = cJSON_GetObjectItem(first, "dial_string");
		cJSON *juuid = cJSON_GetObjectItem(first, "uuid");
		cJSON *jcidn = cJSON_GetObjectItem(first, "cid_number");
		cJSON *jcidnm = cJSON_GetObjectItem(first, "cid_name");

		if (!jdial || !cJSON_IsString(jdial) || zstr(jdial->valuestring)) {
			return SWITCH_STATUS_FALSE;
		}

		if (juuid && cJSON_IsString(juuid) && !zstr(juuid->valuestring)) {
			snprintf(vars + strlen(vars), sizeof(vars) - strlen(vars), "origination_uuid=%s,", juuid->valuestring);
		}
		if (jcidn && cJSON_IsString(jcidn) && !zstr(jcidn->valuestring)) {
			snprintf(vars + strlen(vars), sizeof(vars) - strlen(vars), "origination_caller_id_number=%s,", jcidn->valuestring);
		}
		if (jcidnm && cJSON_IsString(jcidnm) && !zstr(jcidnm->valuestring)) {
			snprintf(vars + strlen(vars), sizeof(vars) - strlen(vars), "origination_caller_id_name=%s,", jcidnm->valuestring);
		}
		if (gparams && cJSON_IsObject(gparams)) {
			cJSON *item;
			cJSON_ArrayForEach(item, gparams) {
				if (item->string && cJSON_IsString(item)) {
					snprintf(vars + strlen(vars), sizeof(vars) - strlen(vars), "%s=%s,", item->string, item->valuestring);
				}
			}
		}
		/* strip trailing comma */
		{
			size_t l = strlen(vars);
			if (l && vars[l - 1] == ',') vars[l - 1] = '\0';
		}
		snprintf(dial_cmd, sizeof(dial_cmd), "originate {%s}%s &park", vars, jdial->valuestring);
	}

	SWITCH_STANDARD_STREAM(stream);
	switch_api_execute("bgapi", dial_cmd, NULL, &stream);
	if (stream.data) {
		if ((p = strstr((char *) stream.data, "Job-UUID:")) != NULL) {
			char *end;
			p += strlen("Job-UUID:");
			while (*p == ' ') p++;
			if ((end = strchr(p, '\n')) != NULL) *end = '\0';
			job_uuid = switch_core_strdup(mod_nats_globals.pool, p);
		}
		switch_safe_free(stream.data);
	}

	if (job_uuid) {
		cJSON *id_holder = NULL;
		/* rpc id is attached by the protocol layer via the current request context;
		 * for v0.1 we track with the ctrl mailbox only */
		mod_nats_methods_track_job(job_uuid, jctrl->valuestring, "");
		(void) id_holder;
		cJSON_AddNumberToObject(extra, "code", 202);
		cJSON_AddStringToObject(extra, "message", "accepted");
		cJSON_AddStringToObject(extra, "job_uuid", job_uuid);
	}
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
