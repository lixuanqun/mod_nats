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
/* channel bindings: one owner (Accept) and N observers (Observe)        */

static void chan_cancel_accept_task(mod_nats_chan_t *chan)
{
	uint32_t task_id;

	if (!chan || !chan->accept_task_id) {
		return;
	}
	task_id = chan->accept_task_id;
	chan->accept_task_id = 0;
	switch_scheduler_del_task_id(task_id);
}

static void chan_cancel_lease_task(mod_nats_chan_t *chan)
{
	uint32_t task_id;

	if (!chan || !chan->lease_task_id) {
		return;
	}
	task_id = chan->lease_task_id;
	chan->lease_task_id = 0;
	switch_scheduler_del_task_id(task_id);
}

static void chan_free(void *ptr)
{
	mod_nats_chan_t *chan = (mod_nats_chan_t *) ptr;
	mod_nats_obs_t *obs;

	if (!chan) {
		return;
	}
	chan_cancel_accept_task(chan);
	chan_cancel_lease_task(chan);
	while (chan->observers) {
		obs = chan->observers;
		chan->observers = obs->next;
		free(obs);
	}
	switch_safe_free(chan->params_csv);
	free(chan);
}

static void obs_unlink(mod_nats_chan_t *chan, const char *ctrl_uuid)
{
	mod_nats_obs_t **pp = &chan->observers;

	while (*pp) {
		if (!strcmp((*pp)->ctrl_uuid, ctrl_uuid)) {
			mod_nats_obs_t *dead = *pp;
			*pp = dead->next;
			free(dead);
			return;
		}
		pp = &(*pp)->next;
	}
}

static mod_nats_chan_t *chan_create(const char *uuid)
{
	mod_nats_chan_t *chan = (mod_nats_chan_t *) calloc(1, sizeof(*chan));

	if (!chan) {
		return NULL;
	}
	switch_copy_string(chan->uuid, uuid, sizeof(chan->uuid));
	if (switch_core_hash_insert_destructor(mod_nats_globals.chan_hash, uuid, chan, chan_free) != SWITCH_STATUS_SUCCESS) {
		chan_free(chan);
		return NULL;
	}
	return chan;
}

/* ---------------------------------------------------------------------- */
/* owner lease: ownership expires after owner-lease-ttl seconds without   */
/* a successful owner request or an explicit fs.channel.touch. Expiry     */
/* releases the binding (standby controllers can re-Accept) and announces */
/* Event.OwnerLost. Exactly one scheduler task exists per armed lease:    */
/* arming only moves the deadline while a task is pending, and the task   */
/* itself zeroes lease_task_id before deciding to follow, push or expire. */

static void lease_rearm_locked(mod_nats_chan_t *chan);
static void lease_push(const char *uuid);
static void lease_expire(const char *uuid);

/* Scheduler callback at the armed deadline. */
static void lease_timeout_task(switch_scheduler_task_t *task)
{
	const char *uuid = (const char *) task->cmd_arg;
	switch_core_session_t *session;
	switch_bool_t elapsed = SWITCH_FALSE;

	if (!mod_nats_globals.running || zstr(uuid)) {
		return;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	{
		mod_nats_chan_t *chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
		if (chan) {
			chan->lease_task_id = 0;
			if (!zstr(chan->ctrl_uuid)) {
				if (chan->lease_deadline > switch_time_now()) {
					lease_rearm_locked(chan);	/* renewed meanwhile: follow the new deadline */
				} else if (chan->lease_deadline) {
					elapsed = SWITCH_TRUE;
				}
			}
		}
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);

	if (!elapsed) {
		return;
	}

	/* A uuid with no session yet is a dial reservation still inside
	 * originate: keep the reservation, push the deadline out instead. */
	session = switch_core_session_locate(uuid);
	if (!session) {
		lease_push(uuid);
		return;
	}
	switch_core_session_rwunlock(session);
	lease_expire(uuid);
}

/* Schedule the expiry check at chan->lease_deadline. Caller holds chan_mutex. */
static void lease_rearm_locked(mod_nats_chan_t *chan)
{
	time_t when;
	char *arg;
	uint32_t task_id;

	arg = strdup(chan->uuid);
	if (!arg) {
		chan->lease_deadline = 0;
		return;
	}
	when = (time_t) (chan->lease_deadline / 1000000);
	task_id = switch_scheduler_add_task(when, lease_timeout_task, "mod_nats_owner_lease",
										 MOD_NATS_SCHED_GROUP, 0, arg, SSHF_FREE_ARG);
	if (!task_id) {
		free(arg);
		chan->lease_deadline = 0;	/* cannot guard the deadline: disarm, never expire blindly */
		return;
	}
	chan->lease_task_id = task_id;
}

/* Arm or extend the lease by one ttl. Caller holds chan_mutex. */
static void lease_arm_locked(mod_nats_chan_t *chan)
{
	if (!chan || mod_nats_globals.owner_lease_sec <= 0) {
		return;
	}
	chan->lease_deadline = switch_time_now() + (switch_time_t) mod_nats_globals.owner_lease_sec * 1000000;
	if (!chan->lease_task_id) {
		lease_rearm_locked(chan);
	}
}

/* Push an elapsed lease one ttl out (dial reservation still originating). */
static void lease_push(const char *uuid)
{
	mod_nats_chan_t *chan;

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->ctrl_uuid) && chan->lease_deadline && chan->lease_deadline <= switch_time_now()) {
		chan->lease_deadline = switch_time_now() + (switch_time_t) mod_nats_globals.owner_lease_sec * 1000000;
		lease_rearm_locked(chan);
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

/* Expire an elapsed lease: release the channel and announce Event.OwnerLost. */
static void lease_expire(const char *uuid)
{
	mod_nats_chan_t *chan;
	char owner[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->ctrl_uuid) && chan->lease_deadline && chan->lease_deadline <= switch_time_now()) {
		switch_copy_string(owner, chan->ctrl_uuid, sizeof(owner));
		chan->ctrl_uuid[0] = '\0';
		chan->lease_deadline = 0;
		chan->lease_task_id = 0;	/* this task is running: nothing to cancel */
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);

	if (!zstr(owner)) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
						  MOD_NATS_NAME " owner lease expired, released %s (was ctrl %s)\n",
						  uuid, owner);
		mod_nats_events_publish_ownerlost(uuid, owner);
	}
}

/* Every successful owner-gated request renews the lease. */
static void lease_renew(const char *uuid)
{
	mod_nats_chan_t *chan;

	if (zstr(uuid) || mod_nats_globals.owner_lease_sec <= 0) {
		return;
	}
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->ctrl_uuid)) {
		lease_arm_locked(chan);
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

switch_status_t mod_nats_methods_register_channel(const char *uuid, const char *ctrl_uuid, const char *params_csv)
{
	mod_nats_chan_t *chan;

	if (zstr(uuid) || zstr(ctrl_uuid)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->ctrl_uuid)) {
		if (strcmp(chan->ctrl_uuid, ctrl_uuid)) {
			switch_mutex_unlock(mod_nats_globals.chan_mutex);
			return SWITCH_STATUS_FALSE;	/* already owned: caller gets 419 */
		}
		/* same controller re-asserting (re-Accept, Dial completion):
		 * idempotent success, refreshes the lease */
		lease_arm_locked(chan);
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_SUCCESS;
	}
	if (!chan && !(chan = chan_create(uuid))) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_MEMERR;
	}
	switch_copy_string(chan->ctrl_uuid, ctrl_uuid, sizeof(chan->ctrl_uuid));
	obs_unlink(chan, ctrl_uuid);
	chan_cancel_accept_task(chan);
	lease_arm_locked(chan);
	if (!zstr(params_csv)) {
		switch_safe_free(chan->params_csv);
		chan->params_csv = strdup(params_csv);
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return SWITCH_STATUS_SUCCESS;
}

void mod_nats_methods_unregister_channel(const char *uuid)
{
	if (zstr(uuid)) {
		return;
	}
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	switch_core_hash_delete(mod_nats_globals.chan_hash, uuid);
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

switch_bool_t mod_nats_methods_channel_owned(const char *uuid)
{
	mod_nats_chan_t *chan;
	switch_bool_t owned = SWITCH_FALSE;

	if (zstr(uuid)) {
		return SWITCH_FALSE;
	}
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	owned = (chan && !zstr(chan->ctrl_uuid)) ? SWITCH_TRUE : SWITCH_FALSE;
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return owned;
}

void mod_nats_methods_channel_params_copy(const char *uuid, char *buf, size_t buflen)
{
	mod_nats_chan_t *chan;

	if (!buf || buflen == 0) {
		return;
	}
	buf[0] = '\0';
	if (zstr(uuid)) {
		return;
	}
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->params_csv)) {
		switch_copy_string(buf, chan->params_csv, buflen);
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

void mod_nats_methods_snapshot_audience(const char *uuid, char *owner, size_t owner_len, mod_nats_obs_t **obs)
{
	mod_nats_chan_t *chan;
	mod_nats_obs_t *it;

	if (obs) {
		*obs = NULL;
	}
	if (owner && owner_len) {
		owner[0] = '\0';
	}
	if (zstr(uuid)) {
		return;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan) {
		if (owner && owner_len && !zstr(chan->ctrl_uuid)) {
			switch_copy_string(owner, chan->ctrl_uuid, owner_len);
		}
		if (obs) {
			for (it = chan->observers; it; it = it->next) {
				mod_nats_obs_t *copy = (mod_nats_obs_t *) calloc(1, sizeof(*copy));
				if (!copy) {
					break;
				}
				switch_copy_string(copy->ctrl_uuid, it->ctrl_uuid, sizeof(copy->ctrl_uuid));
				copy->next = *obs;
				*obs = copy;
			}
		}
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

int mod_nats_methods_copy_audience(const char *uuid, char *owner, size_t owner_len,
								   char obs[][SWITCH_UUID_FORMATTED_LENGTH + 1], int max_obs)
{
	mod_nats_chan_t *chan;
	mod_nats_obs_t *it;
	int n = 0;

	if (owner && owner_len) {
		owner[0] = '\0';
	}
	if (zstr(uuid)) {
		return 0;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan) {
		if (owner && owner_len && !zstr(chan->ctrl_uuid)) {
			switch_copy_string(owner, chan->ctrl_uuid, owner_len);
		}
		if (obs && max_obs > 0) {
			for (it = chan->observers; it && n < max_obs; it = it->next) {
				switch_copy_string(obs[n], it->ctrl_uuid, SWITCH_UUID_FORMATTED_LENGTH + 1);
				n++;
			}
		}
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return n;
}

void mod_nats_obs_list_free(mod_nats_obs_t *obs)
{
	while (obs) {
		mod_nats_obs_t *next = obs->next;
		free(obs);
		obs = next;
	}
}

/* Hang up an inbound channel nobody Accepted. cmd_arg is freed by SSHF_FREE_ARG. */
static void accept_timeout_task(switch_scheduler_task_t *task)
{
	const char *uuid = (const char *) task->cmd_arg;
	switch_core_session_t *session;

	if (!mod_nats_globals.running || zstr(uuid) || mod_nats_methods_channel_owned(uuid)) {
		return;
	}
	session = switch_core_session_locate(uuid);
	if (!session) {
		return;
	}
	if (mod_nats_methods_channel_owned(uuid)) {
		switch_core_session_rwunlock(session);
		return;
	}
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, MOD_NATS_NAME " accept timeout, hanging up %s\n", uuid);
	switch_channel_hangup(switch_core_session_get_channel(session), SWITCH_CAUSE_NO_ANSWER);
	switch_core_session_rwunlock(session);
}

void mod_nats_methods_arm_accept_timeout(const char *uuid)
{
	mod_nats_chan_t *chan;
	uint32_t task_id;
	char *arg;
	time_t when;

	if (!mod_nats_globals.running || mod_nats_globals.accept_timeout_sec <= 0 || zstr(uuid)) {
		return;
	}
	if (mod_nats_methods_channel_owned(uuid)) {
		return;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && chan->accept_task_id) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return;
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);

	arg = strdup(uuid);
	if (!arg) {
		return;
	}
	when = switch_epoch_time_now(NULL) + mod_nats_globals.accept_timeout_sec;
	task_id = switch_scheduler_add_task(when, accept_timeout_task, "mod_nats_accept_timeout",
										 MOD_NATS_SCHED_GROUP, 0, arg, SSHF_FREE_ARG);
	if (!task_id) {
		free(arg);
		return;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if ((chan && chan->accept_task_id) || (chan && !zstr(chan->ctrl_uuid))) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		switch_scheduler_del_task_id(task_id);
		return;
	}
	if (!chan && !(chan = chan_create(uuid))) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		switch_scheduler_del_task_id(task_id);
		return;
	}
	chan->accept_task_id = task_id;
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

static switch_status_t add_observer(const char *uuid, const char *ctrl_uuid)
{
	mod_nats_chan_t *chan;
	mod_nats_obs_t *it;
	int n = 0;

	if (zstr(uuid) || zstr(ctrl_uuid)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan && !zstr(chan->ctrl_uuid) && !strcmp(chan->ctrl_uuid, ctrl_uuid)) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_SUCCESS;	/* owner already receives the mailbox */
	}
	if (chan) {
		for (it = chan->observers; it; it = it->next) {
			n++;
			if (!strcmp(it->ctrl_uuid, ctrl_uuid)) {
				switch_mutex_unlock(mod_nats_globals.chan_mutex);
				return SWITCH_STATUS_SUCCESS;
			}
		}
		if (n >= MOD_NATS_MAX_OBSERVERS) {
			switch_mutex_unlock(mod_nats_globals.chan_mutex);
			return SWITCH_STATUS_FALSE;
		}
	} else if (!(chan = chan_create(uuid))) {
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_MEMERR;
	}
	it = (mod_nats_obs_t *) calloc(1, sizeof(*it));
	if (!it) {
		if (zstr(chan->ctrl_uuid) && !chan->observers) {
			switch_core_hash_delete(mod_nats_globals.chan_hash, uuid);
		}
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
		return SWITCH_STATUS_MEMERR;
	}
	switch_copy_string(it->ctrl_uuid, ctrl_uuid, sizeof(it->ctrl_uuid));
	it->next = chan->observers;
	chan->observers = it;
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t remove_observer(const char *uuid, const char *ctrl_uuid)
{
	mod_nats_chan_t *chan;

	if (zstr(uuid) || zstr(ctrl_uuid)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan) {
		obs_unlink(chan, ctrl_uuid);
		if (zstr(chan->ctrl_uuid) && !chan->observers) {
			switch_core_hash_delete(mod_nats_globals.chan_hash, uuid);
		}
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
	return SWITCH_STATUS_SUCCESS;
}

/* Control methods require an Accept owner. Observers are not owners.
 * Unclaimed channels are refused so a public event cannot be used as a control token. */
static switch_status_t require_owner_uuid(mod_nats_req_ctx_t *ctx, cJSON *extra, const char *uuid)
{
	char owner[SWITCH_UUID_FORMATTED_LENGTH + 1];
	const char *caller = NULL;

	if (zstr(uuid)) {
		return SWITCH_STATUS_SUCCESS;
	}
	mod_nats_methods_snapshot_audience(uuid, owner, sizeof(owner), NULL);
	if (zstr(owner)) {
		if (extra) {
			cJSON_AddNumberToObject(extra, "code", 400);
			cJSON_AddStringToObject(extra, "message", "channel not accepted");
		}
		return SWITCH_STATUS_FALSE;
	}
	if (ctx && !zstr(ctx->ctrl_uuid)) {
		caller = ctx->ctrl_uuid;
	}
	if (zstr(caller) || strcmp(caller, owner)) {
		if (extra) {
			cJSON_AddNumberToObject(extra, "code", 419);
			cJSON_AddStringToObject(extra, "message", "channel controlled by another controller");
		}
		return SWITCH_STATUS_FALSE;
	}
	lease_renew(uuid);
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t require_owner(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *juuid = params ? cJSON_GetObjectItem(params, "uuid") : NULL;

	if (!juuid || !cJSON_IsString(juuid)) {
		return SWITCH_STATUS_SUCCESS;
	}
	return require_owner_uuid(ctx, extra, juuid->valuestring);
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

/* Prove the channel exists, then drop the read lock before calling back into
 * IVR. broadcast and uuid_bridge locate the session themselves; a second
 * rdlock on a non-recursive session lock deadlocks the worker. */
static switch_status_t broadcast_unlocked(cJSON *params, const char *path)
{
	switch_core_session_t *session;
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];

	if (zstr(path)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, uuid, sizeof(uuid)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	switch_core_session_rwunlock(session);
	return switch_ivr_broadcast(uuid, path, SMF_NONE);
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
	if (st == SWITCH_STATUS_MEMERR) {
		return st;
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
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
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
	switch_status_t own;

	own = require_owner(ctx, params, extra);
	if (own != SWITCH_STATUS_SUCCESS) {
		return own;
	}

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
	cJSON *media = cJSON_GetObjectItem(params, "media");
	const char *file = NULL;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}

	if (media) {
		cJSON *jfile = cJSON_GetObjectItem(media, "file");
		if (jfile && cJSON_IsString(jfile)) file = jfile->valuestring;
	}
	if (zstr(file)) {
		return SWITCH_STATUS_FALSE;
	}
	return broadcast_unlocked(params, file);
}

static switch_status_t mn_stop(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	/* "break" is the dialplan token that cancels running playback */
	return broadcast_unlocked(params, "break");
}

static switch_status_t mn_broadcast(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *jfile = cJSON_GetObjectItem(params, "file");
	const char *file = (jfile && cJSON_IsString(jfile)) ? jfile->valuestring : NULL;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (zstr(file)) {
		return SWITCH_STATUS_FALSE;
	}
	return broadcast_unlocked(params, file);
}

static switch_status_t bridge_two(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *a = NULL, *b = NULL;
	cJSON *jpeer = cJSON_GetObjectItem(params, "peer_uuid");
	char uuid_a[SWITCH_UUID_FORMATTED_LENGTH + 1];
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!jpeer || !cJSON_IsString(jpeer) || zstr(jpeer->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if ((own = require_owner_uuid(ctx, extra, jpeer->valuestring)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!(a = session_from_params(params, uuid_a, sizeof(uuid_a)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	if (!(b = switch_core_session_locate(jpeer->valuestring))) {
		switch_core_session_rwunlock(a);
		return SWITCH_STATUS_NOTFOUND;
	}
	switch_core_session_rwunlock(b);
	switch_core_session_rwunlock(a);
	return switch_ivr_uuid_bridge(uuid_a, jpeer->valuestring);
}

static int origin_var_forbidden(const char *s);

static switch_status_t mn_setvar(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	switch_channel_t *channel;
	cJSON *data = cJSON_GetObjectItem(params, "data");
	cJSON *item;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!data || !cJSON_IsObject(data)) {
		return SWITCH_STATUS_FALSE;
	}
	/* same forbidden set as Dial's global_params: a controller must not arm
	 * execute_on_* / api_on_* hooks through a channel variable */
	cJSON_ArrayForEach(item, data) {
		if (item->string && origin_var_forbidden(item->string)) {
			if (extra) {
				cJSON_AddNumberToObject(extra, "code", 400);
				cJSON_AddStringToObject(extra, "message", "forbidden channel variable");
			}
			return SWITCH_STATUS_FALSE;
		}
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
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
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
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	channel = switch_core_session_get_channel(session);
	if (extra) {
		cJSON_AddStringToObject(extra, "state", switch_channel_state_name(switch_channel_get_state(channel)));
		cJSON_AddStringToObject(extra, "answer_state", switch_channel_callstate2str(switch_channel_get_callstate(channel)));
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
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
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
	cJSON *jcmd = cJSON_GetObjectItem(params, "cmd");
	cJSON *jargs = cJSON_GetObjectItem(params, "args");
	switch_application_interface_t *app_interface;
	const char *cmd;
	const char *args;
	char spec[2048];
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!jcmd || !cJSON_IsString(jcmd) || zstr(jcmd->valuestring) || strchr(jcmd->valuestring, ':')) {
		return SWITCH_STATUS_FALSE;
	}
	cmd = jcmd->valuestring;
	args = (jargs && cJSON_IsString(jargs)) ? jargs->valuestring : "";
	/* shell-exec applications are refused regardless of the ownership gate;
	 * fs.native.api (allow-native-api) is the intended escape hatch */
	if (!strcasecmp(cmd, "system") || !strcasecmp(cmd, "bg_system")) {
		if (extra) {
			cJSON_AddNumberToObject(extra, "code", 403);
			cJSON_AddStringToObject(extra, "message", "application not allowed over the bus");
		}
		return SWITCH_STATUS_FALSE;
	}
	if (strlen(cmd) + strlen(switch_str_nil(args)) + 3 >= sizeof(spec)) {
		return SWITCH_STATUS_FALSE;
	}
	app_interface = switch_loadable_module_get_application_interface(cmd);
	if (!app_interface) {
		return SWITCH_STATUS_NOTIMPL;
	}
	UNPROTECT_INTERFACE(app_interface);
	snprintf(spec, sizeof(spec), "%s::%s", cmd, switch_str_nil(args));
	/* Queued onto the session thread. The worker does not run the application. */
	return broadcast_unlocked(params, spec);
}

static switch_status_t mn_nativeapi(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *jcmd = cJSON_GetObjectItem(params, "cmd");
	cJSON *jargs = cJSON_GetObjectItem(params, "args");
	switch_stream_handle_t stream = { 0 };
	char *cmd, *arg;

	if (mod_nats_globals.allow_native_api != SWITCH_TRUE) {
		if (extra) {
			cJSON_AddNumberToObject(extra, "code", 403);
			cJSON_AddStringToObject(extra, "message", "native api disabled");
		}
		return SWITCH_STATUS_FALSE;
	}

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

	if (mod_nats_globals.allow_native_api != SWITCH_TRUE) {
		if (extra) {
			cJSON_AddNumberToObject(extra, "code", 403);
			cJSON_AddStringToObject(extra, "message", "native api disabled");
		}
		return SWITCH_STATUS_FALSE;
	}
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

	if (mod_nats_globals.cpu_mutex) {
		switch_mutex_lock(mod_nats_globals.cpu_mutex);
	}
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
	if (mod_nats_globals.cpu_mutex) {
		switch_mutex_unlock(mod_nats_globals.cpu_mutex);
	}
}
#endif /* __linux__ */

/* node status payload shared by XNode.JStatus and the metrics heartbeat */
cJSON *mod_nats_methods_node_status(void)
{
	int sessions_peak = 0, sps = 0, sps_peak = 0, max_sessions = 0;
	cJSON *data;

	switch_core_session_ctl(SCSC_SESSIONS_PEAK, &sessions_peak);
	switch_core_session_ctl(SCSC_MAX_SESSIONS, &max_sessions);
	switch_core_session_ctl(SCSC_SPS, &sps);
	switch_core_session_ctl(SCSC_SPS_PEAK, &sps_peak);

	data = cJSON_CreateObject();
	cJSON_AddStringToObject(data, "systemStatus", switch_core_ready() ? "READY" : "NOT READY");
	cJSON_AddNumberToObject(data, "uptime", (double) (switch_core_uptime() / 1000000));
	cJSON_AddStringToObject(data, "version", switch_version_full());
	cJSON_AddNumberToObject(data, "sessions", (double) switch_core_session_count());
	cJSON_AddNumberToObject(data, "sessions_peak", (double) sessions_peak);
	cJSON_AddNumberToObject(data, "sessions_max", (double) max_sessions);
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

/* ---------------------------------------------------------------------- */
/* XNode.Dial runs off the request workers. 202 is the enqueue ack;       */
/* Event.Result is produced by a dial thread.                              */

typedef struct mod_nats_dial_job_s {
	char *dial_string;
	char *cid_name;
	char *cid_number;
	char *ctrl_uuid;
	char *rpc_id;
	char *request_id;
	char job_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char channel_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	int rpc_id_is_number;
	uint32_t timeout;
	switch_event_t *ovars;
} mod_nats_dial_job_t;

static void dial_job_free(mod_nats_dial_job_t *job)
{
	if (!job) {
		return;
	}
	switch_safe_free(job->dial_string);
	switch_safe_free(job->cid_name);
	switch_safe_free(job->cid_number);
	switch_safe_free(job->ctrl_uuid);
	switch_safe_free(job->rpc_id);
	switch_safe_free(job->request_id);
	if (job->ovars) {
		switch_event_destroy(&job->ovars);
	}
	free(job);
}

static void dial_job_fail(mod_nats_dial_job_t *job, int code, const char *message)
{
	cJSON *res = cJSON_CreateObject();

	cJSON_AddNumberToObject(res, "code", code);
	cJSON_AddStringToObject(res, "message", message);
	cJSON_AddStringToObject(res, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(res, "job_uuid", job->job_uuid);
	if (!zstr(job->request_id)) {
		cJSON_AddStringToObject(res, "request_id", job->request_id);
	}
	mod_nats_events_send_result(job->ctrl_uuid, job->rpc_id, job->rpc_id_is_number, res);
}

static void dial_job_run(mod_nats_dial_job_t *job)
{
	switch_core_session_t *bleg = NULL;
	switch_call_cause_t cause = SWITCH_CAUSE_NONE;
	char uuid_b[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";

	/* queued but not started when the module is unloading: do not block unload */
	if (!mod_nats_globals.running) {
		mod_nats_methods_unregister_channel(job->channel_uuid);
		dial_job_fail(job, 480, "shutting down");
		dial_job_free(job);
		return;
	}

	if (switch_ivr_originate(NULL, &bleg, &cause, job->dial_string, job->timeout, NULL,
							 job->cid_name, job->cid_number, NULL, job->ovars, SOF_NONE, NULL, NULL) == SWITCH_STATUS_SUCCESS && bleg) {
		cJSON *res;

		switch_copy_string(uuid_b, switch_core_session_get_uuid(bleg), sizeof(uuid_b));
		if (strcmp(uuid_b, job->channel_uuid)) {
			mod_nats_methods_unregister_channel(job->channel_uuid);
			if (mod_nats_methods_register_channel(uuid_b, job->ctrl_uuid, NULL) != SWITCH_STATUS_SUCCESS) {
				switch_channel_hangup(switch_core_session_get_channel(bleg), SWITCH_CAUSE_NORMAL_CLEARING);
				switch_core_session_rwunlock(bleg);
				dial_job_fail(job, 419, "channel uuid was claimed during originate");
				dial_job_free(job);
				return;
			}
		} else if (mod_nats_methods_register_channel(uuid_b, job->ctrl_uuid, NULL) != SWITCH_STATUS_SUCCESS) {
			/* lease elapsed mid-originate and another controller took the channel */
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
							  MOD_NATS_NAME " dial completed on %s but another controller owns it\n", uuid_b);
		}
		switch_ivr_park_session(bleg);
		switch_core_session_rwunlock(bleg);

		res = cJSON_CreateObject();
		if (!zstr(job->request_id)) {
			cJSON_AddStringToObject(res, "request_id", job->request_id);
		}
		cJSON_AddNumberToObject(res, "code", 200);
		cJSON_AddStringToObject(res, "message", "OK");
		cJSON_AddStringToObject(res, "node_uuid", mod_nats_globals.node_uuid);
		cJSON_AddStringToObject(res, "job_uuid", job->job_uuid);
		cJSON_AddStringToObject(res, "uuid", uuid_b);
		mod_nats_events_send_result(job->ctrl_uuid, job->rpc_id, job->rpc_id_is_number, res);
	} else {
		mod_nats_methods_unregister_channel(job->channel_uuid);
		dial_job_fail(job, 480, switch_channel_cause2str(cause));
	}
	dial_job_free(job);
}

static void *SWITCH_THREAD_FUNC dial_thread(switch_thread_t *t, void *data)
{
	while (1) {
		void *pop = NULL;
		switch_status_t st = switch_queue_pop(mod_nats_globals.dial_queue, &pop);

		if (pop) {
			dial_job_run((mod_nats_dial_job_t *) pop);
			continue;
		}
		if (!mod_nats_globals.running) {
			while (switch_queue_trypop(mod_nats_globals.dial_queue, &pop) == SWITCH_STATUS_SUCCESS && pop) {
				dial_job_run((mod_nats_dial_job_t *) pop);
			}
			break;
		}
		if (st != SWITCH_STATUS_SUCCESS) {
			continue;
		}
	}
	return NULL;
}

void mod_nats_methods_dial_start(void)
{
	switch_threadattr_t *thd_attr;
	int i, n;

	n = mod_nats_globals.workers;
	if (n < 1) {
		n = 1;
	}
	if (n > 16) {
		n = 16;
	}
	for (i = 0; i < n; i++) {
		switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
		switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
		if (switch_thread_create(&mod_nats_globals.dial_threads[i], thd_attr, dial_thread, NULL, mod_nats_globals.pool) == SWITCH_STATUS_SUCCESS) {
			mod_nats_globals.dial_thread_count++;
		}
	}
}

void mod_nats_methods_dial_stop(void)
{
	int i;
	switch_status_t st;

	if (mod_nats_globals.dial_queue) {
		switch_queue_interrupt_all(mod_nats_globals.dial_queue);
	}
	for (i = 0; i < mod_nats_globals.dial_thread_count; i++) {
		switch_thread_join(&st, mod_nats_globals.dial_threads[i]);
	}
	mod_nats_globals.dial_thread_count = 0;
}

/* Channel variables that execute dialplan apps or API commands from an originate. */
static int origin_var_forbidden(const char *s)
{
	if (zstr(s)) {
		return 0;
	}
	if (switch_stristr("execute_on_", s) || switch_stristr("api_on_", s) ||
		switch_stristr("api_hangup_hook", s) || switch_stristr("exec_after_", s)) {
		return 1;
	}
	return 0;
}

/* XNode.Dial: direct originate via switch_ivr_originate. dial_string is
 * passed as data, never concatenated into an api command line. The b-leg
 * uuid is reserved (implicit Accept) before originate, so a concurrent
 * Accept cannot take the channel. 202 is the enqueue ack. */
static switch_status_t mn_dial(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *dest = cJSON_GetObjectItem(params, "destination");
	cJSON *call_params, *first, *gparams = NULL;
	const char *dial_string = NULL, *cid_name = NULL, *cid_number = NULL;
	mod_nats_dial_job_t *job;
	char job_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char channel_uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	int timeout = 60;
	switch_status_t reg;

	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}

	if (!mod_nats_globals.running || mod_nats_globals.dial_thread_count < 1) {
		cJSON_AddNumberToObject(extra, "code", 503);
		cJSON_AddStringToObject(extra, "message", "dial unavailable");
		return SWITCH_STATUS_FALSE;
	}
	if (zstr(ctx->ctrl_uuid) || !dest) {
		return SWITCH_STATUS_FALSE;
	}
	call_params = cJSON_GetObjectItem(dest, "call_params");
	if (!call_params || !cJSON_IsArray(call_params) || cJSON_GetArraySize(call_params) < 1) {
		return SWITCH_STATUS_FALSE;
	}
	first = cJSON_GetArrayItem(call_params, 0);
	if (!first) {
		return SWITCH_STATUS_FALSE;
	}
	{
		cJSON *jdial = cJSON_GetObjectItem(first, "dial_string");
		cJSON *jcn = cJSON_GetObjectItem(first, "cid_number");
		cJSON *jcm = cJSON_GetObjectItem(first, "cid_name");
		cJSON *juuid2 = cJSON_GetObjectItem(first, "uuid");
		if (!jdial || !cJSON_IsString(jdial) || zstr(jdial->valuestring)) {
			return SWITCH_STATUS_FALSE;
		}
		dial_string = jdial->valuestring;
		cid_name = (jcm && cJSON_IsString(jcm) && !zstr(jcm->valuestring)) ? jcm->valuestring : NULL;
		cid_number = (jcn && cJSON_IsString(jcn) && !zstr(jcn->valuestring)) ? jcn->valuestring : NULL;
		gparams = cJSON_GetObjectItem(dest, "global_params");
		if (juuid2 && cJSON_IsString(juuid2) && !zstr(juuid2->valuestring)) {
			switch_copy_string(channel_uuid, juuid2->valuestring, sizeof(channel_uuid));
		} else {
			switch_uuid_str(channel_uuid, sizeof(channel_uuid));
		}
	}
	{
		switch_core_session_t *existing = switch_core_session_locate(channel_uuid);
		if (existing) {
			switch_core_session_rwunlock(existing);
			cJSON_AddNumberToObject(extra, "code", 400);
			cJSON_AddStringToObject(extra, "message", "uuid already exists");
			return SWITCH_STATUS_FALSE;
		}
	}
	{
		cJSON *jt = cJSON_GetObjectItem(params, "timeout");
		if (jt && cJSON_IsNumber(jt) && jt->valueint >= 5 && jt->valueint <= 3600) {
			timeout = jt->valueint;
		}
	}
	if (origin_var_forbidden(dial_string)) {
		cJSON_AddNumberToObject(extra, "code", 400);
		cJSON_AddStringToObject(extra, "message", "dial string contains a forbidden channel variable");
		return SWITCH_STATUS_FALSE;
	}
	if (gparams && cJSON_IsObject(gparams)) {
		cJSON *item;
		cJSON_ArrayForEach(item, gparams) {
			if (item->string && origin_var_forbidden(item->string)) {
				cJSON_AddNumberToObject(extra, "code", 400);
				cJSON_AddStringToObject(extra, "message", "forbidden channel variable");
				return SWITCH_STATUS_FALSE;
			}
		}
	}

	reg = mod_nats_methods_register_channel(channel_uuid, ctx->ctrl_uuid, NULL);
	if (reg == SWITCH_STATUS_MEMERR) {
		return reg;
	}
	if (reg != SWITCH_STATUS_SUCCESS) {
		cJSON_AddNumberToObject(extra, "code", 419);
		cJSON_AddStringToObject(extra, "message", "channel already controlled by another controller");
		return SWITCH_STATUS_FALSE;
	}

	job = (mod_nats_dial_job_t *) calloc(1, sizeof(*job));
	if (!job) {
		mod_nats_methods_unregister_channel(channel_uuid);
		return SWITCH_STATUS_MEMERR;
	}
	switch_uuid_str(job->job_uuid, sizeof(job->job_uuid));
	switch_copy_string(job_uuid, job->job_uuid, sizeof(job_uuid));
	switch_copy_string(job->channel_uuid, channel_uuid, sizeof(job->channel_uuid));
	job->rpc_id_is_number = ctx->rpc_id_is_number;
	job->timeout = (uint32_t) timeout;
	job->dial_string = strdup(dial_string);
	job->ctrl_uuid = strdup(ctx->ctrl_uuid);
	job->rpc_id = strdup(switch_str_nil(ctx->rpc_id));
	if (!zstr(cid_name)) {
		job->cid_name = strdup(cid_name);
	}
	if (!zstr(cid_number)) {
		job->cid_number = strdup(cid_number);
	}
	if (!zstr(ctx->request_id)) {
		job->request_id = strdup(ctx->request_id);
	}
	if (!job->dial_string || !job->ctrl_uuid || !job->rpc_id) {
		mod_nats_methods_unregister_channel(channel_uuid);
		dial_job_free(job);
		return SWITCH_STATUS_MEMERR;
	}

	switch_event_create(&job->ovars, SWITCH_EVENT_REQUEST_PARAMS);
	if (job->ovars) {
		switch_event_add_header(job->ovars, SWITCH_STACK_BOTTOM, "origination_uuid", "%s", channel_uuid);
		if (gparams && cJSON_IsObject(gparams)) {
			cJSON *item;
			cJSON_ArrayForEach(item, gparams) {
				if (item->string && cJSON_IsString(item)) {
					switch_event_add_header(job->ovars, SWITCH_STACK_BOTTOM, item->string, "%s", item->valuestring);
				}
			}
		}
	}

	if (switch_queue_trypush(mod_nats_globals.dial_queue, job) != SWITCH_STATUS_SUCCESS) {
		mod_nats_methods_unregister_channel(channel_uuid);
		dial_job_free(job);
		cJSON_AddNumberToObject(extra, "code", 503);
		cJSON_AddStringToObject(extra, "message", "dial queue full");
		return SWITCH_STATUS_FALSE;
	}

	cJSON_AddNumberToObject(extra, "code", 202);
	cJSON_AddStringToObject(extra, "message", "accepted");
	cJSON_AddStringToObject(extra, "job_uuid", job_uuid);
	return SWITCH_STATUS_SUCCESS;
}

/* capability discovery payload: what this node speaks right now */
cJSON *mod_nats_events_capabilities(void)
{
	cJSON *cap = cJSON_CreateObject();
	const mod_nats_method_t *m;
	cJSON *list = cJSON_CreateArray();

	cJSON_AddStringToObject(cap, "node_uuid", mod_nats_globals.node_uuid);
	cJSON_AddStringToObject(cap, "proto_version", MOD_NATS_PROTO_VERSION);
	cJSON_AddStringToObject(cap, "subject_prefix", mod_nats_globals.subject_prefix);
	cJSON_AddBoolToObject(cap, "compat_xcc", mod_nats_globals.compat_xcc == SWITCH_TRUE);
	for (m = mod_nats_methods; m->name; m++) {
		cJSON_AddItemToArray(list, cJSON_CreateString(m->name));
		if (mod_nats_globals.compat_xcc == SWITCH_TRUE && m->xcc_alias) {
			cJSON_AddItemToArray(list, cJSON_CreateString(m->xcc_alias));
		}
	}
	cJSON_AddItemToObject(cap, "capabilities", list);
	return cap;
}

/* fs.channel.observe: watch events on the caller mailbox. Does not take ownership. */
static switch_status_t mn_observe(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
	switch_status_t st;

	if (!jctrl || !cJSON_IsString(jctrl) || zstr(jctrl->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, uuid, sizeof(uuid)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	switch_core_session_rwunlock(session);
	st = add_observer(uuid, jctrl->valuestring);
	if (st == SWITCH_STATUS_FALSE && extra) {
		cJSON_AddStringToObject(extra, "message", "observer limit reached");
	}
	return st;
}

static switch_status_t mn_unobserve(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
	char uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];

	if (!jctrl || !cJSON_IsString(jctrl) || zstr(jctrl->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, uuid, sizeof(uuid)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	switch_core_session_rwunlock(session);
	return remove_observer(uuid, jctrl->valuestring);
}

/* fs.node.hello: request-reply form of Event.NodeUp */
static switch_status_t mn_hello(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	if (!extra) {
		return SWITCH_STATUS_FALSE;
	}
	cJSON_AddItemToObject(extra, "data", mod_nats_events_capabilities());
	return SWITCH_STATUS_SUCCESS;
}

/* fs.channel.touch: renew the lease without any other action. The renewal
 * itself already happened in require_owner_uuid; this is the explicit form
 * for controllers that only listen between calls. */
static switch_status_t mn_touch(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (extra && mod_nats_globals.owner_lease_sec > 0) {
		cJSON_AddNumberToObject(extra, "lease_ttl", mod_nats_globals.owner_lease_sec);
	}
	return SWITCH_STATUS_SUCCESS;
}

/* XNode.Record / fs.channel.record: start/stop/pause/resume/mask/unmask a
 * session recording. Mirrors mod_commands uuid_record semantics: the file
 * path is the recording key, so STOP must repeat the same path. */
static switch_status_t mn_record(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jaction = cJSON_GetObjectItem(params, "action");
	cJSON *jfile = cJSON_GetObjectItem(params, "file");
	cJSON *jlimit = cJSON_GetObjectItem(params, "limit");
	const char *action, *file;
	uint32_t limit = 0;
	switch_status_t st = SWITCH_STATUS_FALSE;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	action = (jaction && cJSON_IsString(jaction) && !zstr(jaction->valuestring)) ? jaction->valuestring : "RECORD";
	file = (jfile && cJSON_IsString(jfile) && !zstr(jfile->valuestring)) ? jfile->valuestring : NULL;
	if (!file || switch_stristr("..", file)) {
		return SWITCH_STATUS_FALSE;
	}
	if (jlimit && cJSON_IsNumber(jlimit) && jlimit->valueint > 0) {
		limit = (uint32_t) jlimit->valueint;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	if (!strcasecmp(action, "RECORD") || !strcasecmp(action, "START")) {
		st = switch_ivr_record_session_event(session, file, limit, NULL, NULL);
	} else if (!strcasecmp(action, "STOP")) {
		st = switch_ivr_stop_record_session(session, file);
	} else if (!strcasecmp(action, "PAUSE")) {
		st = switch_ivr_record_session_pause(session, file, SWITCH_TRUE);
	} else if (!strcasecmp(action, "RESUME")) {
		st = switch_ivr_record_session_pause(session, file, SWITCH_FALSE);
	} else if (!strcasecmp(action, "MASK")) {
		st = switch_ivr_record_session_mask(session, file, SWITCH_TRUE);
	} else if (!strcasecmp(action, "UNMASK")) {
		st = switch_ivr_record_session_mask(session, file, SWITCH_FALSE);
	}
	switch_core_session_rwunlock(session);
	return st == SWITCH_STATUS_SUCCESS ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* XNode.DetectSpeech / fs.channel.detectspeech: background speech detection
 * via the FS core ASR API. mod_nats carries control and text results only;
 * the audio path belongs to whichever module implements switch_asr_interface
 * (mod_test, mod_pocketsphinx, mod_unimrcp, ...). START follows the
 * documented flow: init -> set_params -> load_grammar. */
static switch_status_t mn_detectspeech(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jaction = cJSON_GetObjectItem(params, "action");
	cJSON *jengine = cJSON_GetObjectItem(params, "engine");
	cJSON *jgrammar = cJSON_GetObjectItem(params, "grammar");
	cJSON *jname = cJSON_GetObjectItem(params, "name");
	cJSON *jdest = cJSON_GetObjectItem(params, "dest");
	cJSON *jparams = cJSON_GetObjectItem(params, "params");
	cJSON *item;
	const char *action, *engine, *grammar, *name, *dest;
	switch_status_t st = SWITCH_STATUS_FALSE;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	action = (jaction && cJSON_IsString(jaction) && !zstr(jaction->valuestring)) ? jaction->valuestring : "START";

	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}

	if (!strcasecmp(action, "STOP")) {
		st = switch_ivr_stop_detect_speech(session);
	} else if (!strcasecmp(action, "PAUSE")) {
		st = switch_ivr_pause_detect_speech(session);
	} else if (!strcasecmp(action, "RESUME")) {
		st = switch_ivr_resume_detect_speech(session);
	} else if (!strcasecmp(action, "START")) {
		engine = (jengine && cJSON_IsString(jengine) && !zstr(jengine->valuestring)) ? jengine->valuestring : NULL;
		grammar = (jgrammar && cJSON_IsString(jgrammar) && !zstr(jgrammar->valuestring)) ? jgrammar->valuestring : NULL;
		if (!engine || !grammar) {
			switch_core_session_rwunlock(session);
			return SWITCH_STATUS_FALSE;
		}
		name = (jname && cJSON_IsString(jname) && !zstr(jname->valuestring)) ? jname->valuestring : "grammar";
		dest = (jdest && cJSON_IsString(jdest) && !zstr(jdest->valuestring)) ? jdest->valuestring : NULL;

		st = switch_ivr_detect_speech_init(session, engine, dest, NULL);
		if (st != SWITCH_STATUS_SUCCESS) {
			switch_core_session_rwunlock(session);
			if (extra) {
				cJSON_AddNumberToObject(extra, "code", 400);
				cJSON_AddStringToObject(extra, "message", "asr engine unavailable");
			}
			return SWITCH_STATUS_FALSE;
		}
		if (jparams && cJSON_IsObject(jparams)) {
			cJSON_ArrayForEach(item, jparams) {
				if (item->string && cJSON_IsString(item) && !zstr(item->valuestring)) {
					switch_ivr_set_param_detect_speech(session, item->string, item->valuestring);
				}
			}
		}
		st = switch_ivr_detect_speech_load_grammar(session, grammar, name);
		if (st != SWITCH_STATUS_SUCCESS) {
			switch_ivr_stop_detect_speech(session);
			switch_core_session_rwunlock(session);
			if (extra) {
				cJSON_AddNumberToObject(extra, "code", 400);
				cJSON_AddStringToObject(extra, "message", "grammar load failed");
			}
			return SWITCH_STATUS_FALSE;
		}
	} else {
		switch_core_session_rwunlock(session);
		return SWITCH_STATUS_FALSE;
	}

	switch_core_session_rwunlock(session);
	return st == SWITCH_STATUS_SUCCESS ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* Remember which peer a threeway set aside, so UnBridge2 can find it. */
static void chan_save_peer(const char *uuid, const char *peer)
{
	mod_nats_chan_t *chan;

	if (zstr(uuid)) {
		return;
	}
	switch_mutex_lock(mod_nats_globals.chan_mutex);
	chan = (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, uuid);
	if (chan) {
		if (zstr(peer)) {
			chan->saved_peer[0] = '\0';
		} else {
			switch_copy_string(chan->saved_peer, peer, sizeof(chan->saved_peer));
		}
	}
	switch_mutex_unlock(mod_nats_globals.chan_mutex);
}

/* XNode.Transfer / fs.channel.transfer: move the channel to a dialplan
 * extension (switch_ivr_session_transfer). */
static switch_status_t mn_transfer(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jdest = cJSON_GetObjectItem(params, "dest");
	cJSON *jdialplan = cJSON_GetObjectItem(params, "dialplan");
	cJSON *jcontext = cJSON_GetObjectItem(params, "context");
	const char *dest = (jdest && cJSON_IsString(jdest) && !zstr(jdest->valuestring)) ? jdest->valuestring : NULL;
	switch_status_t st;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!dest) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	st = switch_ivr_session_transfer(session, dest,
									 (jdialplan && cJSON_IsString(jdialplan) && !zstr(jdialplan->valuestring)) ? jdialplan->valuestring : NULL,
									 (jcontext && cJSON_IsString(jcontext) && !zstr(jcontext->valuestring)) ? jcontext->valuestring : NULL);
	switch_core_session_rwunlock(session);
	return st == SWITCH_STATUS_SUCCESS ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* XNode.Hold / fs.channel.hold: hold or unhold the channel. The channel
 * itself is addressed; media falls back to MOH while held. */
static switch_status_t mn_hold(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	cJSON *jaction = cJSON_GetObjectItem(params, "action");
	const char *action = (jaction && cJSON_IsString(jaction) && !zstr(jaction->valuestring)) ? jaction->valuestring : "HOLD";
	cJSON *juuid = cJSON_GetObjectItem(params, "uuid");
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!juuid || !cJSON_IsString(juuid) || zstr(juuid->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if (!strcasecmp(action, "UNHOLD")) {
		return switch_ivr_unhold_uuid(juuid->valuestring) == SWITCH_STATUS_SUCCESS
				   ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
	}
	return switch_ivr_hold_uuid(juuid->valuestring, NULL, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS
			   ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* XNode.Mute / fs.channel.mute: mute or unmute media on the channel
 * (switch_ivr_session_audio). level: read | write | both (default all→both).
 * Unmute stops the audio bug entirely, matching uuid_audio's semantics. */
static switch_status_t mn_mute(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jmute = cJSON_GetObjectItem(params, "mute");
	cJSON *jlevel = cJSON_GetObjectItem(params, "level");
	const char *level;
	switch_status_t st;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	level = (jlevel && cJSON_IsString(jlevel) && !zstr(jlevel->valuestring)) ? jlevel->valuestring : "both";
	if (strcasecmp(level, "read") && strcasecmp(level, "write") && strcasecmp(level, "both")) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	if (jmute && cJSON_IsBool(jmute) && jmute->valueint == 0) {
		st = switch_ivr_stop_session_audio(session);
	} else {
		st = switch_ivr_session_audio(session, "mute", level, 0);
	}
	switch_core_session_rwunlock(session);
	return st == SWITCH_STATUS_SUCCESS ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* XNode.ThreeWay / fs.channel.threeway: bring b_uuid into the call. The
 * current partner (if any) is put on hold and remembered on the binding;
 * UnBridge2 later splits the call and unholds it. */
static switch_status_t mn_threeway(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jb = cJSON_GetObjectItem(params, "b_uuid");
	char uuid_a[SWITCH_UUID_FORMATTED_LENGTH + 1];
	const char *peer;
	switch_status_t st;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!jb || !cJSON_IsString(jb) || zstr(jb->valuestring)) {
		return SWITCH_STATUS_FALSE;
	}
	if ((own = require_owner_uuid(ctx, extra, jb->valuestring)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!(session = session_from_params(params, uuid_a, sizeof(uuid_a)))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	peer = switch_channel_get_partner_uuid(switch_core_session_get_channel(session));
	if (!zstr(peer)) {
		/* silent hold: moh=FALSE keeps switch_ivr_hold from broadcasting
		 * MOH into the peer's session, which would interrupt whatever app
		 * it is running (park, collect, ...) */
		switch_ivr_hold_uuid(peer, NULL, SWITCH_FALSE);
		chan_save_peer(uuid_a, peer);
	}
	switch_core_session_rwunlock(session);
	st = switch_ivr_uuid_bridge(uuid_a, jb->valuestring);
	if (st != SWITCH_STATUS_SUCCESS) {
		chan_save_peer(uuid_a, NULL);
	}
	return st == SWITCH_STATUS_SUCCESS ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
}

/* XNode.UnBridge2 / fs.channel.unbridge2: split the call - both legs are
 * transferred to dest (an extension the deployment provides; typically an
 * answer+park one) so either can be re-bridged. Both legs are pre-armed
 * with park_after_bridge so the unbridge cannot tear either down before
 * its own transfer lands. A peer set aside by threeway is unheld so the
 * original call can resume. */
static switch_status_t mn_unbridge2(mod_nats_req_ctx_t *ctx, cJSON *params, cJSON *extra)
{
	switch_core_session_t *session;
	cJSON *jb = cJSON_GetObjectItem(params, "b_uuid");
	cJSON *jdest = cJSON_GetObjectItem(params, "dest");
	cJSON *jcontext = cJSON_GetObjectItem(params, "context");
	const char *buuid = (jb && cJSON_IsString(jb) && !zstr(jb->valuestring)) ? jb->valuestring : NULL;
	const char *dest = (jdest && cJSON_IsString(jdest) && !zstr(jdest->valuestring)) ? jdest->valuestring : NULL;
	const char *context = (jcontext && cJSON_IsString(jcontext) && !zstr(jcontext->valuestring)) ? jcontext->valuestring : NULL;
	char saved[SWITCH_UUID_FORMATTED_LENGTH + 1] = "";
	switch_status_t st;
	switch_status_t own;

	if ((own = require_owner(ctx, params, extra)) != SWITCH_STATUS_SUCCESS) {
		return own;
	}
	if (!buuid || !dest) {
		return SWITCH_STATUS_FALSE;
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	/* pre-arm both legs: transferring the first one unbridges the pair, and
	 * the freed leg must not run a post-bridge hangup before its own
	 * transfer lands */
	switch_channel_set_variable(switch_core_session_get_channel(session), "park_after_bridge", "true");
	switch_core_session_rwunlock(session);
	{
		switch_core_session_t *bs = switch_core_session_locate(buuid);
		if (bs) {
			switch_channel_set_variable(switch_core_session_get_channel(bs), "park_after_bridge", "true");
			switch_core_session_rwunlock(bs);
		}
	}
	if (!(session = session_from_params(params, NULL, 0))) {
		return SWITCH_STATUS_NOTFOUND;
	}
	st = switch_ivr_session_transfer(session, dest, NULL, context);
	switch_core_session_rwunlock(session);
	if (st != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}
	{
		switch_core_session_t *bs = switch_core_session_locate(buuid);
		if (bs) {
			switch_ivr_session_transfer(bs, dest, NULL, context);
			switch_core_session_rwunlock(bs);
		}
	}
	{
		/* recall the peer this module held aside during threeway */
		cJSON *juuid = cJSON_GetObjectItem(params, "uuid");
		const char *u = (juuid && cJSON_IsString(juuid)) ? juuid->valuestring : NULL;
		mod_nats_chan_t *chan;

		switch_mutex_lock(mod_nats_globals.chan_mutex);
		chan = u ? (mod_nats_chan_t *) switch_core_hash_find(mod_nats_globals.chan_hash, u) : NULL;
		if (chan) {
			switch_copy_string(saved, chan->saved_peer, sizeof(saved));
			chan->saved_peer[0] = '\0';
		}
		switch_mutex_unlock(mod_nats_globals.chan_mutex);
	}
	if (!zstr(saved) && strcmp(saved, buuid)) {
		switch_ivr_unhold_uuid(saved);
	}
	return SWITCH_STATUS_SUCCESS;
}

/* Method table: canonical fs.* names with XCC aliases (xctrl SDK compat,
 * gated by the compat-xcc config). Keep names in sync with README. */
const mod_nats_method_t mod_nats_methods[] = {
	{"fs.node.hello", NULL, mn_hello, SWITCH_FALSE},
	{"fs.channel.accept", "XNode.Accept", mn_accept, SWITCH_TRUE},
	{"fs.channel.observe", NULL, mn_observe, SWITCH_TRUE},
	{"fs.channel.unobserve", NULL, mn_unobserve, SWITCH_TRUE},
	{"fs.channel.touch", "XNode.Touch", mn_touch, SWITCH_TRUE},
	{"fs.channel.answer", "XNode.Answer", mn_answer, SWITCH_TRUE},
	{"fs.channel.hangup", "XNode.Hangup", mn_hangup, SWITCH_TRUE},
	{"fs.channel.play", "XNode.Play", mn_play, SWITCH_TRUE},
	{"fs.channel.stop", "XNode.Stop", mn_stop, SWITCH_TRUE},
	{"fs.channel.broadcast", "XNode.Broadcast", mn_broadcast, SWITCH_TRUE},
	{"fs.channel.record", "XNode.Record", mn_record, SWITCH_TRUE},
	{"fs.channel.detectspeech", "XNode.DetectSpeech", mn_detectspeech, SWITCH_TRUE},
	{"fs.channel.transfer", "XNode.Transfer", mn_transfer, SWITCH_TRUE},
	{"fs.channel.hold", "XNode.Hold", mn_hold, SWITCH_TRUE},
	{"fs.channel.mute", "XNode.Mute", mn_mute, SWITCH_TRUE},
	{"fs.channel.threeway", "XNode.ThreeWay", mn_threeway, SWITCH_TRUE},
	{"fs.channel.unbridge2", "XNode.UnBridge2", mn_unbridge2, SWITCH_TRUE},
	{"fs.channel.bridge", "XNode.Bridge", bridge_two, SWITCH_TRUE},
	{"fs.channel.setvar", "XNode.SetVar", mn_setvar, SWITCH_TRUE},
	{"fs.channel.getvar", "XNode.GetVar", mn_getvar, SWITCH_TRUE},
	{"fs.channel.getstate", "XNode.GetState", mn_getstate, SWITCH_TRUE},
	{"fs.channel.data", "XNode.GetChannelData", mn_getchandata, SWITCH_TRUE},
	{"fs.native.app", "XNode.NativeApp", mn_nativeapp, SWITCH_TRUE},
	{"fs.native.api", "XNode.NativeAPI", mn_nativeapi, SWITCH_FALSE},
	{"fs.native.jsapi", "XNode.NativeJSAPI", mn_nativejsapi, SWITCH_FALSE},
	{"fs.node.status", "XNode.JStatus", mn_jstatus, SWITCH_FALSE},
	{"fs.channel.dial", "XNode.Dial", mn_dial, SWITCH_FALSE},
	{NULL, NULL, NULL, SWITCH_FALSE}
};
