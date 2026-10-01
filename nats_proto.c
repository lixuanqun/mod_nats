/*
 * mod_nats protocol layer: XCC-compatible JSON-RPC 2.0 envelope over NATS.
 *
 * Request:  {"jsonrpc":"2.0","id":"<string>","method":"XNode.Answer","params":{...}}
 *           (missing id => notification, no response is sent)
 * Response: {"jsonrpc":"2.0","id":"<string>","result":{"code":200,"message":"OK","node_uuid":"..."}}
 *
 * result.code follows XCC semantics: 200 OK, 202 accepted (result follows as
 * an event), 400 bad request, 404 no such channel, 419 conflict, 500 error,
 * 501 not implemented.
 */
#include "mod_nats.h"

#ifdef _MSC_VER
#define MOD_NATS_TLS __declspec(thread)
#else
#define MOD_NATS_TLS __thread
#endif

const char *mod_nats_subject_node(void)
{
	static MOD_NATS_TLS char buf[MOD_NATS_PREFIX_MAX + SWITCH_UUID_FORMATTED_LENGTH + 16];
	snprintf(buf, sizeof(buf), "%snode.%s", mod_nats_globals.subject_prefix, mod_nats_globals.node_uuid);
	return buf;
}

const char *mod_nats_subject_ctrl(const char *ctrl_uuid)
{
	static MOD_NATS_TLS char buf[MOD_NATS_PREFIX_MAX + SWITCH_UUID_FORMATTED_LENGTH + 16];
	snprintf(buf, sizeof(buf), "%sctrl.%s", mod_nats_globals.subject_prefix, switch_str_nil(ctrl_uuid));
	return buf;
}

const char *mod_nats_subject_event(const char *event_name)
{
	static MOD_NATS_TLS char buf[MOD_NATS_PREFIX_MAX + 128];
	snprintf(buf, sizeof(buf), "%sevent.%s", mod_nats_globals.subject_prefix, switch_str_nil(event_name));
	return buf;
}

/* CDR and the metrics heartbeat are the durable classes when js-cdr /
 * js-metrics are on. Match the exact subject those publishers use, including
 * a custom cdr-subject. Channel events stay core NATS on purpose. */
int mod_nats_subject_is_persistent(const char *subject)
{
	if (zstr(subject)) {
		return 0;
	}
	if (mod_nats_globals.js_cdr == SWITCH_TRUE && !strcmp(subject, mod_nats_subject_cdr())) {
		return 1;
	}
	if (mod_nats_globals.js_metrics == SWITCH_TRUE && !strcmp(subject, mod_nats_subject_metrics())) {
		return 1;
	}
	return 0;
}

const char *mod_nats_subject_metrics(void)
{
	static MOD_NATS_TLS char buf[MOD_NATS_PREFIX_MAX + 32];
	snprintf(buf, sizeof(buf), "%smetrics", mod_nats_globals.subject_prefix);
	return buf;
}

const char *mod_nats_subject_cdr(void)
{
	static MOD_NATS_TLS char buf[512];

	if (mod_nats_globals.mutex) {
		switch_mutex_lock(mod_nats_globals.mutex);
	}
	if (!zstr(mod_nats_globals.cdr_subject)) {
		switch_copy_string(buf, mod_nats_globals.cdr_subject, sizeof(buf));
		if (mod_nats_globals.mutex) {
			switch_mutex_unlock(mod_nats_globals.mutex);
		}
		return buf;
	}
	if (mod_nats_globals.mutex) {
		switch_mutex_unlock(mod_nats_globals.mutex);
	}
	return mod_nats_subject_event("cdr");
}

static cJSON *build_result(int code, const char *message)
{
	cJSON *result = cJSON_CreateObject();
	cJSON_AddNumberToObject(result, "code", code);
	cJSON_AddStringToObject(result, "message", switch_str_nil(message));
	cJSON_AddStringToObject(result, "node_uuid", mod_nats_globals.node_uuid);
	return result;
}

/* --------------------------------------------------------------------- */
/* idempotency result cache: (uuid|node, ctrl_uuid, key) -> serialized   */
/* 2xx reply. First store wins, failures are never cached so a retry     */
/* after a failure re-executes, and eviction is FIFO by insertion.       */

typedef struct mod_nats_idem_entry_s {
	char *json;
} mod_nats_idem_entry_t;

static void idem_entry_free(void *ptr)
{
	mod_nats_idem_entry_t *e = (mod_nats_idem_entry_t *) ptr;

	if (e) {
		switch_safe_free(e->json);
		free(e);
	}
}

void mod_nats_proto_idem_init(void)
{
	if (mod_nats_globals.idem_cache_size <= 0 || mod_nats_globals.idem_hash) {
		return;
	}
	if (switch_core_hash_init(&mod_nats_globals.idem_hash) != SWITCH_STATUS_SUCCESS) {
		return;
	}
	if (switch_queue_create(&mod_nats_globals.idem_fifo, 65536, mod_nats_globals.pool) != SWITCH_STATUS_SUCCESS) {
		mod_nats_globals.idem_fifo = NULL;
	}
}

void mod_nats_proto_idem_shutdown(void)
{
	void *pop = NULL;

	while (mod_nats_globals.idem_fifo &&
		   switch_queue_trypop(mod_nats_globals.idem_fifo, &pop) == SWITCH_STATUS_SUCCESS && pop) {
		free(pop);
	}
	if (mod_nats_globals.idem_hash) {
		switch_core_hash_destroy(&mod_nats_globals.idem_hash);
		mod_nats_globals.idem_hash = NULL;
	}
	mod_nats_globals.idem_fifo = NULL;
}

/* Returns a freshly parsed copy of the stored result (caller consumes). */
cJSON *mod_nats_proto_idem_lookup(const char *key)
{
	mod_nats_idem_entry_t *e = NULL;
	cJSON *parsed = NULL;

	if (!mod_nats_globals.idem_hash || zstr(key)) {
		return NULL;
	}
	switch_mutex_lock(mod_nats_globals.mutex);
	e = (mod_nats_idem_entry_t *) switch_core_hash_find(mod_nats_globals.idem_hash, key);
	if (e && e->json) {
		parsed = cJSON_Parse(e->json);
	}
	if (e) {
		mod_nats_globals.idem_hits++;
	}
	switch_mutex_unlock(mod_nats_globals.mutex);
	return parsed;
}

void mod_nats_proto_idem_store(const char *key, const char *json)
{
	mod_nats_idem_entry_t *e;
	char *fifo_key;

	if (!mod_nats_globals.idem_hash || !mod_nats_globals.idem_fifo || zstr(key) || zstr(json)) {
		return;
	}
	switch_mutex_lock(mod_nats_globals.mutex);
	if (switch_core_hash_find(mod_nats_globals.idem_hash, key)) {
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;						/* first store wins */
	}
	e = (mod_nats_idem_entry_t *) calloc(1, sizeof(*e));
	if (!e) {
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;
	}
	e->json = strdup(json);
	if (!e->json) {
		switch_safe_free(e);
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;
	}
	if (switch_core_hash_insert_destructor(mod_nats_globals.idem_hash, key, e, idem_entry_free) != SWITCH_STATUS_SUCCESS) {
		idem_entry_free(e);
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;
	}
	while (switch_queue_size(mod_nats_globals.idem_fifo) >= (unsigned int) mod_nats_globals.idem_cache_size) {
		void *old = NULL;

		if (switch_queue_trypop(mod_nats_globals.idem_fifo, &old) != SWITCH_STATUS_SUCCESS || !old) {
			break;
		}
		switch_core_hash_delete(mod_nats_globals.idem_hash, (const char *) old);
		free(old);
	}
	if ((fifo_key = strdup(key)) != NULL) {
		if (switch_queue_trypush(mod_nats_globals.idem_fifo, fifo_key) != SWITCH_STATUS_SUCCESS) {
			free(fifo_key);
		}
	}
	mod_nats_globals.idem_stores++;
	switch_mutex_unlock(mod_nats_globals.mutex);
}

void mod_nats_proto_send_reply_hdr(const char *reply, const char *rpc_id, int rpc_id_is_number, const char *rpc_id_header, cJSON *result)
{
	cJSON *env;
	char *payload;

	if (zstr(reply) || !result) {
		if (result) {
			cJSON_Delete(result);
		}
		return;
	}

	env = cJSON_CreateObject();
	cJSON_AddStringToObject(env, "jsonrpc", "2.0");
	if (zstr(rpc_id)) {
		/* JSON-RPC 2.0: unknown request id is reported as null */
		cJSON_AddNullToObject(env, "id");
	} else if (rpc_id_is_number) {
		cJSON_AddNumberToObject(env, "id", atof(rpc_id));
	} else {
		cJSON_AddStringToObject(env, "id", rpc_id);
	}
	cJSON_AddItemToObject(env, "result", result);

	payload = cJSON_PrintUnformatted(env);
	if (payload) {
		mod_nats_publish_enqueue_hdr(reply, NULL, payload, rpc_id_header);
		free(payload);
	}
	cJSON_Delete(env);
}

void mod_nats_proto_send_reply(const char *reply, const char *rpc_id, cJSON *result)
{
	mod_nats_proto_send_reply_hdr(reply, rpc_id, 0, NULL, result);
}

void mod_nats_proto_send_error(const char *reply, const char *rpc_id, int code, const char *message)
{
	mod_nats_proto_send_reply_hdr(reply, rpc_id, 0, NULL, build_result(code, message));
}

static const mod_nats_method_t *find_method(const char *name)
{
	const mod_nats_method_t *m;

	if (zstr(name)) {
		return NULL;
	}
	for (m = mod_nats_methods; m->name; m++) {
		if (!strcmp(m->name, name)) {
			return m;
		}
		if (mod_nats_globals.compat_xcc == SWITCH_TRUE && m->xcc_alias && !strcmp(m->xcc_alias, name)) {
			return m;
		}
	}
	return NULL;
}

void mod_nats_proto_handle_request(mod_nats_req_t *req)
{
	cJSON *json = NULL;
	cJSON *id = NULL, *method = NULL, *params = NULL, *params_owned = NULL, *extra = NULL;
	const mod_nats_method_t *m;
	char rpc_id[128] = "";
	char req_hdr[128] = "";
	char idem_key[512] = "";
	int rpc_id_is_number = 0;
	int replied = 0;
	switch_status_t st;
	int code = 200;
	const char *msg = "OK";

	if (!req || zstr(req->payload)) {
		return;
	}

	switch_mutex_lock(mod_nats_globals.mutex);
	mod_nats_globals.msgs_in++;
	switch_mutex_unlock(mod_nats_globals.mutex);

	json = cJSON_Parse(req->payload);
	if (!json) {
		mod_nats_proto_send_error(req->reply, "", 400, "invalid JSON payload");
		return;
	}

	id = cJSON_GetObjectItem(json, "id");
	method = cJSON_GetObjectItem(json, "method");
	params = cJSON_GetObjectItem(json, "params");
	if (!params) {
		params = params_owned = cJSON_CreateObject();
	}

	if (id && cJSON_IsString(id) && !zstr(id->valuestring)) {
		switch_copy_string(rpc_id, id->valuestring, sizeof(rpc_id));
	} else if (id && cJSON_IsNumber(id)) {
		rpc_id_is_number = 1;
		if (id->valuedouble == (double) id->valueint) {
			snprintf(rpc_id, sizeof(rpc_id), "%d", id->valueint);
		} else {
			snprintf(rpc_id, sizeof(rpc_id), "%g", id->valuedouble);
		}
	}
	/* no id => notification: execute but never reply */

	extra = cJSON_CreateObject();
	{
		mod_nats_req_ctx_t ctx;
		cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
		memset(&ctx, 0, sizeof(ctx));
		switch_copy_string(ctx.rpc_id, rpc_id, sizeof(ctx.rpc_id));
		ctx.rpc_id_is_number = rpc_id_is_number;
		if (!zstr(req->request_id)) {
			switch_copy_string(ctx.request_id, req->request_id, sizeof(ctx.request_id));
		}
		switch_copy_string(req_hdr, ctx.request_id, sizeof(req_hdr));
		if (jctrl && cJSON_IsString(jctrl) && !zstr(jctrl->valuestring)) {
			switch_copy_string(ctx.ctrl_uuid, jctrl->valuestring, sizeof(ctx.ctrl_uuid));
		}

		/* idempotency key: optional client-supplied retry token scoped by
		 * (params.uuid or the node, ctrl_uuid). A repeat whose previous
		 * execution finished 200/202 replays the stored result instead of
		 * running the method again; failures are never cached. */
		{
			cJSON *jkey = cJSON_GetObjectItem(params, "idempotency_key");

			if (jkey && cJSON_IsString(jkey) && !zstr(jkey->valuestring)) {
				if (strlen(jkey->valuestring) > 128) {
					code = 400;
					msg = "idempotency key too long";
					goto finish;
				} else {
					cJSON *juuid = cJSON_GetObjectItem(params, "uuid");
					const char *scope = (juuid && cJSON_IsString(juuid) && !zstr(juuid->valuestring)) ? juuid->valuestring : "*";
					cJSON *cached;

					snprintf(idem_key, sizeof(idem_key), "%s\x1f%s\x1f%s", scope, ctx.ctrl_uuid, jkey->valuestring);
					if ((cached = mod_nats_proto_idem_lookup(idem_key)) != NULL) {
						cJSON_AddBoolToObject(cached, "idempotent_replay", SWITCH_TRUE);
						if (!zstr(rpc_id)) {
							mod_nats_proto_send_reply_hdr(req->reply, rpc_id, rpc_id_is_number,
														  req_hdr[0] ? req_hdr : NULL, cached);
						} else {
							cJSON_Delete(cached);
						}
						replied = 1;
						goto finish;
					}
				}
			}
		}

	if (cJSON_IsString(method) && !zstr(method->valuestring)) {
		m = find_method(method->valuestring);
		if (!m) {
			code = 501;
			msg = "method not implemented";
		} else {
			if (m->needs_channel) {
				cJSON *juuid = cJSON_GetObjectItem(params, "uuid");
				if (!juuid || !cJSON_IsString(juuid) || zstr(juuid->valuestring)) {
					code = 400;
					msg = "missing params.uuid";
					goto finish;
				}
			}
			st = m->fn(&ctx, params, extra);
			switch (st) {
			case SWITCH_STATUS_SUCCESS:
				code = 200;
				msg = "OK";
				break;
			case SWITCH_STATUS_NOTFOUND:
				code = 404;
				msg = "channel not found";
				break;
			case SWITCH_STATUS_FALSE:
				code = 400;
				msg = "request refused";
				break;
			case SWITCH_STATUS_NOTIMPL:
				code = 501;
				msg = "method not implemented";
				break;
			default:
				code = 500;
				msg = "internal error";
				break;
			}
		}
	} else {
		code = 400;
		msg = "missing method";
	}
	} /* ctx scope */

  finish:
	/* A request with an id gets a reply; a keyed notification (no id) skips
	 * the reply but still caches its 2xx result so the retry replays. */
	if (!replied && (!zstr(rpc_id) || idem_key[0])) {
		cJSON *result = build_result(code, msg);
		cJSON *item;
		char *stored = NULL;

		/* method extras override defaults (e.g. Dial's code 202 + job_uuid) */
		while (extra && extra->child && (item = cJSON_DetachItemViaPointer(extra, extra->child)) != NULL) {
			if (!zstr(item->string) && cJSON_GetObjectItem(result, item->string)) {
				cJSON_DeleteItemFromObject(result, item->string);
			}
			if (!zstr(item->string)) {
				cJSON_AddItemToObject(result, item->string, item);
			} else {
				cJSON_Delete(item);
			}
		}
		{
			cJSON *jcode = cJSON_GetObjectItem(result, "code");
			if (idem_key[0] && jcode && (jcode->valueint == 200 || jcode->valueint == 202)) {
				stored = cJSON_PrintUnformatted(result);
			}
		}
		if (!zstr(rpc_id)) {
			mod_nats_proto_send_reply_hdr(req->reply, rpc_id, rpc_id_is_number, req_hdr[0] ? req_hdr : NULL, result);
			result = NULL;
		}
		if (idem_key[0]) {
			if (stored) {
				mod_nats_proto_idem_store(idem_key, stored);
			}
			switch_safe_free(stored);
		}
		if (result) {
			cJSON_Delete(result);
		}
	}

	if (extra) {
		cJSON_Delete(extra);
	}
	if (params_owned) {
		cJSON_Delete(params_owned);
	}
	if (json) {
		cJSON_Delete(json);
	}
}
