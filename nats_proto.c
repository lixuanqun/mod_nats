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

const char *mod_nats_subject_metrics(void)
{
	static MOD_NATS_TLS char buf[MOD_NATS_PREFIX_MAX + 32];
	snprintf(buf, sizeof(buf), "%smetrics", mod_nats_globals.subject_prefix);
	return buf;
}

const char *mod_nats_subject_cdr(void)
{
	if (!zstr(mod_nats_globals.cdr_subject)) {
		return mod_nats_globals.cdr_subject;
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

void mod_nats_proto_send_reply_hdr(const char *reply, const char *rpc_id, const char *rpc_id_header, cJSON *result)
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
	mod_nats_proto_send_reply_hdr(reply, rpc_id, NULL, result);
}

void mod_nats_proto_send_error(const char *reply, const char *rpc_id, int code, const char *message)
{
	mod_nats_proto_send_reply_hdr(reply, rpc_id, NULL, build_result(code, message));
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
	}
	/* no id => notification: execute but never reply */

	extra = cJSON_CreateObject();
	{
		mod_nats_req_ctx_t ctx;
		cJSON *jctrl = cJSON_GetObjectItem(params, "ctrl_uuid");
		memset(&ctx, 0, sizeof(ctx));
		switch_copy_string(ctx.rpc_id, rpc_id, sizeof(ctx.rpc_id));
		if (!zstr(req->request_id)) {
			switch_copy_string(ctx.request_id, req->request_id, sizeof(ctx.request_id));
		}
		switch_copy_string(req_hdr, ctx.request_id, sizeof(req_hdr));
		if (jctrl && cJSON_IsString(jctrl) && !zstr(jctrl->valuestring)) {
			switch_copy_string(ctx.ctrl_uuid, jctrl->valuestring, sizeof(ctx.ctrl_uuid));
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
	if (!zstr(rpc_id)) {
		cJSON *result = build_result(code, msg);
		cJSON *item;

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
		mod_nats_proto_send_reply_hdr(req->reply, rpc_id, req_hdr[0] ? req_hdr : NULL, result);
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
