/*
 * mod_nats connection layer: owns the cnats connection, the inbound
 * subscription and the outbound publish queue.
 *
 * Threading rules:
 *  - cnats delivers subscription messages on its own threads; the handler
 *    only copies and enqueues, never blocks, never touches FS core.
 *  - Request workers (nats_proto.c) drain req_queue and execute FS calls.
 *  - A single publisher thread drains pub_queue so FS event callbacks are
 *    never blocked by socket I/O; overflow drops and counts.
 */
#include "mod_nats.h"

static void *SWITCH_THREAD_FUNC publisher_thread(switch_thread_t *t, void *data)
{
	mod_nats_pub_t *pub;

	while (mod_nats_globals.running) {
		void *pop = NULL;
		switch_status_t st = switch_queue_pop(mod_nats_globals.pub_queue, &pop);

		if (!mod_nats_globals.running) {
			if (pop) {
				pub = (mod_nats_pub_t *) pop;
				switch_safe_free(pub->subject);
				switch_safe_free(pub->reply);
				switch_safe_free(pub->payload);
				switch_safe_free(pub);
			}
			continue;
		}

		if (st != SWITCH_STATUS_SUCCESS || !pop) {
			continue;
		}

		pub = (mod_nats_pub_t *) pop;

		if (mod_nats_globals.nc && (natsConnection_Status(mod_nats_globals.nc) == NATS_CONN_STATUS_CONNECTED)) {
			natsStatus ns;
			if (!zstr(pub->request_id)) {
				natsMsg *hm = NULL;
				if (natsMsg_Create(&hm, pub->subject, zstr(pub->reply) ? NULL : pub->reply,
									  pub->payload, (int) strlen(pub->payload)) == NATS_OK) {
					natsMsgHeader_Set(hm, "X-Request-Id", pub->request_id);
					ns = natsConnection_PublishMsg(mod_nats_globals.nc, hm);
					natsMsg_Destroy(hm);
				} else {
					ns = NATS_ERR;
				}
			} else if (!zstr(pub->reply)) {
				ns = natsConnection_PublishRequest(mod_nats_globals.nc, pub->subject, pub->reply,
												   (const void *) pub->payload, (int) strlen(pub->payload));
			} else {
				ns = natsConnection_Publish(mod_nats_globals.nc, pub->subject,
											(const void *) pub->payload, (int) strlen(pub->payload));
			}
			if (ns == NATS_OK) {
				switch_mutex_lock(mod_nats_globals.mutex);
				mod_nats_globals.msgs_out++;
				switch_mutex_unlock(mod_nats_globals.mutex);
			} else {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  MOD_NATS_NAME " publish to '%s' failed: %s\n", pub->subject, natsStatus_GetText(ns));
				switch_mutex_lock(mod_nats_globals.mutex);
				mod_nats_globals.pub_errors++;
				switch_mutex_unlock(mod_nats_globals.mutex);
			}
		} else {
			switch_mutex_lock(mod_nats_globals.mutex);
			mod_nats_globals.msgs_dropped++;
			switch_mutex_unlock(mod_nats_globals.mutex);
		}

		switch_safe_free(pub->subject);
		switch_safe_free(pub->reply);
		switch_safe_free(pub->payload);
		switch_safe_free(pub->request_id);
		switch_safe_free(pub);
	}

	return NULL;
}

/* Runs on a cnats thread: copy and enqueue only. */
static void on_node_message(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
	mod_nats_req_t *req;
	const char *subj = natsMsg_GetSubject(msg);
	const char *reply = natsMsg_GetReply(msg);
	const char *data = natsMsg_GetData(msg);
	int len = (int) natsMsg_GetDataLength(msg);

	/* transient object: malloc/free, not pool memory (requests are unbounded) */
	req = (mod_nats_req_t *) malloc(sizeof(*req));
	if (!req) {
		natsMsg_Destroy(msg);
		return;
	}
	memset(req, 0, sizeof(*req));
	req->subject = strdup(subj);
	req->reply = zstr(reply) ? NULL : strdup(reply);
	if (len > 0) {
		req->payload = (char *) malloc((size_t) len + 1);
		memcpy(req->payload, data, (size_t) len);
		req->payload[len] = '\0';
	} else {
		req->payload = strdup("{}");
	}
	if (!req->subject || !req->payload) {
		switch_safe_free(req->subject);
		switch_safe_free(req->reply);
		switch_safe_free(req->payload);
		switch_safe_free(req->request_id);
		switch_safe_free(req);
		natsMsg_Destroy(msg);
		return;
	}

	/* capture the X-Request-Id NATS header for correlation */
	{
		const char *rid = NULL;
		if (natsMsgHeader_Get(msg, "X-Request-Id", &rid) == NATS_OK && !zstr(rid)) {
			req->request_id = strdup(rid);
		}
	}

	if (switch_queue_trypush(mod_nats_globals.req_queue, req) != SWITCH_STATUS_SUCCESS) {
		/* request backlog full: reply 503 instead of letting the client time out */
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " request queue full, replying 503\n");
		if (!zstr(reply)) {
			mod_nats_proto_send_error(reply, "", 503, "request queue full");
		}
		switch_safe_free(req->subject);
		switch_safe_free(req->reply);
		switch_safe_free(req->payload);
		switch_safe_free(req);
	}

	natsMsg_Destroy(msg);
}

switch_status_t mod_nats_publish_enqueue_hdr(const char *subject, const char *reply, const char *payload, const char *request_id)
{
	mod_nats_pub_t *pub;

	if (zstr(subject) || zstr(payload)) {
		return SWITCH_STATUS_FALSE;
	}

	pub = (mod_nats_pub_t *) malloc(sizeof(*pub));
	if (!pub) {
		return SWITCH_STATUS_MEMERR;
	}
	memset(pub, 0, sizeof(*pub));
	pub->subject = strdup(subject);
	pub->reply = zstr(reply) ? NULL : strdup(reply);
	pub->payload = strdup(payload);
	pub->request_id = zstr(request_id) ? NULL : strdup(request_id);

	if (!pub->subject || !pub->payload || switch_queue_trypush(mod_nats_globals.pub_queue, pub) != SWITCH_STATUS_SUCCESS) {
		switch_safe_free(pub->subject);
		switch_safe_free(pub->reply);
		switch_safe_free(pub->payload);
		switch_safe_free(pub->request_id);
		switch_safe_free(pub);
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t mod_nats_publish_enqueue(const char *subject, const char *reply, const char *payload)
{
	return mod_nats_publish_enqueue_hdr(subject, reply, payload, NULL);
}
const char *mod_nats_conn_state_name(void)
{
	/* live query: no connection callbacks are registered, so there is no
	 * state to maintain (and nothing for cnats async threads to run during
	 * connection teardown on reload). */
	switch_mutex_lock(mod_nats_globals.mutex);
	if (mod_nats_globals.nc && natsConnection_Status(mod_nats_globals.nc) == NATS_CONN_STATUS_CONNECTED) {
		switch_mutex_unlock(mod_nats_globals.mutex);
		return "UP";
	}
	switch_mutex_unlock(mod_nats_globals.mutex);
	return "DOWN";
}

switch_status_t mod_nats_conn_start(void)
{
	switch_status_t status;
	natsStatus ns;
	natsOptions *opts = NULL;
	switch_threadattr_t *thd_attr;

	ns = natsOptions_Create(&opts);
	if (ns != NATS_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " natsOptions_Create failed\n");
		return SWITCH_STATUS_GENERR;
	}

	natsOptions_SetURL(opts, mod_nats_globals.urls);
	natsOptions_SetReconnectWait(opts, 2000);
	natsOptions_SetMaxReconnect(opts, -1);	/* reconnect forever */
	natsOptions_SetRetryOnFailedConnect(opts, 1, NULL, NULL);	/* connect async, conn usable immediately */

	if (!zstr(mod_nats_globals.credentials)) {
		natsOptions_SetUserCredentialsFromFiles(opts, mod_nats_globals.credentials, NULL);
	} else if (!zstr(mod_nats_globals.user)) {
		natsOptions_SetUserInfo(opts, mod_nats_globals.user, switch_str_nil(mod_nats_globals.password));
	}

	ns = natsConnection_Connect(&mod_nats_globals.nc, opts);
	natsOptions_Destroy(opts);
	if (ns != NATS_OK) {
		const char *err = nats_GetLastError(NULL);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " connect to '%s' failed: %s (%s)\n",
						  mod_nats_globals.urls, natsStatus_GetText(ns), switch_str_nil(err));
		return SWITCH_STATUS_GENERR;
	}

	ns = natsConnection_Subscribe(&mod_nats_globals.sub_node, mod_nats_globals.nc,
								  mod_nats_subject_node(), on_node_message, NULL);
	if (ns != NATS_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " subscribe '%s' failed: %s\n",
						  mod_nats_subject_node(), natsStatus_GetText(ns));
		natsConnection_Destroy(mod_nats_globals.nc);
		mod_nats_globals.nc = NULL;
		return SWITCH_STATUS_GENERR;
	}

	switch_mutex_lock(mod_nats_globals.mutex);
	mod_nats_globals.conn_state = (natsConnection_Status(mod_nats_globals.nc) == NATS_CONN_STATUS_CONNECTED) ? MN_CONN_UP : MN_CONN_CONNECTING;
	switch_mutex_unlock(mod_nats_globals.mutex);

	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	switch_threadattr_detach_set(thd_attr, 1);
	status = switch_thread_create(&mod_nats_globals.pub_thread, thd_attr, publisher_thread, NULL, mod_nats_globals.pool);
	if (status != SWITCH_STATUS_SUCCESS) {
		return status;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
					  MOD_NATS_NAME " connected: urls=%s prefix=%s node=%s listen=%s\n",
					  mod_nats_globals.urls, mod_nats_globals.subject_prefix,
					  mod_nats_globals.node_uuid, mod_nats_subject_node());

	return SWITCH_STATUS_SUCCESS;
}

void mod_nats_conn_stop(void)
{
	if (mod_nats_globals.sub_node) {
		natsSubscription_Destroy(mod_nats_globals.sub_node);
		mod_nats_globals.sub_node = NULL;
	}
	if (mod_nats_globals.nc) {
		natsConnection_Destroy(mod_nats_globals.nc);
		mod_nats_globals.nc = NULL;
	}
	switch_mutex_lock(mod_nats_globals.mutex);
	mod_nats_globals.conn_state = MN_CONN_DOWN;
	switch_mutex_unlock(mod_nats_globals.mutex);
}
