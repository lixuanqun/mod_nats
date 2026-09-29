/*
 * mod_nats connection layer: owns the cnats connection, the inbound
 * subscription and the outbound publish queue.
 *
 * Threading rules:
 *  - cnats delivers subscription messages on its own threads; the handler
 *    only copies and enqueues, never blocks, never touches FS core.
 *  - Request workers (nats_proto.c) drain req_queue and execute FS calls.
 *  - reply_queue carries RPC replies and Event.Result. Its thread never
 *    waits on a JetStream ack.
 *  - pub_queue carries channel events and node announcements on core NATS.
 *  - js_queue carries CDR and metrics. A JetStream ack cannot stall the
 *    channel-event publisher.
 *  While the connection is down a publisher holds the current message and
 *  leaves the rest queued; it does not drain-and-drop across a reconnect.
 */
#include "mod_nats.h"

static int conn_is_up(void)
{
	natsConnection *nc;

	switch_mutex_lock(mod_nats_globals.mutex);
	nc = mod_nats_globals.nc;
	switch_mutex_unlock(mod_nats_globals.mutex);
	return nc && natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED;
}

/* Block only this message. The rest of the queue stays put until the
 * connection is back. pub_stop ends the wait so unload cannot hang here. */
static void wait_until_connected(void)
{
	while (!mod_nats_globals.pub_stop && !conn_is_up()) {
		switch_sleep(100000);
	}
}

/* One warning per second. Every failure is still counted. */
static int warn_due(void)
{
	static switch_time_t last = 0;
	switch_time_t now = switch_time_now();

	if (last && (now - last) < 1000000) {
		return 0;
	}
	last = now;
	return 1;
}

static void publish_one(mod_nats_pub_t *pub, int allow_js)
{
	natsConnection *nc;

	if (!pub) {
		return;
	}

	if (!conn_is_up()) {
		wait_until_connected();
	}

	switch_mutex_lock(mod_nats_globals.mutex);
	nc = mod_nats_globals.nc;
	switch_mutex_unlock(mod_nats_globals.mutex);

	if (nc && natsConnection_Status(nc) == NATS_CONN_STATUS_CONNECTED) {
		natsStatus ns = NATS_ERR;
		jsCtx *js = NULL;
		int via_js;

		/* snapshot under the mutex; js_Publish blocks on the ack and must not hold it */
		switch_mutex_lock(mod_nats_globals.mutex);
		js = mod_nats_globals.js;
		switch_mutex_unlock(mod_nats_globals.mutex);
		if (allow_js && !js && mod_nats_subject_is_persistent(pub->subject)) {
			mod_nats_js_ensure();
			switch_mutex_lock(mod_nats_globals.mutex);
			js = mod_nats_globals.js;
			switch_mutex_unlock(mod_nats_globals.mutex);
		}
		via_js = allow_js && (js != NULL) && mod_nats_subject_is_persistent(pub->subject);

		if (via_js) {
			jsPubAck *pa = NULL;
			jsPubOptions po;

			/* synchronous ack: a missing stream fails here, so the core-NATS
			 * fallback is real. A timeout is not a failure we can retry —
			 * the server may already have stored the message. */
			jsPubOptions_Init(&po);
			po.MaxWait = 1000;
			ns = js_Publish(&pa, js, pub->subject, (const void *) pub->payload,
							(int) strlen(pub->payload), &po, NULL);
			if (pa) {
				jsPubAck_Destroy(pa);
			}
			if (ns == NATS_TIMEOUT) {
				if (warn_due()) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
									  MOD_NATS_NAME " JetStream ack timeout for '%s'; not republishing\n",
									  pub->subject);
				}
			} else if (ns != NATS_OK) {
				if (warn_due()) {
					switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
									  MOD_NATS_NAME " JetStream publish to '%s' failed: %s; core NATS fallback\n",
									  pub->subject, natsStatus_GetText(ns));
				}
				switch_mutex_lock(mod_nats_globals.mutex);
				mod_nats_globals.js_fallbacks++;
				switch_mutex_unlock(mod_nats_globals.mutex);
				ns = natsConnection_Publish(nc, pub->subject,
										   (const void *) pub->payload, (int) strlen(pub->payload));
			}
		} else if (!zstr(pub->request_id)) {
			natsMsg *hm = NULL;
			if (natsMsg_Create(&hm, pub->subject, zstr(pub->reply) ? NULL : pub->reply,
								  pub->payload, (int) strlen(pub->payload)) == NATS_OK) {
				natsMsgHeader_Set(hm, "X-Request-Id", pub->request_id);
				ns = natsConnection_PublishMsg(nc, hm);
				natsMsg_Destroy(hm);
			} else {
				ns = NATS_ERR;
			}
		} else if (!zstr(pub->reply)) {
			ns = natsConnection_PublishRequest(nc, pub->subject, pub->reply,
											   (const void *) pub->payload, (int) strlen(pub->payload));
		} else {
			ns = natsConnection_Publish(nc, pub->subject,
										(const void *) pub->payload, (int) strlen(pub->payload));
		}
		if (ns == NATS_OK) {
			switch_mutex_lock(mod_nats_globals.mutex);
			mod_nats_globals.msgs_out++;
			switch_mutex_unlock(mod_nats_globals.mutex);
		} else if (ns == NATS_TIMEOUT) {
			/* Already logged. The server may have stored the message, so this is not pub_errors. */
		} else {
			if (warn_due()) {
				switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
								  MOD_NATS_NAME " publish to '%s' failed: %s\n", pub->subject, natsStatus_GetText(ns));
			}
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

static void publisher_loop(switch_queue_t *queue, int allow_js)
{
	/* pub_stop is raised only after dial threads have enqueued their last results */
	while (!mod_nats_globals.pub_stop) {
		void *pop = NULL;
		switch_status_t st = switch_queue_pop_timeout(queue, &pop, 200000);

		if (pop) {
			publish_one((mod_nats_pub_t *) pop, allow_js);
		}
		if (mod_nats_globals.pub_stop) {
			break;
		}
		if (st != SWITCH_STATUS_SUCCESS) {
			continue;
		}
	}

	{
		void *pop = NULL;
		while (switch_queue_trypop(queue, &pop) == SWITCH_STATUS_SUCCESS && pop) {
			publish_one((mod_nats_pub_t *) pop, allow_js);
		}
	}
}

static void *SWITCH_THREAD_FUNC publisher_thread(switch_thread_t *t, void *data)
{
	publisher_loop(mod_nats_globals.pub_queue, 0);
	return NULL;
}

static void *SWITCH_THREAD_FUNC js_publisher_thread(switch_thread_t *t, void *data)
{
	publisher_loop(mod_nats_globals.js_queue, 1);
	return NULL;
}

static void *SWITCH_THREAD_FUNC reply_thread(switch_thread_t *t, void *data)
{
	publisher_loop(mod_nats_globals.reply_queue, 0);
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

	if (len < 0 || (size_t) len > MOD_NATS_MAX_PAYLOAD) {
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
		if (warn_due()) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
							  MOD_NATS_NAME " dropping oversized payload (%d bytes)\n", len);
		}
		if (!zstr(reply)) {
			mod_nats_proto_send_error(reply, "", 400, "payload too large");
		}
		natsMsg_Destroy(msg);
		return;
	}

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
		if (!req->payload) {
			switch_safe_free(req->subject);
			switch_safe_free(req->reply);
			switch_safe_free(req);
			natsMsg_Destroy(msg);
			return;
		}
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
		if (warn_due()) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " request queue full, replying 503\n");
		}
		if (!zstr(reply)) {
			mod_nats_proto_send_error(reply, "", 503, "request queue full");
		}
		switch_safe_free(req->subject);
		switch_safe_free(req->reply);
		switch_safe_free(req->payload);
		switch_safe_free(req->request_id);
		switch_safe_free(req);
	}

	natsMsg_Destroy(msg);
}

static switch_status_t enqueue_pub(switch_queue_t *queue, const char *subject, const char *reply, const char *payload, const char *request_id)
{
	mod_nats_pub_t *pub;

	if (!queue || zstr(subject) || zstr(payload)) {
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

	if (!pub->subject || !pub->payload || switch_queue_trypush(queue, pub) != SWITCH_STATUS_SUCCESS) {
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

switch_status_t mod_nats_publish_enqueue_hdr(const char *subject, const char *reply, const char *payload, const char *request_id)
{
	/* Replies and Event.Result stay off the event queue so a JetStream ack cannot stall them. */
	return enqueue_pub(mod_nats_globals.reply_queue, subject, reply, payload, request_id);
}

switch_status_t mod_nats_publish_enqueue(const char *subject, const char *reply, const char *payload)
{
	switch_queue_t *queue = mod_nats_globals.pub_queue;

	if (mod_nats_globals.js_queue && mod_nats_subject_is_persistent(subject)) {
		queue = mod_nats_globals.js_queue;
	}
	return enqueue_pub(queue, subject, reply, payload, NULL);
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
	if (ns == NATS_OK) {
		/* Slow request workers must not grow the cnats buffer without a bound. */
		ns = natsSubscription_SetPendingLimits(mod_nats_globals.sub_node, 4096, 8 * 1024 * 1024);
		if (ns != NATS_OK) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
							  MOD_NATS_NAME " pending limits not applied: %s\n", natsStatus_GetText(ns));
			ns = NATS_OK;
		}
	}
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

	mod_nats_js_ensure();

	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	/* joinable: shutdown joins these threads before destroying the connection */
	status = switch_thread_create(&mod_nats_globals.pub_thread, thd_attr, publisher_thread, NULL, mod_nats_globals.pool);
	if (status != SWITCH_STATUS_SUCCESS) {
		mod_nats_conn_stop();
		return status;
	}
	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	status = switch_thread_create(&mod_nats_globals.js_thread, thd_attr, js_publisher_thread, NULL, mod_nats_globals.pool);
	if (status != SWITCH_STATUS_SUCCESS) {
		mod_nats_publishers_stop();
		mod_nats_conn_stop();
		return status;
	}
	switch_threadattr_create(&thd_attr, mod_nats_globals.pool);
	switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
	status = switch_thread_create(&mod_nats_globals.reply_thread, thd_attr, reply_thread, NULL, mod_nats_globals.pool);
	if (status != SWITCH_STATUS_SUCCESS) {
		mod_nats_publishers_stop();
		mod_nats_conn_stop();
		return status;
	}

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
					  MOD_NATS_NAME " connected: urls=%s prefix=%s node=%s listen=%s\n",
					  mod_nats_globals.urls, mod_nats_globals.subject_prefix,
					  mod_nats_globals.node_uuid, mod_nats_subject_node());

	return SWITCH_STATUS_SUCCESS;
}

void mod_nats_js_ensure(void)
{
	jsOptions opts;
	jsCtx *created = NULL;
	natsConnection *nc = NULL;
	natsStatus ns = NATS_ERR;
	static int binding = 0;
	static switch_time_t next_try = 0;

	if (mod_nats_globals.js_cdr != SWITCH_TRUE && mod_nats_globals.js_metrics != SWITCH_TRUE) {
		return;
	}

	switch_mutex_lock(mod_nats_globals.mutex);
	if (mod_nats_globals.js || !mod_nats_globals.nc || binding) {
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;
	}
	if (next_try && switch_time_now() < next_try) {
		switch_mutex_unlock(mod_nats_globals.mutex);
		return;
	}
	binding = 1;
	nc = mod_nats_globals.nc;
	switch_mutex_unlock(mod_nats_globals.mutex);

	/* JetStream() can touch the socket; do not hold mod_nats mutex across it */
	jsOptions_Init(&opts);
	opts.Wait = 1000;
	ns = natsConnection_JetStream(&created, nc, &opts);

	switch_mutex_lock(mod_nats_globals.mutex);
	binding = 0;
	if (ns == NATS_OK && created && !mod_nats_globals.js && mod_nats_globals.nc == nc) {
		mod_nats_globals.js = created;
		created = NULL;
		next_try = 0;
	} else if (ns != NATS_OK) {
		next_try = switch_time_now() + (5 * 1000000);
	}
	switch_mutex_unlock(mod_nats_globals.mutex);

	if (created) {
		jsCtx_Destroy(created);
	}
	if (ns == NATS_OK && !created) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, MOD_NATS_NAME " JetStream context bound\n");
	} else if (ns != NATS_OK) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
						  MOD_NATS_NAME " JetStream unavailable (%s), core NATS only\n", natsStatus_GetText(ns));
	}
}

void mod_nats_publishers_stop(void)
{
	switch_status_t join_status;

	mod_nats_globals.pub_stop = SWITCH_TRUE;
	if (mod_nats_globals.pub_queue) {
		switch_queue_interrupt_all(mod_nats_globals.pub_queue);
	}
	if (mod_nats_globals.js_queue) {
		switch_queue_interrupt_all(mod_nats_globals.js_queue);
	}
	if (mod_nats_globals.reply_queue) {
		switch_queue_interrupt_all(mod_nats_globals.reply_queue);
	}
	if (mod_nats_globals.pub_thread) {
		switch_thread_join(&join_status, mod_nats_globals.pub_thread);
		mod_nats_globals.pub_thread = NULL;
	}
	if (mod_nats_globals.js_thread) {
		switch_thread_join(&join_status, mod_nats_globals.js_thread);
		mod_nats_globals.js_thread = NULL;
	}
	if (mod_nats_globals.reply_thread) {
		switch_thread_join(&join_status, mod_nats_globals.reply_thread);
		mod_nats_globals.reply_thread = NULL;
	}
}

void mod_nats_conn_stop(void)
{
	jsCtx *js = NULL;

	/* jsCtx holds the connection: drop it first, and only after the publisher has been joined */
	switch_mutex_lock(mod_nats_globals.mutex);
	js = mod_nats_globals.js;
	mod_nats_globals.js = NULL;
	mod_nats_globals.conn_state = MN_CONN_DOWN;
	switch_mutex_unlock(mod_nats_globals.mutex);
	if (js) {
		jsCtx_Destroy(js);
	}

	if (mod_nats_globals.sub_node) {
		natsSubscription_Destroy(mod_nats_globals.sub_node);
		mod_nats_globals.sub_node = NULL;
	}
	if (mod_nats_globals.nc) {
		natsConnection_Destroy(mod_nats_globals.nc);
		mod_nats_globals.nc = NULL;
	}
}
