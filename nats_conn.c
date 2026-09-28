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
			if (!zstr(pub->reply)) {
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
		switch_safe_free(req);
		natsMsg_Destroy(msg);
		return;
	}

	if (switch_queue_trypush(mod_nats_globals.req_queue, req) != SWITCH_STATUS_SUCCESS) {
		/* request backlog full: drop rather than stall the cnats dispatcher */
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " request queue full, message dropped\n");
	}

	natsMsg_Destroy(msg);
}

static void on_disconnected(natsConnection *nc, void *closure)
{
	switch_mutex_lock(mod_nats_globals.mutex);
	mod_nats_globals.conn_state = MN_CONN_CONNECTING;
	switch_mutex_unlock(mod_nats_globals.mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, MOD_NATS_NAME " disconnected from NATS, auto-reconnect in progress\n");
}

static void on_reconnected(natsConnection *nc, void *closure)
{
	switch_mutex_lock(mod_nats_globals.mutex);
	mod_nats_globals.conn_state = MN_CONN_UP;
	switch_mutex_unlock(mod_nats_globals.mutex);
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, MOD_NATS_NAME " reconnected to NATS\n");
}

switch_status_t mod_nats_publish_enqueue(const char *subject, const char *reply, const char *payload)
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

	if (!pub->subject || !pub->payload || switch_queue_trypush(mod_nats_globals.pub_queue, pub) != SWITCH_STATUS_SUCCESS) {
		switch_safe_free(pub->subject);
		switch_safe_free(pub->reply);
		switch_safe_free(pub->payload);
		switch_safe_free(pub);
		switch_mutex_lock(mod_nats_globals.mutex);
		mod_nats_globals.msgs_dropped++;
		switch_mutex_unlock(mod_nats_globals.mutex);
		return SWITCH_STATUS_FALSE;
	}

	return SWITCH_STATUS_SUCCESS;
}

const char *mod_nats_conn_state_name(void)
{
	switch_mutex_lock(mod_nats_globals.mutex);
	{
		const char *name = mod_nats_globals.conn_state == MN_CONN_UP ? "UP" :
			mod_nats_globals.conn_state == MN_CONN_CONNECTING ? "CONNECTING" : "DOWN";
		switch_mutex_unlock(mod_nats_globals.mutex);
		return name;
	}
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
	natsOptions_SetDisconnectedCB(opts, on_disconnected, NULL);
	natsOptions_SetReconnectedCB(opts, on_reconnected, NULL);

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
