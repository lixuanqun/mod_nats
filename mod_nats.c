/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2026, All Rights Reserved.
 *
 * mod_nats: NATS message bus integration (XCC-compatible control surface).
 *
 * See README.md in this directory for the architecture, subject layout and
 * SDK compatibility notes.
 */
#include "mod_nats.h"

mod_nats_globals_t mod_nats_globals = { 0 };

SWITCH_MODULE_LOAD_FUNCTION(mod_nats_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_nats_shutdown);
SWITCH_MODULE_DEFINITION(mod_nats, mod_nats_load, mod_nats_shutdown, NULL);

static void free_req(mod_nats_req_t *req)
{
	if (!req) {
		return;
	}
	switch_safe_free(req->subject);
	switch_safe_free(req->reply);
	switch_safe_free(req->payload);
	switch_safe_free(req->request_id);
	switch_safe_free(req);
}

static void *SWITCH_THREAD_FUNC request_worker(switch_thread_t *t, void *data)
{
	while (mod_nats_globals.running) {
		void *pop = NULL;
		mod_nats_req_t *req;

		if (switch_queue_pop(mod_nats_globals.req_queue, &pop) != SWITCH_STATUS_SUCCESS || !pop) {
			continue;
		}

		req = (mod_nats_req_t *) pop;
		mod_nats_proto_handle_request(req);
		free_req(req);
	}

	{
		void *pop = NULL;
		while (switch_queue_trypop(mod_nats_globals.req_queue, &pop) == SWITCH_STATUS_SUCCESS && pop) {
			mod_nats_req_t *req = (mod_nats_req_t *) pop;
			if (!zstr(req->reply)) {
				mod_nats_proto_send_error(req->reply, "", 480, "shutting down");
			}
			free_req(req);
		}
	}
	return NULL;
}

static int same_cstr(const char *a, const char *b)
{
	if (!a && !b) {
		return 1;
	}
	if (!a || !b) {
		return 0;
	}
	return strcmp(a, b) == 0;
}

static switch_status_t config_load(switch_bool_t reload)
{
	switch_xml_t cfg, xml, settings, param;
	const char *val;
	char urls_save[MOD_NATS_URLS_MAX];
	char prefix_save[MOD_NATS_PREFIX_MAX];
	char node_save[SWITCH_UUID_FORMATTED_LENGTH + 1];
	char *user_save = NULL, *pass_save = NULL, *cred_save = NULL;
	int workers_save = 0, pub_save = 0, req_save = 0;

	if (reload) {
		switch_copy_string(urls_save, mod_nats_globals.urls, sizeof(urls_save));
		switch_copy_string(prefix_save, mod_nats_globals.subject_prefix, sizeof(prefix_save));
		switch_copy_string(node_save, mod_nats_globals.node_uuid, sizeof(node_save));
		workers_save = mod_nats_globals.workers;
		pub_save = mod_nats_globals.pub_qlen;
		req_save = mod_nats_globals.req_qlen;
		if (mod_nats_globals.user) {
			user_save = strdup(mod_nats_globals.user);
		}
		if (mod_nats_globals.password) {
			pass_save = strdup(mod_nats_globals.password);
		}
		if (mod_nats_globals.credentials) {
			cred_save = strdup(mod_nats_globals.credentials);
		}
	}

	if (!(xml = switch_xml_open_cfg("nats.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " cannot open nats.conf.xml\n");
		switch_safe_free(user_save);
		switch_safe_free(pass_save);
		switch_safe_free(cred_save);
		return SWITCH_STATUS_FALSE;
	}

	if ((settings = switch_xml_child(cfg, "settings"))) {
		for (param = switch_xml_child(settings, "param"); param; param = param->next) {
			const char *name = switch_xml_attr_soft(param, "name");
			val = switch_xml_attr_soft(param, "value");

			if (zstr(name) || zstr(val)) {
				continue;
			} else if (!strcmp(name, "urls")) {
				switch_copy_string(mod_nats_globals.urls, val, sizeof(mod_nats_globals.urls));
			} else if (!strcmp(name, "subject-prefix")) {
				switch_copy_string(mod_nats_globals.subject_prefix, val, sizeof(mod_nats_globals.subject_prefix));
			} else if (!strcmp(name, "node-uuid")) {
				switch_copy_string(mod_nats_globals.node_uuid, val, sizeof(mod_nats_globals.node_uuid));
			} else if (!strcmp(name, "user")) {
				switch_safe_free(mod_nats_globals.user);
				mod_nats_globals.user = strdup(val);
			} else if (!strcmp(name, "password")) {
				switch_safe_free(mod_nats_globals.password);
				mod_nats_globals.password = strdup(val);
			} else if (!strcmp(name, "credentials")) {
				switch_safe_free(mod_nats_globals.credentials);
				mod_nats_globals.credentials = strdup(val);
			} else if (!strcmp(name, "cdr-subject")) {
				if (reload && mod_nats_globals.mutex) {
					switch_mutex_lock(mod_nats_globals.mutex);
				}
				switch_safe_free(mod_nats_globals.cdr_subject);
				mod_nats_globals.cdr_subject = strdup(val);
				if (reload && mod_nats_globals.mutex) {
					switch_mutex_unlock(mod_nats_globals.mutex);
				}
			} else if (!strcmp(name, "channel-params")) {
				if (reload && mod_nats_globals.mutex) {
					switch_mutex_lock(mod_nats_globals.mutex);
				}
				switch_safe_free(mod_nats_globals.channel_params);
				mod_nats_globals.channel_params = strdup(val);
				if (reload && mod_nats_globals.mutex) {
					switch_mutex_unlock(mod_nats_globals.mutex);
				}
			} else if (!strcmp(name, "publish-events")) {
				mod_nats_globals.enable_events = switch_true(val);
			} else if (!strcmp(name, "enable-cdr")) {
				mod_nats_globals.enable_cdr = switch_true(val);
			} else if (!strcmp(name, "accept-timeout")) {
				mod_nats_globals.accept_timeout_sec = atoi(val);
			} else if (!strcmp(name, "metrics-interval")) {
				mod_nats_globals.metrics_interval = atoi(val);
			} else if (!strcmp(name, "compat-xcc")) {
				mod_nats_globals.compat_xcc = switch_true(val);
			} else if (!strcmp(name, "js-cdr")) {
				mod_nats_globals.js_cdr = switch_true(val);
			} else if (!strcmp(name, "js-metrics")) {
				mod_nats_globals.js_metrics = switch_true(val);
			} else if (!strcmp(name, "event-routing")) {
				if (!strcasecmp(val, "mailbox")) {
					mod_nats_globals.event_routing = EVENT_ROUTE_MAILBOX;
				} else if (!strcasecmp(val, "broadcast")) {
					mod_nats_globals.event_routing = EVENT_ROUTE_BROADCAST;
				} else {
					mod_nats_globals.event_routing = EVENT_ROUTE_BOTH;
				}
			} else if (!strcmp(name, "publish-native-events")) {
				mod_nats_globals.publish_native_events = switch_true(val);
			} else if (!strcmp(name, "allow-native-api")) {
				mod_nats_globals.allow_native_api = switch_true(val);
			} else if (!strcmp(name, "workers")) {
				mod_nats_globals.workers = atoi(val);
			} else if (!strcmp(name, "pub-qsize")) {
				mod_nats_globals.pub_qlen = atoi(val);
			} else if (!strcmp(name, "req-qsize")) {
				mod_nats_globals.req_qlen = atoi(val);
			}
		}
	}
	switch_xml_free(xml);

	if (!reload && zstr(mod_nats_globals.node_uuid)) {
		switch_uuid_str(mod_nats_globals.node_uuid, sizeof(mod_nats_globals.node_uuid));
	}

	if (mod_nats_globals.workers < 1) mod_nats_globals.workers = MOD_NATS_DEFAULT_WORKERS;
	if (mod_nats_globals.workers > 16) mod_nats_globals.workers = 16;
	if (mod_nats_globals.pub_qlen < 64) mod_nats_globals.pub_qlen = MOD_NATS_DEFAULT_PUB_QLEN;
	if (mod_nats_globals.req_qlen < 16) mod_nats_globals.req_qlen = MOD_NATS_DEFAULT_REQ_QLEN;
	if (mod_nats_globals.metrics_interval > 3600) mod_nats_globals.metrics_interval = 3600;
	if (mod_nats_globals.accept_timeout_sec < 0) mod_nats_globals.accept_timeout_sec = 0;
	if (mod_nats_globals.accept_timeout_sec > 86400) mod_nats_globals.accept_timeout_sec = 86400;

	if (reload) {
		int conn_changed = strcmp(mod_nats_globals.urls, urls_save) ||
			strcmp(mod_nats_globals.subject_prefix, prefix_save) ||
			strcmp(mod_nats_globals.node_uuid, node_save) ||
			mod_nats_globals.workers != workers_save ||
			mod_nats_globals.pub_qlen != pub_save ||
			mod_nats_globals.req_qlen != req_save ||
			!same_cstr(mod_nats_globals.user, user_save) ||
			!same_cstr(mod_nats_globals.password, pass_save) ||
			!same_cstr(mod_nats_globals.credentials, cred_save);

		if (conn_changed) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
							  MOD_NATS_NAME " reload keeps the live connection, prefix, node-uuid and pool sizes; restart the module to apply those\n");
			switch_copy_string(mod_nats_globals.urls, urls_save, sizeof(mod_nats_globals.urls));
			switch_copy_string(mod_nats_globals.subject_prefix, prefix_save, sizeof(mod_nats_globals.subject_prefix));
			switch_copy_string(mod_nats_globals.node_uuid, node_save, sizeof(mod_nats_globals.node_uuid));
			mod_nats_globals.workers = workers_save;
			mod_nats_globals.pub_qlen = pub_save;
			mod_nats_globals.req_qlen = req_save;
			switch_safe_free(mod_nats_globals.user);
			switch_safe_free(mod_nats_globals.password);
			switch_safe_free(mod_nats_globals.credentials);
			mod_nats_globals.user = user_save;
			mod_nats_globals.password = pass_save;
			mod_nats_globals.credentials = cred_save;
			user_save = pass_save = cred_save = NULL;
		}
		switch_safe_free(user_save);
		switch_safe_free(pass_save);
		switch_safe_free(cred_save);
	}

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t mod_nats_config_reload(void)
{
	switch_status_t st = config_load(SWITCH_TRUE);

	if (st == SWITCH_STATUS_SUCCESS) {
		mod_nats_events_rebind();
		mod_nats_js_ensure();
		if (mod_nats_globals.metrics_interval > 0 && !mod_nats_globals.metrics_thread) {
			mod_nats_metrics_start();
		}
	}
	return st;
}

static switch_status_t api_status(switch_stream_handle_t *stream)
{
	uint64_t in, out, dropped, errs, ev, since, js_fb;

	switch_mutex_lock(mod_nats_globals.mutex);
	in = mod_nats_globals.msgs_in;
	out = mod_nats_globals.msgs_out;
	dropped = mod_nats_globals.msgs_dropped;
	errs = mod_nats_globals.pub_errors;
	ev = mod_nats_globals.events_out;
	js_fb = mod_nats_globals.js_fallbacks;
	since = (uint64_t) ((switch_time_now() - mod_nats_globals.started) / 1000000);
	switch_mutex_unlock(mod_nats_globals.mutex);

	stream->write_function(stream, "======================================\n");
	stream->write_function(stream, "conn_state    %s\n", mod_nats_conn_state_name());
	stream->write_function(stream, "urls          %s\n", mod_nats_globals.urls);
	stream->write_function(stream, "prefix        %s\n", mod_nats_globals.subject_prefix);
	stream->write_function(stream, "node_uuid     %s\n", mod_nats_globals.node_uuid);
	stream->write_function(stream, "listen        %s\n", mod_nats_subject_node());
	stream->write_function(stream, "jetstream     cdr=%s metrics=%s ctx=%s fallbacks=%lu\n",
						   mod_nats_globals.js_cdr ? "on" : "off",
						   mod_nats_globals.js_metrics ? "on" : "off",
						   mod_nats_globals.js ? "bound" : "none",
						   (unsigned long) js_fb);
	stream->write_function(stream, "routing       %s\n",
						   mod_nats_globals.event_routing == EVENT_ROUTE_MAILBOX ? "mailbox" :
						   mod_nats_globals.event_routing == EVENT_ROUTE_BOTH ? "both" : "broadcast");
	stream->write_function(stream, "metrics       interval=%ds out=%lu\n",
						   mod_nats_globals.metrics_interval, (unsigned long) mod_nats_globals.metrics_out);
	stream->write_function(stream, "events        %s (native: %s) cdr: %s\n",
						   mod_nats_globals.enable_events ? "on" : "off",
						   mod_nats_globals.publish_native_events ? "on" : "off",
						   mod_nats_globals.enable_cdr ? "on" : "off");
	stream->write_function(stream, "native api    %s\n", mod_nats_globals.allow_native_api ? "on" : "off");
	stream->write_function(stream, "accept timeout %ds\n", mod_nats_globals.accept_timeout_sec);
	stream->write_function(stream, "uptime        %lus\n", (unsigned long) since);
	stream->write_function(stream, "msgs in/out   %lu / %lu\n", (unsigned long) in, (unsigned long) out);
	stream->write_function(stream, "events out    %lu\n", (unsigned long) ev);
	stream->write_function(stream, "dropped       %lu\n", (unsigned long) dropped);
	stream->write_function(stream, "pub errors    %lu\n", (unsigned long) errs);
	stream->write_function(stream, "dial          workers=%d queued=%u\n",
						   mod_nats_globals.dial_thread_count,
						   mod_nats_globals.dial_queue ? switch_queue_size(mod_nats_globals.dial_queue) : 0);
	stream->write_function(stream, "queues        pub=%u js=%u reply=%u event=%u req=%u\n",
						   mod_nats_globals.pub_queue ? switch_queue_size(mod_nats_globals.pub_queue) : 0,
						   mod_nats_globals.js_queue ? switch_queue_size(mod_nats_globals.js_queue) : 0,
						   mod_nats_globals.reply_queue ? switch_queue_size(mod_nats_globals.reply_queue) : 0,
						   mod_nats_globals.event_queue ? switch_queue_size(mod_nats_globals.event_queue) : 0,
						   mod_nats_globals.req_queue ? switch_queue_size(mod_nats_globals.req_queue) : 0);
	stream->write_function(stream, "sessions      %lu\n", (unsigned long) switch_core_session_count());
	stream->write_function(stream, "======================================\n");
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_STANDARD_API(api_function)
{
	if (zstr(cmd)) {
		goto usage;
	}
	if (!strcasecmp(cmd, "status")) {
		return api_status(stream);
	} else if (!strcasecmp(cmd, "reload")) {
		return mod_nats_config_reload();
	}

  usage:
	stream->write_function(stream, "Usage: nats status | nats reload\n");
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_nats_load)
{
	switch_api_interface_t *api_interface;
	switch_status_t status;
	switch_threadattr_t *thd_attr;
	int i;

	*module_interface = switch_loadable_module_create_module_interface(pool, modname);

	memset(&mod_nats_globals, 0, sizeof(mod_nats_globals));
	mod_nats_globals.pool = pool;
	mod_nats_globals.running = SWITCH_TRUE;
	mod_nats_globals.started = switch_time_now();
	mod_nats_globals.enable_events = SWITCH_TRUE;
	mod_nats_globals.enable_cdr = SWITCH_TRUE;
	mod_nats_globals.accept_timeout_sec = 0;
	mod_nats_globals.compat_xcc = SWITCH_TRUE;
	mod_nats_globals.event_routing = EVENT_ROUTE_BOTH;
	switch_copy_string(mod_nats_globals.urls, "nats://127.0.0.1:4222", sizeof(mod_nats_globals.urls));
	switch_copy_string(mod_nats_globals.subject_prefix, MOD_NATS_DEFAULT_PREFIX, sizeof(mod_nats_globals.subject_prefix));

	if (config_load(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_init(&mod_nats_globals.mutex, SWITCH_MUTEX_NESTED, pool);
	switch_mutex_init(&mod_nats_globals.chan_mutex, SWITCH_MUTEX_NESTED, pool);
	switch_mutex_init(&mod_nats_globals.cpu_mutex, SWITCH_MUTEX_NESTED, pool);
	switch_core_hash_init(&mod_nats_globals.chan_hash);
	switch_queue_create(&mod_nats_globals.pub_queue, mod_nats_globals.pub_qlen, pool);
	switch_queue_create(&mod_nats_globals.js_queue, mod_nats_globals.pub_qlen, pool);
	switch_queue_create(&mod_nats_globals.reply_queue, mod_nats_globals.req_qlen, pool);
	switch_queue_create(&mod_nats_globals.event_queue, mod_nats_globals.pub_qlen, pool);
	switch_queue_create(&mod_nats_globals.req_queue, mod_nats_globals.req_qlen, pool);
	switch_queue_create(&mod_nats_globals.dial_queue, mod_nats_globals.req_qlen, pool);

	SWITCH_ADD_API(api_interface, "nats", "NATS bus interface", api_function, "status | reload");
	switch_console_set_complete("add nats status");
	switch_console_set_complete("add nats reload");

	if ((status = mod_nats_conn_start()) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " connection failed; module not loaded\n");
		mod_nats_globals.running = SWITCH_FALSE;
		switch_safe_free(mod_nats_globals.user);
		switch_safe_free(mod_nats_globals.password);
		switch_safe_free(mod_nats_globals.credentials);
		switch_safe_free(mod_nats_globals.cdr_subject);
		switch_safe_free(mod_nats_globals.channel_params);
		switch_core_hash_destroy(&mod_nats_globals.chan_hash);
		return SWITCH_STATUS_FALSE;
	}

	mod_nats_methods_dial_start();

	for (i = 0; i < mod_nats_globals.workers; i++) {
		switch_threadattr_create(&thd_attr, pool);
		switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
		/* joinable: the shutdown path joins worker threads */
		if (switch_thread_create(&mod_nats_globals.req_threads[i], thd_attr, request_worker, NULL, pool) == SWITCH_STATUS_SUCCESS) {
			mod_nats_globals.req_thread_count++;
		}
	}

	if ((status = mod_nats_events_start()) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " event bind failed\n");
		mod_nats_globals.running = SWITCH_FALSE;
		switch_scheduler_del_task_group(MOD_NATS_SCHED_GROUP);
		switch_queue_interrupt_all(mod_nats_globals.req_queue);
		for (i = 0; i < mod_nats_globals.req_thread_count; i++) {
			switch_status_t join_status;
			switch_thread_join(&join_status, mod_nats_globals.req_threads[i]);
		}
		mod_nats_methods_dial_stop();
		mod_nats_publishers_stop();
		mod_nats_conn_stop();
		switch_safe_free(mod_nats_globals.user);
		switch_safe_free(mod_nats_globals.password);
		switch_safe_free(mod_nats_globals.credentials);
		switch_safe_free(mod_nats_globals.cdr_subject);
		switch_safe_free(mod_nats_globals.channel_params);
		switch_core_hash_destroy(&mod_nats_globals.chan_hash);
		return SWITCH_STATUS_FALSE;
	}

	mod_nats_metrics_start();

	mod_nats_events_publish_nodeup();

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
					  MOD_NATS_NAME " loaded: proto=%s workers=%d listen=%s\n",
					  MOD_NATS_PROTO_VERSION, mod_nats_globals.req_thread_count, mod_nats_subject_node());

	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_nats_shutdown)
{
	int i;
	switch_status_t join_status;

	mod_nats_globals.running = SWITCH_FALSE;
	switch_scheduler_del_task_group(MOD_NATS_SCHED_GROUP);
	/* unbind first so new callbacks stop; in-flight ones finish during the sleep */
	mod_nats_events_unbind();
	switch_sleep(250000);
	mod_nats_events_shutdown();
	mod_nats_metrics_stop();
	switch_queue_interrupt_all(mod_nats_globals.req_queue);
	/* apr_thread_join dereferences retval unconditionally - never pass NULL */
	for (i = 0; i < mod_nats_globals.req_thread_count; i++) {
		switch_thread_join(&join_status, mod_nats_globals.req_threads[i]);
	}
	/* dial threads may still be inside originate; they enqueue Event.Result before exiting */
	mod_nats_methods_dial_stop();
	mod_nats_publishers_stop();
	/* connection last: all publisher threads have stopped by now.
	 * NOTE: nats_Close() is a one-shot global library teardown and must NOT
	 * run here - a reload re-initializes the library immediately after and
	 * the race crashes FreeSWITCH intermittently. OS reclaims at exit. */
	mod_nats_conn_stop();

	switch_safe_free(mod_nats_globals.user);
	switch_safe_free(mod_nats_globals.password);
	switch_safe_free(mod_nats_globals.credentials);
	switch_safe_free(mod_nats_globals.cdr_subject);
	switch_safe_free(mod_nats_globals.channel_params);

	switch_core_hash_destroy(&mod_nats_globals.chan_hash);

	return SWITCH_STATUS_SUCCESS;
}
