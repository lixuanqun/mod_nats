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

static void *SWITCH_THREAD_FUNC request_worker(switch_thread_t *t, void *data)
{
	while (mod_nats_globals.running) {
		void *pop = NULL;
		mod_nats_req_t *req;

		if (switch_queue_pop(mod_nats_globals.req_queue, &pop) != SWITCH_STATUS_SUCCESS || !pop) {
			continue;
		}
		if (!mod_nats_globals.running) {
			mod_nats_req_t *r = (mod_nats_req_t *) pop;
			switch_safe_free(r->subject);
			switch_safe_free(r->reply);
			switch_safe_free(r->payload);
			switch_safe_free(r);
			continue;
		}

		req = (mod_nats_req_t *) pop;
		mod_nats_proto_handle_request(req);
		switch_safe_free(req->subject);
		switch_safe_free(req->reply);
		switch_safe_free(req->payload);
		switch_safe_free(req);
	}
	return NULL;
}

static switch_status_t config_load(switch_bool_t reload)
{
	switch_xml_t cfg, xml, settings, param;
	const char *val;

	if (!(xml = switch_xml_open_cfg("nats.conf", &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " cannot open nats.conf.xml\n");
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
				switch_safe_free(mod_nats_globals.cdr_subject);
				mod_nats_globals.cdr_subject = strdup(val);
			} else if (!strcmp(name, "channel-params")) {
				switch_safe_free(mod_nats_globals.channel_params);
				mod_nats_globals.channel_params = strdup(val);
			} else if (!strcmp(name, "publish-events")) {
				mod_nats_globals.enable_events = switch_true(val);
			} else if (!strcmp(name, "enable-cdr")) {
				mod_nats_globals.enable_cdr = switch_true(val);
			} else if (!strcmp(name, "accept-timeout")) {
				mod_nats_globals.accept_timeout_sec = atoi(val);
			} else if (!strcmp(name, "metrics-interval")) {
				mod_nats_globals.metrics_interval = atoi(val);
			} else if (!strcmp(name, "publish-native-events")) {
				mod_nats_globals.publish_native_events = switch_true(val);
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

	if (zstr(mod_nats_globals.node_uuid)) {
		switch_uuid_str(mod_nats_globals.node_uuid, sizeof(mod_nats_globals.node_uuid));
	}

	if (mod_nats_globals.workers < 1) mod_nats_globals.workers = MOD_NATS_DEFAULT_WORKERS;
	if (mod_nats_globals.workers > 16) mod_nats_globals.workers = 16;
	if (mod_nats_globals.pub_qlen < 64) mod_nats_globals.pub_qlen = MOD_NATS_DEFAULT_PUB_QLEN;
	if (mod_nats_globals.req_qlen < 16) mod_nats_globals.req_qlen = MOD_NATS_DEFAULT_REQ_QLEN;
	if (mod_nats_globals.metrics_interval > 3600) mod_nats_globals.metrics_interval = 3600;

	return SWITCH_STATUS_SUCCESS;
}

switch_status_t mod_nats_config_reload(void)
{
	return config_load(SWITCH_TRUE);
}

static switch_status_t api_status(switch_stream_handle_t *stream)
{
	uint64_t in, out, dropped, errs, ev, since;

	switch_mutex_lock(mod_nats_globals.mutex);
	in = mod_nats_globals.msgs_in;
	out = mod_nats_globals.msgs_out;
	dropped = mod_nats_globals.msgs_dropped;
	errs = mod_nats_globals.pub_errors;
	ev = mod_nats_globals.events_out;
	since = (uint64_t) ((switch_time_now() - mod_nats_globals.started) / 1000000);
	switch_mutex_unlock(mod_nats_globals.mutex);

	stream->write_function(stream, "======================================\n");
	stream->write_function(stream, "conn_state    %s\n", mod_nats_conn_state_name());
	stream->write_function(stream, "urls          %s\n", mod_nats_globals.urls);
	stream->write_function(stream, "prefix        %s\n", mod_nats_globals.subject_prefix);
	stream->write_function(stream, "node_uuid     %s\n", mod_nats_globals.node_uuid);
	stream->write_function(stream, "listen        %s\n", mod_nats_subject_node());
	stream->write_function(stream, "metrics       interval=%ds out=%lu\n",
						   mod_nats_globals.metrics_interval, (unsigned long) mod_nats_globals.metrics_out);
	stream->write_function(stream, "events        %s (native: %s) cdr: %s\n",
						   mod_nats_globals.enable_events ? "on" : "off",
						   mod_nats_globals.publish_native_events ? "on" : "off",
						   mod_nats_globals.enable_cdr ? "on" : "off");
	stream->write_function(stream, "uptime        %lus\n", (unsigned long) since);
	stream->write_function(stream, "msgs in/out   %lu / %lu\n", (unsigned long) in, (unsigned long) out);
	stream->write_function(stream, "events out    %lu\n", (unsigned long) ev);
	stream->write_function(stream, "dropped       %lu\n", (unsigned long) dropped);
	stream->write_function(stream, "pub errors    %lu\n", (unsigned long) errs);
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
	mod_nats_globals.accept_timeout_sec = 10;
	switch_copy_string(mod_nats_globals.urls, "nats://127.0.0.1:4222", sizeof(mod_nats_globals.urls));
	switch_copy_string(mod_nats_globals.subject_prefix, MOD_NATS_DEFAULT_PREFIX, sizeof(mod_nats_globals.subject_prefix));

	if (config_load(SWITCH_FALSE) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_init(&mod_nats_globals.mutex, SWITCH_MUTEX_NESTED, pool);
	switch_mutex_init(&mod_nats_globals.chan_mutex, SWITCH_MUTEX_NESTED, pool);
	switch_core_hash_init(&mod_nats_globals.chan_hash);
	switch_queue_create(&mod_nats_globals.pub_queue, mod_nats_globals.pub_qlen, pool);
	switch_queue_create(&mod_nats_globals.req_queue, mod_nats_globals.req_qlen, pool);

	SWITCH_ADD_API(api_interface, "nats", "NATS bus interface", api_function, "status | reload");
	switch_console_set_complete("add nats status");
	switch_console_set_complete("add nats reload");

	if ((status = mod_nats_conn_start()) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, MOD_NATS_NAME " connection failed; module not loaded\n");
		return SWITCH_STATUS_FALSE;
	}

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
		return SWITCH_STATUS_FALSE;
	}

	mod_nats_metrics_start();

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
	mod_nats_events_stop();
	/* unbind only delists the binding; give in-flight event callbacks a
	 * moment to return before we tear down the hashes/mutexes they touch */
	mod_nats_metrics_stop();
	switch_sleep(250000);
	switch_queue_interrupt_all(mod_nats_globals.req_queue);
	switch_queue_interrupt_all(mod_nats_globals.pub_queue);
	/* apr_thread_join dereferences retval unconditionally - never pass NULL */
	for (i = 0; i < mod_nats_globals.req_thread_count; i++) {
		switch_thread_join(&join_status, mod_nats_globals.req_threads[i]);
	}
	if (mod_nats_globals.pub_thread) {
		switch_thread_join(&join_status, mod_nats_globals.pub_thread);
	}
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
