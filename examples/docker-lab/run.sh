#!/bin/sh
# Start NATS, FreeSWITCH and the consumer, then run the functional suite and the load test.
set -eu
cd "$(dirname "$0")"
docker compose up -d --build nats fs consumer
docker compose run --rm --no-deps consumer drive
