#!/usr/bin/env bash
# Runs the st-sync test suite against a fake upstream: bash test/run-local.sh [load-test env...]
# Ports 18345/18355; everything it starts is stopped on exit.
set -u
cd "$(dirname "$0")/.."
T=$(mktemp -d)
trap 'kill $UP $PROXY $SAMPLER 2>/dev/null; rm -rf "$T"' EXIT

PORT=18355 node test/fake-upstream.js > "$T/up.log" 2>&1 & UP=$!
ST_SYNC_LISTEN_HOST=127.0.0.1 ST_SYNC_LISTEN_PORT=18345 ST_SYNC_UPSTREAM_PORT=18355 \
ST_SYNC_STORE_DIR="$T/store" ST_SYNC_LOG_FILE="$T/access.log" ST_SYNC_OFF_FILE="$T/off" \
    node --max-old-space-size=96 proxy.js > "$T/proxy.log" 2>&1 & PROXY=$!
sleep 1.5

status=0
node test/cdc-test.js | tail -1 || status=1
OFF_FILE="$T/off" node test/protocol-test.js 18345 | grep -v '^PASS' ; [ "${PIPESTATUS[0]}" = 0 ] || status=1

(while sleep 0.5; do curl -s -m 2 127.0.0.1:18345/__sync/health; echo; done > "$T/health.txt") & SAMPLER=$!
env "$@" node test/load-test.js 18345 $PROXY || status=1
kill $SAMPLER 2>/dev/null
echo "memory samples (heavy rss heap external, MB):"
sed -nE 's/.*"heavy":([0-9]+),"waiting":[0-9]+,"rssMB":([0-9]+),"heapMB":([0-9]+),"externalMB":([0-9]+).*/\1 \2 \3 \4/p' "$T/health.txt" | uniq | tr '\n' '|'
echo
grep -v -e health -e '"s":200' "$T/access.log" | cut -c1-200 | tail -5
grep -v listening "$T/proxy.log" | tail -5
exit $status
