#!/bin/bash
# Runs the protocol and load tests on the server, every process in a memory/CPU-capped transient unit.
# The proxy gets the same limits as st-sync.service. Ports 18345/18355; everything is removed on exit.
set -u
cd "$(dirname "$0")/.."
D=$(pwd)
NODE=$(command -v node)
T=/var/tmp/stsync-test
rm -rf "$T"; mkdir -p "$T"
cleanup() {
    systemctl stop stsync-test-proxy stsync-test-up stsync-test-sampler 2>/dev/null
    systemctl reset-failed stsync-test-proxy stsync-test-up stsync-test-sampler 2>/dev/null
    rm -rf "$T"
}
trap cleanup EXIT

systemd-run -q --unit=stsync-test-up -p MemoryMax=200M -p CPUQuota=50% --setenv=PORT=18355 \
    $NODE $D/test/fake-upstream.js
systemd-run -q --unit=stsync-test-proxy -p MemoryHigh=200M -p MemoryMax=256M -p MemorySwapMax=0 -p CPUQuota=80% \
    --setenv=MALLOC_ARENA_MAX=2 --setenv=ST_SYNC_LISTEN_HOST=127.0.0.1 --setenv=ST_SYNC_LISTEN_PORT=18345 \
    --setenv=ST_SYNC_UPSTREAM_PORT=18355 --setenv=ST_SYNC_STORE_DIR=$T/store --setenv=ST_SYNC_LOG_FILE=$T/access.log \
    --setenv=ST_SYNC_OFF_FILE=$T/off $NODE --max-old-space-size=96 $D/proxy.js
sleep 2
PID=$(systemctl show -p MainPID --value stsync-test-proxy)
CG=/sys/fs/cgroup/system.slice/stsync-test-proxy.service
systemd-run -q --unit=stsync-test-sampler -p MemoryMax=16M /bin/bash -c \
    "while sleep 0.5; do echo \"\$(curl -s -m 2 127.0.0.1:18345/__sync/health) cg=\$((\$(cat $CG/memory.current) / 1048576))\"; done > $T/health.txt"

status=0
run() { systemd-run -q --scope -p MemoryMax=600M -p MemorySwapMax=0 -p CPUQuota=50% "$@"; }
OFF_FILE=$T/off run $NODE test/protocol-test.js 18345 | grep -v '^PASS'; [ "${PIPESTATUS[0]}" = 0 ] || status=1
run /usr/bin/env TIMEOUT_MS=300000 LIMIT_MB=${LIMIT_MB:-230} "$@" $NODE test/load-test.js 18345 "$PID" || status=1
systemctl stop stsync-test-sampler

echo "memory samples (heavy rss heap external cgroup, MB):"
sed -nE 's/.*"heavy":([0-9]+),"waiting":[0-9]+,"rssMB":([0-9]+),"heapMB":([0-9]+),"externalMB":([0-9]+).* cg=([0-9]+)/\1 \2 \3 \4 \5/p' "$T/health.txt" | uniq | tr '\n' '|'
echo
echo "cgroup peak: $(($(cat $CG/memory.peak 2>/dev/null || echo 0) / 1048576))MB; events: $(tr '\n' ' ' < $CG/memory.events)"
echo "restarts: $(systemctl show -p NRestarts --value stsync-test-proxy), main pid now $(systemctl show -p MainPID --value stsync-test-proxy) (was $PID)"
grep -v -e health -e '"s":200' "$T/access.log" | cut -c1-200 | tail -5
journalctl -u stsync-test-proxy --no-pager -n 5 | grep -v -e listening -e Started | cut -c1-200
exit $status
