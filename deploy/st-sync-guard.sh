#!/bin/bash
# While st-sync is active, probes it every 5s. Three failures in a row (no health answer, or the
# proxy cannot reach SillyTavern) switch the public port to direct SillyTavern.
. /etc/st-sync/env
host=${ST_SYNC_LISTEN_HOST:-0.0.0.0}
[ "$host" = 0.0.0.0 ] && host=127.0.0.1
base="http://$host:$ST_SYNC_LISTEN_PORT"
fails=0
while sleep 5; do
    if ! systemctl is-active -q st-sync.service; then
        fails=0
        continue
    fi
    health=$(curl -s -m 3 -o /dev/null -w '%{http_code}' "$base/__sync/health")
    page=skipped
    [ "$health" = 200 ] && page=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$base/favicon.ico")
    if [ "$health" = 200 ] && [ "$page" != 502 ] && [ "$page" != 000 ]; then
        fails=0
    else
        fails=$((fails + 1))
        logger -t st-sync-guard "probe failed ($fails): health=$health page=$page"
    fi
    if [ "$fails" -ge 3 ]; then
        logger -t st-sync-guard "switching port $ST_SYNC_LISTEN_PORT to direct SillyTavern"
        systemctl start sillytavern-direct.service
        fails=0
    fi
done
