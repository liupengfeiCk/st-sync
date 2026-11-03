#!/bin/bash
# Installs st-sync in front of an existing SillyTavern (Linux, systemd):
#   sudo deploy/install.sh --st-dir /path/to/SillyTavern [--port N] [--upstream-port N]
#                          [--user NAME] [--listen-host ADDR] [--prefix DIR]
# Re-running it updates the code and units. It never switches modes: whatever serves the port now
# keeps serving it until `st-mode proxy`.
set -euo pipefail

usage() {
    sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
}
die() {
    echo "install.sh: $*" >&2
    exit 1
}

SRC=$(cd "$(dirname "$0")/.." && pwd)
ST_DIR='' PORT='' UPSTREAM_PORT='' ST_USER='' LISTEN_HOST=0.0.0.0 PREFIX=/opt/st-sync
while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || usage
    case "$1" in
        --st-dir) ST_DIR=$2 ;;
        --port) PORT=$2 ;;
        --upstream-port) UPSTREAM_PORT=$2 ;;
        --user) ST_USER=$2 ;;
        --listen-host) LISTEN_HOST=$2 ;;
        --prefix) PREFIX=$2 ;;
        *) usage ;;
    esac
    shift 2
done

[ "$(id -u)" = 0 ] || die "must run as root"
[ -n "$ST_DIR" ] || usage
[ -f "$ST_DIR/server.js" ] || die "$ST_DIR does not look like a SillyTavern directory (no server.js)"
ST_DIR=$(cd "$ST_DIR" && pwd)
command -v systemctl > /dev/null || die "systemd is required"
command -v curl > /dev/null || die "curl is required"
NODE=$(command -v node) || die "node is not on PATH"
"$NODE" -e 'const [a, b] = process.versions.node.split(".").map(Number); process.exit(a > 18 || (a === 18 && b >= 15) ? 0 : 1)' \
    || die "Node.js >= 18.15 is required, found $("$NODE" --version)"

# By default st-sync takes over the port SillyTavern uses today, so clients keep their URL.
[ -n "$PORT" ] || PORT=$(sed -nE 's/^port:[[:space:]]*([0-9]+).*/\1/p' "$ST_DIR/config.yaml" 2>/dev/null | head -1)
PORT=${PORT:-8000}
UPSTREAM_PORT=${UPSTREAM_PORT:-$((PORT + 10))}
[ "$PORT" != "$UPSTREAM_PORT" ] || die "--port and --upstream-port must differ"
ST_USER=${ST_USER:-$(stat -c %U "$ST_DIR")}
id "$ST_USER" > /dev/null 2>&1 || die "user $ST_USER does not exist"

if [ "$SRC" != "$PREFIX" ]; then
    mkdir -p "$PREFIX"
    cp -r "$SRC"/cdc.js "$SRC"/proxy.js "$SRC"/shim.js "$SRC"/worker.js "$SRC"/deploy "$SRC"/test "$SRC"/README.md "$PREFIX"/
fi
chmod 0755 "$PREFIX/deploy/st-sync-guard.sh" "$PREFIX/deploy/st-mode"

mkdir -p /etc/st-sync /var/lib/st-sync /var/log/st-sync
for dir in /var/lib/st-sync /var/log/st-sync; do
    [ "$(stat -c %U "$dir")" = "$ST_USER" ] || chown -R "$ST_USER" "$dir"
done

# Keep any extra ST_SYNC_* settings the admin added to the env file.
touch /etc/st-sync/env
{
    grep -vE '^(ST_SYNC_LISTEN_HOST|ST_SYNC_LISTEN_PORT|ST_SYNC_UPSTREAM_PORT|ST_SYNC_ST_DIR)=' /etc/st-sync/env || true
    echo "ST_SYNC_LISTEN_HOST=$LISTEN_HOST"
    echo "ST_SYNC_LISTEN_PORT=$PORT"
    echo "ST_SYNC_UPSTREAM_PORT=$UPSTREAM_PORT"
    echo "ST_SYNC_ST_DIR=$ST_DIR"
} > /etc/st-sync/env.new
mv /etc/st-sync/env.new /etc/st-sync/env

for unit in sillytavern-direct.service sillytavern.service st-sync.service st-sync-guard.service; do
    sed -e "s|@ST_DIR@|$ST_DIR|g" -e "s|@ST_USER@|$ST_USER|g" -e "s|@NODE@|$NODE|g" \
        -e "s|@PREFIX@|$PREFIX|g" -e "s|@PORT@|$PORT|g" -e "s|@UPSTREAM_PORT@|$UPSTREAM_PORT|g" \
        "$PREFIX/deploy/$unit" > "/etc/systemd/system/$unit"
done
install -m 0755 "$PREFIX/deploy/st-mode" /usr/local/bin/st-mode
systemctl daemon-reload
systemctl enable -q st-sync-guard.service
systemctl restart st-sync-guard.service
# Until the admin opts in with `st-mode boot proxy`, plain SillyTavern is what starts at boot.
systemctl is-enabled -q st-sync.service || systemctl enable -q sillytavern-direct.service

echo "st-sync installed in $PREFIX: port $PORT (public) -> SillyTavern on 127.0.0.1:$UPSTREAM_PORT, user $ST_USER"
if ! systemctl is-active -q st-sync.service sillytavern-direct.service; then
    holder=$(ss -ltnpH "sport = :$PORT" 2>/dev/null | sed -nE 's/.*users:\(\("([^"]+)".*/\1/p' | head -1)
    [ -z "$holder" ] || echo "note: port $PORT is in use by '$holder'; stop whatever started SillyTavern before 'st-mode proxy'"
fi
echo "next: st-mode proxy   (and st-mode boot proxy to keep it after reboot)"
