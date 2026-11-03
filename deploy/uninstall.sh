#!/bin/bash
# Removes st-sync and gives the public port back to SillyTavern: sudo deploy/uninstall.sh [--purge]
# sillytavern-direct.service stays installed, running and enabled at boot. --purge also deletes the
# chunk store, logs, settings and the installed code.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "uninstall.sh: must run as root" >&2; exit 1; }
PREFIX=$(cd "$(dirname "$0")/.." && pwd)

systemctl disable -q --now st-sync-guard.service 2>/dev/null || true
systemctl enable -q sillytavern-direct.service
systemctl start sillytavern-direct.service
systemctl disable -q st-sync.service 2>/dev/null || true
rm -f /etc/systemd/system/st-sync.service /etc/systemd/system/st-sync-guard.service \
    /etc/systemd/system/sillytavern.service /usr/local/bin/st-mode
systemctl daemon-reload
systemctl reset-failed st-sync.service st-sync-guard.service sillytavern.service 2>/dev/null || true

if [ "${1:-}" = --purge ]; then
    rm -rf /var/lib/st-sync /var/log/st-sync /etc/st-sync "$PREFIX"
fi
echo "st-sync removed; SillyTavern serves the port directly (sillytavern-direct.service)"
