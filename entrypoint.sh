#!/usr/bin/env sh
set -eu

# Defaults match your image/environment
: "${MONBOT_BASE_DIR:=/data}"
: "${MONBOT_CACHE_DIR:=/cache}"
: "${MONBOT_REPORTS_DIR:=/reports}"
: "${APP_USER:=app}"
: "${APP_UID:=10001}"

# Ensure dirs exist
mkdir -p "$MONBOT_BASE_DIR" "$MONBOT_CACHE_DIR" "$MONBOT_REPORTS_DIR"

# Fix ownership on mounted volumes
chown -R "$APP_UID":"$APP_UID" "$MONBOT_BASE_DIR" "$MONBOT_CACHE_DIR" "$MONBOT_REPORTS_DIR" || true

# Drop privileges and exec the passed command
exec gosu "$APP_UID":"$APP_UID" "$@"
