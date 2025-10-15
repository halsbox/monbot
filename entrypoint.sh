#!/usr/bin/env sh
set -eu

# Defaults match your image/environment
: "${MONBOT_BASE_DIR:=/data}"
: "${MONBOT_CACHE_DIR:=/cache}"
: "${APP_USER:=app}"
: "${APP_UID:=10001}"
: "${CHOWN_DATA_CACHE:=1}"  # set to 0 to skip chown (faster on large volumes)

# Ensure dirs exist
mkdir -p "$MONBOT_BASE_DIR" "$MONBOT_CACHE_DIR"

# Fix ownership on mounted volumes
chown -R "$APP_UID":"$APP_UID" "$MONBOT_BASE_DIR" "$MONBOT_CACHE_DIR" || true

# Drop privileges and exec the passed command
exec gosu "$APP_UID":"$APP_UID" "$@"
