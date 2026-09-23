#!/usr/bin/env bash

# Emits the unique log body used by alert-resolution-demo-workflow.json for five minutes.
# The workflow targets any Android app and the alert uses a five-minute count window.
# After this script exits, the alert resolves once the final bucket ages out of that window.

set -euo pipefail

logger_cli_bin="${LOGGER_CLI_BIN:-logger-cli}"
logger_host="${LOGGER_HOST:-localhost}"
logger_port="${LOGGER_PORT:-5501}"
active_for_seconds=300
emit_interval_seconds=20
message="alert-resolution-demo-U08ACDWL38A"

if ! nc -z "$logger_host" "$logger_port" 2>/dev/null; then
  echo "logger-cli is not listening on ${logger_host}:${logger_port}. Start it, then rerun this script." >&2
  exit 1
fi

echo "Emitting alert-resolution test logs for five minutes."
started_at=$SECONDS
while (( SECONDS - started_at < active_for_seconds )); do
  "$logger_cli_bin" --host "$logger_host" --port "$logger_port" log \
    --log-type normal --log-level info "$message"
  sleep "$emit_interval_seconds"
done

echo "Emission complete. No more matching logs will be sent; wait for the alert to resolve."
