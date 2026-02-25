#!/usr/bin/env bash
set -euo pipefail

# Emits logs that match the "cardinalityOverflow" workflow and generate
# configurable cardinality on custom group-by fields for count charts.
#
# Workflow match: log ~ "cardinalityOverflow"
# Group-by: single | multiple | cardinality (trackUnique true)
#
# Recommended for a single aggregated dimension:
# - Use a very high DISTINCT_USERS relative to ITERATIONS (unique-only)
# - Set TopK count low (e.g., 1) in the UI to force overflow aggregation
#
# Modes:
#   MODE=cardinality   -> High cardinality on field "cardinality"
#   MODE=multi-series  -> A small fixed number of series on field "multiple"
#   MODE=single        -> A single series on field "single"
#
# Usage:
#   LOGGER_CLI_BIN=logger-cli MODE=cardinality ITERATIONS=1500 DISTINCT_USERS=1500 UNIQUE_ONLY=1 \
#     ./emit_cardinality_overflow_count.sh
#   LOGGER_CLI_BIN=logger-cli MODE=multi-series MULTI_SERIES_USERS=20 ITERATIONS=2000 \
#     ./emit_cardinality_overflow_count.sh
#   LOGGER_CLI_BIN=logger-cli MODE=single ITERATIONS=500 \
#     ./emit_cardinality_overflow_count.sh

LOGGER_CLI_BIN=${LOGGER_CLI_BIN:-logger-cli}
MODE=${MODE:-cardinality}
ITERATIONS=${ITERATIONS:-1500}
DISTINCT_USERS=${DISTINCT_USERS:-1500}
MULTI_SERIES_USERS=${MULTI_SERIES_USERS:-20}
SINGLE_USER=${SINGLE_USER:-single_value}
UNIQUE_ONLY=${UNIQUE_ONLY:-1}
HOT_USER_RATIO=${HOT_USER_RATIO:-0}
SLEEP_MS=${SLEEP_MS:-0}
VERBOSE=${VERBOSE:-0}

rand_index() {
  local max=$1
  echo $((RANDOM % max))
}

rand_range() {
  local min=$1
  local max=$2
  echo $((min + RANDOM % (max - min + 1)))
}

sleep_ms() {
  local ms=$1
  if [[ "$ms" -gt 0 ]]; then
    sleep "0.$(printf "%03d" "$ms")"
  fi
}

build_users() {
  local users=()
  local i=0
  local target_count="$DISTINCT_USERS"

  case "$MODE" in
    single)
      users+=("$SINGLE_USER")
      echo "${users[@]}"
      return
      ;;
    multi-series)
      target_count="$MULTI_SERIES_USERS"
      ;;
    cardinality)
      target_count="$DISTINCT_USERS"
      ;;
  esac

  while [[ ${#users[@]} -lt "$target_count" ]]; do
    users+=("user_${i}_$(printf "%06x" "$RANDOM")")
    i=$((i + 1))
  done
  echo "${users[@]}"
}

users=( $(build_users) )

field_name="cardinality"
case "$MODE" in
  single)
    field_name="single"
    ;;
  multi-series)
    field_name="multiple"
    ;;
  cardinality)
    field_name="cardinality"
    ;;
esac

for i in $(seq 1 "$ITERATIONS"); do
  case "$MODE" in
    single)
      field_value="$SINGLE_USER"
      ;;
    multi-series|cardinality)
      if [[ "$UNIQUE_ONLY" -eq 1 ]]; then
        field_value=${users[$(( (i - 1) % ${#users[@]} ))]}
      else
        if [[ "$HOT_USER_RATIO" -gt 0 ]] && [[ $((RANDOM % 100)) -lt "$HOT_USER_RATIO" ]]; then
          field_value="hot_value"
        else
          field_value=${users[$(rand_index "${#users[@]}")]}
        fi
      fi
      ;;
    *)
      field_value=${users[$(rand_index "${#users[@]}")]}
      ;;
  esac

  if [[ "$VERBOSE" -eq 1 ]]; then
    echo "[log] ${field_name}=${field_value}"
  fi

  "$LOGGER_CLI_BIN" log --log-type normal --log-level info \
    --field os "Android" \
    --field "$field_name" "$field_value" \
    --field log "cardinalityOverflow" \
    "cardinalityOverflow"

  sleep_ms "$SLEEP_MS"
done
