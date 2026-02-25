#!/usr/bin/env bash
set -euo pipefail

# Emits high-cardinality network response series for plot/line charts to trigger
# cardinality overflow. Uses HTTPRequest/HTTPResponse span fields (Network Response).
#
# Usage:
#   LOGGER_CLI_BIN=logger-cli ITERATIONS=1200 DISTINCT_PATHS=600 DISTINCT_HOSTS=10 SLEEP_MS=5 \
#     ./emit_network_plot_cardinality_overflow.sh

LOGGER_CLI_BIN=${LOGGER_CLI_BIN:-logger-cli}
ITERATIONS=${ITERATIONS:-1200}
SLEEP_MS=${SLEEP_MS:-5}
VERBOSE=${VERBOSE:-0}
DISTINCT_PATHS=${DISTINCT_PATHS:-600}
DISTINCT_HOSTS=${DISTINCT_HOSTS:-10}

methods=(GET POST PUT DELETE PATCH)
statuses=(200 201 202 204 301 302 400 401 403 404 409 429 500 502 503)
content_types=(application/json application/protobuf text/plain)
error_types=(none timeout dns connection tls)

rand_index() {
  local max=$1
  echo $((RANDOM % max))
}

rand_pick() {
  local idx
  local count=$#
  if [[ "$count" -eq 0 ]]; then
    echo ""
    return
  fi
  idx=$(rand_index "$count")
  echo "${@:$((idx + 1)):1}"
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

new_span_id() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen
  else
    printf "%08x%08x%08x" "$RANDOM" "$RANDOM" "$RANDOM"
  fi
}

build_hosts() {
  local base=(api.bitdrift.dev api.bitdrift.io api.example.com uploads.example.com)
  local hosts=()
  local i=0
  while [[ ${#hosts[@]} -lt "$DISTINCT_HOSTS" ]]; do
    local seed=${base[$((i % ${#base[@]}))]}
    hosts+=("${i}.${seed}")
    i=$((i + 1))
  done
  echo "${hosts[@]}"
}

build_paths() {
  local paths=()
  local i=0
  while [[ ${#paths[@]} -lt "$DISTINCT_PATHS" ]]; do
    local group=$((i % 8))
    case "$group" in
      0) paths+=("/v1/sessions/${i}") ;;
      1) paths+=("/v1/logs/${i}") ;;
      2) paths+=("/v1/ingest/${i}") ;;
      3) paths+=("/v1/metrics/${i}") ;;
      4) paths+=("/v2/query/${i}") ;;
      5) paths+=("/v3/workflows/${i}") ;;
      6) paths+=("/v4/alerts/${i}") ;;
      7) paths+=("/v5/export/${i}") ;;
    esac
    i=$((i + 1))
  done
  echo "${paths[@]}"
}

emit_request() {
  local span_id=$1
  local method=$2
  local host=$3
  local path=$4
  local req_bytes=$5
  local content_type=$6
  local os_tag=${7:-Android}

  if [[ "$VERBOSE" -eq 1 ]]; then
    echo "[request] span_id=$span_id method=$method host=$host path=$path bytes=$req_bytes content_type=$content_type"
  fi

  "$LOGGER_CLI_BIN" log --log-type span --log-level info \
    --field _span_id "$span_id" \
    --field _span_type start \
    --field os "$os_tag" \
    --field log_type 8 \
    --field _method "$method" \
    --field _host "$host" \
    --field _path_template "$path" \
    --field _request_body_bytes_sent_count "$req_bytes" \
    --field _content_type "$content_type" \
    HTTPRequest
}

emit_response() {
  local span_id=$1
  local method=$2
  local host=$3
  local path=$4
  local status=$5
  local duration_ms=$6
  local resp_bytes=$7
  local ttfb_ms=$8
  local dns_ms=$9
  local connect_ms=${10}
  local tls_ms=${11}
  local error_type=${12}
  local os_tag=${13:-Android}

  if [[ "$VERBOSE" -eq 1 ]]; then
    echo "[response] span_id=$span_id status=$status duration_ms=$duration_ms resp_bytes=$resp_bytes ttfb_ms=$ttfb_ms dns_ms=$dns_ms connect_ms=$connect_ms tls_ms=$tls_ms error_type=$error_type"
  fi

  "$LOGGER_CLI_BIN" log --log-type span --log-level info \
    --field _span_id "$span_id" \
    --field _span_type end \
    --field os "$os_tag" \
    --field log_type 8 \
    --field _method "$method" \
    --field _host "$host" \
    --field _path_template "$path" \
    --field _status_code "$status" \
    --field _duration_ms "$duration_ms" \
    --field _response_body_bytes_received_count "$resp_bytes" \
    --field _request_body_bytes_sent_count "$req_bytes" \
    --field _ttfb_ms "$ttfb_ms" \
    --field _dns_ms "$dns_ms" \
    --field _connect_ms "$connect_ms" \
    --field _tls_ms "$tls_ms" \
    --field _error_type "$error_type" \
    HTTPResponse
}

hosts=( $(build_hosts) )
paths=( $(build_paths) )

for _ in $(seq 1 "$ITERATIONS"); do
  span_id=$(new_span_id)
  method=$(rand_pick "${methods[@]}")
  host=$(rand_pick "${hosts[@]}")
  path=$(rand_pick "${paths[@]}")
  status=$(rand_pick "${statuses[@]}")
  content_type=$(rand_pick "${content_types[@]}")
  os_tag="Android"

  req_bytes=$(rand_range 200 20000)
  resp_bytes=$(rand_range 500 300000)
  duration_ms=$(rand_range 5 9000)
  ttfb_ms=$(rand_range 1 1500)
  dns_ms=$(rand_range 1 120)
  connect_ms=$(rand_range 1 300)
  tls_ms=$(rand_range 1 250)

  if [[ "$status" -ge 500 ]]; then
    error_type=connection
  elif [[ "$status" -ge 400 ]]; then
    error_type=timeout
  else
    error_type=$(rand_pick "${error_types[@]}")
  fi

  emit_request "$span_id" "$method" "$host" "$path" "$req_bytes" "$content_type" "$os_tag"
  emit_response "$span_id" "$method" "$host" "$path" "$status" "$duration_ms" "$resp_bytes" "$ttfb_ms" "$dns_ms" "$connect_ms" "$tls_ms" "$error_type" "$os_tag"

  sleep_ms "$SLEEP_MS"
done
