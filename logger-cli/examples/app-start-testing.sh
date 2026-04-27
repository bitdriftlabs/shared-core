#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

LOGGER_CLI="${LOGGER_CLI:-logger-cli}"
LOGGER_HOST="${LOGGER_HOST:-localhost}"
LOGGER_PORT="${LOGGER_PORT:-5501}"
BASE_PORT="${BASE_PORT:-$LOGGER_PORT}"
API_KEY="${API_KEY:-your-api-key-here}"
API_URL="${API_URL:-https://api.bitdrift.io}"
APP_ID="${APP_ID:-io.bitdrift.cli}"
APP_VERSION="${APP_VERSION:-1.0.0}"
APP_VERSION_CODE="${APP_VERSION_CODE:-10}"
PLATFORM="${PLATFORM:-android}"
MODEL="${MODEL:-Pixel-8}"
APP_START_OS="${APP_START_OS:-Android}"
OBSERVE_STATS_ACTION_ID="${OBSERVE_STATS_ACTION_ID:-}"
OBSERVE_STATS_OUTPUT_BASE="${OBSERVE_STATS_OUTPUT_BASE:-}"
OUTPUT_DIRECTORY_BASE="${OUTPUT_DIRECTORY_BASE:-}"
EXPECTED_UPLOADED_COUNTER_VALUE="${EXPECTED_UPLOADED_COUNTER_VALUE:-}"
MAX_ITERATIONS="${MAX_ITERATIONS:-10}"
PARALLELISM="${PARALLELISM:-1}"
CLEAR_DATA_DIR_PERCENT="${CLEAR_DATA_DIR_PERCENT:-0}"
SDK_DIRECTORY_BASE="${SDK_DIRECTORY_BASE:-}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-15}"
SHUTDOWN_TIMEOUT_SECONDS="${SHUTDOWN_TIMEOUT_SECONDS:-15}"
STARTUP_SLEEP_SECONDS="${STARTUP_SLEEP_SECONDS:-1}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
  echo ""
  echo -e "${BLUE}=================================================================${NC}"
  echo -e "${BLUE}$1${NC}"
  echo -e "${BLUE}=================================================================${NC}"
}

print_info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Repeatedly start logger-cli, emit a lifecycle AppStart log, then flush and shut down.

Options:
  --max-iterations N          Number of start/log/shutdown cycles to run (default: ${MAX_ITERATIONS})
  --parallelism N             Number of logger workers to run in parallel (default: ${PARALLELISM})
  --base-port N               Starting port for worker allocation (default: ${BASE_PORT})
  --sdk-directory-base PATH   Base directory for worker SDK directories
  --observe-stats-action-id ID
                              Observe uploads for a single action-ID metric
  --observe-stats-output-base PATH
                              Base directory for per-worker observer JSONL files
  --output-directory-base PATH
                              Base directory for per-worker worker logs
  --expect-uploaded-counter-value N
                              Fail if the observed action ID is not uploaded and acknowledged
                              with counter value N during an iteration
  --clear-data-dir-percent N  Percentage chance [0-100] to start with --clean-data-dir (default: ${CLEAR_DATA_DIR_PERCENT})
  --startup-timeout N         Seconds to wait for the logger port to open (default: ${STARTUP_TIMEOUT_SECONDS})
  --shutdown-timeout N        Seconds to wait for the logger process to exit after SIGINT (default: ${SHUTDOWN_TIMEOUT_SECONDS})
  --startup-sleep N           Seconds to wait after the port opens before sending AppStart (default: ${STARTUP_SLEEP_SECONDS})
  --help                      Show this help text

Environment overrides:
  LOGGER_CLI, LOGGER_HOST, LOGGER_PORT, API_KEY, API_URL, APP_ID,
  APP_VERSION, APP_VERSION_CODE, PLATFORM, MODEL, APP_START_OS,
  OBSERVE_STATS_ACTION_ID, OBSERVE_STATS_OUTPUT_BASE, OUTPUT_DIRECTORY_BASE,
  EXPECTED_UPLOADED_COUNTER_VALUE,
  BASE_PORT, PARALLELISM, SDK_DIRECTORY_BASE
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    print_error "required command not found: $1"
    exit 1
  fi
}

validate_percent() {
  case "$1" in
    ''|*[!0-9]*)
      return 1
      ;;
    *)
      [ "$1" -ge 0 ] && [ "$1" -le 100 ]
      ;;
  esac
}

validate_positive_int() {
  case "$1" in
    ''|*[!0-9]*)
      return 1
      ;;
    *)
      [ "$1" -gt 0 ]
      ;;
  esac
}

validate_non_negative_int() {
  case "$1" in
    ''|*[!0-9]*)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

validate_port() {
  case "$1" in
    ''|*[!0-9]*)
      return 1
      ;;
    *)
      [ "$1" -ge 1 ] && [ "$1" -le 65535 ]
      ;;
  esac
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --max-iterations)
        MAX_ITERATIONS="$2"
        shift 2
        ;;
      --parallelism)
        PARALLELISM="$2"
        shift 2
        ;;
      --base-port)
        BASE_PORT="$2"
        shift 2
        ;;
      --sdk-directory-base)
        SDK_DIRECTORY_BASE="$2"
        shift 2
        ;;
      --observe-stats-action-id)
        OBSERVE_STATS_ACTION_ID="$2"
        shift 2
        ;;
      --observe-stats-output-base)
        OBSERVE_STATS_OUTPUT_BASE="$2"
        shift 2
        ;;
      --output-directory-base)
        OUTPUT_DIRECTORY_BASE="$2"
        shift 2
        ;;
      --expect-uploaded-counter-value)
        EXPECTED_UPLOADED_COUNTER_VALUE="$2"
        shift 2
        ;;
      --clear-data-dir-percent)
        CLEAR_DATA_DIR_PERCENT="$2"
        shift 2
        ;;
      --startup-timeout)
        STARTUP_TIMEOUT_SECONDS="$2"
        shift 2
        ;;
      --shutdown-timeout)
        SHUTDOWN_TIMEOUT_SECONDS="$2"
        shift 2
        ;;
      --startup-sleep)
        STARTUP_SLEEP_SECONDS="$2"
        shift 2
        ;;
      --help)
        usage
        exit 0
        ;;
      *)
        print_error "unknown argument: $1"
        usage
        exit 1
        ;;
    esac
  done
}

wait_for_port() {
  local timeout_seconds="$1"
  local waited=0

  while [ "$waited" -lt "$timeout_seconds" ]; do
    if nc -z "$LOGGER_HOST" "$LOGGER_PORT" 2>/dev/null; then
      return 0
    fi

    sleep 1
    waited=$((waited + 1))
  done

  return 1
}

wait_for_process_exit() {
  local pid="$1"
  local timeout_seconds="$2"
  local waited=0

  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$timeout_seconds" ]; then
      return 1
    fi

    sleep 1
    waited=$((waited + 1))
  done

  return 0
}

should_clean_data_dir() {
  [ $((RANDOM % 100)) -lt "$CLEAR_DATA_DIR_PERCENT" ]
}

default_sdk_directory_base() {
  echo "${REPO_ROOT}/.tmp/logger-cli-app-start-workers"
}

default_worker_observe_output_path() {
  local worker_sdk_directory="$1"
  echo "${worker_sdk_directory}/stats-observation/metrics.jsonl"
}

default_output_directory_base() {
  if [ -n "$OBSERVE_STATS_OUTPUT_BASE" ]; then
    echo "$OBSERVE_STATS_OUTPUT_BASE"
  else
    echo "${SDK_DIRECTORY_BASE}/output"
  fi
}

default_worker_log_output_path() {
  local worker_index="$1"
  echo "${OUTPUT_DIRECTORY_BASE}/worker-${worker_index}.log"
}

prepare_observe_output_path() {
  local observe_output_path="$1"

  mkdir -p "$(dirname "$observe_output_path")"
  rm -f "$observe_output_path"
}

prepare_worker_log_output_path() {
  local worker_log_output_path="$1"

  mkdir -p "$(dirname "$worker_log_output_path")"
  : > "$worker_log_output_path"
}

assert_uploaded_counter_value() {
  local observe_output_path="$1"
  local worker_index="$2"
  local iteration="$3"

  if [ ! -f "$observe_output_path" ]; then
    print_error "worker ${worker_index} iteration ${iteration} did not create ${observe_output_path}"
    return 1
  fi

  local upload_uuid=""
  upload_uuid="$(jq -r --argjson expected_value "$EXPECTED_UPLOADED_COUNTER_VALUE" '
    select(.phase == "upload_attempt")
    | select(any(.metrics[]?; .metric_type == "counter" and .value == $expected_value))
    | .upload_uuid // empty
  ' "$observe_output_path" | tail -n1)"

  if [ -z "$upload_uuid" ]; then
    print_error "worker ${worker_index} iteration ${iteration} did not upload action ID ${OBSERVE_STATS_ACTION_ID} with counter value ${EXPECTED_UPLOADED_COUNTER_VALUE}"
    jq -c '.' "$observe_output_path" >&2 || true
    return 1
  fi

  if ! jq -e --arg upload_uuid "$upload_uuid" '
    select(.phase == "upload_ack")
    | select(.upload_uuid == $upload_uuid and .success == true)
  ' "$observe_output_path" >/dev/null; then
    print_error "worker ${worker_index} iteration ${iteration} uploaded action ID ${OBSERVE_STATS_ACTION_ID} with counter value ${EXPECTED_UPLOADED_COUNTER_VALUE}, but no successful ack was observed"
    jq -c '.' "$observe_output_path" >&2 || true
    return 1
  fi

  print_info "worker ${worker_index} iteration ${iteration} observed acknowledged upload for action ID ${OBSERVE_STATS_ACTION_ID} with counter value ${EXPECTED_UPLOADED_COUNTER_VALUE}"
}

start_logger() {
  local clean_flag="$1"
  local observe_output_path="$2"

  local -a command=(
    "$LOGGER_CLI"
    --host "$LOGGER_HOST"
    --port "$LOGGER_PORT"
    --sdk-directory "$LOGGER_SDK_DIRECTORY"
    start
    --api-key "$API_KEY"
    --api-url "$API_URL"
    --app-id "$APP_ID"
    --app-version "$APP_VERSION"
    --app-version-code "$APP_VERSION_CODE"
    --platform "$PLATFORM"
    --model "$MODEL"
  )

  if [ -n "$OBSERVE_STATS_ACTION_ID" ]; then
    command+=(--observe-stats-action-id "$OBSERVE_STATS_ACTION_ID")
  fi

  if [ -n "$observe_output_path" ]; then
    command+=(--observe-stats-output "$observe_output_path")
  fi

  if [ "$clean_flag" = "true" ]; then
    command+=(--clean-data-dir)
  fi

  "${command[@]}" &
  LOGGER_PID=$!
}

send_app_start() {
  "$LOGGER_CLI" --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
    --log-type lifecycle \
    --log-level info \
    --field "os" "$APP_START_OS" \
    "AppStart"
}

shutdown_logger() {
  local pid="$1"

  kill -INT "$pid"

  if ! wait_for_process_exit "$pid" "$SHUTDOWN_TIMEOUT_SECONDS"; then
    print_error "logger process $pid did not exit within ${SHUTDOWN_TIMEOUT_SECONDS}s"
    kill -TERM "$pid" 2>/dev/null || true
    return 1
  fi

  wait "$pid"
}

iterations_for_worker() {
  local worker_index="$1"
  local total_workers="$2"
  local total_iterations="$3"
  local base_iterations=$((total_iterations / total_workers))
  local remainder=$((total_iterations % total_workers))

  if [ "$worker_index" -le "$remainder" ]; then
    echo $((base_iterations + 1))
  else
    echo "$base_iterations"
  fi
}

run_worker() {
  local worker_index="$1"
  local worker_iterations="$2"
  local worker_port="$3"
  local worker_sdk_directory="$4"
  local worker_observe_output_path=""
  local worker_log_output_path="$5"
  local worker_clean_count=0
  local iteration=1

  LOGGER_PORT="$worker_port"
  LOGGER_SDK_DIRECTORY="$worker_sdk_directory"

  if [ -n "$OBSERVE_STATS_ACTION_ID" ]; then
    if [ -n "$OBSERVE_STATS_OUTPUT_BASE" ]; then
      worker_observe_output_path="${OBSERVE_STATS_OUTPUT_BASE}/worker-${worker_index}.jsonl"
    else
      worker_observe_output_path="$(default_worker_observe_output_path "$worker_sdk_directory")"
    fi
  fi

  print_header "Worker ${worker_index}"
  print_info "worker port: ${LOGGER_PORT}"
  print_info "worker sdk directory: ${LOGGER_SDK_DIRECTORY}"
  print_info "worker iterations: ${worker_iterations}"
  print_info "worker log output: ${worker_log_output_path}"
  if [ -n "$OBSERVE_STATS_ACTION_ID" ]; then
    print_info "worker observed action ID: ${OBSERVE_STATS_ACTION_ID}"
    if [ -n "$worker_observe_output_path" ]; then
      print_info "worker stats output: ${worker_observe_output_path}"
    else
      print_info "worker stats output: ${LOGGER_SDK_DIRECTORY}/stats-observation/metrics.jsonl"
    fi
  fi

  while [ "$iteration" -le "$worker_iterations" ]; do
    local clean_flag=false
    if should_clean_data_dir; then
      clean_flag=true
      worker_clean_count=$((worker_clean_count + 1))
    fi

    print_info "worker ${worker_index} iteration ${iteration}/${worker_iterations} clean=${clean_flag}"

    if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ]; then
      prepare_observe_output_path "$worker_observe_output_path"
    fi

    start_logger "$clean_flag" "$worker_observe_output_path"

    if ! wait_for_port "$STARTUP_TIMEOUT_SECONDS"; then
      print_error "worker ${worker_index} logger did not open ${LOGGER_HOST}:${LOGGER_PORT} within ${STARTUP_TIMEOUT_SECONDS}s"
      kill -TERM "$LOGGER_PID" 2>/dev/null || true
      wait "$LOGGER_PID" || true
      return 1
    fi

    sleep "$STARTUP_SLEEP_SECONDS"
    send_app_start
    shutdown_logger "$LOGGER_PID"

    if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ]; then
      if ! assert_uploaded_counter_value "$worker_observe_output_path" "$worker_index" "$iteration"; then
        return 1
      fi
    fi

    print_info "worker ${worker_index} iteration ${iteration} complete"
    iteration=$((iteration + 1))
  done

  print_info "worker ${worker_index} complete; data directory resets: ${worker_clean_count}"
}

run_workers() {
  local worker_count="$1"
  local -a worker_pids=()
  local -a worker_log_output_paths=()
  local -a worker_indexes=()
  local failure=0
  local worker_index=1

  while [ "$worker_index" -le "$worker_count" ]; do
    local worker_iterations
    worker_iterations="$(iterations_for_worker "$worker_index" "$worker_count" "$MAX_ITERATIONS")"
    if [ "$worker_iterations" -gt 0 ]; then
      local worker_port=$((BASE_PORT + worker_index - 1))
      local worker_sdk_directory="${SDK_DIRECTORY_BASE}/worker-${worker_index}"
      local worker_log_output_path
      worker_log_output_path="$(default_worker_log_output_path "$worker_index")"

      prepare_worker_log_output_path "$worker_log_output_path"

      print_info "starting worker ${worker_index} on ${LOGGER_HOST}:${worker_port}; log: ${worker_log_output_path}"

      (
        exec > >(perl -pe 's/\e\[[0-9;?]*[ -\/]*[@-~]//g' >> "$worker_log_output_path") 2>&1
        run_worker \
          "$worker_index" \
          "$worker_iterations" \
          "$worker_port" \
          "$worker_sdk_directory" \
          "$worker_log_output_path"
      ) &
      worker_pids+=("$!")
      worker_indexes+=("$worker_index")
      worker_log_output_paths+=("$worker_log_output_path")
    fi

    worker_index=$((worker_index + 1))
  done

  local index=0
  while [ "$index" -lt "${#worker_pids[@]}" ]; do
    local worker_pid="${worker_pids[$index]}"
    local finished_worker_index="${worker_indexes[$index]}"
    local finished_worker_log="${worker_log_output_paths[$index]}"

    if ! wait "$worker_pid"; then
      failure=1
      print_error "worker ${finished_worker_index} failed; log: ${finished_worker_log}"
    else
      print_info "worker ${finished_worker_index} succeeded; log: ${finished_worker_log}"
    fi

    index=$((index + 1))
  done

  return "$failure"
}

main() {
  parse_args "$@"

  require_command "$LOGGER_CLI"
  require_command nc
  require_command perl

  if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ]; then
    require_command jq
  fi

  if [ "$API_KEY" = "your-api-key-here" ]; then
    print_error "set API_KEY before running this script"
    exit 1
  fi

  if ! validate_positive_int "$MAX_ITERATIONS"; then
    print_error "--max-iterations must be a positive integer"
    exit 1
  fi

  if ! validate_positive_int "$PARALLELISM"; then
    print_error "--parallelism must be a positive integer"
    exit 1
  fi

  if ! validate_port "$BASE_PORT"; then
    print_error "--base-port must be an integer between 1 and 65535"
    exit 1
  fi

  if [ $((BASE_PORT + PARALLELISM - 1)) -gt 65535 ]; then
    print_error "--base-port plus --parallelism exceeds port 65535"
    exit 1
  fi

  if ! validate_percent "$CLEAR_DATA_DIR_PERCENT"; then
    print_error "--clear-data-dir-percent must be an integer between 0 and 100"
    exit 1
  fi

  if ! validate_positive_int "$STARTUP_TIMEOUT_SECONDS"; then
    print_error "--startup-timeout must be a positive integer"
    exit 1
  fi

  if ! validate_positive_int "$SHUTDOWN_TIMEOUT_SECONDS"; then
    print_error "--shutdown-timeout must be a positive integer"
    exit 1
  fi

  if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ] && ! validate_non_negative_int "$EXPECTED_UPLOADED_COUNTER_VALUE"; then
    print_error "--expect-uploaded-counter-value must be a non-negative integer"
    exit 1
  fi

  if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ] && [ -z "$OBSERVE_STATS_ACTION_ID" ]; then
    print_error "--expect-uploaded-counter-value requires --observe-stats-action-id"
    exit 1
  fi

  if [ -z "$SDK_DIRECTORY_BASE" ]; then
    SDK_DIRECTORY_BASE="$(default_sdk_directory_base)"
  fi

  if [ -z "$OUTPUT_DIRECTORY_BASE" ]; then
    OUTPUT_DIRECTORY_BASE="$(default_output_directory_base)"
  fi

  print_header "AppStart Iteration Test"
  print_info "iterations: ${MAX_ITERATIONS}"
  print_info "parallelism: ${PARALLELISM}"
  print_info "base port: ${BASE_PORT}"
  print_info "clear data dir chance: ${CLEAR_DATA_DIR_PERCENT}%"
  print_info "AppStart os field: ${APP_START_OS}"
  if [ -n "$OBSERVE_STATS_ACTION_ID" ]; then
    print_info "observed action ID: ${OBSERVE_STATS_ACTION_ID}"
    if [ -n "$OBSERVE_STATS_OUTPUT_BASE" ]; then
      print_info "observer output base: ${OBSERVE_STATS_OUTPUT_BASE}"
    fi
  fi
  if [ -n "$EXPECTED_UPLOADED_COUNTER_VALUE" ]; then
    print_info "expected uploaded counter value: ${EXPECTED_UPLOADED_COUNTER_VALUE}"
  fi
  if [ "$PARALLELISM" -eq 1 ]; then
    print_info "target logger: ${LOGGER_HOST}:${BASE_PORT}"
  else
    print_info "worker port range: ${LOGGER_HOST}:${BASE_PORT}-$((BASE_PORT + PARALLELISM - 1))"
  fi

  if [ -n "$SDK_DIRECTORY_BASE" ]; then
    print_info "SDK directory base: ${SDK_DIRECTORY_BASE}"
  fi
  print_info "output directory base: ${OUTPUT_DIRECTORY_BASE}"

  if ! run_workers "$PARALLELISM"; then
    print_error "one or more workers failed"
    exit 1
  fi

  print_header "Run Complete"
  print_info "completed iterations: ${MAX_ITERATIONS}"
  print_info "parallel workers: ${PARALLELISM}"
}

main "$@"
