#!/usr/bin/env bash
# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE file or at:
# https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

config_path=""
logs_path=""

usage() {
  cat <<'EOF'
Run the workflow replay Criterion benchmark.

Usage:
  bd-workflow-bench/criterion.sh [--config PATH --logs PATH] [-- Criterion options]

With no corpus options, Criterion uses the checked-in synthetic fixture. Supplying both --config
and --logs replays the provided local corpus. Everything after -- is passed to Criterion.

Examples:
  bd-workflow-bench/criterion.sh
  bd-workflow-bench/criterion.sh --config /path/to/config.json --logs /path/to/session.json
  bd-workflow-bench/criterion.sh -- --save-baseline before-index
EOF
}

absolute_path() {
  local path="$1"
  local parent
  parent="$(cd -- "$(dirname -- "$path")" && pwd -P)"
  printf '%s/%s\n' "$parent" "$(basename -- "$path")"
}

while (($# > 0)); do
  case "$1" in
  --config)
    config_path="${2:?missing value for --config}"
    shift 2
    ;;
  --logs)
    logs_path="${2:?missing value for --logs}"
    shift 2
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  --)
    shift
    break
    ;;
  *)
    printf 'unknown wrapper option: %s\n\n' "$1" >&2
    usage >&2
    exit 2
    ;;
  esac
done

if [[ -z "$config_path" && -n "$logs_path" ]] || [[ -n "$config_path" && -z "$logs_path" ]]; then
  printf '%s\n' '--config and --logs must be supplied together' >&2
  exit 2
fi

if [[ -n "$config_path" ]]; then
  # Cargo runs benchmark binaries from the package directory, so preserve the caller's relative
  # paths by resolving them before handing them to the process.
  export BD_WORKFLOW_BENCH_CONFIG="$(absolute_path "$config_path")"
  export BD_WORKFLOW_BENCH_LOGS="$(absolute_path "$logs_path")"
fi

# A mounted macOS worktree can expose a Cargo config that points at a macOS-only sccache binary.
SKIP_PROTO_GEN=1 cargo --config 'build.rustc-wrapper=""' bench -p bd-workflow-bench \
  --bench workflow_replay -- "$@"
