#!/usr/bin/env bash
# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE file or at:
# https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
callgrind_output="/tmp/workflow-bench-$(date +%Y%m%d-%H%M%S).callgrind.out"
report_output=""
open_trace=false
repeat_count=1

usage() {
  cat <<'EOF'
Run the workflow benchmark under Linux Callgrind.

Usage:
  bd-workflow-bench/profile.sh [wrapper options] [-- benchmark options]

Wrapper options:
  --callgrind-output PATH  Destination Callgrind profile (default: /tmp/workflow-bench-*.out)
  --repeat COUNT            Fresh-engine corpus replays (default: 1)
  --open                    Open the profile with KCachegrind after recording
  -h, --help                Show this help text

With no benchmark options, the checked-in synthetic fixture is profiled. Everything after -- is
passed directly to bd-workflow-bench, which permits a local provided corpus. For example:
  bd-workflow-bench/profile.sh --callgrind-output /tmp/workflow-bench.out -- \
    --config /path/to/config.json \
    --logs /path/to/session.json \
    --output-dir /path/to/report
EOF
}

while (($# > 0)); do
  case "$1" in
  --callgrind-output)
    callgrind_output="${2:?missing value for --callgrind-output}"
    shift 2
    ;;
  --repeat)
    repeat_count="${2:?missing value for --repeat}"
    shift 2
    ;;
  --open)
    open_trace=true
    shift
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

if ! [[ "$repeat_count" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' "--repeat must be a positive integer: $repeat_count" >&2
  exit 2
fi

if [[ "$(uname -s)" != Linux ]]; then
  printf 'Callgrind profiling must run inside a Linux VM.\n' >&2
  exit 1
fi

if ! command -v valgrind >/dev/null; then
  printf 'valgrind is required; install it in the Linux VM.\n' >&2
  exit 1
fi

if [[ -e "$callgrind_output" ]]; then
  printf 'Callgrind output already exists: %s\n' "$callgrind_output" >&2
  exit 1
fi

# The workspace's bench profile is release-optimized and retains debug symbols, making Callgrind
# source mappings useful without measuring a debug build.
# The worktree can be mounted from macOS, whose ancestor Cargo config may point at a
# macOS-only sccache binary. Profiling needs a native Linux build, so bypass any
# configured wrapper for this invocation.
SKIP_PROTO_GEN=1 cargo --config 'build.rustc-wrapper=""' build --profile bench -p bd-workflow-bench

benchmark_binary="${repo_root}/target/release/bd-workflow-bench"
if (($# == 0)); then
  report_output="$(mktemp -d /tmp/workflow-bench-callgrind-report.XXXXXX)"
  set -- \
    --config "${script_dir}/fixtures/small/config.json" \
    --logs "${script_dir}/fixtures/small/logs.ndjson" \
    --output-dir "$report_output"
fi

valgrind \
  --tool=callgrind \
  --instr-atstart=no \
  --callgrind-out-file="$callgrind_output" \
  "$benchmark_binary" "$@" \
  --replay-count "$repeat_count" \
  --summary-only \
  --callgrind-instrument

printf 'Callgrind profile written to: %s\n' "$callgrind_output"
if [[ -n "$report_output" ]]; then
  printf 'Fixed-corpus report written to: %s\n' "$report_output"
fi
if [[ "$open_trace" == true ]]; then
  kcachegrind "$callgrind_output"
fi
