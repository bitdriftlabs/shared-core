#!/usr/bin/env bash
# --- begin runfiles.bash initialization v2 ---
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -d ' ' -f 2-)" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -d ' ' -f 2-)" 2>/dev/null || \
  { echo >&2 "ERROR: cannot find $f"; exit 1; }
set -e
# --- end runfiles.bash initialization v2 ---

target_name="${0##*/}"
workspace="${TEST_WORKSPACE:-_main}"
fuzz_binary="$(rlocation "${workspace}/shared-core/fuzz/${target_name}_binary")"
asan_runtime="$(rlocation "rules_rs++toolchains+rustc_macos_aarch64_nightly_2026_09_15/lib/librustc-nightly_rt.asan.dylib")"

export DYLD_LIBRARY_PATH="$(dirname "$asan_runtime")${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
exec "$fuzz_binary" "$@"
