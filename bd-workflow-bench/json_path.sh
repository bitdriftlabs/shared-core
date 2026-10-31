#!/usr/bin/env bash
# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE.polyform file or at:
# https://polyformproject.org/licenses/strict/1.0.0

set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

# A mounted macOS worktree can expose a Cargo config that points at a macOS-only sccache binary.
SKIP_PROTO_GEN=1 cargo --config 'build.rustc-wrapper=""' bench -p bd-workflow-bench \
  --bench json_path -- "$@"
