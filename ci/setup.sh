#!/bin/bash

set -x
set -e

# Initialize proto submodule
git submodule update --init --recursive

# Install library compile deps.
if ! [[ -z "$RUNNER_TEMP" ]]; then
  sudo apt-get update
  sudo apt-get install lld
fi
