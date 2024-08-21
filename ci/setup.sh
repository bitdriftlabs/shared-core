#!/bin/bash

set -x
set -e

# Initialize submodules
git submodule update --init --recursive

# Install library compile deps.
if ! [[ -z "$RUNNER_TEMP" ]]; then
  sudo apt-get update
  sudo apt-get install lld
fi

# Install protoc
if ! protoc --version &> /dev/null; then
  if [[ -z "$RUNNER_TEMP" ]]; then
    echo "Not running in GHA. Install protoc in your path"
    exit 1
  fi

  pushd .
  PROTOC_VERSION=27.3
  cd "$RUNNER_TEMP"
  curl -Lfs -o protoc-"${PROTOC_VERSION}"-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v"${PROTOC_VERSION}"/protoc-"${PROTOC_VERSION}"-linux-x86_64.zip
  sudo unzip protoc-"${PROTOC_VERSION}"-linux-x86_64.zip
  sudo mv bin/protoc /usr/local/bin/protoc
  popd
fi

# Install flatc
if ! flatc --version &> /dev/null; then
  if [[ -z "$RUNNER_TEMP" ]]; then
    echo "Not running in GHA. Install flatc in your path"
    exit 1
  fi

  FLATC_VERSION=24.3.7
  pushd .
  cd "$RUNNER_TEMP"
  curl -Lfs -o Linux.flatc.binary.clang++-15.zip https://github.com/google/flatbuffers/releases/download/v"${FLATC_VERSION}"/Linux.flatc.binary.clang++-15.zip
  sudo unzip Linux.flatc.binary.clang++-15.zip
  sudo mv flatc /usr/local/bin/flatc
  sudo chmod +x /usr/local/bin/flatc
  popd
fi
