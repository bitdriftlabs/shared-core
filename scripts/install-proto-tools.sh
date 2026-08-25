#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$script_dir/proto-tools.env"

usage() {
  echo "Usage: $0 --directory <tool-directory>" >&2
}

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

verify_sha256() {
  local expected=$1
  local archive=$2
  local actual
  actual=$(sha256 "$archive")
  if [[ "$actual" != "$expected" ]]; then
    echo "SHA-256 mismatch for $archive: expected $expected, got $actual" >&2
    exit 1
  fi
}

tool_dir=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --directory)
      tool_dir=${2:-}
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$tool_dir" ]]; then
  usage
  exit 1
fi

download_dir="$tool_dir/downloads"
mkdir -p "$tool_dir/bin" "$tool_dir/protoc" "$download_dir"
trap 'rm -rf "$download_dir"' EXIT

flatc_archive="$download_dir/flatc.zip"
curl --fail --location --retry 3 --silent --show-error \
  --output "$flatc_archive" \
  "https://github.com/google/flatbuffers/releases/download/v$flatc_version/Linux.flatc.binary.g%2B%2B-13.zip"
verify_sha256 "$flatc_sha256" "$flatc_archive"
unzip -p "$flatc_archive" flatc > "$tool_dir/bin/flatc"
chmod +x "$tool_dir/bin/flatc"

protoc_archive="$download_dir/protoc.zip"
curl --fail --location --retry 3 --silent --show-error \
  --output "$protoc_archive" \
  "https://github.com/protocolbuffers/protobuf/releases/download/v$protoc_version/protoc-$protoc_version-linux-x86_64.zip"
verify_sha256 "$protoc_sha256" "$protoc_archive"
unzip -qo "$protoc_archive" -d "$tool_dir/protoc"
