#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
config_file="$script_dir/proto-tools.env"
source "$config_file"

usage() {
  cat >&2 <<EOF
Usage: $0 [--flatc-version <version>] [--protoc-version <version>] [--dry-run]

Downloads the Linux CI release assets, calculates SHA-256 checksums, and updates
scripts/proto-tools.env. Specify at least one version.
EOF
}

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

new_flatc_version=
new_protoc_version=
dry_run=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --flatc-version)
      new_flatc_version=${2:-}
      shift 2
      ;;
    --protoc-version)
      new_protoc_version=${2:-}
      shift 2
      ;;
    --dry-run)
      dry_run=true
      shift
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

if [[ -z "$new_flatc_version" && -z "$new_protoc_version" ]]; then
  usage
  exit 1
fi

repo_root=$(cd "$script_dir/.." && pwd)
download_dir="$repo_root/.tmp/proto-tools-update"
rm -rf "$download_dir"
mkdir -p "$download_dir"
trap 'rm -rf "$download_dir"' EXIT

if [[ -n "$new_flatc_version" ]]; then
  flatc_version=$new_flatc_version
  flatc_archive="$download_dir/flatc.zip"
  curl --fail --location --retry 3 --silent --show-error \
    --output "$flatc_archive" \
    "https://github.com/google/flatbuffers/releases/download/v$flatc_version/Linux.flatc.binary.g%2B%2B-13.zip"
  flatc_sha256=$(sha256 "$flatc_archive")
fi

if [[ -n "$new_protoc_version" ]]; then
  protoc_version=$new_protoc_version
  protoc_archive="$download_dir/protoc.zip"
  curl --fail --location --retry 3 --silent --show-error \
    --output "$protoc_archive" \
    "https://github.com/protocolbuffers/protobuf/releases/download/v$protoc_version/protoc-$protoc_version-linux-x86_64.zip"
  protoc_sha256=$(sha256 "$protoc_archive")
fi

cat <<EOF
flatc_version=$flatc_version
flatc_sha256=$flatc_sha256
protoc_version=$protoc_version
protoc_sha256=$protoc_sha256
EOF

if [[ "$dry_run" == true ]]; then
  exit 0
fi

cat > "$config_file" <<EOF
flatc_version=$flatc_version
flatc_sha256=$flatc_sha256
protoc_version=$protoc_version
protoc_sha256=$protoc_sha256
EOF
