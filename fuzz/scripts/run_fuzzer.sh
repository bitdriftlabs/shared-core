#!/bin/bash

set -e

NAME="$@"
TMP_CORPUS=/tmp/corpus_"${NAME}"
FINAL_CORPUS=./fuzz/corpus/"${NAME}"
rm -fr "${TMP_CORPUS}"
mkdir -p "${TMP_CORPUS}"
mkdir -p "${FINAL_CORPUS}"

function merge_corpus {
  echo "Merging corpus..."
  RUST_LOG=off cargo +nightly fuzz run "${NAME}" -- -merge=1 "${FINAL_CORPUS}" "${TMP_CORPUS}"
}
trap merge_corpus EXIT

RUST_LOG=off cargo +nightly fuzz run "${NAME}" -- "${TMP_CORPUS}"
