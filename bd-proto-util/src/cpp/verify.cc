// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#include "buffer_log_generated.h"

extern "C" {

bool verify_log_buffer(const uint8_t* buf, size_t buf_len) {
  auto verifier = flatbuffers::Verifier(buf, buf_len);
  return bitdrift_public::fbs::logging::v1::VerifyLogBuffer(verifier);
}

}
