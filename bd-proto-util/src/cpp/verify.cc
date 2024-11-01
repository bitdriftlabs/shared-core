#include "buffer_log_generated.h"

extern "C" {

bool verify_log_buffer(const uint8_t* buf, size_t buf_len) {
  auto verifier = flatbuffers::Verifier(buf, buf_len);
  return bitdrift_public::fbs::logging::v1::VerifyLogBuffer(verifier);
}

}
