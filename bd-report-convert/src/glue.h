// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum BDReaderErr {
  BDReaderErrCouldNotReadInputFile = -1,
  BDReaderErrCouldNotParseSpec = -2,
  BDReaderErrCouldNotParseInputData = -3,
  BDReaderErrCouldNotValidateOutput = -4,
};

struct Schema {
  const char *data;
  const char *path;
};

uint8_t *bdrc_make_bin_from_json(const struct Schema schemas[],
                                 size_t schema_count,
                                 const char *json_data_path,
                                 int32_t * /* out */ length_or_err);
#ifdef __cplusplus
}; // extern "C"
#endif
