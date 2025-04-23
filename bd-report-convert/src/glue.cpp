// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#include <cstdint>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>

#include <flatbuffers/idl.h>
#include <flatbuffers/util.h>
#include <flatbuffers/minireflect.h>

#include "flatbuffers/report_generated.h"

enum BDReaderErr {
  BDReaderErrCouldNotReadInputFile = -1,
  BDReaderErrCouldNotParseSpec = -2,
  BDReaderErrCouldNotParseInputData = -3,
  BDReaderErrCouldNotValidateOutput = -4,
};

using namespace bitdrift_public::fbs::issue_reporting::v1;
extern "C" {
int bdrc_print_json(char *bin_data_path) {
  std::ifstream data_file;
  data_file.open(bin_data_path, std::ios::binary | std::ios::in);
  data_file.seekg(0,std::ios::end);

  int length = data_file.tellg();
  data_file.seekg(0,std::ios::beg);
  char *bin_data = new char[length];
  data_file.read(bin_data, length);
  data_file.close();

  auto output = flatbuffers::FlatBufferToString(
      reinterpret_cast<uint8_t *>(bin_data),
      ReportTypeTable(),
      false, true, "", true);

  std::cout << output << std::endl;
  return 0;
}

/**
 * Create a buffer containing the binary representation of a flatbuffer file
 *
 * @param schema_data     contents of the API spec as a nul-terminated byte array
 * @param json_data_path  the path to the data to convert in JSON format
 * @param length_or_err   destination for the length of the buffer or an error
 *
 * @return an allocated buffer containing the binary data or null if parsing
 *         failed and the value of length_or_err should be checked for the
 *         problem
 */
uint8_t *bdrc_make_bin_from_json(
    uint8_t *schema_data,
    char *json_data_path,
    int32_t */* out */length_or_err
) {
  std::string json_data;

  if (!flatbuffers::LoadFile(json_data_path, false, &json_data)) {
    *length_or_err = BDReaderErrCouldNotReadInputFile;
    return nullptr;
  }

  flatbuffers::Parser parser;
  if (!parser.Parse(reinterpret_cast<const char *>(schema_data), nullptr, "report.fbs")) {
    *length_or_err = BDReaderErrCouldNotParseSpec;
    return nullptr;
  }

  if (!parser.Parse(json_data.c_str())) {
    *length_or_err = BDReaderErrCouldNotParseInputData;
    return nullptr;
  }

  auto ptr = parser.builder_.GetBufferPointer();
  std::string output;

  if (flatbuffers::GenText(parser, ptr, &output)) {
    *length_or_err = BDReaderErrCouldNotValidateOutput;
    return nullptr;
  }

  *length_or_err = parser.builder_.GetSize();
  return ptr;
}
}
