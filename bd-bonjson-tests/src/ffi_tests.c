// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include <bd-bonjson/ffi.h>


bool open_writer(BDCrashWriterHandle handle, const char* path) {
    return bdcrw_open_writer(handle, path);
}

void close_writer(BDCrashWriterHandle handle) {
    bdcrw_close_writer(handle);
}

bool flush_writer(BDCrashWriterHandle handle) {
    return bdcrw_flush_writer(handle);
}

bool write_boolean(BDCrashWriterHandle handle, bool value) {
    return bdcrw_write_boolean(handle, value);
}

bool write_null(BDCrashWriterHandle handle) {
    return bdcrw_write_null(handle);
}

bool write_signed(BDCrashWriterHandle handle, int64_t value) {
    return bdcrw_write_signed(handle, value);
}

bool write_unsigned(BDCrashWriterHandle handle, uint64_t value) {
    return bdcrw_write_unsigned(handle, value);
}

bool write_float(BDCrashWriterHandle handle, double value) {
    return bdcrw_write_float(handle, value);
}

bool write_str(BDCrashWriterHandle handle, const char* value) {
    return bdcrw_write_str(handle, value);
}

bool write_array_begin(BDCrashWriterHandle handle) {
    return bdcrw_write_array_begin(handle);
}

bool write_map_begin(BDCrashWriterHandle handle) {
    return bdcrw_write_map_begin(handle);
}

bool write_container_end(BDCrashWriterHandle handle) {
    return bdcrw_write_container_end(handle);
}
