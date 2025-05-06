// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#include <stdlib.h>
#include <stdint.h>

#include <bd-report-writer/ffi.h>

static const int8_t NATIVE_CRASH = 5;

/* Helper functions for inserting report data */

static void add_binary_images(BDProcessorHandle handle);
static BDStackFrame *make_frame_data(int *count);
static void add_threads(BDProcessorHandle handle);
static void add_errors(BDProcessorHandle handle);
static void add_device(BDProcessorHandle handle);
static void add_app(BDProcessorHandle handle);

/* Test function hooks */

const uint8_t *load_binary_data_only(BDProcessorHandle handle, uint64_t *len) {
  add_binary_images(handle);
  return bdrw_get_completed_buffer(handle, len);
}

const uint8_t *load_error_data_only(BDProcessorHandle handle, uint64_t *len) {
  add_errors(handle);
  return bdrw_get_completed_buffer(handle, len);
}

const uint8_t *load_thread_data_only(BDProcessorHandle handle, uint64_t *len) {
  add_threads(handle);
  return bdrw_get_completed_buffer(handle, len);
}

const uint8_t *load_full_report(BDProcessorHandle handle, uint64_t *len) {
  add_app(handle);
  add_device(handle);
  add_errors(handle);
  add_binary_images(handle);
  add_threads(handle);
  return bdrw_get_completed_buffer(handle, len);
}

void create_handle(BDProcessorHandle handle) {
  bdrw_create_buffer_handle(handle, NATIVE_CRASH, "com.example.core-tests", "1.0.2x");
}

void dispose_handle(BDProcessorHandle handle) {
  bdrw_dispose_buffer_handle(handle);
}

static void add_binary_images(BDProcessorHandle handle) {
  BDBinaryImage image1 = {
    .load_address = 640393727,
    .path = "path/to/other",
    .id = "02160235-06f6-4677-ba14-e76de7460481",
  };
  BDBinaryImage image2 = {
    .load_address = 2652165712,
    .path = "path/to/something",
    .id = "4a684168-7d9e-4296-9145-72c82f2208a7",
  };
  bdrw_add_binary_image(handle, &image1);
  bdrw_add_binary_image(handle, &image2);
}

/** allocates and populates a few frames */
static BDStackFrame *make_frame_data(int *count) {
  const char **state = calloc(2, sizeof(char *));
  state[0] = "awaiting lock";
  state[1] = "holding lock";
  BDCPURegister *regs = calloc(4, sizeof(BDCPURegister));
  regs[0] = (BDCPURegister) {"x0", 65372};
  regs[1] = (BDCPURegister) {"x1", 918612};
  regs[2] = (BDCPURegister) {"x2", 3786423942};
  regs[3] = (BDCPURegister) {"x9", 1261267};
  BDStackFrame *frames = calloc(2, sizeof(BDStackFrame));
  frames[0] = (BDStackFrame) {
    .frame_address = 39762317,
    .symbol_address = 39995736,
    .symbol_name = "get_stuff",
    .file_name = "stuff.cpp",
    .image_id = "08048567-2523-4bf9-a4e4-38ac6f8e9fb9",
    .line = 63,
    .column = 0,
    .type_ = 2,
    .state_count = 2,
    .state = state,
    .reg_count = 4,
    .regs = regs,
  };
  frames[1] = (BDStackFrame) {
    .frame_address = 1891926278,
    .symbol_address = 1778616897,
    .symbol_name = "pull",
    .image_id = "7feb183e-a826-42a5-824a-cf49c2df834d",
    .type_ = 2,
  };
  *count = 2;
  return frames;
}

static void add_threads(BDProcessorHandle handle) {
  BDThread thread = {
    .active = true,
    .index = 56,
    .name = "com.example.stuff.queue",
    .quality_of_service = 2,
    .priority = 0.8,
  };
  int count = 0;
  BDStackFrame *frames = make_frame_data(&count);
  bdrw_add_thread(handle, 5, &thread, count, frames);
  free((void *)frames[0].regs);
  free((void *)frames[0].state);
  free((void *)frames);
}

static void add_errors(BDProcessorHandle handle) {
  int count = 0;
  BDStackFrame *frames1 = make_frame_data(&count);
  bdrw_add_error(
    handle,
    "resource allocation failure",
    "unable to secure resource lock",
    1,
    count,
    frames1);
  free((void *)frames1[0].regs);
  free((void *)frames1[0].state);
  free((void *)frames1);

  bdrw_add_error(handle, "image loader blocked", 0, 1, 0, 0);
}

static void add_app(BDProcessorHandle handle) {
  BDAppMetrics app = {
    .app_id = "com.example.my-app",
    .cf_bundle_version = "5.0.2.32",
    .version = "5.0.2",
    .memory_free = 3627,
    .memory_used = 640,
    .memory_total = 23872786,
    .running_state = "inactive",
  };
  bdrw_add_app(handle, &app);
}

static void add_device(BDProcessorHandle handle) {
  const char **abis = calloc(2, sizeof(const char *));
  abis[0] = "armv7";
  abis[1] = "arm64";
  BDDeviceMetrics device = {
    .architecture = 2,
    .cpu_abi_count = 2,
    .cpu_abis = abis,
    .display_height = 640,
    .display_width = 480,
    .display_density_dpi = 300,
    .network_state = 3,
    .manufacturer = "DynaTouch",
    .model = "Mini II",
    .os_brand = "waveOS",
    .os_version = "21.2.0",
    .os_fingerprint = "65457405-ec5b-4f46-9bcc-0d98dd4584fb",
    .power_charge_percent = 20,
    .power_state = 1,
    .platform = 1,
    .rotation = 2,
    .time_seconds = 1746205766,
    .time_nanos = 3278,
    .timezone = "TVT",
  };
  bdrw_add_device(handle, &device);
  free((void *)abis);
}
