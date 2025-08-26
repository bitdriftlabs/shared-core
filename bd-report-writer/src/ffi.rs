// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// The C and Friends interface to creating a Report from raw parts.
//
// To begin, create a processor using `bdrw_create_buffer_handle()`, holding on
// to this very special pointer for the remaining operations to append to the
// report, then dispose of it using `bdrw_dispose_buffer_handle()`.

// Wildcard imports seem reasonable once importing 20+ components of a package
#[allow(clippy::wildcard_imports)]
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::*;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use std::ffi::{CStr, c_char, c_void};
use std::ptr::null;
use std::slice;

// Type alias for the gross pointers passed around for FFI to improve
// general readability
pub type BDProcessorHandle = *mut *const c_void;

pub struct ReportProcessor<'a> {
  builder: FlatBufferBuilder<'a>,
  binary_images: Vec<WIPOffset<BinaryImage<'a>>>,
  system_thread_count: u16,
  threads: Vec<WIPOffset<Thread<'a>>>,
  errors: Vec<WIPOffset<Error<'a>>>,
  report_type: ReportType,
  sdk: WIPOffset<SDKInfo<'a>>,
  app: Option<WIPOffset<AppMetrics<'a>>>,
  device: Option<WIPOffset<DeviceMetrics<'a>>>,
}

impl ReportProcessor<'_> {
  #[must_use]
  pub fn into_raw(self) -> *const c_void {
    Box::into_raw(Box::new(self)) as *const _
  }
}

impl TryFrom<BDProcessorHandle> for &mut ReportProcessor<'_> {
  type Error = ();

  /// # Safety
  /// This function dereferences a raw pointer and must be called with a valid handle
  #[allow(clippy::not_unsafe_ptr_arg_deref)]
  fn try_from(value: BDProcessorHandle) -> Result<Self, Self::Error> {
    unsafe {
      if value.is_null() || (*value).is_null() {
        return Err(());
      }
      (*value as *mut ReportProcessor<'_>).as_mut().ok_or(())
    }
  }
}

// Prefixed struct types (to differentiate from the associated generated
// flatbuffer types)
#[repr(C)]
pub struct BDAppMetrics {
  app_id: *const c_char,
  version: *const c_char,
  version_code: i64,
  cf_bundle_version: *const c_char,
  running_state: *const c_char,
  memory_used: u64,
  memory_free: u64,
  memory_total: u64,
}

#[repr(C)]
pub struct BDDeviceMetrics {
  time_seconds: u64,
  time_nanos: u32,
  timezone: *const c_char,
  manufacturer: *const c_char,
  model: *const c_char,
  os_version: *const c_char,
  os_brand: *const c_char,
  os_fingerprint: *const c_char,
  os_kernversion: *const c_char,
  power_state: i8,
  power_charge_percent: u8,
  network_state: i8,
  architecture: i8,
  display_height: u32,
  display_width: u32,
  display_density_dpi: u32,
  platform: i8,
  rotation: i8,
  cpu_abi_count: u8,
  cpu_abis: *const *const c_char,
}

#[repr(C)]
pub struct BDBinaryImage {
  id: *const c_char,
  path: *const c_char,
  load_address: u64,
}

#[repr(C)]
pub struct BDCPURegister {
  name: *const c_char,
  value: u64,
}

#[repr(C)]
pub struct BDStackFrame {
  type_: i8,
  frame_address: u64,
  symbol_address: u64,
  symbol_name: *const c_char,
  class_name: *const c_char,
  file_name: *const c_char,
  line: i64,
  column: i64,
  image_id: *const c_char,
  state_count: usize,
  state: *const *const c_char,
  reg_count: usize,
  regs: *const BDCPURegister,
}

#[repr(C)]
pub struct BDThread {
  name: *const c_char,
  state: *const c_char,
  active: bool,
  index: u32,
  priority: f32,
  quality_of_service: i8,
}

/// Entry point to creating a new Report
///
/// * `handle`      - a reference to a pointer which will be replaced with a valid report processor.
///   This reference should be retained and used for all other calls to `bdrw_*` functions.
/// * `report_type` - raw value of the `ReportType` which will be written
/// * `sdk_id`      - identifier of the integration sending the report
/// * `sdk_version` - version of the integration sending the report
///
/// This function returns an object which is not managed by normal lifetime semantics and must be
/// discarded using `bdrw_dispose_buffer_handle()`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_create_buffer_handle(
  handle: BDProcessorHandle,
  report_type: i8,
  sdk_id: *const c_char,
  sdk_version: *const c_char,
) {
  let mut builder = flatbuffers::FlatBufferBuilder::new();
  let id = append_string(&mut builder, sdk_id);
  let version = append_string(&mut builder, sdk_version);
  let sdk = SDKInfo::create(&mut builder, &SDKInfoArgs { id, version });
  let processor = ReportProcessor {
    builder,
    sdk,
    report_type: ReportType(report_type),
    binary_images: vec![],
    system_thread_count: 0,
    threads: vec![],
    errors: vec![],
    app: None,
    device: None,
  };
  unsafe {
    *handle = processor.into_raw();
  }
}

/// Attempt to convert a raw handle into a reference to a `ReportProcessor`
///
/// * `$handle_ptr_ptr`  - raw handle
/// * `$failure_ret_val` - value to return upon failure to convert
macro_rules! try_into_processor {
  ($handle_ptr_ptr:ident, $failure_ret_val:expr) => {{
    let proc: Result<&mut ReportProcessor<'_>, _> = $handle_ptr_ptr.try_into();
    if proc.is_err() {
      return $failure_ret_val;
    }
    proc.unwrap()
  }};
}

/// Finishes Report creation and returns a pointer to the memory buffer
/// containing the report
///
/// This function returns a null pointer in the event that the `handle`
/// pointer is invalid. It should also only be called **once**.
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_get_completed_buffer(
  handle: BDProcessorHandle,
  buffer_length: *mut u64,
) -> *const u8 {
  let processor = try_into_processor!(handle, null());
  let binary_images = processor
    .builder
    .create_vector(processor.binary_images.as_slice());
  let threads = processor
    .builder
    .create_vector(processor.threads.as_slice());
  let thread_details = if processor.system_thread_count > 0 || !processor.threads.is_empty() {
    Some(ThreadDetails::create(
      &mut processor.builder,
      &ThreadDetailsArgs {
        count: processor.system_thread_count,
        threads: Some(threads),
      },
    ))
  } else {
    None
  };
  let errors = processor.builder.create_vector(processor.errors.as_slice());
  let report = Report::create(
    &mut processor.builder,
    &ReportArgs {
      binary_images: Some(binary_images),
      sdk: Some(processor.sdk),
      type_: processor.report_type,
      app_metrics: processor.app,
      device_metrics: processor.device,
      errors: Some(errors),
      thread_details,
      feature_flags: None,
      state: None,
    },
  );
  processor.builder.finish(report, None);
  let data = processor.builder.finished_data();
  if !buffer_length.is_null() {
    unsafe {
      *buffer_length = data.len() as u64;
    }
  }
  data.as_ptr()
}

/// Disposes of the handle and *the associated memory buffer*.
///
/// All actions involving the memory buffer returned from
/// `bdrw_get_completed_buffer()` should be completed prior to calling this
/// function.
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_dispose_buffer_handle(handle: BDProcessorHandle) {
  unsafe {
    if handle.is_null() || (*handle).is_null() {
      return;
    }
    drop(Box::<ReportProcessor<'_>>::from_raw(*handle as *mut _));
    *handle = null();
  }
}

/// Add a new binary image to the report. A valid value for both `image.id`
/// and `image.path` is required to add the image to the report or else the
/// input is discarded (returning `false`).
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_binary_image(
  handle: BDProcessorHandle,
  image: *const BDBinaryImage,
) -> bool {
  let processor = try_into_processor!(handle, false);
  if let Some(unpacked_image) = unsafe { image.as_ref() }.and_then(|image| {
    let id = append_string(&mut processor.builder, image.id);
    let path = append_string(&mut processor.builder, image.path);
    if id.is_some() && path.is_some() {
      Some(BinaryImage::create(
        &mut processor.builder,
        &BinaryImageArgs {
          id,
          path,
          load_address: image.load_address,
        },
      ))
    } else {
      None
    }
  }) {
    processor.binary_images.push(unpacked_image);
    true
  } else {
    false
  }
}

/// Add a thread to the report, where `system_thread_count` is the (purely
/// informational) total number of threads running on the host system
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_thread(
  handle: BDProcessorHandle,
  system_thread_count: u16,
  thread_ptr: *const BDThread,
  stack_count: u32,
  stack: *const BDStackFrame,
) -> bool {
  let processor = try_into_processor!(handle, false);
  processor.system_thread_count = system_thread_count;

  if let Some(thread) = unsafe { thread_ptr.as_ref() } {
    let name = append_string(&mut processor.builder, thread.name);
    let state = append_string(&mut processor.builder, thread.state);
    let frames = append_frames(&mut processor.builder, stack_count, stack);
    let stack_trace = processor.builder.create_vector(frames.as_slice());
    let thread = Thread::create(
      &mut processor.builder,
      &ThreadArgs {
        name,
        active: thread.active,
        index: thread.index,
        state,
        priority: thread.priority,
        quality_of_service: thread.quality_of_service,
        stack_trace: Some(stack_trace),
        summary: None,
      },
    );
    processor.threads.push(thread);
    true
  } else {
    false
  }
}

/// Add an error to a report
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_error(
  handle: BDProcessorHandle,
  name: *const c_char,
  reason: *const c_char,
  relation_to_next: i8,
  stack_count: u32,
  stack: *const BDStackFrame,
) -> bool {
  let processor = try_into_processor!(handle, false);
  let name = append_string(&mut processor.builder, name);
  let reason = append_string(&mut processor.builder, reason);
  let frames = append_frames(&mut processor.builder, stack_count, stack);
  let stack_trace = Some(processor.builder.create_vector(frames.as_slice()));
  let error = Error::create(
    &mut processor.builder,
    &ErrorArgs {
      name,
      reason,
      stack_trace,
      relation_to_next: ErrorRelation(relation_to_next),
    },
  );
  processor.errors.push(error);
  true
}

/// Add device info to the report
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_device(
  handle: BDProcessorHandle,
  device_ptr: *const BDDeviceMetrics,
) -> bool {
  let processor = try_into_processor!(handle, false);
  if let Some(device) = unsafe { device_ptr.as_ref() } {
    let timezone = append_string(&mut processor.builder, device.timezone);
    let manufacturer = append_string(&mut processor.builder, device.manufacturer);
    let model = append_string(&mut processor.builder, device.model);
    let version = append_string(&mut processor.builder, device.os_version);
    let brand = append_string(&mut processor.builder, device.os_brand);
    let fingerprint = append_string(&mut processor.builder, device.os_fingerprint);
    let kern_osversion = append_string(&mut processor.builder, device.os_kernversion);
    let stamp = Timestamp::new(device.time_seconds, device.time_nanos);
    let time = (device.time_seconds > 0).then_some(&stamp);
    let os_build = OSBuild::create(
      &mut processor.builder,
      &OSBuildArgs {
        version,
        brand,
        fingerprint,
        kern_osversion,
      },
    );
    let cpu_abis = append_string_slice(
      &mut processor.builder,
      device.cpu_abis,
      device.cpu_abi_count as usize,
    );
    let power_metrics = if device.power_state > 0 || device.power_charge_percent > 0 {
      Some(PowerMetrics::create(
        &mut processor.builder,
        &PowerMetricsArgs {
          power_state: PowerState(device.power_state),
          charge_percent: device.power_charge_percent,
        },
      ))
    } else {
      None
    };
    let display = if (device.display_width > 0 && device.display_height > 0)
      || device.display_density_dpi > 0
    {
      Some(Display::create(
        &mut processor.builder,
        &DisplayArgs {
          height: device.display_height,
          width: device.display_width,
          density_dpi: device.display_density_dpi,
        },
      ))
    } else {
      None
    };
    let metrics = DeviceMetrics::create(
      &mut processor.builder,
      &DeviceMetricsArgs {
        time,
        timezone,
        power_metrics,
        network_state: NetworkState(device.network_state),
        rotation: Rotation(device.rotation),
        arch: Architecture(device.architecture),
        display,
        manufacturer,
        model,
        os_build: Some(os_build),
        platform: Platform(device.platform),
        cpu_abis,
        low_power_mode_enabled: false,
        cpu_usage: None,
        thermal_state: 0,
      },
    );
    processor.device = Some(metrics);
  }

  true
}

/// Add app info to the report
///
/// # Safety
/// This function is safe to call provided `handle` is a valid reference to
/// a report processor, created with `bdrw_create_buffer_handle`
#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_app(handle: BDProcessorHandle, app_ptr: *const BDAppMetrics) -> bool {
  let processor = try_into_processor!(handle, false);
  if let Some(app) = unsafe { app_ptr.as_ref() } {
    let app_id = append_string(&mut processor.builder, app.app_id);
    let version = append_string(&mut processor.builder, app.version);
    let cf_bundle_version = append_string(&mut processor.builder, app.cf_bundle_version);
    let running_state = append_string(&mut processor.builder, app.running_state);
    let build_number = AppBuildNumber::create(
      &mut processor.builder,
      &AppBuildNumberArgs {
        version_code: app.version_code,
        cf_bundle_version,
      },
    );
    let mem_struct = Memory::new(app.memory_total, app.memory_free, app.memory_used);
    let memory =
      (app.memory_used > 0 || app.memory_free > 0 || app.memory_total > 0).then_some(&mem_struct);
    let metrics = AppMetrics::create(
      &mut processor.builder,
      &AppMetricsArgs {
        app_id,
        memory,
        version,
        build_number: Some(build_number),
        running_state,
        process_id: 0,
        lifecycle_event: None,
        region_format: None,
        cpu_usage: None,
      },
    );
    processor.app = Some(metrics);
  }

  true
}

/// Validate whether the pointer is non-null, copying the string into the
/// in-progress report buffer if so
fn append_string<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_ptr: *const c_char,
) -> Option<WIPOffset<&'a str>> {
  if str_ptr.is_null() {
    return None;
  }
  unsafe { CStr::from_ptr(str_ptr) }
    .to_str()
    .map(|contents| builder.create_string(contents))
    .ok()
}

fn append_string_slice<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_slice_ptr: *const *const c_char,
  str_slice_count: usize,
) -> Option<WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>>> {
  let strs = optional_slice_from_raw_parts(str_slice_ptr, str_slice_count)
    .unwrap_or_default()
    .iter()
    .filter(|maybe_str| !maybe_str.is_null())
    .map(|maybe_str| unsafe { CStr::from_ptr(*maybe_str) }.to_str())
    .filter_map(Result::ok)
    .map(|contents| builder.create_string(contents))
    .collect::<Vec<_>>();

  if strs.is_empty() {
    return None;
  }

  Some(builder.create_vector(strs.as_slice()))
}

fn optional_slice_from_raw_parts<'a, T>(raw_parts: *const T, count: usize) -> Option<&'a [T]> {
  if raw_parts.is_null() || count == 0 {
    return None;
  }
  Some(unsafe { slice::from_raw_parts(raw_parts, count) })
}

fn append_frames<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  stack_count: u32,
  stack: *const BDStackFrame,
) -> Vec<WIPOffset<Frame<'a>>> {
  optional_slice_from_raw_parts(stack, stack_count as usize)
    .unwrap_or_default()
    .iter()
    .map(|frame| {
      let source_file = append_string(builder, frame.file_name).map(|path| {
        SourceFile::create(
          builder,
          &SourceFileArgs {
            path: Some(path),
            line: frame.line,
            column: frame.column,
          },
        )
      });
      let symbol_name = append_string(builder, frame.symbol_name);
      let class_name = append_string(builder, frame.class_name);
      let image_id = append_string(builder, frame.image_id);
      let state = append_string_slice(builder, frame.state, frame.state_count);
      let reg_vec = optional_slice_from_raw_parts(frame.regs, frame.reg_count)
        .unwrap_or_default()
        .iter()
        .filter_map(|reg| {
          append_string(builder, reg.name).map(|name| {
            CPURegister::create(
              builder,
              &CPURegisterArgs {
                name: Some(name),
                value: reg.value,
              },
            )
          })
        })
        .collect::<Vec<_>>();
      let registers = if reg_vec.is_empty() {
        None
      } else {
        Some(builder.create_vector(reg_vec.as_slice()))
      };
      Frame::create(
        builder,
        &FrameArgs {
          symbol_address: frame.symbol_address,
          frame_address: frame.frame_address,
          source_file,
          symbol_name,
          class_name,
          image_id,
          state,
          type_: FrameType(frame.type_),
          registers,
          ..Default::default()
        },
      )
    })
    .collect::<Vec<_>>()
}
