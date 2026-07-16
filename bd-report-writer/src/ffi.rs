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
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::ptr::null;
use std::slice;

// Type alias for the gross pointers passed around for FFI to improve
// general readability
pub type BDProcessorHandle = *mut *const c_void;

type FrameOffsetSequence = Vec<u32>;
type FrameVectorOffset<'a> = WIPOffset<Vector<'a, ForwardsUOffset<Frame<'a>>>>;

pub struct ReportProcessor<'a> {
  builder: FlatBufferBuilder<'a>,
  binary_images: Vec<WIPOffset<BinaryImage<'a>>>,
  crash_info: Vec<WIPOffset<CrashInfo<'a>>>,
  is_file_size_optimization_enabled: bool,
  stack_trace_offsets: HashMap<FrameOffsetSequence, FrameVectorOffset<'a>>,
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
  memory_pressure_level: i8,
  region_format: *const c_char,
}

#[repr(C)]
pub struct BDNSException {
  name: *const c_char,
  reason: *const c_char,
}

#[repr(C)]
pub struct BDMachException {
  type_: u32,
  code: u64,
  subcode: u64,
}

#[repr(C)]
pub struct BDPosixSignal {
  number: i32,
  code: i32,
  errno_value: i32,
  has_fault_address: bool,
  fault_address: u64,
}

#[repr(C)]
pub struct BDAppleTermination {
  domain: *const c_char,
  code: *const c_char,
  explanation: *const c_char,
  process_visibility: *const c_char,
  process_state: *const c_char,
  watchdog_event: *const c_char,
  watchdog_visibility: *const c_char,
}

#[repr(C)]
pub struct BDAppleCrashInfoPayload {
  has_nsexception: bool,
  nsexception: BDNSException,
  has_mach_exception: bool,
  mach_exception: BDMachException,
  has_posix_signal: bool,
  posix_signal: BDPosixSignal,
  has_termination: bool,
  termination: BDAppleTermination,
}

#[repr(C)]
pub struct BDCrashInfoThread {
  thread: BDThread,
  stack_count: u32,
  stack: *const BDStackFrame,
}

#[repr(C)]
pub struct BDCrashInfoThreadDetails {
  count: u16,
  threads_count: usize,
  threads: *const BDCrashInfoThread,
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
  low_power_mode_enabled: bool,
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
  is_file_size_optimization_enabled: bool,
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
    crash_info: vec![],
    is_file_size_optimization_enabled,
    stack_trace_offsets: HashMap::new(),
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
  let thread_details = if processor.system_thread_count > 0 || !processor.threads.is_empty() {
    let threads = processor
      .builder
      .create_vector(processor.threads.as_slice());
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
  let crash_info = if processor.crash_info.is_empty() {
    None
  } else {
    Some(
      processor
        .builder
        .create_vector(processor.crash_info.as_slice()),
    )
  };
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
      fields: None,
      processing_result: None,
      crash_info,
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
    let frames = append_frames(
      &mut processor.builder,
      stack_count,
      stack,
      processor.is_file_size_optimization_enabled,
    );
    let stack_trace = create_or_reuse_stack_trace(processor, &frames);
    let thread = build_thread(
      &mut processor.builder,
      thread,
      stack_trace,
      processor.is_file_size_optimization_enabled,
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
  let frames = append_frames(
    &mut processor.builder,
    stack_count,
    stack,
    processor.is_file_size_optimization_enabled,
  );
  let stack_trace = Some(create_or_reuse_stack_trace(processor, &frames));
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
        low_power_mode_enabled: device.low_power_mode_enabled,
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
    let region_format = append_string(&mut processor.builder, app.region_format);
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
        region_format,
        cpu_usage: None,
        javascript_engine: JavaScriptEngine::UnknownJsEngine,
        memory_pressure_level: MemoryPressureLevel(app.memory_pressure_level),
      },
    );
    processor.app = Some(metrics);
  }

  true
}

#[unsafe(no_mangle)]
extern "C-unwind" fn bdrw_add_apple_crash_info(
  handle: BDProcessorHandle,
  reporter_scope: i8,
  reporter: i8,
  occurred_at_seconds: u64,
  occurred_at_nanos: u32,
  payload_ptr: *const BDAppleCrashInfoPayload,
  thread_details_ptr: *const BDCrashInfoThreadDetails,
) -> bool {
  let processor = try_into_processor!(handle, false);
  let Some(payload) = (unsafe { payload_ptr.as_ref() }) else {
    return false;
  };
  let Some(details) = build_apple_crash_details(&mut processor.builder, payload) else {
    return false;
  };
  let thread_details = build_crash_info_thread_details(processor, thread_details_ptr);
  let occurred_at = Timestamp::new(occurred_at_seconds, occurred_at_nanos);
  let crash_info = CrashInfo::create(
    &mut processor.builder,
    &CrashInfoArgs {
      reporter_scope: CrashReporterScope(reporter_scope),
      reporter: CrashReporter(reporter),
      occurred_at: Some(&occurred_at),
      details_type: CrashInfoDetails::AppleCrashDetails,
      details: Some(details.as_union_value()),
      thread_details,
    },
  );
  processor.crash_info.push(crash_info);
  true
}

/// Validate whether the pointer is non-null, copying the string into the
/// in-progress report buffer if so
fn append_string<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_ptr: *const c_char,
) -> Option<WIPOffset<&'a str>> {
  append_string_with(builder, str_ptr, |builder, contents| {
    builder.create_string(contents)
  })
}

fn append_shared_string<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_ptr: *const c_char,
) -> Option<WIPOffset<&'a str>> {
  append_string_with(builder, str_ptr, |builder, contents| {
    builder.create_shared_string(contents)
  })
}

fn append_string_with<'a, F>(
  builder: &mut FlatBufferBuilder<'a>,
  str_ptr: *const c_char,
  create: F,
) -> Option<WIPOffset<&'a str>>
where
  F: FnOnce(&mut FlatBufferBuilder<'a>, &str) -> WIPOffset<&'a str>,
{
  if str_ptr.is_null() {
    return None;
  }
  unsafe { CStr::from_ptr(str_ptr) }
    .to_str()
    .map(|contents| create(builder, contents))
    .ok()
}

fn append_string_slice<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_slice_ptr: *const *const c_char,
  str_slice_count: usize,
) -> Option<WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>>> {
  append_string_slice_with(
    builder,
    str_slice_ptr,
    str_slice_count,
    |builder, contents| builder.create_string(contents),
  )
}

fn append_shared_string_slice<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  str_slice_ptr: *const *const c_char,
  str_slice_count: usize,
) -> Option<WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>>> {
  append_string_slice_with(
    builder,
    str_slice_ptr,
    str_slice_count,
    |builder, contents| builder.create_shared_string(contents),
  )
}

fn append_string_slice_with<'a, F>(
  builder: &mut FlatBufferBuilder<'a>,
  str_slice_ptr: *const *const c_char,
  str_slice_count: usize,
  create: F,
) -> Option<WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>>>
where
  F: Copy + Fn(&mut FlatBufferBuilder<'a>, &str) -> WIPOffset<&'a str>,
{
  let strs = optional_slice_from_raw_parts(str_slice_ptr, str_slice_count)
    .unwrap_or_default()
    .iter()
    .filter(|maybe_str| !maybe_str.is_null())
    .map(|maybe_str| unsafe { CStr::from_ptr(*maybe_str) }.to_str())
    .filter_map(Result::ok)
    .map(|contents| create(builder, contents))
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

fn build_thread<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  thread: &BDThread,
  stack_trace: WIPOffset<Vector<'a, ForwardsUOffset<Frame<'a>>>>,
  is_file_size_optimization_enabled: bool,
) -> WIPOffset<Thread<'a>> {
  let name = append_string(builder, thread.name);
  let state = if is_file_size_optimization_enabled {
    append_shared_string(builder, thread.state)
  } else {
    append_string(builder, thread.state)
  };

  Thread::create(
    builder,
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
  )
}

fn build_crash_info_thread_details<'a>(
  processor: &mut ReportProcessor<'a>,
  thread_details_ptr: *const BDCrashInfoThreadDetails,
) -> Option<WIPOffset<ThreadDetails<'a>>> {
  let thread_details = (unsafe { thread_details_ptr.as_ref() })?;

  let threads = optional_slice_from_raw_parts(thread_details.threads, thread_details.threads_count)
    .unwrap_or_default()
    .iter()
    .map(|thread| {
      let frames = append_frames(
        &mut processor.builder,
        thread.stack_count,
        thread.stack,
        processor.is_file_size_optimization_enabled,
      );
      let stack_trace = processor.builder.create_vector(frames.as_slice());
      build_thread(
        &mut processor.builder,
        &thread.thread,
        stack_trace,
        processor.is_file_size_optimization_enabled,
      )
    })
    .collect::<Vec<_>>();

  if thread_details.count == 0 && threads.is_empty() {
    return None;
  }

  let threads = (!threads.is_empty()).then(|| processor.builder.create_vector(threads.as_slice()));
  Some(ThreadDetails::create(
    &mut processor.builder,
    &ThreadDetailsArgs {
      count: thread_details.count,
      threads,
    },
  ))
}

fn build_apple_crash_details<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  payload: &BDAppleCrashInfoPayload,
) -> Option<WIPOffset<AppleCrashDetails<'a>>> {
  let nsexception = payload.has_nsexception.then(|| {
    let name = append_string(builder, payload.nsexception.name);
    let reason = append_string(builder, payload.nsexception.reason);
    NSException::create(
      builder,
      &NSExceptionArgs {
        name,
        reason,
        user_info: None,
      },
    )
  });
  let mach_exception = payload.has_mach_exception.then(|| {
    MachException::create(
      builder,
      &MachExceptionArgs {
        type_: payload.mach_exception.type_,
        code: payload.mach_exception.code,
        subcode: payload.mach_exception.subcode,
      },
    )
  });
  let posix_signal = payload.has_posix_signal.then(|| {
    PosixSignal::create(
      builder,
      &PosixSignalArgs {
        number: payload.posix_signal.number,
        code: payload.posix_signal.code,
        errno_value: payload.posix_signal.errno_value,
        has_fault_address: payload.posix_signal.has_fault_address,
        fault_address: payload.posix_signal.fault_address,
      },
    )
  });
  let termination = payload.has_termination.then(|| {
    let domain = append_string(builder, payload.termination.domain);
    let code = append_string(builder, payload.termination.code);
    let explanation = append_string(builder, payload.termination.explanation);
    let process_visibility = append_string(builder, payload.termination.process_visibility);
    let process_state = append_string(builder, payload.termination.process_state);
    let watchdog_event = append_string(builder, payload.termination.watchdog_event);
    let watchdog_visibility = append_string(builder, payload.termination.watchdog_visibility);
    AppleTermination::create(
      builder,
      &AppleTerminationArgs {
        domain,
        code,
        explanation,
        process_visibility,
        process_state,
        watchdog_event,
        watchdog_visibility,
      },
    )
  });

  if nsexception.is_none()
    && mach_exception.is_none()
    && posix_signal.is_none()
    && termination.is_none()
  {
    return None;
  }

  Some(AppleCrashDetails::create(
    builder,
    &AppleCrashDetailsArgs {
      nsexception,
      mach_exception,
      posix_signal,
      termination,
    },
  ))
}

fn create_or_reuse_stack_trace<'a>(
  processor: &mut ReportProcessor<'a>,
  frames: &[WIPOffset<Frame<'a>>],
) -> FrameVectorOffset<'a> {
  if !processor.is_file_size_optimization_enabled {
    return processor.builder.create_vector(frames);
  }

  let frame_offset_key = frames.iter().map(|frame| frame.value()).collect::<Vec<_>>();
  processor
    .stack_trace_offsets
    .get(&frame_offset_key)
    .copied()
    .unwrap_or_else(|| {
      let stack_trace = processor.builder.create_vector(frames);
      processor
        .stack_trace_offsets
        .insert(frame_offset_key, stack_trace);
      stack_trace
    })
}

fn append_frames<'a>(
  builder: &mut FlatBufferBuilder<'a>,
  stack_count: u32,
  stack: *const BDStackFrame,
  is_file_size_optimization_enabled: bool,
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
      let symbol_name = if is_file_size_optimization_enabled {
        append_shared_string(builder, frame.symbol_name)
      } else {
        append_string(builder, frame.symbol_name)
      };
      let class_name = if is_file_size_optimization_enabled {
        append_shared_string(builder, frame.class_name)
      } else {
        append_string(builder, frame.class_name)
      };
      let image_id = if is_file_size_optimization_enabled {
        append_shared_string(builder, frame.image_id)
      } else {
        append_string(builder, frame.image_id)
      };
      let state = if is_file_size_optimization_enabled {
        append_shared_string_slice(builder, frame.state, frame.state_count)
      } else {
        append_string_slice(builder, frame.state, frame.state_count)
      };
      let reg_vec = optional_slice_from_raw_parts(frame.regs, frame.reg_count)
        .unwrap_or_default()
        .iter()
        .filter_map(|reg| {
          let name = if is_file_size_optimization_enabled {
            append_shared_string(builder, reg.name)
          } else {
            append_string(builder, reg.name)
          };
          name.map(|name| {
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
