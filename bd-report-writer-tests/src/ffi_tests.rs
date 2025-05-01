// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::*;
use bd_report_writer::ffi::BDProcessorHandle;
use bd_test_helpers::float_eq;
use flatbuffers::{ForwardsUOffset, Vector};
use std::ptr::null;
use std::slice;

extern "C-unwind" {
  fn create_handle(handle: BDProcessorHandle);
  fn dispose_handle(handle: BDProcessorHandle);
  fn load_binary_data_only(handle: BDProcessorHandle, len: *mut u64) -> *const u8;
  fn load_thread_data_only(handle: BDProcessorHandle, len: *mut u64) -> *const u8;
  fn load_error_data_only(handle: BDProcessorHandle, len: *mut u64) -> *const u8;
  fn load_full_report(handle: BDProcessorHandle, len: *mut u64) -> *const u8;
}

#[test]
fn make_and_dispose_handle_test() {
  let mut handle = null();
  unsafe {
    create_handle(&mut handle);
    assert!(!handle.is_null());
    dispose_handle(&mut handle);
    assert!(handle.is_null());
  }
}

#[test]
fn invalid_handle_no_panics_test() {
  let mut handle = null();
  let mut len = 0;
  let _ = unsafe { load_full_report(&mut handle, &mut len) };
  assert_eq!(0, len);
  assert_eq!(null(), handle);
  unsafe {
    dispose_handle(&mut handle);
  }
}

#[test]
fn create_binary_images_in_order_test() {
  let mut handle = null();
  let report = unsafe {
    create_handle(&mut handle);
    let mut len = 0;
    let buf = load_binary_data_only(&mut handle, &mut len);
    assert!(!buf.is_null());
    assert_ne!(0, len);

    let data = slice::from_raw_parts(buf, usize::try_from(len).unwrap());
    root_as_report_unchecked(data)
  };
  assert_eq!(ReportType::NativeCrash, report.type_());
  assert!(report.app_metrics().is_none());
  assert!(report.device_metrics().is_none());
  assert!(report.thread_details().is_none());
  check_sdk_data(&report);
  check_binary_images(&report);


  unsafe {
    dispose_handle(&mut handle);
  }
}

#[test]
fn create_errors_test() {
  let mut handle = null();
  let report = unsafe {
    create_handle(&mut handle);
    let mut len = 0;
    let buf = load_error_data_only(&mut handle, &mut len);
    assert!(!buf.is_null());
    assert_ne!(0, len);

    let data = slice::from_raw_parts(buf, usize::try_from(len).unwrap());
    root_as_report_unchecked(data)
  };
  assert_eq!(ReportType::NativeCrash, report.type_());
  assert!(report.app_metrics().is_none());
  assert!(report.device_metrics().is_none());
  assert!(report.thread_details().is_none());
  check_sdk_data(&report);
  check_errors(&report);

  unsafe {
    dispose_handle(&mut handle);
  }
}

#[test]
fn create_threads_test() {
  let mut handle = null();
  let report = unsafe {
    create_handle(&mut handle);
    let mut len = 0;
    let buf = load_thread_data_only(&mut handle, &mut len);
    assert!(!buf.is_null());
    assert_ne!(0, len);

    let data = slice::from_raw_parts(buf, usize::try_from(len).unwrap());
    root_as_report_unchecked(data)
  };
  assert_eq!(ReportType::NativeCrash, report.type_());
  assert!(report.app_metrics().is_none());
  assert!(report.device_metrics().is_none());
  check_sdk_data(&report);
  check_thread_details(&report);

  unsafe {
    dispose_handle(&mut handle);
  }
}

#[test]
fn full_report_app_test() {
  let mut handle = null();
  let report = unsafe {
    create_handle(&mut handle);
    let mut len = 0;
    let buf = load_full_report(&mut handle, &mut len);
    assert!(!buf.is_null());
    assert_ne!(0, len);

    let data = slice::from_raw_parts(buf, usize::try_from(len).unwrap());
    root_as_report_unchecked(data)
  };
  assert_eq!(ReportType::NativeCrash, report.type_());
  check_sdk_data(&report);
  check_thread_details(&report);
  check_errors(&report);

  let app = report.app_metrics().unwrap();
  assert_eq!(Some("com.example.my-app"), app.app_id());
  assert_eq!(
    Some("5.0.2.32"),
    app.build_number().unwrap().cf_bundle_version()
  );
  assert_eq!(Some("5.0.2"), app.version());
  assert_eq!(Some("inactive"), app.running_state());
  assert_eq!(3_627, app.memory().unwrap().free());
  assert_eq!(640, app.memory().unwrap().used());
  assert_eq!(23_872_786, app.memory().unwrap().total());

  unsafe {
    dispose_handle(&mut handle);
  }
}

#[test]
fn full_report_device_test() {
  let mut handle = null();
  let report = unsafe {
    create_handle(&mut handle);
    let mut len = 0;
    let buf = load_full_report(&mut handle, &mut len);
    assert!(!buf.is_null());
    assert_ne!(0, len);

    let data = slice::from_raw_parts(buf, usize::try_from(len).unwrap());
    root_as_report_unchecked(data)
  };
  assert_eq!(ReportType::NativeCrash, report.type_());
  check_sdk_data(&report);
  check_thread_details(&report);
  check_errors(&report);

  let device = report.device_metrics().unwrap();
  assert_eq!(Some("TVT"), device.timezone());
  assert_eq!(1_746_205_766, device.time().unwrap().seconds());
  assert_eq!(3_278, device.time().unwrap().nanos());
  assert_eq!(Rotation::LandscapeRight, device.rotation());
  assert_eq!(Platform::Android, device.platform());
  assert_eq!(NetworkState::WiFi, device.network_state());
  assert_eq!(Some("DynaTouch"), device.manufacturer());
  assert_eq!(Some("Mini II"), device.model());

  let display = device.display().unwrap();
  assert_eq!(640, display.height());
  assert_eq!(480, display.width());
  assert_eq!(300, display.density_dpi());

  let power = device.power_metrics().unwrap();
  assert_eq!(PowerState::RunningOnBattery, power.power_state());
  assert_eq!(20, power.charge_percent());

  let os_build = device.os_build().unwrap();
  assert_eq!(Some("waveOS"), os_build.brand());
  assert_eq!(Some("21.2.0"), os_build.version());
  assert_eq!(
    Some("65457405-ec5b-4f46-9bcc-0d98dd4584fb"),
    os_build.fingerprint(),
  );

  let abis = device.cpu_abis().unwrap();
  assert_eq!(2, abis.len());
  assert_eq!("armv7", abis.get(0));
  assert_eq!("arm64", abis.get(1));

  unsafe {
    dispose_handle(&mut handle);
  }
}

fn check_binary_images(report: &Report<'_>) {
  let binary_images = report.binary_images().unwrap();

  assert_eq!(2, binary_images.len());
  assert_eq!(640_393_727, binary_images.get(0).load_address());
  assert_eq!(Some("path/to/other"), binary_images.get(0).path());
  assert_eq!(
    Some("02160235-06f6-4677-ba14-e76de7460481"),
    binary_images.get(0).id(),
  );
  assert_eq!(2_652_165_712, binary_images.get(1).load_address());
  assert_eq!(Some("path/to/something"), binary_images.get(1).path());
  assert_eq!(
    Some("4a684168-7d9e-4296-9145-72c82f2208a7"),
    binary_images.get(1).id(),
  );
}

fn check_errors(report: &Report<'_>) {
  let errors = report.errors().unwrap();
  assert_eq!(2, errors.len());

  let error1 = errors.get(0);
  check_frame_data(error1.stack_trace().unwrap());
  assert_eq!(Some("resource allocation failure"), error1.name());
  assert_eq!(Some("unable to secure resource lock"), error1.reason());
  assert_eq!(ErrorRelation::CausedBy, error1.relation_to_next());

  let error2 = errors.get(1);
  assert_eq!(Some("image loader blocked"), error2.name());
  assert!(error2.reason().is_none());
  assert_eq!(0, error2.stack_trace().unwrap().len());
}

fn check_thread_details(report: &Report<'_>) {
  let thread_details = report.thread_details().unwrap();
  assert_eq!(5, thread_details.count());

  let threads = thread_details.threads().unwrap();
  let thread = threads.get(0);
  assert_eq!(1, threads.len());
  assert_eq!(Some("com.example.stuff.queue"), thread.name());
  assert!(float_eq!(0.8, thread.priority(), f32));
  assert_eq!(2, thread.quality_of_service());
  assert_eq!(56, thread.index());
  assert!(thread.active());

  let frames = thread.stack_trace().unwrap();
  check_frame_data(frames);
}

fn check_sdk_data(report: &Report<'_>) {
  let sdk = report.sdk().unwrap();
  assert_eq!(Some("1.0.2x"), sdk.version());
  assert_eq!(Some("com.example.core-tests"), sdk.id());
}

fn check_frame_data(frames: Vector<'_, ForwardsUOffset<Frame<'_>>>) {
  let frame1 = frames.get(0);
  assert_eq!(2, frames.len());
  assert_eq!(Some("get_stuff"), frame1.symbol_name());
  assert_eq!(FrameType::DWARF, frame1.type_());
  assert_eq!(
    Some("08048567-2523-4bf9-a4e4-38ac6f8e9fb9"),
    frame1.image_id()
  );
  assert_eq!("awaiting lock", frame1.state().unwrap().get(0));
  assert_eq!("holding lock", frame1.state().unwrap().get(1));
  assert_eq!(Some("stuff.cpp"), frame1.source_file().unwrap().path());
  assert_eq!(63, frame1.source_file().unwrap().line());
  assert_eq!(0, frame1.source_file().unwrap().column());
  assert_eq!(4, frame1.registers().unwrap().len());
  assert_eq!(Some("x0"), frame1.registers().unwrap().get(0).name());
  assert_eq!(Some("x1"), frame1.registers().unwrap().get(1).name());
  assert_eq!(Some("x2"), frame1.registers().unwrap().get(2).name());
  assert_eq!(Some("x9"), frame1.registers().unwrap().get(3).name());
  assert_eq!(65_372, frame1.registers().unwrap().get(0).value());
  assert_eq!(918_612, frame1.registers().unwrap().get(1).value());
  assert_eq!(3_786_423_942, frame1.registers().unwrap().get(2).value());
  assert_eq!(1_261_267, frame1.registers().unwrap().get(3).value());

  let frame2 = frames.get(1);
  assert_eq!(Some("pull"), frame2.symbol_name());
  assert_eq!(FrameType::DWARF, frame2.type_());
  assert_eq!(
    Some("7feb183e-a826-42a5-824a-cf49c2df834d"),
    frame2.image_id()
  );
  assert!(frame2.source_file().is_none());
  assert!(frame2.registers().is_none());
  assert!(frame2.state().is_none());
}
