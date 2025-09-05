// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  AppMetricsArgs,
  DeviceMetricsArgs,
  Frame,
  FrameType,
  Report,
  Thread,
};
use flatbuffers::FlatBufferBuilder;
use nom_language::error::convert_error;

/// Run a parser and either return the successful value or print a verbose
/// description of where the parser failed
///
/// ```
/// let (remainder, output) = run_parser!(parser_func, id_collector, "some data");
/// ```
macro_rules! run_parser {
  ($parser:ident, $builder:ident, $ids:expr, $input:expr) => {
    match $parser(&mut $builder, $ids, $input.clone()) {
      Ok(value) => value,
      Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
        panic!("failed to parse: {:#?}", convert_error($input, e))
      },
      Err(e) => panic!("failed to parse: {e:#?}"),
    }
  };
}

/// Coerce data from a builder into a `FlatBuffers` table representation
macro_rules! get_table {
  ($type:ident, $builder:ident, $offset:expr) => {{
    $builder.finish($offset, None);
    match flatbuffers::root::<$type<'_>>($builder.finished_data()) {
      Ok(root) => root,
      Err(e) => panic!("failed to parse: {e:#?}"),
    }
  }};
}

fn read_fixture(path: &str) -> String {
  use std::path::PathBuf;

  let mut full_path = PathBuf::from("./fixtures/");
  full_path.push(path);
  assert!(
    full_path.exists(),
    "'{}' does not exist",
    full_path.display()
  );
  std::fs::read_to_string(full_path).unwrap()
}

#[test]
fn native_frame_test() {
  let mut builder = FlatBufferBuilder::new();
  #[rustfmt::skip]
  let input =  " native: #01 pc 000000000004c35c  /apex/com.android.runtime/lib64/bionic/libc.so (syscall+28)\n";
  let mut images = BTreeMap::new();
  let (remainder, table_offset) = run_parser!(build_frame, builder, &mut images, input);
  let frame = get_table!(Frame, builder, table_offset);
  assert_eq!("", remainder);
  assert_eq!(frame.frame_address() - 28, frame.symbol_address());

  assert_eq!(FrameType::AndroidNative, frame.type_());
  assert_eq!(0x4c35c, frame.frame_address());
  assert_eq!(1, images.len());
  assert_eq!(
    images.get(&("/apex/com.android.runtime/lib64/bionic/libc.so", None)),
    Some(&Some("a87908b48b368e6282bcc9f34bcfc28c")),
  );

  assert_eq!(Some("syscall"), frame.symbol_name());
  assert_eq!(None, frame.image_id());
}

#[test]
fn android_thread_test() {
  let mut builder = FlatBufferBuilder::new();
  #[rustfmt::skip]
  let input = "\"ADB-JDWP Connection Control Thread\" daemon prio=0 tid=7 WaitingInMainDebuggerLoop
  | group=\"system\" sCount=1 ucsCount=0 flags=1 obj=0x13040270 self=0xb400007453a03140
  | sysTid=18571 nice=-20 cgrp=top-app sched=0/0 handle=0x72c93ffcb0
  | state=S schedstat=( 1062749 127626 10 ) utm=0 stm=0 core=3 HZ=100
  | stack=0x72c9308000-0x72c930a000 stackSize=991KB
  | held mutexes=
  native: #00 pc 000000000009e698  /apex/com.android.runtime/lib64/bionic/libc.so (__ppoll+8) (BuildId: a87908b48b368e6282bcc9f34bcfc28c)
  native: #01 pc 000000000005bb10  /apex/com.android.runtime/lib64/bionic/libc.so (poll+92) (BuildId: a87908b48b368e6282bcc9f34bcfc28c)
  native: #02 pc 0000000000009dac  /apex/com.android.art/lib64/libadbconnection.so (adbconnection::AdbConnectionState::RunPollLoop(art::Thread*)+752) (BuildId: 45c8d53209d6d1f93b97abcc2d918d4d)
  native: #03 pc 000000000000840c  /apex/com.android.art/lib64/libadbconnection.so (adbconnection::CallbackFunction(void*)+1484) (BuildId: 45c8d53209d6d1f93b97abcc2d918d4d)
  native: #04 pc 00000000000b1910  /apex/com.android.runtime/lib64/bionic/libc.so (__pthread_start(void*)+264) (BuildId: a87908b48b368e6282bcc9f34bcfc28c)
  native: #05 pc 00000000000513f0  /apex/com.android.runtime/lib64/bionic/libc.so (__start_thread+64) (BuildId: a87908b48b368e6282bcc9f34bcfc28c)
  native: #06 pc 0000000000d021ec  /data/app/~~en3p1SUq==/com.example-bhTJ==/base.apk (offset 2408000) (???) (BuildId: a79f72711db804c5)
  (no managed stack frames)
";
  let mut images = BTreeMap::new();
  let (remainder, args) = run_parser!(build_thread, builder, &mut images, input);
  let table_offset = Thread::create(&mut builder, &args);
  let thread = get_table!(Thread, builder, table_offset);
  assert_eq!("", remainder);
  assert_eq!(3, images.len());
  assert_eq!(
    images.get(&("/apex/com.android.runtime/lib64/bionic/libc.so", None)),
    Some(&Some("a87908b48b368e6282bcc9f34bcfc28c")),
  );
  assert_eq!(
    images.get(&("/apex/com.android.art/lib64/libadbconnection.so", None)),
    Some(&Some("45c8d53209d6d1f93b97abcc2d918d4d")),
  );
  assert_eq!(
    images.get(&(
      "/data/app/~~en3p1SUq==/com.example-bhTJ==/base.apk",
      Some(2_408_000)
    )),
    Some(&Some("a79f72711db804c5")),
  );
  insta::assert_debug_snapshot!(thread);
}

macro_rules! assert_parsed_anr_eq {
  ($filename:expr) => {
    let mut builder = FlatBufferBuilder::new();
    let input = read_fixture($filename);
    let mut app_info = AppMetricsArgs {
      app_id: Some(builder.create_string("com.example.MyApp")),
      ..Default::default()
    };
    let mut device_info = DeviceMetricsArgs {
      model: Some(builder.create_string("Monaco")),
      ..Default::default()
    };
    let mut timestamp = None;
    match build_anr(
      &mut builder,
      &mut app_info,
      &mut device_info,
      &mut timestamp,
      &input.clone(),
    ) {
      Ok((_, offset)) => {
        let report = get_table!(Report, builder, offset);
        insta::assert_debug_snapshot!(report);
      },
      Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
        panic!("failed to parse: {:#?}", e)
      },
      Err(e) => panic!("failed to parse: {e:#?}"),
    }
  };
}

#[test]
fn anr_name_test() {
  assert_eq!(
    "User Perceived ANR",
    anr_name(Some("Input dispatching timed out"))
  );
  assert_eq!(
    "User Perceived ANR",
    anr_name(Some("User request after error"))
  );

  assert_eq!(
    "Broadcast Receiver ANR",
    anr_name(Some(
      "Broadcast of Intent { act=android.intent.action.MAIN cmp=com.example.app/.MainActivity}"
    ))
  );
  assert_eq!(
    "Service ANR",
    anr_name(Some(
      "Executing service. { act=android.intent.action.MAIN \ncmp=com.example.app/.MainActivity}"
    ))
  );
  assert_eq!(
    "Background ANR",
    anr_name(Some(
      "bg anr: Input dispatching timed out (85a07c0 com.acme.app/com.acme.app.MainActivity is not \
       responding. Waited 5001ms for MotionEvent)\n"
    ))
  );
  assert_eq!("Undetermined ANR", anr_name(Some("Full moon")));
  assert_eq!("Undetermined ANR", anr_name(None));
}

#[test]
fn full_anr1_test() {
  assert_parsed_anr_eq!("anr1.txt");
}

#[test]
fn full_anr2_test() {
  assert_parsed_anr_eq!("anr2.txt");
}

#[test]
fn anr_blocking_get_test() {
  assert_parsed_anr_eq!("anr_blocking_get.txt");
}

#[test]
fn anr_broadcast_receiver_test() {
  assert_parsed_anr_eq!("anr_broadcast_receiver.txt");
}

#[test]
fn anr_coroutines_test() {
  assert_parsed_anr_eq!("anr_coroutines.txt");
}

#[test]
fn anr_deadlock_test() {
  assert_parsed_anr_eq!("anr_deadlock.txt");
}

#[test]
fn anr_sleep_main_thread_test() {
  assert_parsed_anr_eq!("anr_sleep_main_thread.txt");
}

#[test]
fn anr_latency_test() {
  assert_parsed_anr_eq!("anr_latency_test.txt");
}
