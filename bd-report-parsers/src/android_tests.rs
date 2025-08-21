// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use nom_language::error::{VerboseError, convert_error};
use std::path::PathBuf;

/// Run a parser and either return the successful value or print a verbose
/// description of where the parser failed
///
/// ```
/// let (remainder, output) = run_parser!(parser_func, "some data");
/// ```
macro_rules! run_parser {
  ($parser:ident, $input:expr) => {
    match $parser::<VerboseError<&str>>($input.clone()) {
      Ok(value) => value,
      Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
        panic!("failed to parse: {:#?}", convert_error($input, e.into()))
      },
      Err(e) => panic!("failed to parse: {e:#?}"),
    }
  };
}

/// Read file contents from the `{module}/fixtures/` directory
fn read_fixture(path: &str) -> String {
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
fn android_thread_test() {
  #[rustfmt::skip]
  let input = "\"ADB-JDWP Connection Control Thread\" daemon prio=0 tid=7 WaitingInMainDebuggerLoop
  | group=\"system\" sCount=1 ucsCount=0 flags=1 obj=0x13040270 self=0xb400007453a03140
  | sysTid=18571 nice=-20 cgrp=top-app sched=0/0 handle=0x72c93ffcb0
  | state=S schedstat=( 1062749 127626 10 ) utm=0 stm=0 core=3 HZ=100
  | stack=0x72c9308000-0x72c930a000 stackSize=991KB
  | held mutexes=
  native: #00 pc 000000000009e698  /apex/com.android.runtime/lib64/bionic/libc.so (__ppoll+8)
  native: #01 pc 000000000005bb10  /apex/com.android.runtime/lib64/bionic/libc.so (poll+92)
  native: #02 pc 0000000000009dac  /apex/com.android.art/lib64/libadbconnection.so (adbconnection::AdbConnectionState::RunPollLoop(art::Thread*)+752)
  native: #03 pc 000000000000840c  /apex/com.android.art/lib64/libadbconnection.so (adbconnection::CallbackFunction(void*)+1484)
  native: #04 pc 00000000000b1910  /apex/com.android.runtime/lib64/bionic/libc.so (__pthread_start(void*)+264)
  native: #05 pc 00000000000513f0  /apex/com.android.runtime/lib64/bionic/libc.so (__start_thread+64)
  (no managed stack frames)
";
  let (remainder, thread) = run_parser!(any_thread, input);
  assert_eq!("", remainder);
  insta::assert_debug_snapshot!(thread);
}

#[test]
fn android_thread_header_test() {
  let mut input = "\"FinalizerWatchdogDaemon\" daemon prio=5 tid=13 Waiting\n";
  let (remainder, thread) = run_parser!(thread_header, input);
  assert_eq!("", remainder);
  assert_eq!("FinalizerWatchdogDaemon", thread.name);
  assert!(thread.is_daemon);
  assert_eq!(Some(5.0), thread.priority);
  assert_eq!(Some(13), thread.tid);
  assert_eq!("Waiting", thread.state);

  input = "\"tokio-runtime-w\" prio=10 (not attached)\n";
  let (remainder, thread) = run_parser!(thread_header, input);
  assert_eq!("", remainder);
  assert_eq!("tokio-runtime-w", thread.name);
  assert!(!thread.is_daemon);
  assert_eq!(Some(10.0), thread.priority);
  assert_eq!(None, thread.tid);
  assert_eq!("(not attached)", thread.state);
}

#[test]
fn thread_props_test() {
  let input = "  | sysTid=18593 nice=-10 cgrp=top-app\n";
  let (remainder, props) = run_parser!(thread_props, input);
  assert_eq!("", remainder);
  assert_eq!(3, props.len());
  assert_eq!(("sysTid", "18593"), props[0]);
  assert_eq!(("nice", "-10"), props[1]);
  assert_eq!(("cgrp", "top-app"), props[2]);

  let input = "  | state=R schedstat=( 504874 321916 6 ) utm=0 stm=0 core=2 HZ=100\n";
  let (remainder, props) = run_parser!(thread_props, input);
  assert_eq!("", remainder);
  assert_eq!(6, props.len());
  assert_eq!(("state", "R"), props[0]);
  assert_eq!(("schedstat", "( 504874 321916 6 )"), props[1]);
  assert_eq!(("utm", "0"), props[2]);
  assert_eq!(("stm", "0"), props[3]);
  assert_eq!(("core", "2"), props[4]);
  assert_eq!(("HZ", "100"), props[5]);
}

#[test]
fn source_location_test() {
  let input = "File:64";
  let (remainder, source) = run_parser!(source_location, input);
  assert_eq!("", remainder);
  assert_eq!("File", source.path);
  assert_eq!(Some(64), source.lineno);
}

#[test]
fn source_location_no_lineno_test() {
  let input = "generated-source";
  let (remainder, source) = run_parser!(source_location, input);
  assert_eq!("", remainder);
  assert_eq!("generated-source", source.path);
  assert_eq!(None, source.lineno);
}

#[test]
fn java_frame_test() {
  let input = " at android.os.MessageQueue.next(MessageQueue.java:335)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);
  let Frame::Java {
    symbol,
    source,
    state,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(source.lineno, Some(335));
  assert_eq!(source.path, "MessageQueue.java");
  assert_eq!(symbol, "android.os.MessageQueue.next");
  assert_eq!(0, state.len());
}

#[test]
fn jni_frame_test() {
  let input = " at io.bitdrift.capture.CaptureJniLibrary.writeLog(Native method)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);
  let Frame::Java {
    symbol,
    source,
    state,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(source.lineno, None);
  assert_eq!(source.path, "Native method");
  assert_eq!(symbol, "io.bitdrift.capture.CaptureJniLibrary.writeLog");
  assert_eq!(0, state.len());
}

#[test]
fn nested_java_class_test() {
  #[rustfmt::skip]
  let input =  " at java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill(SynchronousQueue.java:461)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);
  let Frame::Java {
    symbol,
    source,
    state,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(source.lineno, Some(461));
  assert_eq!(source.path, "SynchronousQueue.java");
  assert_eq!(
    symbol,
    "java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill",
  );
  assert_eq!(0, state.len());
}

#[test]
fn native_symbolless_frame_test() {
  #[rustfmt::skip]
  let input =  " native: #05 pc 000023e8  /data/app/~~4MfcA_a2Cu2GFu6-cqewmw==/io.bitdrift.capture-naOLfV17CJ7k1lXo5DXD_w==/lib/arm64/libbugsnag-plugin-android-anr.so (???) (BuildId: a197d6316d22762904cef0c6cb53113da5e6ff0c)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);

  let Frame::Native {
    index,
    address,
    path,
    symbol,
    build_id,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(5, index);
  assert_eq!(0x23e8, address);
  assert_eq!(
    "/data/app/~~4MfcA_a2Cu2GFu6-cqewmw==/io.bitdrift.capture-naOLfV17CJ7k1lXo5DXD_w==/lib/arm64/\
     libbugsnag-plugin-android-anr.so",
    path
  );

  let Some((symbol, offset)) = symbol else {
    panic!("failed to parse symbol");
  };
  assert_eq!("???", symbol);
  assert_eq!(None, offset);
  assert_eq!(Some("a197d6316d22762904cef0c6cb53113da5e6ff0c"), build_id);
}

#[test]
fn native_java_frame_test() {
  #[rustfmt::skip]
  let input =  " native: #09 pc 0002bb10  /apex/com.android.art/javalib/core-libart.jar (java.lang.Daemons$HeapTaskDaemon.runInternal)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);

  let Frame::Native {
    index,
    address,
    path,
    symbol,
    build_id,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(9, index);
  assert_eq!(0x2bb10, address);
  assert_eq!("/apex/com.android.art/javalib/core-libart.jar", path);

  let Some((symbol, offset)) = symbol else {
    panic!("failed to parse symbol");
  };
  assert_eq!("java.lang.Daemons$HeapTaskDaemon.runInternal", symbol);
  assert_eq!(None, offset);
  assert_eq!(None, build_id);
}

#[test]
fn native_frame_test() {
  #[rustfmt::skip]
  let input =  " native: #01 pc 000000000004c35c  /apex/com.android.runtime/lib64/bionic/libc.so (syscall+28)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);

  let Frame::Native {
    index,
    address,
    path,
    symbol,
    build_id,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(1, index);
  assert_eq!(0x4c35c, address);
  assert_eq!("/apex/com.android.runtime/lib64/bionic/libc.so", path);

  let Some((symbol, offset)) = symbol else {
    panic!("failed to parse symbol");
  };
  assert_eq!("syscall", symbol);
  assert_eq!(Some(28), offset);
  assert_eq!(None, build_id);
}

#[test]
fn native_cache_frame_test() {
  #[rustfmt::skip]
  let input = " native: #27 pc 00000000020db5bc  /memfd:jit-cache (deleted) (offset 2000000) (io.bitdrift.capture.LoggerImpl.logFields+956) (BuildId: d22b3b69a6db691fdd84720465c7a214)\n";
  let (remainder, frame) = run_parser!(any_frame, input);
  assert_eq!("", remainder);
  let Frame::Native {
    index,
    address,
    path,
    symbol,
    build_id,
  } = frame
  else {
    panic!("wrong type of frame: {frame:?}");
  };
  assert_eq!(27, index);
  assert_eq!(0x20d_b5bc, address);
  assert_eq!("/memfd:jit-cache (deleted) (offset 2000000)", path);

  let Some((symbol, offset)) = symbol else {
    panic!("failed to parse symbol");
  };
  assert_eq!("io.bitdrift.capture.LoggerImpl.logFields", symbol,);
  assert_eq!(Some(956), offset);
  assert_eq!(Some("d22b3b69a6db691fdd84720465c7a214"), build_id);
}

macro_rules! assert_parsed_anr_eq {
  ($filename:expr) => {
    let input = read_fixture($filename);
    let (_, report) = run_parser!(anr, input.as_str());
    insta::assert_debug_snapshot!(report);
    let unattached_thread_count = report
      .threads
      .iter()
      .filter(|t| t.header.state == "(not attached)")
      .count();
    assert_eq!(
      report.attached_thread_count + unattached_thread_count,
      report.threads.len()
    );
  };
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
