// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

fn main() {
  println!("cargo:rerun-if-changed=src/cpp/verify.cc");
  println!("cargo:rerun-if-changed=../bd-proto/src/flatbuffers");
  println!("cargo:rerun-if-changed=../thirdparty/flatbuffers/include");
  if let Ok(buffer_log_header) = std::env::var("BD_PROTO_BUFFER_LOG_HEADER") {
    println!("cargo:rerun-if-changed={buffer_log_header}");
  }
  if let Ok(flatbuffers_header) = std::env::var("FLATBUFFERS_HEADER") {
    println!("cargo:rerun-if-changed={flatbuffers_header}");
  }

  #[cfg(feature = "buffer-log-validate")]
  // Compile verifier library for use in verifying incoming flatbuffers. Currently the Rust
  // verifier is not production ready.
  cc::Build::new()
    .cpp(true)
    .flag("-std=c++11")
    .file("src/cpp/verify.cc")
    .include(
      std::env::var("BD_PROTO_BUFFER_LOG_HEADER")
        .ok()
        .and_then(|path| std::path::Path::new(&path).parent().map(std::path::Path::to_owned))
        .as_deref()
        .unwrap_or_else(|| std::path::Path::new("../bd-proto/src/flatbuffers")),
    )
    .include(
      std::env::var("FLATBUFFERS_HEADER")
        .ok()
        .and_then(|path| std::path::Path::new(&path).parent().map(std::path::Path::to_owned))
        .as_deref()
        .unwrap_or_else(|| std::path::Path::new("../thirdparty/flatbuffers/include")),
    )
    .compile("libverify.a");
}
