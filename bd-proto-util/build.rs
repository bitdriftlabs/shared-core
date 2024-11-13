// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

fn main() {
  // Compile verifier library for use in verifying incoming flatbuffers. Currently the Rust
  // verifier is not production ready.
  cc::Build::new()
    .cpp(true)
    .flag("-std=c++11")
    .file("src/cpp/verify.cc")
    .include("../bd-proto/src/flatbuffers")
    .include("../thirdparty/flatbuffers/include")
    .compile("libverify.a");
}
