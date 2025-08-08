// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub fn main() {
  println!("cargo:rerun-if-changed=src/ffi_tests.c");
  println!("cargo:rerun-if-changed=../bd-bonjson/src/ffi.rs");
  cc::Build::new()
    .cpp(false)
    .file("src/ffi_tests.c")
    .include("../bd-bonjson/include")
    .compile("bdbj");
}
