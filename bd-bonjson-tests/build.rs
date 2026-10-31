// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

pub fn main() {
  println!("cargo:rerun-if-changed=src/ffi_tests.c");
  println!("cargo:rerun-if-changed=../bd-bonjson-ffi/src/lib.rs");
  cc::Build::new()
    .cpp(false)
    .file("src/ffi_tests.c")
    .include("../bd-bonjson-ffi/include")
    .compile("bdbj");
}
