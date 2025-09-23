// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use cbindgen::Config;
use std::path::Path;

pub fn main() {
  if std::env::var("SKIP_FILE_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=src/lib.rs");
  cbindgen::Builder::new()
    .with_crate(Path::new("."))
    .with_config(Config::from_file(Path::new("cbindgen.toml")).unwrap())
    .generate()
    .unwrap()
    .write_to_file(Path::new("include/bd-bonjson/ffi.h"));
}
