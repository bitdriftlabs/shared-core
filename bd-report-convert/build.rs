// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::path::Path;

const FBS: &[&str] = &[
  "../api/src/bitdrift_public/fbs/common/v1/common.fbs",
  "../api/src/bitdrift_public/fbs/issue-reporting/v1/report.fbs",
];

pub fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_err() {
    for &spec in FBS {
      println!("cargo:rerun-if-changed={spec}");
    }
    std::fs::create_dir_all("src/flatbuffers").unwrap();

    flatc_rust::run(flatc_rust::Args {
      lang: "cpp",
      inputs: &FBS.iter().map(Path::new).collect::<Vec<_>>(),
      out_dir: Path::new("src/flatbuffers"),
      includes: &[Path::new("../api/src")],
      extra: &["--reflect-names"],
      ..flatc_rust::Args::default()
    })
    .unwrap();
  }

  let fbs = cmake::Config::new("../thirdparty/flatbuffers")
    .build_target("flatbuffers")
    .build();
  // the build destination is not entirely obvious
  // https://github.com/rust-lang/cmake-rs/issues/56
  println!("cargo:rustc-link-search={}/build", fbs.display());
  println!("cargo:rustc-link-lib=static=flatbuffers");

  println!("cargo:rerun-if-changed=src/glue.cpp");
  cc::Build::new()
    .cpp(true)
    .file("src/glue.cpp")
    .include("../thirdparty/flatbuffers/include")
    .std("c++17")
    .compile("bdrc");
}
