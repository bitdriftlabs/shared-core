// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf_codegen::Customize;

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=../api/");
  println!("cargo:rerun-if-changed=src/test_protos/");

  // Compile just the validation protos for use in the validation implementation.
  // TODO(mattklein123): This is split out so that this code can be moved to nonstd or more likely
  // just open sourced.
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(Customize::default().oneofs_non_exhaustive(false))
    .includes(["../api/thirdparty"])
    .input("../api/thirdparty/validate/validate.proto")
    .out_dir("src/generated/protos/")
    .capture_stderr()
    .run_from_script();

  // Compile just the test validation protos for use in the validation implementation.
  // TODO(mattklein123): OSS this per the previous TODO.
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false),
    )
    .includes(["../api/thirdparty", "src/test_protos"])
    .inputs(["src/test_protos/test_validate.proto"])
    .out_dir("src/generated/test_protos/")
    .capture_stderr()
    .run_from_script();
}
