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

  println!("cargo:rerun-if-changed=src/proto/");

  std::fs::create_dir_all("src/generated/proto").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(Customize::default().gen_mod_rs(false))
    .includes(["src/proto", "../api/protoc-gen-validate"])
    .inputs(["src/proto/test.proto"])
    .out_dir("src/generated/proto/")
    .capture_stderr()
    .run_from_script();
}
