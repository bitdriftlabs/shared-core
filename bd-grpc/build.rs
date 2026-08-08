// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
