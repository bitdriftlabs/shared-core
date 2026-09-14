// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use protobuf_codegen::Customize;
use std::path::PathBuf;

pub fn generate_protos() {
  set_working_directory_to_package_root();

  // Compile the validation protos used by the validation implementation.
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(Customize::default().oneofs_non_exhaustive(false))
    .includes(["../api/thirdparty"])
    .input("../api/thirdparty/validate/validate.proto")
    .out_dir("src/generated/protos/")
    .capture_stderr()
    .run_from_script();

  // Compile the test protos used to exercise validation behavior.
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

// Support Cargo build scripts from bd-pgv and Bazel runs from the monorepo root.
fn set_working_directory_to_package_root() {
  let current_dir = std::env::current_dir().unwrap();
  let mut candidates = vec![
    current_dir.clone(),
    current_dir.join("bd-pgv"),
    current_dir.join("shared-core/bd-pgv"),
  ];
  if let Ok(workspace_dir) = std::env::var("BUILD_WORKSPACE_DIRECTORY") {
    candidates.push(PathBuf::from(workspace_dir).join("shared-core/bd-pgv"));
  }

  let package_root = candidates
    .into_iter()
    .find(|path| {
      path.join("Cargo.toml").is_file()
        && path.join("src/test_protos/test_validate.proto").is_file()
    })
    .unwrap_or_else(|| panic!("could not find the bd-pgv package root from {current_dir:?}"));

  std::env::set_current_dir(package_root).unwrap();
}
