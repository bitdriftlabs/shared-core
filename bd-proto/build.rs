// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf_codegen::Customize;
use std::path::Path;

const GENERATED_HEADER: &str = r"// proto - bitdrift's client/server API definitions
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code and APIs are governed by a source available license that can be found in
// the LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
";

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=../api/");

  // Perform Rust protobuf compile.
  // TODO(mattklein123): This is manually split up into directories for sanity. This could be
  // done automatically by walking the input directories and mapping it to output directories.
  // This can be done later.
  std::fs::create_dir_all("src/protos/pulse").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .tokio_bytes(true)
        .tokio_bytes_for_string(true)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/pulse/v1/pulse.proto"])
    .out_dir("src/protos/pulse/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/bdtail").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs([
      "../api/src/bitdrift_public/protobuf/bdtail/v1/bdtail_config.proto",
      "../api/src/bitdrift_public/protobuf/bdtail/v1/bdtail_api.proto",
    ])
    .out_dir("src/protos/bdtail/")
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .tokio_bytes(true)
        .tokio_bytes_for_string(true)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/client").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs([
      "../api/src/bitdrift_public/protobuf/client/v1/api.proto",
      "../api/src/bitdrift_public/protobuf/client/v1/metric.proto",
      "../api/src/bitdrift_public/protobuf/client/v1/runtime.proto",
      "../api/src/bitdrift_public/protobuf/client/v1/matcher.proto",
    ])
    .out_dir("src/protos/client/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/config/v1").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/config/v1/config.proto"])
    .out_dir("src/protos/config/v1/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/logging").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/logging/v1/payload.proto"])
    .out_dir("src/protos/logging/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/log_matcher").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/matcher/v1/log_matcher.proto"])
    .out_dir("src/protos/log_matcher/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/workflow").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/workflow/v1/workflow.proto"])
    .out_dir("src/protos/workflow/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/insight").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/insight/v1/insight.proto"])
    .out_dir("src/protos/insight/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/filter").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/filter/v1/filter.proto"])
    .out_dir("src/protos/filter/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/mme").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false)
        .file_header(GENERATED_HEADER.to_string()),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs(["../api/src/bitdrift_public/protobuf/mme/v1/service.proto"])
    .out_dir("src/protos/mme/")
    .capture_stderr()
    .run_from_script();

  // The following are vendored third-party protos and do not have file headers.
  std::fs::create_dir_all("src/protos/google/api").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .oneofs_non_exhaustive(false),
    )
    .includes(["../api/thirdparty"])
    .inputs([
      "../api/thirdparty/google/api/http.proto",
      "../api/thirdparty/google/api/annotations.proto",
    ])
    .out_dir("src/protos/google/api/")
    .capture_stderr()
    .run_from_script();
  std::fs::create_dir_all("src/protos/prometheus/prompb").unwrap();
  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .tokio_bytes(true)
        .tokio_bytes_for_string(true)
        .oneofs_non_exhaustive(false),
    )
    .includes(["../api/thirdparty", "../api/src"])
    .inputs([
      "../api/thirdparty/gogoproto/gogo.proto",
      "../api/thirdparty/prometheus/prompb/remote.proto",
      "../api/thirdparty/prometheus/prompb/types.proto",
    ])
    .out_dir("src/protos/prometheus/prompb/")
    .capture_stderr()
    .run_from_script();

  // Perform flatbuffer compile for Rust.
  std::fs::create_dir_all("src/flatbuffers").unwrap();
  flatc_rust::run(flatc_rust::Args {
    inputs: &[Path::new(
      "../api/src/bitdrift_public/fbs/logging/v1/buffer_log.fbs",
    )],
    out_dir: Path::new("src/flatbuffers"),
    ..flatc_rust::Args::default()
  })
  .unwrap();
}
