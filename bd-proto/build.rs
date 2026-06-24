// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use proto_config::ProtoConfig;
use protobuf_codegen::Customize;
use std::fmt::Display;
use std::path::Path;

#[path = "src/proto_config.rs"]
mod proto_config;

const GENERATED_HEADER: &str = r"// proto - bitdrift's client/server API definitions
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code and APIs are governed by a source available license that can be found in
// the LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
";

fn handle_codegen_result<E: Display>(result: Result<(), E>) {
  if let Err(error) = result {
    let error = format!("{error:#}")
      .replace("\\n", "\n")
      .replace("\\\"", "\"")
      .replace("stderr: \"", "stderr:\n");
    let error = error
      .strip_suffix('"')
      .map_or(error.as_str(), |value| value);
    panic!("codegen failed:\n{error}");
  }
}

// Generate proto files for a set of configs. When `include_source_info` is true, the
// `--include_source_info` flag is passed to protoc so generated descriptors retain comments.
fn generate_protos(
  configs: Vec<ProtoConfig>,
  output_dir_override: Option<&dyn Fn(&str) -> String>,
  include_source_info: bool,
) {
  for config in configs {
    let output_dir =
      output_dir_override.map_or_else(|| config.output_dir.to_string(), |f| f(config.output_dir));
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut customize = Customize::default()
      .gen_mod_rs(false)
      .oneofs_non_exhaustive(false);

    if config.use_tokio_bytes {
      customize = customize.tokio_bytes(true).tokio_bytes_for_string(true);
    }

    if config.file_header {
      customize = customize.file_header(GENERATED_HEADER.to_string());
    }

    let mut codegen = protobuf_codegen::Codegen::new();
    codegen.protoc();

    if include_source_info {
      codegen.protoc_extra_arg("--include_source_info");
    }

    handle_codegen_result(
      codegen
        .customize(customize)
        .includes(config.includes)
        .inputs(config.inputs)
        .out_dir(&output_dir)
        .capture_stderr()
        .run(),
    );
  }
}

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=../api/");

  // Perform Rust protobuf compile using config-driven approach.
  generate_protos(proto_config::get_proto_configs(), None, false);

  // Generate a second copy of existing protos with source info. When the `with-source-info`
  // feature is enabled, each module's mod.rs swaps to these via `#[path]` so that comments
  // propagate when public API protos reference these types.
  generate_protos(
    proto_config::get_proto_configs(),
    Some(&|dir: &str| format!("{dir}/with_source")),
    true,
  );

  // Generate public API protos without source info.
  generate_protos(proto_config::get_public_api_proto_configs(), None, false);

  // Generate public API protos with source info into `_with_source` directories.
  generate_protos(
    proto_config::get_public_api_proto_configs(),
    Some(&|dir: &str| dir.replacen("public_api", "public_api_with_source", 1)),
    true,
  );

  // Perform flatbuffer compile for Rust.
  std::fs::create_dir_all("src/flatbuffers").unwrap();
  flatc_rust::run(flatc_rust::Args {
    inputs: &[
      Path::new("../api/src/bitdrift_public/fbs/common/v1/common.fbs"),
      Path::new("../api/src/bitdrift_public/fbs/logging/v1/buffer_log.fbs"),
    ],
    includes: &[Path::new("../api/src")],
    out_dir: Path::new("src/flatbuffers"),
    extra: &["--gen-object-api"],
    ..flatc_rust::Args::default()
  })
  .unwrap();
  flatc_rust::run(flatc_rust::Args {
    inputs: &[
      Path::new("../api/src/bitdrift_public/fbs/common/v1/common.fbs"),
      Path::new("../api/src/bitdrift_public/fbs/issue-reporting/v1/report.fbs"),
    ],
    includes: &[Path::new("../api/src")],
    out_dir: Path::new("src/flatbuffers"),
    extra: &["--gen-object-api"],
    ..flatc_rust::Args::default()
  })
  .unwrap();

  // Perform flatbuffer compile for C++.
  flatc_rust::run(flatc_rust::Args {
    lang: "cpp",
    inputs: &[
      Path::new("../api/src/bitdrift_public/fbs/common/v1/common.fbs"),
      Path::new("../api/src/bitdrift_public/fbs/logging/v1/buffer_log.fbs"),
    ],
    includes: &[Path::new("../api/src")],
    out_dir: Path::new("src/flatbuffers"),
    ..flatc_rust::Args::default()
  })
  .unwrap();

  // Perform JSON schema generation
  flatc_rust::run(flatc_rust::Args {
    lang: "jsonschema",
    inputs: &[Path::new(
      "../api/src/bitdrift_public/fbs/issue-reporting/v1/report.fbs",
    )],
    includes: &[Path::new("../api/src")],
    out_dir: Path::new("src/flatbuffers"),
    ..Default::default()
  })
  .unwrap();
}
