// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf_codegen::Customize;
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

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=../api/");

  // Perform Rust protobuf compile using config-driven approach.
  for config in proto_config::get_proto_configs() {
    std::fs::create_dir_all(config.output_dir).unwrap();

    let mut customize = Customize::default()
      .gen_mod_rs(false)
      .oneofs_non_exhaustive(false);

    if config.use_tokio_bytes {
      customize = customize.tokio_bytes(true).tokio_bytes_for_string(true);
    }

    if config.file_header {
      customize = customize.file_header(GENERATED_HEADER.to_string());
    }

    protobuf_codegen::Codegen::new()
      .protoc()
      .customize(customize)
      .includes(config.includes)
      .inputs(config.inputs)
      .out_dir(config.output_dir)
      .capture_stderr()
      .run()
      .unwrap_or_else(|error| {
        let error = format!("{error:#}")
          .replace("\\n", "\n")
          .replace("\\\"", "\"")
          .replace("stderr: \"", "stderr:\n");
        let error = error
          .strip_suffix('"')
          .map_or(error.as_str(), |value| value);
        panic!("codegen failed:\n{error}");
      });
  }

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
}
