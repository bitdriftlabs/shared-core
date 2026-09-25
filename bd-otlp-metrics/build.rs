// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf_codegen::Customize;

const GENERATED_HEADER: &str = r"// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
";

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=proto/");
  std::fs::create_dir_all("src/protos").unwrap();
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
    .includes(["proto"])
    .inputs([
      "proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
      "proto/opentelemetry/proto/common/v1/common.proto",
      "proto/opentelemetry/proto/metrics/v1/metrics.proto",
      "proto/opentelemetry/proto/resource/v1/resource.proto",
    ])
    .out_dir("src/protos")
    .capture_stderr()
    .run_from_script();
}
