// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// ProtoConfig
//

/// Configuration for a single protobuf codegen target.
pub struct ProtoConfig {
  pub output_dir: &'static str,
  pub inputs: &'static [&'static str],
  pub includes: &'static [&'static str],
  pub use_tokio_bytes: bool,
  /// Whether to include the bitdrift file header in generated code. Set to false for vendored
  /// third-party protos.
  pub file_header: bool,
}

pub const PROTO_INCLUDES: &[&str] = &["../api/thirdparty", "../api/src"];
const THIRDPARTY_INCLUDES: &[&str] = &["../api/thirdparty"];

#[must_use]
pub fn get_proto_configs() -> Vec<ProtoConfig> {
  vec![
    ProtoConfig {
      output_dir: "src/protos/bdtail",
      inputs: &[
        "../api/src/bitdrift_public/protobuf/bdtail/v1/bdtail_config.proto",
        "../api/src/bitdrift_public/protobuf/bdtail/v1/bdtail_api.proto",
      ],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: true,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/client",
      inputs: &[
        "../api/src/bitdrift_public/protobuf/client/v1/api.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/metric.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/artifact.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/feature_flag.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/key_value.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/runtime.proto",
        "../api/src/bitdrift_public/protobuf/client/v1/matcher.proto",
      ],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/config/v1",
      inputs: &["../api/src/bitdrift_public/protobuf/config/v1/config.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/logging",
      inputs: &["../api/src/bitdrift_public/protobuf/logging/v1/payload.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/value_matcher",
      inputs: &["../api/src/bitdrift_public/protobuf/value_matcher/v1/value_matcher.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/state",
      inputs: &[
        "../api/src/bitdrift_public/protobuf/state/v1/payload.proto",
        "../api/src/bitdrift_public/protobuf/state/v1/scope.proto",
        "../api/src/bitdrift_public/protobuf/state/v1/matcher.proto",
      ],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/log_matcher",
      inputs: &["../api/src/bitdrift_public/protobuf/matcher/v1/log_matcher.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/workflow",
      inputs: &["../api/src/bitdrift_public/protobuf/workflow/v1/workflow.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/filter",
      inputs: &["../api/src/bitdrift_public/protobuf/filter/v1/filter.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    ProtoConfig {
      output_dir: "src/protos/mme",
      inputs: &["../api/src/bitdrift_public/protobuf/mme/v1/service.proto"],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: false,
      file_header: true,
    },
    // Vendored third-party protos without bitdrift file headers.
    ProtoConfig {
      output_dir: "src/protos/google/api",
      inputs: &[
        "../api/thirdparty/google/api/http.proto",
        "../api/thirdparty/google/api/annotations.proto",
      ],
      includes: THIRDPARTY_INCLUDES,
      use_tokio_bytes: false,
      file_header: false,
    },
    ProtoConfig {
      output_dir: "src/protos/prometheus/prompb",
      inputs: &[
        "../api/thirdparty/prometheus/prompb/remote.proto",
        "../api/thirdparty/prometheus/prompb/types.proto",
      ],
      includes: PROTO_INCLUDES,
      use_tokio_bytes: true,
      file_header: false,
    },
  ]
}
