// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1;
use flatbuffers::FlatBufferBuilder;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_till, take_while1};
use nom::combinator::{map, rest};
use nom::sequence::{delimited, pair, preceded, separated_pair};
use nom::{IResult, Parser};

#[derive(Debug, Clone)]
struct JavaScriptFrame {
  pub function_name: String,
  pub file_path: String,
  pub line: i64,
  pub column: i64,
  pub bundle_path: Option<String>,
  pub image_id: Option<String>,
  pub in_app: bool,
}

#[derive(Debug, Clone)]
pub struct JavaScriptDeviceMetrics {
  pub platform: v_1::Platform,
  pub manufacturer: Option<String>,
  pub model: Option<String>,
  pub os_version: Option<String>,
  pub os_brand: Option<String>,
  pub os_kernversion: Option<String>,
  pub architecture: Option<v_1::Architecture>,
  pub cpu_abis: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct JavaScriptAppMetrics {
  pub app_id: Option<String>,
  pub version: Option<String>,
  pub version_code: Option<i64>,
  pub javascript_engine: v_1::JavaScriptEngine,
}

fn parse_javascript_stack_trace(stack_trace: &str, debug_id: Option<&str>) -> Vec<JavaScriptFrame> {
  let lines: Vec<&str> = stack_trace
    .lines()
    .map(str::trim)
    .filter(|line| !line.is_empty())
    .collect();

  let frames: Vec<JavaScriptFrame> = lines
    .iter()
    .filter_map(|line| parse_javascript_frame(line, debug_id))
    .collect();
  frames
}


fn parse_javascript_frame(line: &str, debug_id: Option<&str>) -> Option<JavaScriptFrame> {
  let trimmed = line.trim();

  if trimmed == "[native code]" {
    return Some(JavaScriptFrame {
      function_name: "[native code]".to_string(),
      file_path: "[native code]".to_string(),
      line: 0,
      column: 0,
      bundle_path: None,
      image_id: debug_id
        .filter(|id| !id.is_empty())
        .map(ToString::to_string),
      in_app: false,
    });
  }

  if let Ok((_, (function_name, location))) = parse_frame_with_nom(trimmed) {
    let (file_path, line_num, column_num, bundle_path) = parse_location(location);
    // Always set in_app to false; backend will re-evaluate in_app after symbolication based on
    // the actual symbolicated source file path. This ensures accurate in_app determination
    // for both debug builds (with source paths) and release builds (with bundle paths).
    let in_app = false;
    return Some(JavaScriptFrame {
      function_name: function_name.to_string(),
      file_path,
      line: line_num.unwrap_or(0),
      column: column_num.unwrap_or(0),
      bundle_path,
      image_id: debug_id
        .filter(|id| !id.is_empty())
        .map(ToString::to_string),
      in_app,
    });
  }

  None
}

fn parse_frame_with_nom(input: &str) -> IResult<&str, (&str, &str)> {
  alt((
    map(
      separated_pair(take_till(|c| c == '@'), tag("@"), rest),
      |(func, loc): (&str, &str)| (func.trim(), loc.trim()),
    ),
    map(
      separated_pair(take_till(|c| c == ' '), tag(" at "), rest),
      |(func, loc): (&str, &str)| (func.trim(), loc.trim()),
    ),
    preceded(
      tag("at "),
      map(
        pair(
          take_till(|c| c == '('),
          delimited(tag("("), take_while1(|c| c != ')'), tag(")")),
        ),
        |(func, loc): (&str, &str)| (func.trim(), loc.trim()),
      ),
    ),
    preceded(
      tag("at "),
      map(rest, |location: &str| ("", location.trim())),
    ),
  ))
  .parse(input)
}

fn parse_location(location: &str) -> (String, Option<i64>, Option<i64>, Option<String>) {
  if let Some(last_colon) = location.rfind(':')
    && let Some(second_last_colon) = location[.. last_colon].rfind(':')
  {
    let mut file_path = location[.. second_last_colon].to_string();

    if file_path.starts_with("address at ") {
      file_path = file_path["address at ".len() ..].to_string();
    }

    let line_num = location[second_last_colon + 1 .. last_colon]
      .parse::<i64>()
      .ok();
    let column_num = location[last_colon + 1 ..].parse::<i64>().ok();

    let bundle_path = if file_path.ends_with(".bundle") || file_path.ends_with(".jsbundle") {
      // Extract just the filename (no leading slash) to match source map format
      file_path.rsplit('/').next().map(String::from)
    } else {
      None
    };

    return (file_path, line_num, column_num, bundle_path);
  }

  let mut file_path = location.to_string();
  if file_path.starts_with("address at ") {
    file_path = file_path["address at ".len() ..].to_string();
  }
  (file_path, None, None, None)
}


fn build_javascript_error_report(
  error_name: &str,
  error_message: &str,
  stack_trace: &str,
  is_fatal: bool,
  debug_id: Option<&str>,
  timestamp_seconds: u64,
  timestamp_nanos: u32,
  device_metrics: &JavaScriptDeviceMetrics,
  app_metrics: &JavaScriptAppMetrics,
  sdk_id: &str,
  sdk_version: &str,
) -> Vec<u8> {
  let mut builder = FlatBufferBuilder::new();
  let timestamp = v_1::Timestamp::new(timestamp_seconds, timestamp_nanos);

  let frames = parse_javascript_stack_trace(stack_trace, debug_id);

  let error_name_offset = builder.create_string(error_name);
  let error_message_offset = builder.create_string(error_message);

  let frame_offsets: Vec<_> = frames
    .iter()
    .map(|frame| {
      let function_name_offset = builder.create_string(&frame.function_name);
      let bundle_path_offset = frame
        .bundle_path
        .as_ref()
        .map(|path| builder.create_string(path));
      let image_id_offset = frame.image_id.as_ref().map(|id| builder.create_string(id));

      let file_path_offset = builder.create_string(&frame.file_path);
      let source_file = v_1::SourceFile::create(
        &mut builder,
        &v_1::SourceFileArgs {
          path: Some(file_path_offset),
          line: frame.line,
          column: frame.column,
        },
      );

      v_1::Frame::create(
        &mut builder,
        &v_1::FrameArgs {
          type_: v_1::FrameType::JavaScript,
          symbol_name: Some(function_name_offset),
          symbolicated_name: Some(function_name_offset),
          source_file: Some(source_file),
          image_id: image_id_offset,
          js_bundle_path: bundle_path_offset,
          in_app: frame.in_app,
          frame_status: v_1::FrameStatus::Symbolicated,
          ..Default::default()
        },
      )
    })
    .collect();

  let frames_vector = builder.create_vector(&frame_offsets);

  let error_args = v_1::ErrorArgs {
    name: Some(error_name_offset),
    reason: Some(error_message_offset),
    stack_trace: Some(frames_vector),
    ..Default::default()
  };
  let error = v_1::Error::create(&mut builder, &error_args);

  let report_type = if is_fatal {
    v_1::ReportType::JavaScriptFatalError
  } else {
    v_1::ReportType::JavaScriptNonFatalError
  };

  let manufacturer_offset = device_metrics
    .manufacturer
    .as_ref()
    .map(|m| builder.create_string(m));
  let model_offset = device_metrics
    .model
    .as_ref()
    .map(|m| builder.create_string(m));

  let mut device_info = v_1::DeviceMetricsArgs {
    platform: device_metrics.platform,
    manufacturer: manufacturer_offset,
    model: model_offset,
    time: Some(&timestamp),
    ..Default::default()
  };

  if device_metrics.os_version.is_some()
    || device_metrics.os_brand.is_some()
    || device_metrics.os_kernversion.is_some()
  {
    let os_version_offset = device_metrics
      .os_version
      .as_ref()
      .map(|v| builder.create_string(v));
    let os_brand_offset = device_metrics
      .os_brand
      .as_ref()
      .map(|b| builder.create_string(b));
    let os_kernversion_offset = device_metrics
      .os_kernversion
      .as_ref()
      .map(|k| builder.create_string(k));
    let os_build = v_1::OSBuild::create(
      &mut builder,
      &v_1::OSBuildArgs {
        version: os_version_offset,
        brand: os_brand_offset,
        kern_osversion: os_kernversion_offset,
        ..Default::default()
      },
    );
    device_info.os_build = Some(os_build);
  }

  if let Some(arch) = device_metrics.architecture {
    device_info.arch = arch;
  }

  if let Some(ref cpu_abis) = device_metrics.cpu_abis {
    let cpu_abis_offsets: Vec<_> = cpu_abis
      .iter()
      .map(|abi| builder.create_string(abi))
      .collect();
    device_info.cpu_abis = Some(builder.create_vector(&cpu_abis_offsets));
  }

  let mut app_info = v_1::AppMetricsArgs {
    app_id: app_metrics
      .app_id
      .as_ref()
      .map(|id| builder.create_string(id)),
    version: app_metrics
      .version
      .as_ref()
      .map(|v| builder.create_string(v)),
    javascript_engine: app_metrics.javascript_engine,
    ..Default::default()
  };

  if let Some(version_code) = app_metrics.version_code {
    let build_number = v_1::AppBuildNumber::create(
      &mut builder,
      &v_1::AppBuildNumberArgs {
        version_code,
        ..Default::default()
      },
    );
    app_info.build_number = Some(build_number);
  }

  let sdk_id_offset = builder.create_string(sdk_id);
  let sdk_version_offset = builder.create_string(sdk_version);
  let sdk_info = v_1::SDKInfo::create(
    &mut builder,
    &v_1::SDKInfoArgs {
      id: Some(sdk_id_offset),
      version: Some(sdk_version_offset),
    },
  );

  let thread_details = v_1::ThreadDetails::create(
    &mut builder,
    &v_1::ThreadDetailsArgs {
      count: 0,
      threads: None,
    },
  );

  let args = v_1::ReportArgs {
    sdk: Some(sdk_info),
    type_: report_type,
    errors: Some(builder.create_vector(&[error])),
    app_metrics: Some(v_1::AppMetrics::create(&mut builder, &app_info)),
    device_metrics: Some(v_1::DeviceMetrics::create(&mut builder, &device_info)),
    thread_details: Some(thread_details),
    ..Default::default()
  };
  let report_offset = v_1::Report::create(&mut builder, &args);

  builder.finish(report_offset, None);
  builder.finished_data().to_vec()
}

pub fn build_javascript_error_report_to_file(
  error_name: &str,
  error_message: &str,
  stack_trace: &str,
  is_fatal: bool,
  debug_id: Option<&str>,
  timestamp_seconds: u64,
  timestamp_nanos: u32,
  device_metrics: &JavaScriptDeviceMetrics,
  app_metrics: &JavaScriptAppMetrics,
  sdk_id: &str,
  sdk_version: &str,
  destination_path: &str,
) -> anyhow::Result<()> {
  let bytes = build_javascript_error_report(
    error_name,
    error_message,
    stack_trace,
    is_fatal,
    debug_id,
    timestamp_seconds,
    timestamp_nanos,
    device_metrics,
    app_metrics,
    sdk_id,
    sdk_version,
  );
  std::fs::write(destination_path, bytes)
    .map_err(|e| anyhow::anyhow!("failed to write report to file: {e}"))?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_android_react_native_stack() {
    let stack = "Error: test\nat \
                 render(/home/username/sample-workspace/sampleapp.collect.react/src/components/\
                 GpsMonitorScene.js:78:24)\nat \
                 _renderValidatedComponentWithoutOwnerOrContext(/home/username/sample-workspace/\
                 sampleapp.collect.react/node_modules/react-native/Libraries/Renderer/src/\
                 renderers/shared/stack/reconciler/ReactCompositeComponent.js:1050:29)";
    let frames = parse_javascript_stack_trace(stack, None);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].function_name, "render");
    assert_eq!(
      frames[0].file_path,
      "/home/username/sample-workspace/sampleapp.collect.react/src/components/GpsMonitorScene.js"
    );
    assert_eq!(frames[0].line, 78);
    assert_eq!(frames[0].column, 24);
    assert!(!frames[0].in_app);
    assert_eq!(
      frames[1].function_name,
      "_renderValidatedComponentWithoutOwnerOrContext"
    );
    assert_eq!(
      frames[1].file_path,
      "/home/username/sample-workspace/sampleapp.collect.react/node_modules/react-native/\
       Libraries/Renderer/src/renderers/shared/stack/reconciler/ReactCompositeComponent.js"
    );
    assert!(!frames[1].in_app);
  }

  #[test]
  fn parse_ios_react_native_1_stack() {
    let stack = "_exampleFunction@/home/test/project/App.js:125:13\n_depRunCallbacks@/home/test/\
                 project/node_modules/dep/index.js:77:45\ntryCallTwo@/home/test/project/\
                 node_modules/react-native/node_modules/promise/lib/core.js:45:5";
    let frames = parse_javascript_stack_trace(stack, None);
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].function_name, "_exampleFunction");
    assert_eq!(frames[0].file_path, "/home/test/project/App.js");
    assert_eq!(frames[0].line, 125);
    assert_eq!(frames[0].column, 13);
    assert!(!frames[0].in_app);
    assert!(!frames[1].in_app);
    assert!(!frames[2].in_app);
  }

  #[test]
  fn parse_android_react_native_prod_stack() {
    let stack = "value@index.android.bundle:12:1917\nonPress@index.android.bundle:12:2336\\
                 ntouchableHandlePress@index.android.bundle:258:1497\n[native code]";
    let frames = parse_javascript_stack_trace(stack, None);
    assert_eq!(frames.len(), 4);
    assert_eq!(frames[0].function_name, "value");
    assert_eq!(frames[0].file_path, "index.android.bundle");
    assert_eq!(frames[0].line, 12);
    assert_eq!(frames[0].column, 1917);
    assert_eq!(
      frames[0].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[0].in_app);
    assert_eq!(frames[1].function_name, "onPress");
    assert_eq!(frames[1].file_path, "index.android.bundle");
    assert_eq!(
      frames[1].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[1].in_app);
    assert_eq!(frames[3].function_name, "[native code]");
    assert_eq!(frames[3].file_path, "[native code]");
    assert_eq!(frames[3].line, 0);
    assert_eq!(frames[3].column, 0);
    assert!(!frames[3].in_app);
  }

  #[test]
  fn parse_release_build_with_address_at_prefix() {
    let stack = "triggerGlobalJsError@address at \
                 index.android.bundle:1:726416\n_performTransitionSideEffects@address at \
                 index.android.bundle:1:460430";
    let frames = parse_javascript_stack_trace(stack, None);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].function_name, "triggerGlobalJsError");
    assert_eq!(frames[0].file_path, "index.android.bundle");
    assert_eq!(frames[0].line, 1);
    assert_eq!(frames[0].column, 726_416);
    assert_eq!(
      frames[0].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[0].in_app);
    assert_eq!(frames[1].function_name, "_performTransitionSideEffects");
    assert_eq!(frames[1].file_path, "index.android.bundle");
    assert_eq!(frames[1].line, 1);
    assert_eq!(frames[1].column, 460_430);
    assert_eq!(
      frames[1].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[1].in_app);
  }

  #[test]
  fn parse_release_build_at_format_with_address_at() {
    let stack = "Error: Triggered Global JS Error - Intentional for testing\n    at \
                 triggerGlobalJsError (address at index.android.bundle:1:726416)\n    at \
                 _performTransitionSideEffects (address at index.android.bundle:1:460430)\n    at \
                 forEach (native)";
    let frames = parse_javascript_stack_trace(stack, None);
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].function_name, "triggerGlobalJsError");
    assert_eq!(frames[0].file_path, "index.android.bundle");
    assert_eq!(frames[0].line, 1);
    assert_eq!(frames[0].column, 726_416);
    assert_eq!(
      frames[0].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[0].in_app);
    assert_eq!(frames[1].function_name, "_performTransitionSideEffects");
    assert_eq!(frames[1].file_path, "index.android.bundle");
    assert_eq!(frames[1].line, 1);
    assert_eq!(frames[1].column, 460_430);
    assert_eq!(
      frames[1].bundle_path,
      Some("index.android.bundle".to_string())
    );
    assert!(!frames[1].in_app);
    assert_eq!(frames[2].function_name, "forEach");
    assert_eq!(frames[2].file_path, "native");
    assert_eq!(frames[2].line, 0);
    assert_eq!(frames[2].column, 0);
    assert!(!frames[2].in_app);
  }

  #[test]
  fn parse_ios_jsbundle_release_build() {
    let stack = "triggerGlobalJsError@main.jsbundle:1:717458\n\
                 onPress@main.jsbundle:1:720868\n\
                 _performTransitionSideEffects@main.jsbundle:1:456325\n\
                 [native code]";
    let frames = parse_javascript_stack_trace(stack, Some("5aa81d1c6931a5db1800b6f5ee411e9e"));
    assert_eq!(frames.len(), 4);
    assert_eq!(frames[0].function_name, "triggerGlobalJsError");
    assert_eq!(frames[0].file_path, "main.jsbundle");
    assert_eq!(frames[0].line, 1);
    assert_eq!(frames[0].column, 717_458);
    assert_eq!(frames[0].bundle_path, Some("main.jsbundle".to_string()));
    assert_eq!(
      frames[0].image_id,
      Some("5aa81d1c6931a5db1800b6f5ee411e9e".to_string())
    );
    assert!(!frames[0].in_app);
    assert_eq!(frames[1].function_name, "onPress");
    assert_eq!(frames[1].file_path, "main.jsbundle");
    assert_eq!(frames[1].bundle_path, Some("main.jsbundle".to_string()));
    assert!(!frames[1].in_app);
    assert_eq!(frames[3].function_name, "[native code]");
    assert_eq!(frames[3].file_path, "[native code]");
    assert!(!frames[3].in_app);
  }
}
