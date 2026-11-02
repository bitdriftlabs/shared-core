// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

//! Synthetic issue reports in the platform layer's on-disk format, for local data generators.

#[cfg(test)]
#[path = "./issue_report_test.rs"]
mod tests;

use crate::types::Platform;
use anyhow::anyhow;
use bd_proto::flatbuffers::common::bitdrift_public::fbs::common::v_1 as common_fbs;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  AppBuildNumber,
  AppBuildNumberArgs,
  AppMetrics,
  AppMetricsArgs,
  Architecture,
  DeviceMetrics,
  DeviceMetricsArgs,
  Error,
  ErrorArgs,
  ErrorRelation,
  FeatureFlag,
  FeatureFlagArgs,
  Frame,
  FrameArgs,
  FrameType,
  OSBuild,
  OSBuildArgs,
  Platform as ReportPlatform,
  Report,
  ReportArgs,
  ReportType,
  SDKInfo,
  SDKInfoArgs,
  SourceFile,
  SourceFileArgs,
  Thread,
  ThreadArgs,
  ThreadDetails,
  ThreadDetailsArgs,
  Timestamp,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use time::OffsetDateTime;

//
// IssueKind
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssueKind {
  NativeCrash,
  JvmCrash,
  AppNotResponding,
  HandledError,
}

impl IssueKind {
  const fn report_type(self) -> ReportType {
    match self {
      Self::NativeCrash => ReportType::NativeCrash,
      Self::JvmCrash => ReportType::JVMCrash,
      Self::AppNotResponding => ReportType::AppNotResponding,
      Self::HandledError => ReportType::HandledError,
    }
  }
}

//
// StackFrame
//

/// One in-app frame, symbolicated as the platform layer would report it.
#[derive(Clone, Debug)]
pub struct StackFrame {
  pub class_name: String,
  pub symbol_name: String,
  pub file: String,
  pub line: i64,
}

//
// FeatureFlagExposure
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureFlagExposure {
  pub name: String,
  pub value: Option<String>,
}

//
// IssueReport
//

#[derive(Clone, Debug)]
pub struct IssueReport {
  pub kind: IssueKind,
  pub platform: Platform,
  pub app_id: String,
  pub app_version: String,
  pub app_build_id: String,
  /// The error name, such as the exception class. The backend groups issues by it.
  pub name: String,
  /// The error message.
  pub reason: String,
  /// The failing thread's stack, most recent frame first.
  pub frames: Vec<StackFrame>,
  /// Flags recorded in the report itself. Reports processed by a running SDK also carry the
  /// exposures the SDK tracked.
  pub feature_flags: Vec<FeatureFlagExposure>,
  pub timestamp: OffsetDateTime,
}

impl IssueReport {
  /// Serializes the report as the platform layer writes it to the reports directory.
  pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
    let mut builder = FlatBufferBuilder::new();
    let seconds = u64::try_from(self.timestamp.unix_timestamp())
      .map_err(|_| anyhow!("report timestamp precedes the Unix epoch"))?;

    let sdk_id = builder.create_string("logger-cli");
    let sdk_version = builder.create_string(env!("CARGO_PKG_VERSION"));
    let sdk = SDKInfo::create(
      &mut builder,
      &SDKInfoArgs {
        id: Some(sdk_id),
        version: Some(sdk_version),
      },
    );

    let frame_type = match self.platform {
      Platform::Android => FrameType::JVM,
      Platform::Apple => FrameType::DWARF,
    };
    let error_frames = self.frames(&mut builder, frame_type);
    let name = builder.create_string(&self.name);
    let reason = builder.create_string(&self.reason);
    let error = Error::create(
      &mut builder,
      &ErrorArgs {
        name: Some(name),
        reason: Some(reason),
        stack_trace: error_frames,
        relation_to_next: ErrorRelation::CausedBy,
      },
    );
    let errors = Some(builder.create_vector(&[error]));

    let app_id = builder.create_string(&self.app_id);
    let version = builder.create_string(&self.app_version);
    // Match the build-number encoding that the crash uploader later turns back into state fields.
    let build_number = Some(match self.platform {
      Platform::Android => AppBuildNumber::create(
        &mut builder,
        &AppBuildNumberArgs {
          version_code: self
            .app_build_id
            .parse::<i64>()
            .map_err(|_| anyhow!("android app build ID must be an integer"))?,
          ..Default::default()
        },
      ),
      Platform::Apple => {
        let cf_bundle_version = Some(builder.create_string(&self.app_build_id));
        AppBuildNumber::create(
          &mut builder,
          &AppBuildNumberArgs {
            cf_bundle_version,
            ..Default::default()
          },
        )
      },
    });
    let app_metrics = Some(AppMetrics::create(
      &mut builder,
      &AppMetricsArgs {
        app_id: Some(app_id),
        build_number,
        version: Some(version),
        ..Default::default()
      },
    ));

    let os_version = builder.create_string(match self.platform {
      Platform::Android => "14",
      Platform::Apple => "17.5",
    });
    let os_build = Some(OSBuild::create(
      &mut builder,
      &OSBuildArgs {
        version: Some(os_version),
        ..Default::default()
      },
    ));
    let timestamp = Timestamp::new(seconds, self.timestamp.nanosecond());
    let manufacturer = Some(builder.create_string(match self.platform {
      Platform::Android => "Google",
      Platform::Apple => "Apple",
    }));
    let model = Some(builder.create_string(match self.platform {
      Platform::Android => "Pixel 8",
      Platform::Apple => "iPhone15,3",
    }));
    let device_metrics = Some(DeviceMetrics::create(
      &mut builder,
      &DeviceMetricsArgs {
        time: Some(&timestamp),
        manufacturer,
        model,
        os_build,
        arch: Architecture::arm64,
        platform: report_platform(self.platform),
        ..Default::default()
      },
    ));

    let feature_flags = if self.feature_flags.is_empty() {
      None
    } else {
      let mut feature_flag_timestamp = common_fbs::TimestampT::default();
      feature_flag_timestamp.seconds = self.timestamp.unix_timestamp();
      let flags = self
        .feature_flags
        .iter()
        .map(|flag| {
          let name = builder.create_string(&flag.name);
          let value = flag
            .value
            .as_deref()
            .map(|value| builder.create_string(value));
          let timestamp = feature_flag_timestamp.pack(&mut builder);

          FeatureFlag::create(
            &mut builder,
            &FeatureFlagArgs {
              name: Some(name),
              value,
              timestamp: Some(timestamp),
            },
          )
        })
        .collect::<Vec<_>>();
      Some(builder.create_vector(&flags))
    };

    let thread_frames = self.frames(&mut builder, frame_type);
    let thread_name = builder.create_string("main");
    // An ANR is reported while the main thread is blocked rather than running.
    let thread_state = builder.create_string(match self.kind {
      IssueKind::AppNotResponding => "BLOCKED",
      _ => "RUNNING",
    });
    let thread = Thread::create(
      &mut builder,
      &ThreadArgs {
        name: Some(thread_name),
        index: 0,
        state: Some(thread_state),
        priority: 10.0,
        active: true,
        stack_trace: thread_frames,
        ..Default::default()
      },
    );
    let threads = Some(builder.create_vector(&[thread]));
    let thread_details = Some(ThreadDetails::create(
      &mut builder,
      &ThreadDetailsArgs { count: 1, threads },
    ));

    let report = Report::create(
      &mut builder,
      &ReportArgs {
        sdk: Some(sdk),
        type_: self.kind.report_type(),
        app_metrics,
        device_metrics,
        errors,
        feature_flags,
        thread_details,
        ..Default::default()
      },
    );
    builder.finish_minimal(report);

    Ok(builder.finished_data().to_vec())
  }

  fn frames<'a>(
    &self,
    builder: &mut FlatBufferBuilder<'a>,
    frame_type: FrameType,
  ) -> Option<WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Frame<'a>>>>> {
    if self.frames.is_empty() {
      return None;
    }
    let frames = self
      .frames
      .iter()
      .enumerate()
      .map(|(index, frame)| {
        let class_name = builder.create_string(&frame.class_name);
        let symbol_name = builder.create_string(&frame.symbol_name);
        let path = builder.create_string(&frame.file);
        let source_file = SourceFile::create(
          builder,
          &SourceFileArgs {
            path: Some(path),
            line: frame.line,
            ..Default::default()
          },
        );
        Frame::create(
          builder,
          &FrameArgs {
            type_: frame_type,
            class_name: Some(class_name),
            symbol_name: Some(symbol_name),
            source_file: Some(source_file),
            original_index: index as u64,
            in_app: true,
            ..Default::default()
          },
        )
      })
      .collect::<Vec<_>>();
    Some(builder.create_vector(&frames))
  }
}

const fn report_platform(platform: Platform) -> ReportPlatform {
  match platform {
    Platform::Android => ReportPlatform::Android,
    Platform::Apple => ReportPlatform::iOS,
  }
}
