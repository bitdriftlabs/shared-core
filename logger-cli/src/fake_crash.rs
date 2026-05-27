// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fake_crash_test.rs"]
mod tests;

use crate::cli::FakeCrashCommand;
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
  OSBuild,
  OSBuildArgs,
  Platform as ReportPlatform,
  Report,
  ReportArgs,
  ReportType,
  SDKInfo,
  SDKInfoArgs,
  Thread,
  ThreadArgs,
  ThreadDetails,
  ThreadDetailsArgs,
  Timestamp,
};
use flatbuffers::FlatBufferBuilder;
use logger_cli::types::Platform;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const REPORTS_DIRECTORY: &str = "reports/new";

pub fn write_pending_report(
  sdk_directory: &Path,
  command: &FakeCrashCommand,
) -> anyhow::Result<PathBuf> {
  let report_dir = sdk_directory.join(REPORTS_DIRECTORY);
  std::fs::create_dir_all(&report_dir)?;

  let report_path = report_dir.join(normalize_file_name(command.file_name.as_deref()));
  std::fs::write(&report_path, build_report(command)?)?;
  Ok(report_path)
}

fn normalize_file_name(file_name: Option<&str>) -> String {
  let base_name = file_name.map_or_else(default_file_name, std::string::ToString::to_string);
  if Path::new(&base_name)
    .extension()
    .is_some_and(|extension| extension.eq_ignore_ascii_case("cap"))
  {
    base_name
  } else {
    format!("{base_name}.cap")
  }
}

fn default_file_name() -> String {
  let millis = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis();
  format!("fake-crash-{millis}.cap")
}

fn build_report(command: &FakeCrashCommand) -> anyhow::Result<Vec<u8>> {
  let mut builder = FlatBufferBuilder::new();
  let report_timestamp = current_unix_timestamp();

  let sdk_id = builder.create_string("logger-cli");
  let sdk_version = builder.create_string(env!("CARGO_PKG_VERSION"));
  let sdk = SDKInfo::create(
    &mut builder,
    &SDKInfoArgs {
      id: Some(sdk_id),
      version: Some(sdk_version),
    },
  );

  let reason = builder.create_string(&command.reason);
  let detail = builder.create_string(&command.detail);
  let error = Error::create(
    &mut builder,
    &ErrorArgs {
      name: Some(reason),
      reason: Some(detail),
      stack_trace: None,
      relation_to_next: ErrorRelation::CausedBy,
    },
  );
  let errors = Some(builder.create_vector(&[error]));

  let app_id = builder.create_string(&command.app_id);
  let version = builder.create_string(&command.app_version);
  // Match the build-number encoding that the crash uploader later turns back into state fields.
  let build_number = Some(match command.platform {
    Platform::Android => AppBuildNumber::create(
      &mut builder,
      &AppBuildNumberArgs {
        version_code: command
          .app_build_id
          .parse::<i64>()
          .map_err(|_| anyhow!("android app build ID must be an integer"))?,
        ..Default::default()
      },
    ),
    Platform::Apple => {
      let cf_bundle_version = Some(builder.create_string(&command.app_build_id));
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

  let os_version = builder.create_string(match command.platform {
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
  let timestamp = Timestamp::new(report_timestamp, 0);
  let manufacturer = Some(builder.create_string(match command.platform {
    Platform::Android => "Google",
    Platform::Apple => "Apple",
  }));
  let model = Some(builder.create_string(match command.platform {
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
      platform: report_platform(command.platform),
      ..Default::default()
    },
  ));

  let feature_flags = if command.feature_flags.is_empty() {
    None
  } else {
    let mut feature_flag_timestamp = common_fbs::TimestampT::default();
    feature_flag_timestamp.seconds = report_timestamp.try_into().unwrap_or_default();
    let flags = command
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

  let thread_name = builder.create_string("main");
  let thread_state = builder.create_string("RUNNING");
  let thread = Thread::create(
    &mut builder,
    &ThreadArgs {
      name: Some(thread_name),
      index: 0,
      state: Some(thread_state),
      priority: 10.0,
      active: true,
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
      type_: ReportType::NativeCrash,
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

fn report_platform(platform: Platform) -> ReportPlatform {
  match platform {
    Platform::Android => ReportPlatform::Android,
    Platform::Apple => ReportPlatform::iOS,
  }
}

fn current_unix_timestamp() -> u64 {
  let now = time::OffsetDateTime::now_utc().unix_timestamp();
  u64::try_from(now).unwrap_or_default()
}
