// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::setup::{InitialStreamSetup, Setup, SetupOptions};
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::api::stats_upload_request::{
  Snapshot as StatsSnapshot,
  UploadReason,
};
use bd_proto::protos::client::metric::metric::{Data as MetricData, Metric_name_type};
use bd_proto::protos::client::metric::pending_aggregation_index::PendingFile;
use bd_proto::protos::client::metric::{Counter, Metric, MetricsList, PendingAggregationIndex};
use bd_runtime::runtime::FeatureFlag as _;
use bd_stats_common::Counter as _;
use bd_test_helpers::runtime::ValueKind;
use bd_test_helpers::test_api_server::{
  ExpectedStreamEvent,
  HandshakeResponsePlan,
  StartupStatsUploadResponse,
  StatsUploadResponsePlan,
};
use bd_time::OffsetDateTimeExt;
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use time::OffsetDateTime;
use time::ext::NumericalDuration;

const MAX_SNAPSHOTS_PER_UPLOAD: usize = 10;
const STALE_SNAPSHOT_FILE: &str = "stale_snapshot";
const STATS_DIRECTORY: &str = "stats_uploads";
const PENDING_AGGREGATION_INDEX_FILE: &str = "pending_aggregation_index.pb";

fn snapshot(metric_name: &str, value: u64) -> StatsSnapshot {
  StatsSnapshot {
    snapshot_type: Some(Snapshot_type::Metrics(MetricsList {
      metric: vec![Metric {
        metric_name_type: Some(Metric_name_type::Name(metric_name.to_string())),
        tags: HashMap::new(),
        data: Some(MetricData::Counter(Counter {
          value,
          ..Default::default()
        })),
        ..Default::default()
      }],
      ..Default::default()
    })),
    occurred_at: Some(Occurred_at::Aggregated(Aggregated::default())),
    ..Default::default()
  }
}

fn seed_stale_snapshot(directory: &TempDir) {
  seed_snapshots(
    directory,
    &[(
      STALE_SNAPSHOT_FILE,
      "test:stale",
      1,
      OffsetDateTime::UNIX_EPOCH,
    )],
  );
}

fn seed_snapshots(directory: &TempDir, snapshots: &[(&str, &str, u64, OffsetDateTime)]) {
  let stats_directory = directory.path().join(STATS_DIRECTORY);
  fs::create_dir_all(&stats_directory).unwrap();

  let index = PendingAggregationIndex {
    pending_files: snapshots
      .iter()
      .map(|(file_name, _, _, period_start)| PendingFile {
        name: (*file_name).to_string(),
        period_start: period_start.into_proto(),
        ..Default::default()
      })
      .collect(),
    ..Default::default()
  };
  fs::write(
    stats_directory.join(PENDING_AGGREGATION_INDEX_FILE),
    write_compressed_protobuf(&index).unwrap(),
  )
  .unwrap();

  for (file_name, metric_name, value, _) in snapshots {
    let request = StatsUploadRequest {
      upload_uuid: (*file_name).to_string(),
      snapshot: vec![snapshot(metric_name, *value)],
      ..Default::default()
    };
    fs::write(
      stats_directory.join(file_name),
      write_compressed_protobuf(&request).unwrap(),
    )
    .unwrap();
  }
}

fn seed_corrupt_stale_snapshot(directory: &TempDir, file_name: &str) {
  let stats_directory = directory.path().join(STATS_DIRECTORY);
  fs::create_dir_all(&stats_directory).unwrap();
  fs::write(stats_directory.join(file_name), b"not a protobuf").unwrap();

  let index = PendingAggregationIndex {
    pending_files: vec![PendingFile {
      name: file_name.to_string(),
      period_start: OffsetDateTime::UNIX_EPOCH.into_proto(),
      ..Default::default()
    }],
    ..Default::default()
  };
  fs::write(
    stats_directory.join(PENDING_AGGREGATION_INDEX_FILE),
    write_compressed_protobuf(&index).unwrap(),
  )
  .unwrap();
}

fn read_index(setup: &Setup) -> PendingAggregationIndex {
  read_compressed_protobuf(&fs::read(setup.pending_aggregation_index_file_path()).unwrap()).unwrap()
}

#[test]
fn inline_startup_upload_success_is_acked_and_reported_on_next_connection() {
  let directory = TempDir::new().unwrap();
  seed_stale_snapshot(&directory);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 3,
      }),
      ..Default::default()
    }],
    ..Default::default()
  });

  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    first_handshake
      .analytics
      .as_ref()
      .unwrap()
      .connection_count_since_process_start,
    1
  );
  let startup_upload = first_handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(startup_upload.snapshot.len(), 1);
  assert!(startup_upload.sent_at.is_some());
  assert_eq!(
    startup_upload.upload_reason.enum_value_or_default(),
    UploadReason::UPLOAD_REASON_PERIODIC
  );
  assert!(
    !setup
      .sdk_directory
      .path()
      .join(STATS_DIRECTORY)
      .join(STALE_SNAPSHOT_FILE)
      .exists()
  );
  assert!(read_index(&setup).pending_files.is_empty());

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      analytics_ack: Some(bd_test_helpers::test_api_server::AnalyticsAck::Echo),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    second_handshake
      .analytics
      .as_ref()
      .unwrap()
      .connection_count_since_process_start,
    2
  );
  let report = second_handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  assert!(!report.report_id.is_empty());
  let analytics = report.analytics.as_ref().unwrap();
  assert_eq!(analytics.stats_uploads_acknowledged_successfully, 1);
  assert_eq!(analytics.stats_uploads_acknowledged_unsuccessfully, 0);
  assert!(
    read_index(&setup)
      .pending_stats_pipeline_analytics_report
      .is_none()
  );
}

#[test]
fn capped_startup_batch_drains_the_remainder_on_the_next_handshake() {
  let directory = TempDir::new().unwrap();
  let snapshots = (0 ..= MAX_SNAPSHOTS_PER_UPLOAD)
    .map(|index| {
      (
        format!("stale-{index}"),
        format!("test:stale-{index}"),
        index as u64,
        OffsetDateTime::UNIX_EPOCH,
      )
    })
    .collect::<Vec<_>>();
  let snapshot_references = snapshots
    .iter()
    .map(|(file_name, metric_name, value, period_start)| {
      (
        file_name.as_str(),
        metric_name.as_str(),
        *value,
        *period_start,
      )
    })
    .collect::<Vec<_>>();
  seed_snapshots(&directory, &snapshot_references);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    }],
    ..Default::default()
  });

  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let first_upload = first_handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(first_upload.snapshot.len(), MAX_SNAPSHOTS_PER_UPLOAD);
  for (index, snapshot) in first_upload.snapshot.iter().enumerate() {
    let metric = snapshot.metrics().metric.first().unwrap();
    assert_eq!(
      metric.metric_name_type,
      Some(Metric_name_type::Name(format!("test:stale-{index}")))
    );
  }

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let second_upload = second_handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(second_upload.snapshot.len(), 1);
  assert_eq!(
    second_upload.snapshot[0].metrics().metric[0].metric_name_type,
    Some(Metric_name_type::Name(format!(
      "test:stale-{MAX_SNAPSHOTS_PER_UPLOAD}"
    )))
  );
  assert!(read_index(&setup).pending_files.is_empty());
}

#[test]
fn rejected_startup_batch_is_retained_and_retried_with_the_same_uuid() {
  let directory = TempDir::new().unwrap();
  seed_snapshots(
    &directory,
    &[
      ("first", "test:first", 1, OffsetDateTime::UNIX_EPOCH),
      ("second", "test:second", 2, OffsetDateTime::UNIX_EPOCH),
    ],
  );

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: "rejected".to_string(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    }],
    ..Default::default()
  });

  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let first_upload = first_handshake.startup_stats_upload.as_ref().unwrap();
  let first_upload_uuid = first_upload.upload_uuid.clone();
  assert_eq!(first_upload.snapshot.len(), 2);
  assert_eq!(read_index(&setup).pending_files.len(), 2);

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let second_upload = second_handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(second_upload.upload_uuid, first_upload_uuid);
  assert_eq!(second_upload.snapshot.len(), 2);
  assert!(read_index(&setup).pending_files.is_empty());
}

#[test]
fn corrupt_snapshot_in_startup_batch_is_dropped_without_blocking_valid_snapshots() {
  let directory = TempDir::new().unwrap();
  seed_snapshots(
    &directory,
    &[
      ("first", "test:first", 1, OffsetDateTime::UNIX_EPOCH),
      ("corrupt", "test:corrupt", 2, OffsetDateTime::UNIX_EPOCH),
      ("third", "test:third", 3, OffsetDateTime::UNIX_EPOCH),
    ],
  );
  fs::write(
    directory.path().join(STATS_DIRECTORY).join("corrupt"),
    b"not a protobuf",
  )
  .unwrap();

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    ..Default::default()
  });

  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let upload = handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(upload.snapshot.len(), 2);
  assert_eq!(
    upload.snapshot[0].metrics().metric[0].metric_name_type,
    Some(Metric_name_type::Name("test:first".to_string()))
  );
  assert_eq!(
    upload.snapshot[1].metrics().metric[0].metric_name_type,
    Some(Metric_name_type::Name("test:third".to_string()))
  );
  assert!(
    !setup
      .sdk_directory
      .path()
      .join(STATS_DIRECTORY)
      .join("corrupt")
      .exists()
  );

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    });
  setup.restart_stream(false);
}

#[test]
fn startup_batch_uuid_changes_when_the_persisted_batch_changes_across_restart() {
  let directory = std::sync::Arc::new(TempDir::new().unwrap());
  seed_snapshots(
    directory.as_ref(),
    &[
      ("first", "test:first", 1, OffsetDateTime::UNIX_EPOCH),
      ("second", "test:second", 2, OffsetDateTime::UNIX_EPOCH),
    ],
  );

  let first_upload_uuid = {
    let mut setup = Setup::new_with_options(SetupOptions {
      sdk_directory: directory.clone(),
      ..Default::default()
    });
    let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
    handshake
      .startup_stats_upload
      .as_ref()
      .unwrap()
      .upload_uuid
      .clone()
  };

  seed_snapshots(
    directory.as_ref(),
    &[
      ("first", "test:first", 1, OffsetDateTime::UNIX_EPOCH),
      ("second", "test:second", 2, OffsetDateTime::UNIX_EPOCH),
      ("third", "test:third", 3, OffsetDateTime::UNIX_EPOCH),
    ],
  );
  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory,
    ..Default::default()
  });
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let upload = handshake.startup_stats_upload.as_ref().unwrap();
  assert_ne!(upload.upload_uuid, first_upload_uuid);
  assert_eq!(upload.snapshot.len(), 3);
}

#[test]
fn fresh_snapshot_is_excluded_from_a_startup_batch() {
  let directory = TempDir::new().unwrap();
  seed_snapshots(
    &directory,
    &[
      ("old", "test:old", 1, OffsetDateTime::UNIX_EPOCH),
      ("fresh", "test:fresh", 2, OffsetDateTime::now_utc()),
    ],
  );

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    }],
    ..Default::default()
  });
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let upload = handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(upload.snapshot.len(), 1);
  assert_eq!(
    upload.snapshot[0].metrics().metric[0].metric_name_type,
    Some(Metric_name_type::Name("test:old".to_string()))
  );
}

#[test]
fn handshake_close_before_response_retries_the_same_startup_upload() {
  let directory = TempDir::new().unwrap();
  seed_stale_snapshot(&directory);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    initial_stream_setup: InitialStreamSetup::CloseBeforeResponse,
    ..Default::default()
  });

  let first_stream = setup.server.blocking_next_stream().unwrap();
  assert!(first_stream.await_event_with_timeout(
    ExpectedStreamEvent::Handshake {
      matcher: None,
      sleep_mode: false,
    },
    2_i64.seconds(),
  ));
  assert!(first_stream.await_event_with_timeout(ExpectedStreamEvent::Closed, 2_i64.seconds()));
  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let first_upload_uuid = first_handshake
    .startup_stats_upload
    .as_ref()
    .unwrap()
    .upload_uuid
    .clone();

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    });
  setup.initialize_next_stream(false, vec![]);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    second_handshake
      .startup_stats_upload
      .as_ref()
      .unwrap()
      .upload_uuid,
    first_upload_uuid
  );
  assert!(
    second_handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .is_none()
  );

  setup.flush_stats_without_upload();
}

#[test]
fn inline_startup_upload_error_is_retained_and_retried_with_the_same_uuid() {
  let directory = TempDir::new().unwrap();
  seed_stale_snapshot(&directory);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: "rejected".to_string(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    }],
    ..Default::default()
  });

  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let first_upload_uuid = first_handshake
    .startup_stats_upload
    .as_ref()
    .unwrap()
    .upload_uuid
    .clone();
  assert!(
    setup
      .sdk_directory
      .path()
      .join(STATS_DIRECTORY)
      .join(STALE_SNAPSHOT_FILE)
      .exists()
  );

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
        error: String::new(),
        metrics_dropped: 0,
      }),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    second_handshake
      .startup_stats_upload
      .as_ref()
      .unwrap()
      .upload_uuid,
    first_upload_uuid
  );
  assert_eq!(
    second_handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .as_ref()
      .unwrap()
      .analytics
      .as_ref()
      .unwrap()
      .stats_uploads_acknowledged_unsuccessfully,
    1
  );
  assert!(
    !setup
      .sdk_directory
      .path()
      .join(STATS_DIRECTORY)
      .join(STALE_SNAPSHOT_FILE)
      .exists()
  );
}

#[test]
fn omitted_startup_upload_ack_retries_without_recording_an_ack_outcome() {
  let directory = TempDir::new().unwrap();
  seed_stale_snapshot(&directory);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    handshake_response_plans: vec![
      HandshakeResponsePlan::default(),
      HandshakeResponsePlan {
        startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
          error: String::new(),
          metrics_dropped: 0,
        }),
        ..Default::default()
      },
    ],
    ..Default::default()
  });

  let (_, first_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let first_upload_uuid = first_handshake
    .startup_stats_upload
    .as_ref()
    .unwrap()
    .upload_uuid
    .clone();

  setup.restart_stream(false);

  let (_, second_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    second_handshake
      .startup_stats_upload
      .as_ref()
      .unwrap()
      .upload_uuid,
    first_upload_uuid
  );
  assert!(
    second_handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .is_none()
  );
}

#[test]
fn ordinary_stats_upload_outcomes_are_reconciled_in_handshake_analytics() {
  let mut setup = Setup::new_with_options(SetupOptions {
    stats_upload_response_plans: vec![
      StatsUploadResponsePlan::Echo {
        error: "rejected".to_string(),
        metrics_dropped: 0,
      },
      StatsUploadResponsePlan::Echo {
        error: String::new(),
        metrics_dropped: 2,
      },
    ],
    ..Default::default()
  });
  let _initial_handshake = setup.server.blocking_next_handshake_request().unwrap();
  setup.logger.stats().scope("test").counter("value").inc();

  setup.flush_and_upload_stats();
  let first_upload = setup.server.next_stat_upload().unwrap();
  setup.flush_and_upload_stats();
  let second_upload = setup.server.next_stat_upload().unwrap();
  assert_eq!(first_upload.upload_uuid, second_upload.upload_uuid);

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      analytics_ack: Some(bd_test_helpers::test_api_server::AnalyticsAck::Echo),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(analytics.stats_uploads_acknowledged_successfully, 1);
  assert_eq!(analytics.stats_uploads_acknowledged_unsuccessfully, 1);
  assert!(
    read_index(&setup)
      .pending_stats_pipeline_analytics_report
      .is_none()
  );
}

#[test]
fn ordinary_stats_transport_close_is_retained_without_an_ack_outcome() {
  let mut setup = Setup::new_with_options(SetupOptions {
    stats_upload_response_plans: vec![StatsUploadResponsePlan::CloseStream],
    ..Default::default()
  });
  let _initial_handshake = setup.server.blocking_next_handshake_request().unwrap();
  setup.logger.stats().scope("test").counter("value").inc();

  setup.flush_and_upload_stats();
  let first_upload = setup.server.next_stat_upload().unwrap();
  let closed_stream = setup.current_api_stream.take().unwrap();
  assert!(closed_stream.await_event_with_timeout(ExpectedStreamEvent::Closed, 2_i64.seconds()));
  let sdk_directory = setup.sdk_directory.clone();
  drop(setup);

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory,
    ..Default::default()
  });
  let _restart_handshake = setup.server.blocking_next_handshake_request().unwrap();
  setup.flush_and_upload_stats();
  let second_upload = setup.server.next_stat_upload().unwrap();
  assert_eq!(first_upload.upload_uuid, second_upload.upload_uuid);

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      analytics_ack: Some(bd_test_helpers::test_api_server::AnalyticsAck::Echo),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(analytics.stats_uploads_acknowledged_successfully, 1);
  assert_eq!(analytics.stats_uploads_acknowledged_unsuccessfully, 0);
}

#[test]
fn analytics_report_persists_across_restarts_until_its_matching_ack() {
  let directory = TempDir::new().unwrap();
  seed_stale_snapshot(&directory);
  let sdk_directory = std::sync::Arc::new(directory);

  {
    let mut setup = Setup::new_with_options(SetupOptions {
      sdk_directory: sdk_directory.clone(),
      handshake_response_plans: vec![HandshakeResponsePlan {
        startup_stats_upload_response: Some(StartupStatsUploadResponse::Echo {
          error: String::new(),
          metrics_dropped: 0,
        }),
        ..Default::default()
      }],
      ..Default::default()
    });
    let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
    assert_eq!(
      handshake
        .analytics
        .as_ref()
        .unwrap()
        .connection_count_since_process_start,
      1
    );
    drop(setup);
  }

  let report_id = {
    let mut setup = Setup::new_with_options(SetupOptions {
      sdk_directory: sdk_directory.clone(),
      ..Default::default()
    });
    let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
    assert_eq!(
      handshake
        .analytics
        .as_ref()
        .unwrap()
        .connection_count_since_process_start,
      1
    );
    let report = handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .as_ref()
      .unwrap();
    assert_eq!(
      report
        .analytics
        .as_ref()
        .unwrap()
        .stats_uploads_acknowledged_successfully,
      1
    );
    let report_id = report.report_id.clone();
    drop(setup);
    report_id
  };

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory,
    handshake_response_plans: vec![HandshakeResponsePlan {
      analytics_ack: Some(bd_test_helpers::test_api_server::AnalyticsAck::Fixed(
        "wrong-report-id".to_string(),
      )),
      ..Default::default()
    }],
    ..Default::default()
  });
  let (_, wrong_ack_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    wrong_ack_handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .as_ref()
      .unwrap()
      .report_id,
    report_id
  );
  assert!(
    read_index(&setup)
      .pending_stats_pipeline_analytics_report
      .is_some()
  );

  setup
    .server
    .respond_to_next_handshake(HandshakeResponsePlan {
      analytics_ack: Some(bd_test_helpers::test_api_server::AnalyticsAck::Echo),
      ..Default::default()
    });
  setup.restart_stream(false);

  let (_, matching_ack_handshake) = setup.server.blocking_next_handshake_request().unwrap();
  assert_eq!(
    matching_ack_handshake
      .analytics
      .as_ref()
      .unwrap()
      .stats_pipeline
      .as_ref()
      .unwrap()
      .report_id,
    report_id
  );
  assert!(
    read_index(&setup)
      .pending_stats_pipeline_analytics_report
      .is_none()
  );
}

#[test]
fn pending_snapshot_corruption_is_reported_on_the_next_handshake() {
  let directory = TempDir::new().unwrap();
  seed_corrupt_stale_snapshot(&directory, "corrupt-pending-snapshot");

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    ..Default::default()
  });
  let _initial_handshake = setup.server.blocking_next_handshake_request().unwrap();
  assert!(
    !setup
      .sdk_directory
      .path()
      .join(STATS_DIRECTORY)
      .join("corrupt-pending-snapshot")
      .exists()
  );

  setup.restart_stream(false);
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(
    analytics.stats_files_dropped_due_to_pending_snapshot_corruption,
    1
  );
}

#[test]
fn active_snapshot_corruption_is_reported_after_a_disk_flush() {
  let directory = TempDir::new().unwrap();
  let stats_directory = directory.path().join(STATS_DIRECTORY);
  fs::create_dir_all(&stats_directory).unwrap();
  fs::write(
    stats_directory.join("corrupt-active-snapshot"),
    b"not a protobuf",
  )
  .unwrap();
  let index = PendingAggregationIndex {
    pending_files: vec![PendingFile {
      name: "corrupt-active-snapshot".to_string(),
      period_start: OffsetDateTime::now_utc().into_proto(),
      ..Default::default()
    }],
    ..Default::default()
  };
  fs::write(
    stats_directory.join(PENDING_AGGREGATION_INDEX_FILE),
    write_compressed_protobuf(&index).unwrap(),
  )
  .unwrap();

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    ..Default::default()
  });
  let _initial_handshake = setup.server.blocking_next_handshake_request().unwrap();

  setup.flush_stats_without_upload();

  setup.restart_stream(false);
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(
    analytics.stats_files_dropped_due_to_active_snapshot_corruption,
    1
  );
}

#[test]
fn snapshot_rotation_drop_is_reported_on_the_next_handshake() {
  let mut setup = Setup::new_with_options(SetupOptions {
    extra_runtime_values: vec![
      (
        bd_runtime::runtime::stats::MaxAggregatedFilesFlag::path(),
        ValueKind::Int(1),
      ),
      (
        bd_runtime::runtime::stats::MaxAggregationWindowPerFileFlag::path(),
        ValueKind::Int(0),
      ),
    ],
    ..Default::default()
  });
  let _initial_handshake = setup.server.blocking_next_handshake_request().unwrap();

  setup.flush_stats_without_upload();
  setup.flush_stats_without_upload();

  setup.restart_stream(false);
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(analytics.stats_files_dropped_due_to_rotation, 1);
}

#[test]
fn corrupt_index_recovery_reports_snapshot_drops_and_a_recovery_event() {
  let directory = TempDir::new().unwrap();
  let stats_directory = directory.path().join(STATS_DIRECTORY);
  fs::create_dir_all(&stats_directory).unwrap();
  fs::write(
    stats_directory.join(PENDING_AGGREGATION_INDEX_FILE),
    b"corrupt index",
  )
  .unwrap();
  fs::write(stats_directory.join("dropped-snapshot"), b"snapshot").unwrap();
  fs::write(
    stats_directory.join("pending_aggregation_index.tmp"),
    b"temporary index",
  )
  .unwrap();

  let mut setup = Setup::new_with_options(SetupOptions {
    sdk_directory: directory.into(),
    ..Default::default()
  });
  let (_, handshake) = setup.server.blocking_next_handshake_request().unwrap();
  let analytics = handshake
    .analytics
    .as_ref()
    .unwrap()
    .stats_pipeline
    .as_ref()
    .unwrap();
  let analytics = analytics.analytics.as_ref().unwrap();
  assert_eq!(analytics.stats_files_dropped_due_to_index_recovery, 1);
  assert_eq!(analytics.stats_index_recovery_events, 1);
  assert!(setup.pending_aggregation_index_file_path().exists());
}
