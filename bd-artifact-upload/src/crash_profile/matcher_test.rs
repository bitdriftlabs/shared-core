// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use std::slice::from_ref;

fn report() -> ReportCandidate {
  ReportCandidate {
    report_id: Uuid::new_v4(),
    kind: Kind::Anr,
    run: ProcessRun {
      installation_id: Uuid::new_v4(),
      run_id: Uuid::new_v4(),
      pid: 7,
      boot_count: 1,
      start_elapsed_ns: 10,
      wall_ns: 1_000_000_000_050,
      elapsed_ns: 50,
      end_elapsed_ns: Some(100),
    },
    event_elapsed_ns: 100,
  }
}

fn evidence() -> Evidence {
  Evidence {
    boot_epoch_ns: 1_000_000_000_000,
    observations: vec![PidObservation {
      pid: 7,
      first_ns: 20,
      last_ns: 30,
      process_start_ns: None,
    }],
  }
}

fn assert_unmatched(report: ReportCandidate) {
  assert_eq!(
    match_reports(
      report.run.installation_id,
      Kind::Anr,
      &evidence(),
      &[report],
    )
    .unwrap(),
    Vec::<Uuid>::new()
  );
}

#[test]
fn matches_all_eligible_reports_in_source_run_without_using_observer_pid() {
  let first = report();
  let mut second = first.clone();
  second.report_id = Uuid::new_v4();
  let reports = [first.clone(), second.clone()];
  let mut expected = vec![first.report_id, second.report_id];
  expected.sort_unstable();
  for start in [None, Some(first.run.start_elapsed_ns)] {
    let mut profile = evidence();
    profile.observations[0].process_start_ns = start;
    assert_eq!(
      match_reports(first.run.installation_id, Kind::Anr, &profile, &reports).unwrap(),
      expected
    );
  }
  assert_eq!(
    match_reports(Uuid::new_v4(), Kind::Anr, &evidence(), &reports).unwrap(),
    Vec::<Uuid>::new()
  );
  assert_eq!(
    match_reports(
      first.run.installation_id,
      Kind::JavaOutOfMemory,
      &evidence(),
      &reports,
    )
    .unwrap(),
    Vec::<Uuid>::new()
  );
}

#[test]
fn rejects_reboot_clock_jumps_and_ended_lifetimes() {
  let original = report();
  let mut rebooted = original.clone();
  rebooted.run.wall_ns += 2_000_000_000;
  rebooted.run.boot_count += 1;
  assert_unmatched(rebooted);
  let mut ended = original;
  ended.run.end_elapsed_ns = Some(15);
  assert_unmatched(ended);
}

#[test]
fn ambiguous_pid_reuse_fails_explicitly() {
  let first = report();
  let mut reused = first.clone();
  reused.report_id = Uuid::new_v4();
  reused.run.run_id = Uuid::new_v4();
  assert!(
    match_reports(
      first.run.installation_id,
      Kind::Anr,
      &evidence(),
      &[first, reused],
    )
    .is_err()
  );
}

#[test]
fn boot_epoch_tolerance_is_not_an_exact_crash_time_requirement() {
  for delta in [
    -CLOCK_TOLERANCE_NS - 1,
    -CLOCK_TOLERANCE_NS,
    CLOCK_TOLERANCE_NS,
    CLOCK_TOLERANCE_NS + 1,
  ] {
    let mut candidate = report();
    candidate.run.wall_ns += delta;
    let expected = if delta.abs() <= CLOCK_TOLERANCE_NS {
      vec![candidate.report_id]
    } else {
      vec![]
    };
    // The report event is after the last profile sample, but both belong to the same source run.
    assert_eq!(
      match_reports(
        candidate.run.installation_id,
        Kind::Anr,
        &evidence(),
        &[candidate],
      )
      .unwrap(),
      expected
    );
  }
}

#[test]
fn trace_process_age_disambiguates_overlapping_pid_reuse() {
  let mut first = report();
  first.run.start_elapsed_ns = 1_000_000_000;
  first.run.elapsed_ns = 1_050_000_000;
  first.run.wall_ns = evidence().boot_epoch_ns + i128::from(first.run.elapsed_ns);
  first.run.end_elapsed_ns = Some(1_100_000_000);
  first.event_elapsed_ns = 1_100_000_000;
  let mut profile = evidence();
  profile.observations[0].first_ns = 1_020_000_000;
  profile.observations[0].last_ns = 1_030_000_000;
  let expected = vec![first.report_id];
  let installation = first.run.installation_id;
  for delta in [
    0,
    PROCESS_START_TOLERANCE_NS / 2,
    PROCESS_START_TOLERANCE_NS,
    PROCESS_START_TOLERANCE_NS + 1,
    2 * PROCESS_START_TOLERANCE_NS,
  ] {
    let mut reused = first.clone();
    reused.run.run_id = Uuid::new_v4();
    reused.run.start_elapsed_ns += delta;
    reused.report_id = Uuid::new_v4();
    let reports = [first.clone(), reused];
    profile.observations[0].process_start_ns = Some(first.run.start_elapsed_ns);
    for reports in [reports.clone(), [reports[1].clone(), reports[0].clone()]] {
      let result = match_reports(installation, Kind::Anr, &profile, &reports);
      if delta <= PROCESS_START_TOLERANCE_NS {
        assert!(result.unwrap_err().to_string().contains("ambiguous"));
      } else {
        assert_eq!(result.unwrap(), expected);
      }
    }
    profile.observations[0].process_start_ns = None;
    assert!(match_reports(installation, Kind::Anr, &profile, &reports).is_err());
  }
}

#[test]
fn process_age_tolerance_is_explicit() {
  let candidate = report();
  let mut profile = evidence();
  for delta in [PROCESS_START_TOLERANCE_NS, PROCESS_START_TOLERANCE_NS + 1] {
    // Choose activity after both start values to isolate the age-comparison boundary.
    profile.observations[0].process_start_ns = Some(candidate.run.start_elapsed_ns + delta);
    profile.observations[0].first_ns = candidate.run.start_elapsed_ns + delta + 1;
    profile.observations[0].last_ns = profile.observations[0].first_ns;
    let mut candidate = candidate.clone();
    candidate.run.end_elapsed_ns = Some(profile.observations[0].last_ns);
    let expected = if delta <= PROCESS_START_TOLERANCE_NS {
      vec![candidate.report_id]
    } else {
      vec![]
    };
    assert_eq!(
      match_reports(
        candidate.run.installation_id,
        Kind::Anr,
        &profile,
        &[candidate],
      )
      .unwrap(),
      expected
    );
  }
}

#[test]
fn valid_report_history_and_process_evidence_have_no_count_caps() {
  let first = report();
  let reports: Vec<_> = (0 .. 129)
    .map(|_| {
      let mut candidate = first.clone();
      candidate.report_id = Uuid::new_v4();
      candidate
    })
    .collect();
  let mut expected: Vec<_> = reports
    .iter()
    .map(|candidate| candidate.report_id)
    .collect();
  expected.sort_unstable();
  let mut profile = evidence();
  let observation = profile.observations[0].clone();
  profile.observations = (1 ..= 129)
    .map(|pid| PidObservation {
      pid,
      ..observation.clone()
    })
    .collect();
  assert_eq!(
    match_reports(first.run.installation_id, Kind::Anr, &profile, &reports).unwrap(),
    expected
  );
}

#[test]
fn duplicate_report_identity_is_returned_once_for_both_eligible_kinds() {
  for kind in [Kind::Anr, Kind::JavaOutOfMemory] {
    let mut candidate = report();
    candidate.kind = kind;
    let expected = vec![candidate.report_id];
    assert_eq!(
      match_reports(
        candidate.run.installation_id,
        kind,
        &evidence(),
        &[candidate.clone(), candidate],
      )
      .unwrap(),
      expected
    );
  }
}

#[test]
fn invalid_public_evidence_and_local_identity_fail_instead_of_looking_unmatched() {
  let candidate = report();
  let mut profile = evidence();
  profile.observations[0].first_ns = profile.observations[0].last_ns + 1;
  assert!(
    match_reports(
      candidate.run.installation_id,
      Kind::Anr,
      &profile,
      from_ref(&candidate),
    )
    .is_err()
  );
  profile = evidence();
  profile.boot_epoch_ns = i128::MAX;
  assert!(profile.validate().is_err());
  let mut invalid = candidate.clone();
  invalid.report_id = Uuid::nil();
  assert!(
    match_reports(
      candidate.run.installation_id,
      Kind::Anr,
      &evidence(),
      &[invalid],
    )
    .is_err()
  );
  let mut invalid = candidate.clone();
  invalid.event_elapsed_ns = invalid.run.start_elapsed_ns - 1;
  assert!(
    match_reports(
      candidate.run.installation_id,
      Kind::Anr,
      &evidence(),
      &[invalid],
    )
    .is_err()
  );
}
