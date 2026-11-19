// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Process-context attribution for Android system-triggered ANR/Java OOM profiles.
//!
//! The reader supplies typed process activity in nanoseconds since boot. The caller supplies the
//! installation that owns the callback and eligible SDK report/run facts. Matching compares their
//! boot epochs and process lifetimes; it never treats the receiving process as the profile's
//! source. A profile can describe several processes and contextualize several reports. This is
//! deliberately not proof that a particular crash caused the system trigger, nor a nearest-report
//! search.
//!
//! This module is a pure matcher, not a report ledger or upload store. Its inputs describe
//! already-enqueued reports; persistence, callback ownership and first-adoption build metadata stay
//! with the platform integration. Missing/ambiguous evidence returns an error; a valid profile with
//! no eligible parent returns an empty list. The caller diagnoses errors without blocking reports.

#[cfg(test)]
#[path = "./matcher_test.rs"]
mod tests;

pub mod reader;

use anyhow::{bail, ensure};
use uuid::Uuid;

// Matches the native feasibility collector's allowance for wall/elapsed sampling skew. This is
// not a crash/profile time window: timestamps within a run need not coincide with the fatal event.
pub const CLOCK_TOLERANCE_NS: i128 = 1_000_000_000;
// ProcessTree.Process.process_start_from_boot comes from /proc clock ticks (usually 10 ms).
// Android's recorded process-start time must agree to that precision, not to individual ns.
const PROCESS_START_TOLERANCE_NS: u64 = 10_000_000;

//
// Kind
//

/// Eligibility is trigger-specific; a low-memory kill is not an unhandled Java OOM report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
  Anr,
  JavaOutOfMemory,
}

//
// ProcessRun
//

/// Source-process facts captured by the SDK, not by the process receiving a delayed profile.
///
/// `wall_ns` and `elapsed_ns` are a paired startup clock sample. Their difference identifies the
/// boot epoch in the trace's REALTIME domain. `start_elapsed_ns` distinguishes reuse of a numeric
/// PID within that boot; a known SDK exit time clips the activity interval. UUID/boot-count facts
/// distinguish source runs but cannot be recovered from a Perfetto process-table row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessRun {
  pub installation_id: Uuid,
  pub run_id: Uuid,
  pub pid: u32,
  pub boot_count: u64,
  pub start_elapsed_ns: u64,
  pub wall_ns: i128,
  pub elapsed_ns: u64,
  pub end_elapsed_ns: Option<u64>,
}

impl ProcessRun {
  /// Reject internally inconsistent identity/clocks before comparing them with trace evidence.
  pub fn validate(&self) -> anyhow::Result<()> {
    ensure!(
      !self.installation_id.is_nil() && !self.run_id.is_nil(),
      "missing run identity"
    );
    ensure!(self.pid > 0, "missing source process PID");
    ensure!(
      self.start_elapsed_ns <= self.elapsed_ns,
      "invalid process startup clocks"
    );
    if let Some(end) = self.end_elapsed_ns {
      ensure!(end >= self.start_elapsed_ns, "invalid process lifetime");
    }
    self.boot_epoch_ns()?;
    Ok(())
  }

  /// Convert the paired startup clocks, not the report event time, to the trace's boot epoch.
  pub fn boot_epoch_ns(&self) -> anyhow::Result<i128> {
    self
      .wall_ns
      .checked_sub(i128::from(self.elapsed_ns))
      .ok_or_else(|| anyhow::anyhow!("process clock overflow"))
  }
}

//
// ReportCandidate
//

/// Only matching facts for a candidate report; no filenames, payloads or ledger ownership.
///
/// `report_id` is the uploader's canonical artifact UUID, added by the caller only after successful
/// ordinary enqueue or restoration from the pending artifact index. Candidates remain in RAM for
/// the receiving process's lifetime, including after ACK; the matcher owns no dispatch/expiry
/// state. The event time is checked for source consistency, but need not overlap this contextual
/// profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReportCandidate {
  pub report_id: Uuid,
  pub kind: Kind,
  pub run: ProcessRun,
  pub event_elapsed_ns: u64,
}

//
// PidObservation
//

/// First/last typed scheduler-switch or heap-sample activity for one process, in BOOTTIME ns.
///
/// A process-table row alone contributes no activity. The interval is the host collector's
/// min/max summary, not a claim that the process ran continuously between those timestamps.
/// Optional process-start metadata provides a PID-reuse discriminator; absent metadata stays
/// unknown. We do not reconstruct Perfetto's full process/thread lifecycle in this reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PidObservation {
  pub pid: u32,
  pub first_ns: u64,
  pub last_ns: u64,
  pub process_start_ns: Option<u64>,
}

//
// Evidence
//

/// The only trace output needed by matching and captured-time metadata.
///
/// `boot_epoch_ns` is REALTIME minus BOOTTIME from stable clock snapshots, never callback time.
/// Keeping only that offset and process intervals avoids retaining packets or heap data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Evidence {
  pub boot_epoch_ns: i128,
  pub observations: Vec<PidObservation>,
}

impl Evidence {
  /// Validate also for callers/tests constructing evidence without going through the wire reader.
  pub fn validate(&self) -> anyhow::Result<()> {
    for observation in &self.observations {
      ensure!(
        observation.pid > 0 && observation.first_ns <= observation.last_ns,
        "invalid PID observation interval"
      );
    }
    self.captured_at_ns()?;
    Ok(())
  }

  /// Wall time of the last typed sample, used for capture metadata rather than adoption time.
  pub fn captured_at_ns(&self) -> anyhow::Result<i128> {
    let last = self
      .observations
      .iter()
      .map(|observation| observation.last_ns)
      .max()
      .ok_or_else(|| anyhow::anyhow!("missing typed process timing"))?;
    self
      .boot_epoch_ns
      .checked_add(i128::from(last))
      .ok_or_else(|| anyhow::anyhow!("capture clock overflow"))
  }
}

/// Return every eligible canonical report UUID, sorted/deduplicated for stable upload attribution.
///
/// Check scope/kind first, then boot epoch, PID and overlap with the source run.
/// Without process-start metadata, another overlapping run with that PID makes attribution
/// ambiguous even if that other run has a different report kind.
pub fn match_reports(
  installation_id: Uuid,
  kind: Kind,
  evidence: &Evidence,
  reports: &[ReportCandidate],
) -> anyhow::Result<Vec<Uuid>> {
  ensure!(
    !installation_id.is_nil(),
    "missing profile installation identity"
  );
  evidence.validate()?;
  log::debug!(
    "crash profile matching started: kind={kind:?}, candidates={}, observations={}, \
     profile_boot_epoch_ns={}",
    reports.len(),
    evidence.observations.len(),
    evidence.boot_epoch_ns
  );
  let mut matched = Vec::new();
  for report in reports {
    // Invalid local facts are errors, not an apparent successful "no matches" result.
    report.run.validate()?;
    log::debug!(
      "crash profile considering report {}: kind={:?}, pid={}, run_id={}, boot_count={}, \
       start_elapsed_ns={}, end_elapsed_ns={:?}, event_elapsed_ns={}",
      report.report_id,
      report.kind,
      report.run.pid,
      report.run.run_id,
      report.run.boot_count,
      report.run.start_elapsed_ns,
      report.run.end_elapsed_ns,
      report.event_elapsed_ns
    );
    if report.run.installation_id != installation_id {
      log::debug!(
        "crash profile rejected report {}: reason=installation_mismatch",
        report.report_id
      );
      continue;
    }
    if report.kind != kind {
      log::debug!(
        "crash profile rejected report {}: reason=kind_mismatch, profile_kind={kind:?}, \
         report_kind={:?}",
        report.report_id,
        report.kind
      );
      continue;
    }
    ensure!(
      report.event_elapsed_ns >= report.run.start_elapsed_ns,
      "report precedes process start"
    );
    ensure!(
      !report.report_id.is_nil(),
      "missing report artifact identity"
    );
    let report_boot_epoch_ns = report.run.boot_epoch_ns()?;
    let delta = report_boot_epoch_ns
      .checked_sub(evidence.boot_epoch_ns)
      .and_then(i128::checked_abs)
      .ok_or_else(|| anyhow::anyhow!("clock comparison overflow"))?;
    if delta > CLOCK_TOLERANCE_NS {
      log::debug!(
        "crash profile rejected report {}: reason=boot_epoch_mismatch, \
         report_boot_epoch_ns={report_boot_epoch_ns}, profile_boot_epoch_ns={}, delta_ns={delta}, \
         tolerance_ns={CLOCK_TOLERANCE_NS}",
        report.report_id,
        evidence.boot_epoch_ns
      );
      continue;
    }
    let mut observed_pid = false;
    for observation in &evidence.observations {
      if observation.pid != report.run.pid {
        continue;
      }
      observed_pid = true;
      if let Some(start) = observation.process_start_ns
        && start.abs_diff(report.run.start_elapsed_ns) > PROCESS_START_TOLERANCE_NS
      {
        log::debug!(
          "crash profile rejected observation for report {}: reason=process_start_mismatch, \
           pid={}, profile_start_ns={start}, report_start_ns={}, delta_ns={}, \
           tolerance_ns={PROCESS_START_TOLERANCE_NS}",
          report.report_id,
          observation.pid,
          report.run.start_elapsed_ns,
          start.abs_diff(report.run.start_elapsed_ns)
        );
        continue;
      }
      let first = observation
        .first_ns
        .max(report.run.start_elapsed_ns)
        .max(observation.process_start_ns.unwrap_or(0));
      let last = observation
        .last_ns
        .min(report.run.end_elapsed_ns.unwrap_or(u64::MAX));
      if first <= last {
        // A trace start only disambiguates reused PIDs when one overlapping run fits its tolerance.
        if let Some(other) = reports.iter().find(|other| {
          other.run.run_id != report.run.run_id
            && other.run.installation_id == installation_id
            && other.run.pid == report.run.pid
            && other.run.boot_count == report.run.boot_count
            && observation.process_start_ns.is_none_or(|start| {
              start.abs_diff(other.run.start_elapsed_ns) <= PROCESS_START_TOLERANCE_NS
            })
            && other.run.boot_epoch_ns().is_ok_and(|epoch| {
              epoch.abs_diff(evidence.boot_epoch_ns) <= CLOCK_TOLERANCE_NS as u128
            })
            && other.run.start_elapsed_ns <= last
            && other.run.end_elapsed_ns.unwrap_or(u64::MAX) >= first
        }) {
          log::debug!(
            "crash profile attribution ambiguous: report_id={}, conflicting_report_id={}, pid={}, \
             run_id={}, conflicting_run_id={}, overlap_first_ns={first}, overlap_last_ns={last}, \
             profile_process_start_ns={:?}, reason=ambiguous_process_start_with_pid_reuse",
            report.report_id,
            other.report_id,
            report.run.pid,
            report.run.run_id,
            other.run.run_id,
            observation.process_start_ns
          );
          bail!("ambiguous reused source PID");
        }
        log::debug!(
          "crash profile matched report {}: pid={}, overlap_first_ns={first}, \
           overlap_last_ns={last}, profile_process_start_ns={:?}",
          report.report_id,
          report.run.pid,
          observation.process_start_ns
        );
        matched.push(report.report_id);
        break;
      }
      log::debug!(
        "crash profile rejected observation for report {}: reason=no_lifetime_overlap, pid={}, \
         profile_first_ns={}, profile_last_ns={}, overlap_first_ns={first}, overlap_last_ns={last}",
        report.report_id,
        observation.pid,
        observation.first_ns,
        observation.last_ns
      );
    }
    if !observed_pid {
      log::debug!(
        "crash profile rejected report {}: reason=pid_absent, report_pid={}, observations={}",
        report.report_id,
        report.run.pid,
        evidence.observations.len()
      );
    }
  }
  matched.sort_unstable();
  matched.dedup();
  log::debug!(
    "crash profile matching complete: kind={kind:?}, candidates={}, matched_reports={}",
    reports.len(),
    matched.len()
  );
  Ok(matched)
}
