// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Android crash-profile attribution against an in-RAM report candidate set.
//!
//! Candidates are the union of client reports restored from the pending upload index at startup
//! and reports freshly dispatched by normal report discovery at any point in this process's
//! lifetime. Neither readiness nor upload ACK removes them; they live until process exit, so
//! several profiles can attach to one report. After a restart only still-indexed reports are
//! recovered, so a delayed profile for an already acknowledged report may remain unattached.
//!
//! Profiles submitted before the initial previous-run report pass wait for it. A profile is then
//! matched against the candidates present at that moment, and a matched profile is queued as an
//! ordinary attachment naming its parents. A fatal ANR's profile reaches the dying process before
//! its report can exist (Java OOM heap dumps can too), so an unmatched live result is renamed in
//! place in the platform's output directory with [`RETAINED_SUFFIX`] and its kind. The next process
//! resubmits only such files, once, after its own startup report pass. An unmatched resubmission or
//! an unreadable file is deleted. In debug builds with attribution debug logging enabled, errors
//! instead retain the latest failed trace per kind for local diagnosis, never for upload.
//!
//! Eligible reports carry an encoded `CrashProfileSource` in their typed `crash_profile_source`
//! field: ANR reports (facts recovered from the exit record) and unhandled Java `OutOfMemoryError`
//! reports, the only JVM crashes the SDK annotates. [`encode_source`] produces those bytes once per
//! process. Its paired clock sample converts the report's wall time into BOOTTIME and is compared
//! with the trace's boot epoch.

#[cfg(test)]
#[path = "./crash_profile_attribution_test.rs"]
mod tests;

use crate::crash_profile::reader::Reader;
use crate::crash_profile::{
  CLOCK_TOLERANCE_NS,
  Evidence,
  Kind,
  ProcessRun,
  ReportCandidate,
  match_reports,
};
use crate::report_attribution::ReportAttribution;
use crate::{Client, UploadSource};
pub use crate::{PERFETTO_CONTENT_TYPE, encode_source};
use anyhow::{anyhow, bail, ensure};
use bd_client_common::artifact::CRASH_PROFILE_MAX_REPORT_IDS;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Report,
  ReportType,
};
use bd_proto::protos::client::api::CrashProfileMetadata;
use bd_proto::protos::client::artifact::CrashProfileSource;
use bd_runtime::runtime::crash_profiles::{AnrEnabled, OomEnabled};
use bd_runtime::runtime::{BoolWatch, ConfigLoader};
use bd_shutdown::ComponentShutdown;
use parking_lot::Mutex;
use protobuf::Message as _;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf, absolute};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

/// Appended, with `anr` or `oom`, to a retained live result's file name. Platform result names do
/// not reliably record the trigger type, and this keeps SDK-retained files distinct from host
/// results. The Android collector rediscovers the same suffix.
pub const RETAINED_SUFFIX: &str = ".bitdrift-retained-";

fn retained_path(path: &Path, kind: Kind) -> PathBuf {
  let kind = match kind {
    Kind::Anr => "anr",
    Kind::JavaOutOfMemory => "oom",
  };
  let mut name = path.as_os_str().to_owned();
  name.push(format!("{RETAINED_SUFFIX}{kind}"));
  PathBuf::from(name)
}

#[cfg(debug_assertions)]
fn failed_path(path: &Path, kind: Kind) -> PathBuf {
  let name = match kind {
    Kind::Anr => "bitdrift-attribution-failed-anr.perfetto-trace",
    Kind::JavaOutOfMemory => "bitdrift-attribution-failed-oom.perfetto-trace",
  };
  path.with_file_name(name)
}

//
// Candidate
//

#[derive(Clone)]
struct Candidate {
  report: ReportCandidate,
  // Profiles upload under their first parent's session so session sampling agrees with it.
  session_id: String,
}

//
// Request
//

struct Request {
  path: PathBuf,
  kind: Kind,
  retain_unmatched: bool,
}

//
// Upload
//

#[derive(Debug, PartialEq, Eq)]
struct Upload {
  report_ids: Vec<Uuid>,
  captured_at: OffsetDateTime,
  session_id: String,
}

//
// Candidates
//

struct Candidates {
  installation_id: Uuid,
  reports: Mutex<HashMap<Uuid, Candidate>>,
  ready: watch::Sender<bool>,
  #[cfg(any(test, feature = "test-support"))]
  progress: watch::Sender<u64>,
}

impl Candidates {
  /// Match one profile against a snapshot of the current candidates.
  fn attribute(&self, kind: Kind, evidence: &Evidence) -> anyhow::Result<Option<Upload>> {
    let candidates: Vec<Candidate> = self.reports.lock().values().cloned().collect();
    for (index, observation) in evidence.observations.iter().take(16).enumerate() {
      log::debug!("crash profile observation[{index}]: {observation:?}");
    }
    if evidence.observations.len() > 16 {
      log::debug!(
        "crash profile observation log truncated: showing=16, total={}",
        evidence.observations.len()
      );
    }
    let reports: Vec<ReportCandidate> = candidates.iter().map(|c| c.report.clone()).collect();
    let report_ids = match_reports(self.installation_id, kind, evidence, &reports)?;
    let Some(first) = report_ids.first() else {
      log::debug!(
        "crash profile has no eligible parents: kind={kind:?}, candidates={}",
        reports.len()
      );
      return Ok(None);
    };
    // Admission rejects more parents; never silently drop some of the matched reports.
    ensure!(
      report_ids.len() <= CRASH_PROFILE_MAX_REPORT_IDS,
      "profile matches {} reports, more than {CRASH_PROFILE_MAX_REPORT_IDS}",
      report_ids.len()
    );
    let session_id = candidates
      .iter()
      .find(|candidate| candidate.report.report_id == *first)
      .map(|candidate| candidate.session_id.clone())
      .unwrap_or_default();
    Ok(Some(Upload {
      captured_at: OffsetDateTime::from_unix_timestamp_nanos(evidence.captured_at_ns()?)?,
      report_ids,
      session_id,
    }))
  }

  /// Read and match a profile file. A `None` result or error means the file is not uploaded.
  async fn prepare(self: &Arc<Self>, kind: Kind, path: PathBuf) -> anyhow::Result<Option<Upload>> {
    let evidence = tokio::task::spawn_blocking(move || {
      log::debug!(
        "reading crash profile: kind={kind:?}, path={}",
        path.display()
      );
      let evidence = Reader::read(BufReader::new(File::open(&path)?))?;
      log::debug!(
        "read crash profile timing: path={}, boot_epoch_ns={}, observations={}",
        path.display(),
        evidence.boot_epoch_ns,
        evidence.observations.len()
      );
      Ok::<_, anyhow::Error>(evidence)
    })
    .await??;
    self.attribute(kind, &evidence)
  }
}

//
// CrashProfileAttribution
//

pub struct CrashProfileAttribution {
  candidates: Arc<Candidates>,
  requests_tx: mpsc::UnboundedSender<Request>,
  requests_rx: Mutex<Option<mpsc::UnboundedReceiver<Request>>>,
}

impl CrashProfileAttribution {
  /// `installation_id` scopes both reports and profiles, which share this app's private storage.
  #[must_use]
  pub fn new(installation_id: Uuid) -> Self {
    let (requests_tx, requests_rx) = mpsc::unbounded_channel();
    Self {
      candidates: Arc::new(Candidates {
        installation_id,
        reports: Mutex::default(),
        ready: watch::channel(false).0,
        #[cfg(any(test, feature = "test-support"))]
        progress: watch::channel(0).0,
      }),
      requests_tx,
      requests_rx: Mutex::new(Some(requests_rx)),
    }
  }

  /// Queues a successful system-triggered result, handled after the initial report pass and left in
  /// place if its kind is disabled by then. A live result that matches no report is retained for
  /// one resubmission by the next process (`retain_unmatched`); otherwise it is deleted.
  /// Resolve relative paths once so matching, upload persistence and source cleanup use the same
  /// file.
  pub fn submit(&self, path: PathBuf, kind: Kind, retain_unmatched: bool) {
    let path = match absolute(path) {
      Ok(path) => path,
      Err(error) => {
        log::warn!("failed to resolve crash profile path: {error}");
        return;
      },
    };
    log::debug!(
      "crash profile submitted: {kind:?} retain_unmatched={retain_unmatched} {}",
      path.display()
    );
    log::debug!(
      "crash profile submission state: report_recovery_complete={}, candidates={}",
      *self.candidates.ready.borrow(),
      self.candidates.reports.lock().len()
    );
    let request = Request {
      path,
      kind,
      retain_unmatched,
    };
    if self.requests_tx.send(request).is_err() {
      log::debug!("crash profile attribution stopped before submission");
    }
  }

  /// Lifecycle gate: revision 1 means waiting for report recovery; each handled request advances
  /// it.
  #[cfg(any(test, feature = "test-support"))]
  pub fn processing_revision_for_test(&self) -> watch::Receiver<u64> {
    self.candidates.progress.subscribe()
  }
}

/// Build a matcher candidate from a report's source facts, converting the report's wall time into
/// the source process's BOOTTIME domain with its paired startup clock sample. `Ok(None)` means the
/// report carries no source facts and is simply not eligible.
fn report_candidate(
  installation_id: Uuid,
  report_id: Uuid,
  report: &Report<'_>,
) -> anyhow::Result<Option<ReportCandidate>> {
  let Some(source) = report.crash_profile_source() else {
    return Ok(None);
  };
  let kind = match report.type_() {
    ReportType::AppNotResponding => Kind::Anr,
    ReportType::JVMCrash => Kind::JavaOutOfMemory,
    report_type => bail!("unsupported report type {report_type:?}"),
  };
  let source = CrashProfileSource::parse_from_bytes(source.bytes())?;
  let mut run = ProcessRun {
    installation_id,
    run_id: Uuid::from_slice(&source.run_id)?,
    pid: source.pid,
    boot_count: source.boot_count,
    start_elapsed_ns: source.process_start_elapsed_ns,
    wall_ns: i128::from(source.wall_ns),
    elapsed_ns: source.elapsed_ns,
    end_elapsed_ns: None,
  };
  run.validate()?;
  let time = report
    .device_metrics()
    .and_then(|metrics| metrics.time())
    .ok_or_else(|| anyhow!("report has no time"))?;
  let occurred_at_ns = i128::from(time.seconds()) * 1_000_000_000 + i128::from(time.nanos());
  let event_elapsed_ns = occurred_at_ns
    .checked_sub(run.boot_epoch_ns()?)
    .and_then(|event| u64::try_from(event).ok())
    .ok_or_else(|| anyhow!("report time is outside the source boot"))?;
  // An invalid candidate would make every match fail, so reject it before it joins the set.
  ensure!(
    event_elapsed_ns >= run.start_elapsed_ns,
    "report precedes process start"
  );
  // Recovered ANRs use ApplicationExitInfo.timestamp: unlike a live JVM exception, this is a
  // death bound. Allow the paired-clock skew and Android's millisecond timestamp truncation,
  // without turning the event into a contextual-profile matching window.
  if kind == Kind::Anr {
    run.end_elapsed_ns = Some(
      event_elapsed_ns
        .checked_add(u64::try_from(CLOCK_TOLERANCE_NS)?)
        .and_then(|end| end.checked_add(1_000_000))
        .ok_or_else(|| anyhow!("report death bound overflow"))?,
    );
  }
  // TODO: Bound recovered JVM OOM lifetimes using trustworthy process-exit evidence. Native OOM
  // traces can omit process-start metadata, leaving later PID reuse indistinguishable. The
  // exception timestamp is not a death bound: heap capture may continue afterward.
  Ok(Some(ReportCandidate {
    report_id,
    kind,
    run,
    event_elapsed_ns,
  }))
}

impl ReportAttribution for CrashProfileAttribution {
  fn add_report(&self, report_id: Uuid, report: &Report<'_>, session_id: &str) {
    match report_candidate(self.candidates.installation_id, report_id, report) {
      Ok(None) => log::debug!(
        "crash profile skipped report {report_id}: reason=missing_source_facts, report_type={:?}",
        report.type_()
      ),
      Ok(Some(report)) => {
        log::debug!(
          "crash profile candidate added: {report_id} {:?}",
          report.kind
        );
        log::debug!(
          "crash profile candidate source: report_id={report_id}, pid={}, run_id={}, \
           boot_count={}, start_elapsed_ns={}, end_elapsed_ns={:?}, wall_ns={}, elapsed_ns={}, \
           event_elapsed_ns={}",
          report.run.pid,
          report.run.run_id,
          report.run.boot_count,
          report.run.start_elapsed_ns,
          report.run.end_elapsed_ns,
          report.run.wall_ns,
          report.run.elapsed_ns,
          report.event_elapsed_ns
        );
        self.candidates.reports.lock().insert(
          report_id,
          Candidate {
            report,
            session_id: session_id.to_string(),
          },
        );
      },
      Err(error) => log::debug!("report {report_id} is not attributable: {error}"),
    }
  }

  fn mark_ready(&self) {
    if !self.candidates.ready.send_replace(true) {
      log::debug!(
        "crash profile candidates ready: {}",
        self.candidates.reports.lock().len()
      );
    }
  }

  fn start(&self, client: Arc<dyn Client>, runtime: &ConfigLoader, shutdown: ComponentShutdown) {
    let Some(requests) = self.requests_rx.lock().take() else {
      log::debug!("crash profile attribution already started");
      return;
    };
    tokio::spawn(run(
      self.candidates.clone(),
      client,
      requests,
      runtime.register_bool_watch(),
      runtime.register_bool_watch(),
      shutdown,
    ));
  }
}

async fn run(
  candidates: Arc<Candidates>,
  client: Arc<dyn Client>,
  mut requests: mpsc::UnboundedReceiver<Request>,
  anr_enabled: BoolWatch<AnrEnabled>,
  oom_enabled: BoolWatch<OomEnabled>,
  mut shutdown: ComponentShutdown,
) {
  let mut ready = candidates.ready.subscribe();
  log::debug!("crash profile worker waiting for initial report recovery");
  #[cfg(any(test, feature = "test-support"))]
  candidates.progress.send_replace(1);
  tokio::select! {
    result = ready.wait_for(|ready| *ready) => if result.is_err() {
      log::debug!("crash profile worker stopped: report recovery channel closed");
      return;
    },
    () = shutdown.cancelled() => {
      log::debug!("crash profile worker stopped while waiting for initial report recovery");
      return;
    },
  }
  log::debug!("crash profile worker initial report recovery complete");
  loop {
    let request = tokio::select! {
      request = requests.recv() => request,
      () = shutdown.cancelled() => None,
    };
    let Some(Request {
      path,
      kind,
      retain_unmatched,
    }) = request
    else {
      log::debug!("crash profile worker stopped: request channel closed or shutdown requested");
      return;
    };
    let enabled = match kind {
      Kind::Anr => *anr_enabled.read(),
      Kind::JavaOutOfMemory => *oom_enabled.read(),
    };
    log::debug!(
      "crash profile adoption started: kind={kind:?}, path={}, \
       retain_unmatched={retain_unmatched}, runtime_enabled={enabled}, candidates={}",
      path.display(),
      candidates.reports.lock().len()
    );
    if !enabled {
      log::debug!(
        "crash profile adoption disabled for {kind:?}, leaving file in place: {}",
        path.display()
      );
      #[cfg(any(test, feature = "test-support"))]
      candidates.progress.send_modify(|progress| *progress += 1);
      continue;
    }
    tokio::select! {
      () = adopt(&candidates, client.as_ref(), kind, path, retain_unmatched) => {},
      () = shutdown.cancelled() => {
        log::debug!("crash profile worker stopped during adoption");
        return;
      },
    }
    log::debug!("crash profile adoption completed");
    #[cfg(any(test, feature = "test-support"))]
    candidates.progress.send_modify(|progress| *progress += 1);
  }
}

async fn adopt(
  candidates: &Arc<Candidates>,
  client: &dyn Client,
  kind: Kind,
  path: PathBuf,
  retain_unmatched: bool,
) {
  // A resubmitted result can also be redelivered live; whichever attempt runs second finds the
  // file already consumed.
  if !tokio::fs::try_exists(&path).await.unwrap_or(true) {
    log::debug!("crash profile already handled: {}", path.display());
    return;
  }
  let upload = match candidates.prepare(kind, path.clone()).await {
    Ok(Some(upload)) => upload,
    Ok(None) if retain_unmatched => {
      let retained = retained_path(&path, kind);
      log::debug!(
        "retaining unmatched live crash profile for the next process: kind={kind:?}, source={}, \
         target={}",
        path.display(),
        retained.display()
      );
      match tokio::fs::rename(&path, &retained).await {
        Ok(()) => log::debug!(
          "crash profile kept for the next process: {}",
          retained.display()
        ),
        Err(error) => {
          log::warn!("failed to retain crash profile: {error}");
          if let Err(error) = tokio::fs::remove_file(&path).await {
            log::debug!("failed to remove unretained crash profile: {error}");
          }
        },
      }
      return;
    },
    result => {
      match result {
        Ok(_) => log::debug!(
          "crash profile has no matching report; deleting unmatched resubmission: kind={kind:?}, \
           path={}",
          path.display()
        ),
        Err(error) => {
          log::warn!(
            "crash profile attribution failed: kind={kind:?}, path={}, error={error}",
            path.display()
          );
          #[cfg(debug_assertions)]
          if log::log_enabled!(log::Level::Debug) {
            // Fixed names bound retention to one failure per kind and avoid pending-file discovery.
            let failed = failed_path(&path, kind);
            match tokio::fs::rename(&path, &failed).await {
              Ok(()) => {
                log::debug!(
                  "crash profile retained for attribution debugging; replaces previous failure: \
                   kind={kind:?}, path={}",
                  failed.display()
                );
                return;
              },
              Err(error) => log::warn!(
                "failed to retain crash profile for attribution debugging; deleting source: \
                 path={}, error={error}",
                path.display()
              ),
            }
          }
        },
      }
      if let Err(error) = tokio::fs::remove_file(&path).await {
        log::debug!("failed to remove unattributed crash profile: {error}");
      }
      return;
    },
  };
  log::debug!(
    "enqueueing attributed crash profile: kind={kind:?}, path={}, report_ids={:?}, captured_at={}",
    path.display(),
    upload.report_ids,
    upload.captured_at
  );
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let result = client.enqueue_crash_profile(
    UploadSource::Path(path),
    upload.captured_at,
    upload.session_id,
    PERFETTO_CONTENT_TYPE.to_string(),
    CrashProfileMetadata {
      report_ids: upload.report_ids.iter().map(ToString::to_string).collect(),
      ..Default::default()
    },
    Some(persisted_tx),
  );
  match result {
    Ok(profile_id) => {
      log::debug!("crash profile {profile_id} waiting for artifact queue persistence");
      match persisted_rx.await {
        Ok(Ok(())) => log::debug!(
          "queued crash profile {profile_id} for reports {:?}",
          upload.report_ids
        ),
        Ok(Err(error)) => log::warn!("failed to persist crash profile {profile_id}: {error:?}"),
        Err(_) => log::debug!("crash profile {profile_id} persistence was not acknowledged"),
      }
    },
    Err(error) => log::warn!("failed to enqueue crash profile: {error:?}"),
  }
}
