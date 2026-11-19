// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Per-log interning and encoding scenarios shared by the instruction-count and wall-clock
//! benchmarks.
//!
//! Each scenario writes one log the way `LogReplay` does: interning compares the final log with
//! the current field set once, then every matching buffer computes the encoded size and
//! serializes into its reservation. The interner has already written the current version, so no
//! scenario measures a table write.

use bd_field_interning::{FieldInterner, FieldSetTable, Resolution};
use bd_key_value::Store;
use bd_log_filter::ScrubRules;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  EncodableLog,
  InternedFields,
  Log,
  LogFieldKey,
  LogFields,
  LogFieldsRef,
};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _, field_interning};
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::session::InMemoryStorage;
use bd_time::TestTimeProvider;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tempfile::TempDir;
use time::macros::datetime;

/// The persistent fields every scenario's log carries, a large set for an app that attaches many.
const PERSISTENT_FIELDS: usize = 100;

/// The fields a log carries besides the persistent ones.
const LOG_FIELDS: usize = 5;

/// The persistent keys a filter removes in the `Cleared` mode.
const CLEARED_FIELDS: usize = 3;

/// Rounds of changes that make a key volatile: its average gap between changes, starting at the
/// default window of 512 logs, drops below the default minimum of 4 logs per change after 17.
const VOLATILE_ROUNDS: usize = 24;

/// A minimum size that every log's contents exceed, so compression always runs.
const COMPRESS_ALWAYS: u64 = 1;

/// The reservation the encoded log is written into. Every scenario's log fits.
const RESERVATION_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub enum Mode {
  /// Interning is off: `set_interned(None)` and every field inline.
  Off,
  /// Interning is on and the log carries every persistent field unchanged.
  Unchanged,
  /// Interning is on and a filter removed a few persistent keys, which encode as `cleared`.
  Cleared,
}

impl Mode {
  #[must_use]
  pub const fn name(self) -> &'static str {
    match self {
      Self::Off => "off",
      Self::Unchanged => "unchanged",
      Self::Cleared => "cleared",
    }
  }
}

#[derive(Clone, Copy, Debug)]
pub struct Scenario {
  pub mode: Mode,
  pub compressed: bool,
  pub buffers: usize,
}

impl Scenario {
  #[must_use]
  pub const fn new(mode: Mode, compressed: bool, buffers: usize) -> Self {
    Self {
      mode,
      compressed,
      buffers,
    }
  }

  /// Each mode with uncompressed and compressed contents, written to one buffer, and interning off
  /// and on written to three buffers, which share one leave-out decision.
  pub const CASES: [Self; 8] = [
    Self::new(Mode::Off, false, 1),
    Self::new(Mode::Unchanged, false, 1),
    Self::new(Mode::Cleared, false, 1),
    Self::new(Mode::Off, true, 1),
    Self::new(Mode::Unchanged, true, 1),
    Self::new(Mode::Cleared, true, 1),
    Self::new(Mode::Off, false, 3),
    Self::new(Mode::Unchanged, false, 3),
  ];

  #[must_use]
  pub fn name(&self) -> String {
    format!(
      "{}_{}_{}_buffer{}",
      self.mode.name(),
      if self.compressed {
        "compressed"
      } else {
        "uncompressed"
      },
      self.buffers,
      if self.buffers == 1 { "" } else { "s" },
    )
  }
}

fn persistent_field_set() -> AnnotatedLogFields {
  (0 .. PERSISTENT_FIELDS)
    .map(|i| {
      let key: LogFieldKey = format!("persistent_field_{i:03}").into();
      // Shared strings, as the SDK stores most persistent values.
      let value: Arc<str> = format!("persistent-value-{i:03}-0123456789abcdef").into();
      let field = if i % 2 == 0 {
        AnnotatedLogField::new_ootb(DataValue::SharedString(value))
      } else {
        AnnotatedLogField::new_custom(DataValue::SharedString(value))
      };
      (key, field)
    })
    .collect()
}

/// A log carrying every persistent field, as normalization produces, plus its own fields. In the
/// `Cleared` mode a filter has removed the first few persistent keys.
fn log(persistent: &AnnotatedLogFields, mode: Mode) -> Log {
  let mut fields: LogFields = persistent
    .iter()
    .map(|(key, field)| (key.clone(), field.value.clone()))
    .collect();
  if matches!(mode, Mode::Cleared) {
    for i in 0 .. CLEARED_FIELDS {
      fields.remove(format!("persistent_field_{i:03}").as_str());
    }
  }
  for i in 0 .. LOG_FIELDS {
    fields.insert(
      format!("log_field_{i}").into(),
      DataValue::String(format!("log-value-{i}-fedcba9876543210")),
    );
  }
  Log {
    log_level: 2,
    log_type: LogType::NORMAL,
    message: "request completed".into(),
    fields,
    matching_fields: LogFields::default(),
    session_id: "5f0c1b2e-7a4d-4c3b-9e1f-0a2b3c4d5e6f".into(),
    occurred_at: datetime!(2026-11-10 00:00 UTC),
    command_attachment: None,
    capture_session: None,
  }
}

//
// InternerSetup
//

/// An interner that has already written the version for the current persistent fields.
pub struct InternerSetup {
  interner: FieldInterner,
  persistent: AnnotatedLogFields,
  scrub_rules: Arc<ScrubRules>,
  // Runs the table writes that setup makes.
  runtime: tokio::runtime::Runtime,
  // Removed after the interner, which holds the table open in it.
  _directory: TempDir,
}

impl InternerSetup {
  fn new(persistent: AnnotatedLogFields) -> Self {
    let directory = TempDir::new().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap();
    let runtime_loader = ConfigLoader::new(directory.path());
    runtime
      .block_on(runtime_loader.update_snapshot(make_simple_update(vec![(
        field_interning::Enabled::path(),
        ValueKind::Bool(true),
      )])))
      .unwrap();
    let mut interner = FieldInterner::new(
      directory.path(),
      &runtime_loader,
      Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      Arc::new(TestTimeProvider::new(datetime!(2026-11-10 00:00 UTC))),
      &bd_client_stats_store::Collector::default().scope("field_interning"),
    );
    runtime.block_on(interner.prepare());
    let mut setup = Self {
      interner,
      persistent,
      scrub_rules: Arc::new(ScrubRules::default()),
      runtime,
      _directory: directory,
    };
    // The first log writes the version; later logs only compare with it.
    let warmup = log(&setup.persistent, Mode::Unchanged);
    assert!(setup.intern_blocking(&warmup).is_some());
    setup
  }

  /// Interns `log` against a field set that already has a version. That writes nothing, so the
  /// future completes on its first poll, as it does when the event loop awaits it; running it on
  /// the runtime would add about 1,200 instructions of scheduling to every measurement.
  pub fn intern(&mut self, log: &Log) -> Option<InternedFields> {
    let future = pin!(
      self
        .interner
        .intern(log, &self.persistent, &self.scrub_rules)
    );
    match future.poll(&mut Context::from_waker(Waker::noop())) {
      Poll::Ready(interned) => interned,
      Poll::Pending => panic!("interning an unchanged field set waited for I/O"),
    }
  }

  /// Interns `log` on the runtime, writing a version when the field set changed.
  fn intern_blocking(&mut self, log: &Log) -> Option<InternedFields> {
    self.runtime.block_on(
      self
        .interner
        .intern(log, &self.persistent, &self.scrub_rules),
    )
  }

  /// Sets a persistent field to a new value, as a field update does.
  fn update(&mut self, key: &LogFieldKey, value: String) {
    let field = AnnotatedLogField::new_custom(DataValue::String(value));
    let previous = self.persistent.insert(key.clone(), field);
    debug_assert!(previous.is_some());
    self.interner.record_update(key, true, true);
  }

  /// Interns a log carrying the current persistent fields, returning the version it refers to.
  fn intern_current(&mut self) -> u64 {
    let log = log(&self.persistent, Mode::Unchanged);
    let interned = self.intern_blocking(&log).unwrap();
    LogFieldsRef::new(&log.fields, Some(&interned)).field_set_version()
  }
}

//
// WriteSetup
//

/// The state for writing one log to its buffers.
pub struct WriteSetup {
  interner: Option<InternerSetup>,
  persistent: AnnotatedLogFields,
  mode: Mode,
  compression_min_size: u64,
  buffers: usize,
  reservation: Vec<u8>,
}

impl WriteSetup {
  #[must_use]
  pub fn new(scenario: Scenario) -> Self {
    let persistent = persistent_field_set();
    let interner = match scenario.mode {
      Mode::Off => None,
      Mode::Unchanged | Mode::Cleared => Some(InternerSetup::new(persistent.clone())),
    };
    Self {
      interner,
      persistent,
      mode: scenario.mode,
      compression_min_size: if scenario.compressed {
        COMPRESS_ALWAYS
      } else {
        0
      },
      buffers: scenario.buffers,
      reservation: vec![0; RESERVATION_BYTES],
    }
  }

  /// The setup and the log to write.
  #[must_use]
  pub fn with_log(scenario: Scenario) -> (Self, Log) {
    let setup = Self::new(scenario);
    let log = log(&setup.persistent, setup.mode);
    (setup, log)
  }

  /// Builds fresh logs for the scenario without borrowing the setup.
  pub fn log_builder(&self) -> impl Fn() -> Log + use<> {
    let persistent = self.persistent.clone();
    let mode = self.mode;
    move || log(&persistent, mode)
  }

  /// Interns `log` and writes it to every buffer, as `LogReplay` does. Returns the log so that
  /// dropping it is not measured.
  pub fn write(&mut self, log: Log) -> EncodableLog {
    let interned = self.interner.as_mut().and_then(|setup| setup.intern(&log));
    let mut log = EncodableLog::new(log, self.compression_min_size);
    log.set_interned(interned);
    for _ in 0 .. self.buffers {
      let size = usize::try_from(log.compute_size(&[], &[]).unwrap()).unwrap();
      log
        .serialize_to_bytes(&[], &[], &mut self.reservation[.. size])
        .unwrap();
    }
    log
  }
}

//
// InternSetup
//

/// The state for `FieldInterner::intern` alone.
pub struct InternSetup {
  interner: InternerSetup,
  log: Log,
}

impl InternSetup {
  #[must_use]
  pub fn new(mode: Mode) -> Self {
    let persistent = persistent_field_set();
    let log = log(&persistent, mode);
    Self {
      interner: InternerSetup::new(persistent),
      log,
    }
  }

  pub fn intern(&mut self) -> Option<InternedFields> {
    self.interner.intern(&self.log)
  }

  /// The persistent fields plus `volatile_fields` that changed on every recent log and
  /// stay inline. None of them is due for re-admission yet.
  #[must_use]
  pub fn with_volatile(volatile_fields: usize) -> Self {
    let mut persistent = persistent_field_set();
    let volatile: Vec<LogFieldKey> = (0 .. volatile_fields)
      .map(|i| format!("volatile_field_{i:03}").into())
      .collect();
    for key in &volatile {
      persistent.insert(
        key.clone(),
        AnnotatedLogField::new_custom(DataValue::String("initial".into())),
      );
    }
    let mut interner = InternerSetup::new(persistent);
    for round in 0 ..= VOLATILE_ROUNDS {
      if round == VOLATILE_ROUNDS {
        // Volatile keys are out of the set, so changing them writes no version.
        let version = interner.intern_current();
        for key in &volatile {
          interner.update(key, format!("value-{round}"));
        }
        assert_eq!(interner.intern_current(), version);
        break;
      }
      for key in &volatile {
        interner.update(key, format!("value-{round}"));
      }
      interner.intern_current();
    }
    let log = log(&interner.persistent, Mode::Unchanged);
    Self { interner, log }
  }
}

//
// ResolveSetup
//

/// Versions resolved one after another per measurement.
pub const RESOLUTIONS: u64 = 16;

/// A version table holding consecutive versions, each changing one persistent field, with one
/// uploader cursor already positioned.
pub struct ResolveSetup {
  table: FieldSetTable,
  versions: Vec<u64>,
  // The interner owns the table's directory.
  _interner: InternerSetup,
}

impl ResolveSetup {
  fn build() -> (InternerSetup, FieldSetTable, u64) {
    let persistent = persistent_field_set();
    let keys: Vec<LogFieldKey> = persistent.keys().cloned().collect();
    let mut interner = InternerSetup::new(persistent);
    let first = interner.intern_current();
    // Each key changes once, so none becomes volatile.
    for (key, expected) in keys.iter().zip(first + 1 ..= first + RESOLUTIONS) {
      interner.update(key, format!("changed-{key}"));
      assert_eq!(interner.intern_current(), expected);
    }
    let table = interner.interner.table();
    (interner, table, first)
  }

  /// The same version, resolved repeatedly, as uploaders do for consecutive logs that share it.
  #[must_use]
  pub fn same() -> Self {
    let (interner, table, first) = Self::build();
    let version = first + RESOLUTIONS / 2;
    assert!(matches!(table.resolution(version), Resolution::Found(_)));
    Self {
      table,
      versions: vec![version; usize::try_from(RESOLUTIONS).unwrap()],
      _interner: interner,
    }
  }

  /// Consecutive versions, each resolved once, as an uploader reading a buffer oldest first does.
  #[must_use]
  pub fn consecutive() -> Self {
    let (interner, table, first) = Self::build();
    assert!(matches!(table.resolution(first), Resolution::Found(_)));
    Self {
      table,
      versions: (first + 1 ..= first + RESOLUTIONS).collect(),
      _interner: interner,
    }
  }

  #[must_use]
  pub fn resolve(&self) -> Vec<Resolution> {
    self
      .versions
      .iter()
      .map(|&version| self.table.resolution(version))
      .collect()
  }
}

//
// DecideSetup
//

/// The leave-out decision for one log: `InternedFields::new` compares every field of the log with
/// the field set once and records which ones encoding leaves out.
pub struct DecideSetup {
  fields: LogFields,
  field_set: Arc<LogFields>,
}

impl DecideSetup {
  #[must_use]
  pub fn unchanged() -> Self {
    let persistent = persistent_field_set();
    let field_set = persistent
      .iter()
      .map(|(key, field)| (key.clone(), field.value.clone()))
      .collect();
    Self {
      fields: log(&persistent, Mode::Unchanged).fields,
      field_set: Arc::new(field_set),
    }
  }

  #[must_use]
  pub fn decide(&self) -> Option<InternedFields> {
    InternedFields::new(&self.fields, 1, self.field_set.clone())
  }
}
