// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Persistent field interning.
//!
//! Persistent fields (`add_log_field`, `update_ootb_log_field` and the initial fields) are stored
//! once per change as a field-set version in an on-device [`table::VersionTable`]. A log written to
//! a ring buffer leaves out each field of the current version that it carries unchanged and
//! records the version instead. Uploaders add the full field set of every version their logs refer
//! to, and the server restores the fields at ingest.
//!
//! The interner is owned by the task that writes the ring buffers. Uploaders resolve versions
//! through a shared [`FieldSetTable`] handle.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod lib_test;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

pub mod table;

use bd_client_stats_store::{Counter, Histogram, Scope};
use bd_key_value::{Key, Store};
use bd_log_filter::ScrubRules;
use bd_log_primitives::{
  AnnotatedLogFields,
  DataValue,
  InternedFields,
  Log,
  LogFieldKey,
  LogFieldValue,
  LogFields,
  same_value,
};
use bd_runtime::runtime::field_interning::{
  Enabled,
  MinLogsPerChange,
  TableMaxBytes,
  TableRetentionAfterDisable,
  WindowLogs,
};
use bd_runtime::runtime::{BoolWatch, ConfigLoader, DurationWatch, IntWatch};
use bd_stats_common::{Counter as _, Histogram as _};
use bd_time::{OffsetDateTimeExt as _, TimeProvider};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
pub use table::Resolution;
use table::{FieldMap, VersionTable};

/// The directory, under the SDK directory, that holds the version table.
const TABLE_DIRECTORY: &str = "field_sets";

/// The end of the newest reserved block of version numbers. Numbers are never reused: each process
/// reserves a fresh block above this and above the table's newest version.
static RESERVED_VERSION_END: Key<String> = Key::new("field_interning.reserved_version_end");

/// When interning was turned off, in microseconds since the epoch.
static DISABLED_AT: Key<String> = Key::new("field_interning.disabled_at_micros");

const VERSION_BLOCK: u64 = 64;

/// The weight of the newest gap in the moving average of logs between a key's changes.
const CHANGE_RATE_ALPHA: f64 = 0.25;

//
// Stats
//

#[derive(Clone)]
struct Stats {
  versions_written: Counter,
  full_versions_written: Counter,
  write_failures: Counter,
  rotations: Counter,
  window_span_s: Histogram,
  table_bytes: Histogram,
  volatile_keys: Counter,
  tables_deleted: Counter,
  open_failures: Counter,
  logs_interned: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    Self {
      versions_written: scope.counter("versions_written"),
      full_versions_written: scope.counter("full_versions_written"),
      write_failures: scope.counter("write_failures"),
      rotations: scope.counter("rotations"),
      window_span_s: scope.histogram("window_span_s"),
      table_bytes: scope.histogram("table_bytes"),
      volatile_keys: scope.counter("volatile_keys"),
      tables_deleted: scope.counter("tables_deleted"),
      open_failures: scope.counter("open_failures"),
      logs_interned: scope.counter("logs_interned"),
    }
  }
}

#[derive(Clone)]
struct ResolveStats {
  /// Uploaded logs whose version was folded out of the table.
  logs_beyond_window: Counter,
  /// Uploaded logs whose version is not in the table, for example because the table was lost.
  logs_missing_version: Counter,
}

//
// FieldSetTable
//

/// A shared handle to the version table, used by uploaders to resolve the versions their logs refer
/// to.
#[derive(Clone)]
pub struct FieldSetTable {
  table: Arc<Mutex<Option<VersionTable>>>,
  stats: Option<ResolveStats>,
}

impl FieldSetTable {
  #[must_use]
  pub fn new(scope: &Scope) -> Self {
    Self {
      table: Arc::default(),
      stats: Some(ResolveStats {
        logs_beyond_window: scope.counter("logs_beyond_window"),
        logs_missing_version: scope.counter("logs_missing_version"),
      }),
    }
  }

  /// A handle with no table, for uploaders that never see interned logs.
  #[must_use]
  pub fn none() -> Self {
    Self {
      table: Arc::default(),
      stats: None,
    }
  }

  /// A handle to `table`, for tests that drive uploaders against a real table.
  #[doc(hidden)]
  #[must_use]
  pub fn for_test(table: VersionTable, scope: &Scope) -> Self {
    let handle = Self::new(scope);
    *handle.table.lock() = Some(table);
    handle
  }

  /// Runs `f` on the table, for tests that write or rotate it while an uploader holds the handle.
  #[doc(hidden)]
  pub fn with_table<R>(&self, f: impl FnOnce(&mut VersionTable) -> R) -> Option<R> {
    self.table.lock().as_mut().map(f)
  }

  /// Returns the full field set for `version`, or `None` if it cannot be resolved.
  #[must_use]
  pub fn resolve(&self, version: u64) -> Option<Arc<bd_proto::protos::client::api::FieldSet>> {
    match self.resolution(version) {
      Resolution::Found(set) => Some(set),
      Resolution::BeyondWindow | Resolution::Missing => None,
    }
  }

  /// Resolves `version`. A version that is not found uploads without its set, and the server marks
  /// the logs that refer to it.
  #[must_use]
  pub fn resolution(&self, version: u64) -> Resolution {
    self
      .table
      .lock()
      .as_mut()
      .map_or(Resolution::Missing, |table| table.resolve_one(version))
  }

  /// Counts `logs` uploaded logs whose version resolved as `resolution`, if it was not found.
  pub fn record_unresolved(&self, resolution: &Resolution, logs: u64) {
    let Some(stats) = &self.stats else {
      return;
    };
    match resolution {
      Resolution::Found(_) => {},
      Resolution::BeyondWindow => stats.logs_beyond_window.inc_by(logs),
      Resolution::Missing => stats.logs_missing_version.inc_by(logs),
    }
  }
}

//
// KeyRate
//

/// Tracks how often a key changes, in logs written between changes.
struct KeyRate {
  last_change: u64,
  average_gap: f64,
}

//
// FieldInterner
//

struct Flags {
  enabled: BoolWatch<Enabled>,
  table_max_bytes: IntWatch<TableMaxBytes>,
  window_logs: IntWatch<WindowLogs>,
  min_logs_per_change: IntWatch<MinLogsPerChange>,
  retention_after_disable: DurationWatch<TableRetentionAfterDisable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableState {
  Closed,
  Open,
  /// Opening failed; it is not retried in this process.
  Failed,
}

/// The current version and its full field set, restricted to the keys that are interned and with
/// scrub rules applied.
struct Current {
  version: u64,
  fields: Arc<LogFields>,
}

pub struct FieldInterner {
  directory: PathBuf,
  table: FieldSetTable,
  store: Arc<Store>,
  time_provider: Arc<dyn TimeProvider>,
  flags: Flags,
  stats: Stats,

  table_state: TableState,
  /// Whether new versions may be written; false if the number reservation could not be read.
  writable: bool,
  /// Whether interning was enabled at the last `prepare`, or `None` before the first.
  last_enabled: Option<bool>,
  /// When interning was turned off, in microseconds since the epoch, while it is off.
  disabled_at_micros: Option<u64>,

  current: Option<Current>,
  /// Keys changed since the last version, and whether each change counts toward the change rate.
  dirty: HashMap<LogFieldKey, bool>,
  /// Recompute every key, for example after a restart or a scrub-rule change.
  recompute_all: bool,
  /// The next version is written as a full set.
  needs_full: bool,
  scrub_rules: Option<Arc<ScrubRules>>,

  rates: HashMap<LogFieldKey, KeyRate>,
  volatile: HashSet<LogFieldKey>,
  logs_written: u64,
  /// After a failed table write, interning pauses until this many logs have been written, so a
  /// table that cannot hold the field set is not retried on every log.
  paused_until: u64,

  next_version: u64,
  reserved_end: u64,
}

impl FieldInterner {
  pub fn new(
    sdk_directory: &Path,
    runtime_loader: &ConfigLoader,
    store: Arc<Store>,
    time_provider: Arc<dyn TimeProvider>,
    scope: &Scope,
  ) -> Self {
    Self {
      directory: sdk_directory.join(TABLE_DIRECTORY),
      table: FieldSetTable::new(scope),
      store,
      time_provider,
      flags: Flags {
        enabled: runtime_loader.register_bool_watch(),
        table_max_bytes: runtime_loader.register_int_watch(),
        window_logs: runtime_loader.register_int_watch(),
        min_logs_per_change: runtime_loader.register_int_watch(),
        retention_after_disable: runtime_loader.register_duration_watch(),
      },
      stats: Stats::new(scope),
      table_state: TableState::Closed,
      writable: false,
      last_enabled: None,
      disabled_at_micros: None,
      current: None,
      dirty: HashMap::new(),
      recompute_all: true,
      needs_full: false,
      scrub_rules: None,
      rates: HashMap::new(),
      volatile: HashSet::new(),
      logs_written: 0,
      paused_until: 0,
      next_version: 1,
      reserved_end: 0,
    }
  }

  /// The handle uploaders use to resolve versions.
  #[must_use]
  pub fn table(&self) -> FieldSetTable {
    self.table.clone()
  }

  fn enabled(&self) -> bool {
    *self.flags.enabled.read()
  }

  /// Opens the table when interning is enabled or a table from an earlier run exists, and deletes
  /// it once interning has been off for the retention period. Called when the logger is built,
  /// before any uploader can read a buffered log from an earlier run, and before each log is
  /// written; it only does I/O when the runtime flag changes or the table is due for deletion.
  pub fn prepare(&mut self) {
    let enabled = self.enabled();
    if self.last_enabled == Some(enabled) {
      if !enabled && self.table_state == TableState::Open && self.retention_elapsed() {
        self.delete_table();
      }
      return;
    }
    self.last_enabled = Some(enabled);

    if enabled {
      // Collector updates are not tracked while interning is off.
      self.recompute_all = true;
      self.disabled_at_micros = None;
      if let Err(error) = self.store.set_internal_string(&DISABLED_AT, "") {
        log::debug!("failed to clear field interning disable time: {error}");
      }
      if self.table_state == TableState::Closed {
        self.open();
      }
      return;
    }

    self.current = None;
    self.disabled_at_micros = Some(self.load_disabled_at());
    let exists =
      self.table_state == TableState::Open || std::fs::exists(&self.directory).unwrap_or(false);
    if !exists {
      return;
    }
    if self.retention_elapsed() {
      self.delete_table();
    } else if self.table_state == TableState::Closed {
      // Interned logs from an earlier run may still be buffered.
      self.open();
    }
  }

  /// Returns when interning was turned off, recording now if it was not recorded before.
  fn load_disabled_at(&self) -> u64 {
    let now = self.now_micros();
    if let Some(disabled_at) = self
      .store
      .get_internal_string(&DISABLED_AT)
      .ok()
      .flatten()
      .and_then(|value| value.parse::<u64>().ok())
    {
      return disabled_at;
    }
    if let Err(error) = self
      .store
      .set_internal_string(&DISABLED_AT, &now.to_string())
    {
      log::debug!("failed to record field interning disable time: {error}");
    }
    now
  }

  fn now_micros(&self) -> u64 {
    self
      .time_provider
      .now()
      .unix_timestamp_micros()
      .cast_unsigned()
  }

  fn retention_elapsed(&self) -> bool {
    let Some(disabled_at) = self.disabled_at_micros else {
      return false;
    };
    let retention = self
      .flags
      .retention_after_disable
      .read()
      .whole_microseconds()
      .max(0)
      .cast_unsigned();
    u128::from(self.now_micros().saturating_sub(disabled_at)) >= retention
  }

  fn open(&mut self) {
    let max_bytes = usize::try_from(*self.flags.table_max_bytes.read()).unwrap_or(usize::MAX);
    match VersionTable::open(&self.directory, max_bytes, self.time_provider.clone()) {
      Ok((table, recovered)) => {
        // A fresh block above every number used before, even if the table lost its newest
        // entries. If the reservation cannot be read, numbers could be reused, so the table is
        // kept only to resolve buffered logs and nothing new is interned in this process.
        let persisted_end = match self.store.get_internal_string(&RESERVED_VERSION_END) {
          Ok(None) => Some(0),
          Ok(Some(end)) => end.parse::<u64>().ok(),
          Err(error) => {
            log::debug!("failed to read the field-set version reservation: {error}");
            None
          },
        };
        self.writable = persisted_end.is_some();
        if !self.writable {
          self.stats.open_failures.inc();
        }
        self.next_version = persisted_end.unwrap_or(0).max(recovered.newest_version) + 1;
        self.reserved_end = self.next_version - 1;
        self.current = (recovered.newest_version != 0).then(|| Current {
          version: recovered.newest_version,
          fields: Arc::new(to_log_fields(recovered.newest_fields)),
        });
        self.needs_full = recovered.reset;
        self.recompute_all = true;
        *self.table.table.lock() = Some(table);
        self.table_state = TableState::Open;
        log::debug!(
          "opened field-set table at version {}, next version {}",
          recovered.newest_version,
          self.next_version
        );
      },
      Err(error) => {
        log::debug!("failed to open field-set table, interning is off: {error}");
        self.stats.open_failures.inc();
        self.table_state = TableState::Failed;
      },
    }
  }

  /// Deletes the table after interning has been off for the retention period. Interned logs
  /// still buffered upload without their field sets.
  fn delete_table(&mut self) {
    *self.table.table.lock() = None;
    self.table_state = TableState::Closed;
    self.current = None;
    match std::fs::remove_dir_all(&self.directory) {
      Ok(()) => {
        log::debug!("deleted field-set table after interning was off for the retention period");
        self.stats.tables_deleted.inc();
      },
      Err(error) => log::debug!("failed to delete field-set table: {error}"),
    }
  }

  /// Records that a persistent field may have changed from `before` to `after`. The next interned
  /// log materializes one version for all changes since the previous one. Setting a key to its
  /// current value is not a change, so it does not count toward the change rate.
  pub fn record_update(
    &mut self,
    key: &LogFieldKey,
    before: Option<&LogFieldValue>,
    after: Option<&LogFieldValue>,
  ) {
    let unchanged = match (before, after) {
      (Some(before), Some(after)) => same_value(before, after),
      (None, None) => true,
      _ => false,
    };
    if !unchanged && self.table_state == TableState::Open && self.last_enabled == Some(true) {
      self.dirty.insert(key.clone(), true);
    }
    // A removed key's change rate is not kept, so dynamically named keys do not accumulate. A
    // volatile key keeps it until it is re-admitted.
    if after.is_none() && !self.volatile.contains(key) {
      self.rates.remove(key);
    }
  }

  /// Compares `log` with the current field set, first writing a version for any changes since the
  /// last one. Returns the fields to leave out, or `None` if the log should be written fully
  /// inline.
  pub fn intern(
    &mut self,
    log: &Log,
    persistent: &AnnotatedLogFields,
    scrub_rules: &Arc<ScrubRules>,
  ) -> Option<InternedFields> {
    if self.table_state != TableState::Open || !self.writable || self.last_enabled != Some(true) {
      return None;
    }

    if !self
      .scrub_rules
      .as_ref()
      .is_some_and(|previous| Arc::ptr_eq(previous, scrub_rules))
    {
      // New scrub rules may change the stored value of any key. These changes do not count toward
      // the change rate.
      self.scrub_rules = Some(scrub_rules.clone());
      self.recompute_all = true;
    }

    self.readmit_quiet_keys();

    if self.logs_written < self.paused_until {
      self.logs_written += 1;
      return None;
    }
    if let Err(error) = self.materialize(persistent, scrub_rules) {
      log::debug!("failed to write field-set version, writing logs inline for a while: {error}");
      self.logs_written += 1;
      self.paused_until = self.logs_written + u64::from(*self.flags.window_logs.read());
      return None;
    }
    self.logs_written += 1;

    let current = self.current.as_ref()?;
    let interned = InternedFields::new(&log.fields, current.version, current.fields.clone())?;
    self.stats.logs_interned.inc();
    Some(interned)
  }

  /// Makes volatile keys that have not changed for `W` logs eligible again.
  fn readmit_quiet_keys(&mut self) {
    if self.volatile.is_empty() {
      return;
    }
    let window = u64::from(*self.flags.window_logs.read());
    let logs_written = self.logs_written;
    let rates = &mut self.rates;
    let dirty = &mut self.dirty;
    self.volatile.retain(|key| {
      let Some(rate) = rates.get_mut(key) else {
        return false;
      };
      if logs_written.saturating_sub(rate.last_change) < window {
        return true;
      }
      #[allow(clippy::cast_precision_loss)]
      let window = window as f64;
      rate.average_gap = window;
      dirty.entry(key.clone()).or_insert(false);
      false
    });
  }

  /// Updates the change rate of a key that changed, returning whether it is volatile.
  fn record_change(&mut self, key: &LogFieldKey) -> bool {
    #[allow(clippy::cast_precision_loss)]
    let window = f64::from(*self.flags.window_logs.read());
    let min_logs_per_change = f64::from(*self.flags.min_logs_per_change.read());
    let logs_written = self.logs_written;
    let rate = self.rates.entry(key.clone()).or_insert(KeyRate {
      last_change: 0,
      average_gap: window,
    });
    #[allow(clippy::cast_precision_loss)]
    let gap = logs_written.saturating_sub(rate.last_change) as f64;
    rate.average_gap = CHANGE_RATE_ALPHA.mul_add(gap, (1.0 - CHANGE_RATE_ALPHA) * rate.average_gap);
    rate.last_change = logs_written;
    if rate.average_gap < min_logs_per_change && self.volatile.insert(key.clone()) {
      log::debug!("persistent field {key:?} changes too often; keeping it inline");
      self.stats.volatile_keys.inc();
    }
    self.volatile.contains(key)
  }

  fn materialize(
    &mut self,
    persistent: &AnnotatedLogFields,
    scrub_rules: &ScrubRules,
  ) -> anyhow::Result<()> {
    if !self.recompute_all && self.dirty.is_empty() {
      return Ok(());
    }

    if !self.recompute_all && self.current.is_some() {
      // A change to a key that stays inline and is not in the set writes no version, so skip
      // copying the set for it. This keeps a key that changes on every log cheap.
      let changed: Vec<(LogFieldKey, bool)> = self.dirty.drain().collect();
      for (key, counts) in &changed {
        if *counts && persistent.contains_key(key) {
          self.record_change(key);
        }
      }
      let in_set = |key: &LogFieldKey| {
        self
          .current
          .as_ref()
          .is_some_and(|current| current.fields.contains_key(key))
      };
      if changed
        .iter()
        .all(|(key, _)| self.volatile.contains(key) && !in_set(key))
      {
        return Ok(());
      }
      // The change rate is recorded; recompute these keys without counting them again.
      self
        .dirty
        .extend(changed.into_iter().map(|(key, _)| (key, false)));
    }

    let previous = self
      .current
      .as_ref()
      .map(|current| current.fields.clone())
      .unwrap_or_default();
    let changed: Vec<(LogFieldKey, bool)> = if self.recompute_all {
      persistent
        .keys()
        .chain(previous.keys())
        .map(|key| (key.clone(), false))
        .chain(self.dirty.drain())
        .collect()
    } else {
      self.dirty.drain().collect()
    };
    self.recompute_all = false;

    let mut next = (*previous).clone();
    for (key, counts) in changed {
      // An empty key stays inline: the server does not restore a set field without a key.
      if key.is_empty() {
        next.remove(&key);
        continue;
      }
      if !persistent.contains_key(&key) && !self.volatile.contains(&key) {
        // A removed key's change rate is not kept, so dynamically named keys do not accumulate.
        self.rates.remove(&key);
      }
      if counts && persistent.contains_key(&key) && self.record_change(&key) {
        next.remove(&key);
        continue;
      }
      if self.volatile.contains(&key) {
        next.remove(&key);
        continue;
      }
      let value = persistent.get(&key).and_then(|field| {
        let mut value = field.value.clone();
        scrub_rules.scrub_field(&key, &mut value).then_some(value)
      });
      match value {
        Some(value) => {
          next.insert(key, value);
        },
        None => {
          next.remove(&key);
        },
      }
    }

    if self.current.is_some() && same_fields(&next, &previous) {
      return Ok(());
    }
    self.write_version(next, &previous)
  }

  fn write_version(&mut self, next: LogFields, previous: &LogFields) -> anyhow::Result<()> {
    let full = self.needs_full || self.current.is_none();
    let set: FieldMap = next
      .iter()
      .filter(|(key, value)| {
        full
          || !previous
            .get(*key)
            .is_some_and(|previous| same_value(previous, value))
      })
      .map(|(key, value)| (key.to_string(), value.clone().into_proto()))
      .collect();
    let removed: Vec<String> = if full {
      Vec::new()
    } else {
      previous
        .keys()
        .filter(|key| !next.contains_key(*key))
        .map(ToString::to_string)
        .collect()
    };

    let max_bytes = usize::try_from(*self.flags.table_max_bytes.read()).unwrap_or(usize::MAX);
    let result = self.allocate_version().and_then(|version| {
      let mut table = self.table.table.lock();
      let table = table
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("field-set table is closed"))?;
      table.set_max_bytes(max_bytes);
      let rotation = table.write(version, full, &set, &removed)?;
      #[allow(clippy::cast_precision_loss)]
      self.stats.table_bytes.observe(table.size_bytes() as f64);
      Ok((version, rotation))
    });

    match result {
      Ok((version, rotation)) => {
        self.stats.versions_written.inc();
        if full {
          self.stats.full_versions_written.inc();
        }
        if let Some(rotation) = rotation {
          self.stats.rotations.inc();
          #[allow(clippy::cast_precision_loss)]
          self
            .stats
            .window_span_s
            .observe(rotation.window_span_micros as f64 / 1_000_000.0);
        }
        log::debug!(
          "wrote field-set version {version} ({} fields set, {} removed, full: {full})",
          set.len(),
          removed.len()
        );
        self.needs_full = false;
        self.current = Some(Current {
          version,
          fields: Arc::new(next),
        });
        Ok(())
      },
      Err(error) => {
        // The next version is written in full so the chain of changes is never broken, and every
        // key is recomputed for it.
        self.stats.write_failures.inc();
        self.needs_full = true;
        self.recompute_all = true;
        self.current = None;
        Err(error)
      },
    }
  }

  fn allocate_version(&mut self) -> anyhow::Result<u64> {
    if self.next_version > self.reserved_end {
      let end = self.next_version + VERSION_BLOCK - 1;
      self
        .store
        .set_internal_string(&RESERVED_VERSION_END, &end.to_string())?;
      self.reserved_end = end;
    }
    let version = self.next_version;
    self.next_version += 1;
    Ok(version)
  }
}

fn same_fields(a: &LogFields, b: &LogFields) -> bool {
  a.len() == b.len()
    && a
      .iter()
      .all(|(key, value)| b.get(key).is_some_and(|other| same_value(value, other)))
}

fn to_log_fields(fields: FieldMap) -> LogFields {
  fields
    .into_iter()
    .filter_map(|(key, value)| Some((key.into(), DataValue::from_proto(value)?)))
    .collect()
}
