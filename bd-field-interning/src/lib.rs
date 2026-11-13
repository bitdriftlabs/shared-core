// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

//! Persistent field interning.
//!
//! Persistent fields (`add_log_field`, `update_ootb_log_field` and the initial fields) are stored
//! once per change as a field-set version in an on-device [`table::VersionTable`]. A log written to
//! a ring buffer leaves out each field of the current version that it carries unchanged and
//! records the version instead. Uploaders add the full field set of every version their logs refer
//! to, and the server restores the fields at ingest.
//!
//! # How it works
//!
//! * **Versions.** An update to a persistent field marks its key dirty. The next log written to a
//!   ring buffer first writes one version for every dirty key, with scrub rules applied. The log
//!   then leaves out each field whose value equals the current version's. A key of the version that
//!   the log does not carry, because a filter removed it, is written as a `cleared` field so the
//!   server does not restore it.
//! * **Version numbers.** A number is never reused, so a log buffered by an earlier process always
//!   resolves to the set it was written with. Each process reserves blocks of numbers in the
//!   key-value store, above every number used before. If the reservation cannot be read, the table
//!   only resolves buffered logs and nothing new is interned.
//! * **Keys that change often.** A key that changes again within `min_logs_per_change` logs of its
//!   previous change stays inline, because a version per change would cost more than carrying it in
//!   each log. After `window_logs` logs without a change it is interned again.
//! * **Dropping old versions.** The table does not track which versions buffered logs still refer
//!   to, because ring buffers drop logs without telling anyone. Instead its size is capped by
//!   `field_interning.table_max_bytes`. It grows as needed, and at the cap the oldest versions are
//!   folded into its base. A version therefore outlives its logs as long as the buffers turn over
//!   faster than the table. A log whose version was folded away uploads without its field set, the
//!   server marks it as missing its persistent fields, and `logs_beyond_window` counts it so the
//!   cap can be sized during the rollout.
//! * **Restarts.** The table and its version numbers survive a restart, so logs buffered by an
//!   earlier process still resolve. If the table is unreadable or lost entries, it starts empty and
//!   those logs upload without their field sets. A trigger upload staged before a restart resolves
//!   its versions again when it resumes, so the same loss applies if the table was lost or folded
//!   past them in between.
//! * **Turning it off.** When `field_interning.enabled` is off, nothing new is interned and the
//!   table is deleted as soon as the interner sees the flag off. Interned logs still in the buffers
//!   then upload without their field sets, and the server marks them as missing their persistent
//!   fields. The SDK cannot cheaply tell when the last interned log has left every buffer, and
//!   turning interning off is rare, so that loss is accepted instead of keeping the table for a
//!   retention period.
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

use bd_client_common::file_system::remove_dir_if_exists_async;
use bd_client_stats_store::{Counter, Scope};
use bd_key_value::{Key, Store};
use bd_log_filter::ScrubRules;
use bd_log_primitives::{
  AnnotatedLogFields,
  DataValue,
  InternedFields,
  Log,
  LogFieldKey,
  LogFields,
  same_value,
};
use bd_runtime::runtime::field_interning::{Enabled, MinLogsPerChange, TableMaxBytes, WindowLogs};
use bd_runtime::runtime::{BoolWatch, ConfigLoader, IntWatch};
use bd_stats_common::Counter as _;
use bd_time::TimeProvider;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
pub use table::Resolution;
use table::{FieldMap, VersionTable, Written};

/// The directory, under the SDK directory, that holds the version table.
const TABLE_DIRECTORY: &str = "field_sets";

/// The end of the newest reserved block of version numbers. Numbers are never reused: each process
/// reserves a fresh block above this and above the table's newest version.
static RESERVED_VERSION_END: Key<String> = Key::new("field_interning.reserved_version_end");

const VERSION_BLOCK: u64 = 64;

//
// Stats
//

// The server counts interned logs and logs whose set is missing, so the device counts only what
// the server cannot see: `failures` on the interner and `logs_beyond_window` on the table handle.
// Both serve the rollout and are to be removed once the table's cap is settled.

#[derive(Clone)]
struct ResolveStats {
  /// Uploaded logs whose version was folded out of the table: the table's cap is too small.
  logs_beyond_window: Counter,
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

  /// Runs `f` on the table if it is open. Public for tests that inspect it while an uploader holds
  /// the handle.
  #[doc(hidden)]
  pub fn with_table<R>(&self, f: impl FnOnce(&mut VersionTable) -> R) -> Option<R> {
    self.table.lock().as_mut().map(f)
  }

  /// Runs `f` on the open table.
  fn lock_table<R>(
    &self,
    f: impl FnOnce(&mut VersionTable) -> anyhow::Result<R>,
  ) -> anyhow::Result<R> {
    let mut table = self.table.lock();
    f(table
      .as_mut()
      .ok_or_else(|| anyhow::anyhow!("field-set table is closed"))?)
  }

  /// Appends a version, rotating the table when it passes its high-water mark or is full.
  ///
  /// A rotation's file I/O runs without the table's lock: the new journal is built and synced on a
  /// blocking thread while uploaders keep resolving from the old one, then swapped in under the
  /// lock and renamed over the table's file. The interner is the only writer and waits for the
  /// rotation, so the table does not change while it is built. Public for tests that write the
  /// table while an uploader holds the handle.
  #[doc(hidden)]
  pub async fn write(
    &self,
    version: u64,
    full: bool,
    set: &FieldMap,
    removed: &[String],
  ) -> anyhow::Result<()> {
    let written = self.lock_table(|table| table.write(version, full, set, removed))?;
    match written {
      Written::Done { rotate: false } => Ok(()),
      Written::Done { rotate: true } => {
        // The version is written. A failed rotation is retried when the table is full rather than
        // on every write.
        if let Err(error) = self.rotate(0).await {
          log::debug!("failed to rotate field-set table: {error}");
        }
        Ok(())
      },
      Written::Full { bytes } => {
        self.rotate(bytes).await?;
        match self.lock_table(|table| table.write(version, full, set, removed))? {
          Written::Done { .. } => Ok(()),
          Written::Full { .. } => anyhow::bail!("field-set table full after rotation"),
        }
      },
    }
  }

  async fn rotate(&self, additional: usize) -> anyhow::Result<()> {
    let installed = async {
      let rotation = self.lock_table(|table| table.plan_rotation(additional))?;
      let rotated = tokio::task::spawn_blocking(move || rotation.build()).await??;
      self.lock_table(|table| table.install(rotated))
    }
    .await;
    let rename = match installed {
      Ok(rename) => rename,
      Err(error) => {
        self.with_table(VersionTable::rotation_failed);
        return Err(error);
      },
    };
    if let Err(error) = tokio::fs::rename(&rename.from, &rename.to).await {
      self.with_table(VersionTable::rename_failed);
      return Err(error.into());
    }
    Ok(())
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

  /// Counts `logs` uploaded logs whose version resolved as `resolution`, if it was folded out of
  /// the table.
  pub fn record_unresolved(&self, resolution: &Resolution, logs: u64) {
    if let (Some(stats), Resolution::BeyondWindow) = (&self.stats, resolution) {
      stats.logs_beyond_window.inc_by(logs);
    }
  }
}

//
// FieldInterner
//

struct Flags {
  enabled: BoolWatch<Enabled>,
  table_max_bytes: IntWatch<TableMaxBytes>,
  window_logs: IntWatch<WindowLogs>,
  min_logs_per_change: IntWatch<MinLogsPerChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableState {
  Closed,
  /// Open to resolve buffered logs. New versions are written only if `writable`: when the number
  /// reservation cannot be read, numbers could be reused, so nothing new is interned.
  Open {
    writable: bool,
  },
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
  /// Table opens, writes and reservation reads that failed, and tables discarded at open because
  /// they were unreadable or had lost entries. Each one stops or limits interning, which the
  /// server cannot see.
  failures: Counter,

  table_state: TableState,
  /// Whether interning was enabled at the last `prepare`, or `None` before the first.
  last_enabled: Option<bool>,

  current: Option<Current>,
  /// Keys changed since the last version, and whether each change counts toward the change rate.
  dirty: HashMap<LogFieldKey, bool>,
  /// Recompute every key, for example after a restart or a scrub-rule change.
  recompute_all: bool,
  scrub_rules: Option<Arc<ScrubRules>>,

  /// For each key, the log count at its last change that counts toward its change rate.
  last_changes: HashMap<LogFieldKey, u64>,
  volatile: HashSet<LogFieldKey>,
  /// The earliest log count at which a volatile key can be re-admitted.
  next_readmission: u64,
  logs_written: u64,

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
      },
      failures: scope.counter("failures"),
      table_state: TableState::Closed,
      last_enabled: None,
      current: None,
      dirty: HashMap::new(),
      recompute_all: true,
      scrub_rules: None,
      last_changes: HashMap::new(),
      volatile: HashSet::new(),
      next_readmission: u64::MAX,
      logs_written: 0,
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

  const fn table_open(&self) -> bool {
    matches!(self.table_state, TableState::Open { .. })
  }

  /// Whether logs are interned and persistent-field changes tracked: interning is enabled and the
  /// table can take new versions.
  fn interning(&self) -> bool {
    self.last_enabled == Some(true) && self.table_state == TableState::Open { writable: true }
  }

  /// Opens the table when interning is enabled, and deletes it when interning is off. Called when
  /// the logger is built, before any uploader can read a buffered log from an earlier run, and
  /// before each log is written; it only does I/O when the runtime flag changes.
  pub async fn prepare(&mut self) {
    let enabled = self.enabled();
    if self.last_enabled == Some(enabled) {
      return;
    }
    self.last_enabled = Some(enabled);

    if enabled {
      // Collector updates are not tracked while interning is off.
      self.recompute_all = true;
      if self.table_state == TableState::Closed {
        self.open().await;
      }
      return;
    }

    // Changes are not tracked while interning is off; enabling it again recomputes every key.
    self.current = None;
    self.dirty.clear();
    if self.table_open()
      || tokio::fs::try_exists(&self.directory)
        .await
        .unwrap_or(false)
    {
      self.delete_table().await;
    }
  }

  async fn open(&mut self) {
    let max_bytes = usize::try_from(*self.flags.table_max_bytes.read()).unwrap_or(usize::MAX);
    match VersionTable::open(
      self.directory.clone(),
      max_bytes,
      self.time_provider.clone(),
    )
    .await
    {
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
        let writable = persisted_end.is_some();
        if !writable {
          self.failures.inc();
        }
        self.next_version = persisted_end.unwrap_or(0).max(recovered.newest_version) + 1;
        self.reserved_end = self.next_version - 1;
        self.current = (recovered.newest_version != 0).then(|| Current {
          version: recovered.newest_version,
          fields: Arc::new(to_log_fields(recovered.newest_fields)),
        });
        if recovered.discarded {
          // Buffered logs that refer to its versions upload without their field sets.
          log::debug!("discarded an unreadable field-set table");
          self.failures.inc();
        }
        self.recompute_all = true;
        *self.table.table.lock() = Some(table);
        self.table_state = TableState::Open { writable };
        log::debug!(
          "opened field-set table at version {}, next version {}",
          recovered.newest_version,
          self.next_version
        );
      },
      Err(error) => {
        log::debug!("failed to open field-set table, interning is off: {error}");
        self.failures.inc();
        self.table_state = TableState::Failed;
      },
    }
  }

  /// Deletes the table when interning is turned off. Interned logs still buffered upload without
  /// their field sets.
  async fn delete_table(&mut self) {
    *self.table.table.lock() = None;
    self.table_state = TableState::Closed;
    self.current = None;
    match remove_dir_if_exists_async(&self.directory).await {
      Ok(()) => log::debug!("deleted field-set table because interning is off"),
      Err(error) => log::debug!("failed to delete field-set table: {error}"),
    }
  }

  /// Records an update of a persistent field: whether its value changed, and whether the key is
  /// still present. The next interned log materializes one version for all changes since the
  /// previous one.
  pub fn record_update(&mut self, key: &LogFieldKey, changed: bool, present: bool) {
    if changed && self.interning() {
      self.dirty.insert(key.clone(), true);
    }
    // A removed key's last change is not kept, so dynamically named keys do not accumulate. A
    // volatile key keeps it until it is re-admitted.
    if !present && !self.volatile.contains(key) {
      self.last_changes.remove(key);
    }
  }

  /// Compares `log` with the current field set, first writing a version for any changes since the
  /// last one. Returns the fields to leave out, or `None` if the log should be written fully
  /// inline.
  pub async fn intern(
    &mut self,
    log: &Log,
    persistent: &AnnotatedLogFields,
    scrub_rules: &Arc<ScrubRules>,
  ) -> Option<InternedFields> {
    if !self.interning() {
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

    self.logs_written += 1;
    if let Err(error) = self.materialize(persistent, scrub_rules).await {
      // Interning stops for the rest of the process, for example when the table cannot hold the
      // field set; the table stays open to resolve logs already buffered. This costs savings only.
      log::debug!("failed to write field-set version, interning stops for this process: {error}");
      self.failures.inc();
      self.table_state = TableState::Open { writable: false };
      self.current = None;
      return None;
    }

    let current = self.current.as_ref()?;
    InternedFields::new(&log.fields, current.version, current.fields.clone())
  }

  /// Makes volatile keys that have not changed for `W` logs eligible again.
  fn readmit_quiet_keys(&mut self) {
    if self.logs_written < self.next_readmission {
      return;
    }
    let window = u64::from(*self.flags.window_logs.read());
    let logs_written = self.logs_written;
    let last_changes = &self.last_changes;
    let dirty = &mut self.dirty;
    let mut next_readmission = u64::MAX;
    self.volatile.retain(|key| {
      let Some(&last_change) = last_changes.get(key) else {
        return false;
      };
      if logs_written.saturating_sub(last_change) < window {
        next_readmission = next_readmission.min(last_change.saturating_add(window));
        return true;
      }
      dirty.entry(key.clone()).or_insert(false);
      false
    });
    self.next_readmission = next_readmission;
  }

  /// Records a change of a key, returning whether it is volatile: it changed again within
  /// `min_logs_per_change` logs, or it is still in its cooldown of `window_logs` logs without a
  /// change.
  fn record_change(&mut self, key: &LogFieldKey) -> bool {
    let logs_written = self.logs_written;
    let min_logs_per_change = u64::from(*self.flags.min_logs_per_change.read());
    let previous = self.last_changes.insert(key.clone(), logs_written);
    if previous.is_some_and(|previous| logs_written.saturating_sub(previous) < min_logs_per_change)
      && self.volatile.insert(key.clone())
    {
      log::debug!("persistent field {key:?} changes too often; keeping it inline");
    }
    let volatile = self.volatile.contains(key);
    if volatile {
      // Each change of a volatile key restarts its cooldown.
      let window = u64::from(*self.flags.window_logs.read());
      self.next_readmission = self
        .next_readmission
        .min(logs_written.saturating_add(window));
    }
    volatile
  }

  async fn materialize(
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
      .map_or_default(|current| current.fields.clone());
    let changed: Vec<(LogFieldKey, bool)> = if self.recompute_all {
      persistent
        .keys()
        .chain(previous.keys())
        .map(|key| (key.clone(), false))
        // A recompute counts no change: writes before the first log, or while interning was off,
        // are not churn.
        .chain(self.dirty.drain().map(|(key, _)| (key, false)))
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
        // A removed key's last change is not kept, so dynamically named keys do not accumulate.
        self.last_changes.remove(&key);
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
    self.write_version(next, &previous).await
  }

  async fn write_version(&mut self, next: LogFields, previous: &LogFields) -> anyhow::Result<()> {
    let full = self.current.is_none();
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
    let version = self.allocate_version()?;
    self
      .table
      .with_table(|table| table.set_max_bytes(max_bytes));
    self.table.write(version, full, &set, &removed).await?;
    log::debug!(
      "wrote field-set version {version} ({} fields set, {} removed, full: {full})",
      set.len(),
      removed.len()
    );
    self.current = Some(Current {
      version,
      fields: Arc::new(next),
    });
    Ok(())
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
