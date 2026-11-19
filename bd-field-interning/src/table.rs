// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The on-device table of persistent field-set versions.
//!
//! The table is an append-only journal of `FieldSet` entries keyed by kind:
//!
//! * `base`: the full field set at the oldest retained version.
//! * `f/<version>`: a full field set, written for a new table and when interning is turned back on.
//! * `v/<version>`: the fields set (with values) and removed (as `cleared` fields) since the
//!   previous version.
//!
//! A version's full set is found by replaying the entries in order up to it. When the journal
//! reaches its high-water mark it is rewritten into a new file, which replaces the table's file by
//! an atomic rename. A new table's file is small: until it reaches its cap, the rewrite only grows
//! it, a power of two at a time, as the state store does. At the cap, the oldest changes are
//! folded into `base` until the rest fit in half of it. The table never leaves the device.
//!
//! A rotation has three steps so that its file I/O never holds the table's lock: it is planned
//! under the lock ([`VersionTable::plan_rotation`]), built and synced on a blocking thread
//! ([`Rotation::build`]), and swapped in under the lock ([`VersionTable::install`]) before the
//! rename.

#[cfg(test)]
#[path = "./table_test.rs"]
mod table_test;

use bd_proto::protos::client::api::FieldSet;
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::log::Field;
use bd_proto::protos::logging::payload::log::field::Value_type;
use bd_time::TimeProvider;
use bd_versioned_kv::Scope;
use bd_versioned_kv::versioned_kv_journal::journal::PartialDataLoss;
use bd_versioned_kv::versioned_kv_journal::memmapped_journal::MemMappedVersionedJournal;
use bd_versioned_kv::versioned_kv_journal::{HEADER_SIZE, UpdateError};
use protobuf::Message as _;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const JOURNAL_NAME: &str = "field_sets";
const HIGH_WATER_MARK_RATIO: f32 = 0.8;
// The field-set table is a separate file, so its frames reuse an existing scope byte.
const SCOPE: Scope = Scope::System;
const BASE_KEY: &str = "base";
// Framing overhead per entry beyond the payload and key: scope, timestamp, lengths and checksum.
const FRAME_OVERHEAD_BYTES: usize = 32;

/// The smallest and largest table sizes accepted from runtime.
pub const MIN_TABLE_BYTES: usize = 4 * 1024;
/// The size of a new table's file. Rotation grows it, a power of two at a time, up to the cap.
pub const INITIAL_TABLE_BYTES: usize = 16 * 1024;
pub const MAX_TABLE_BYTES: usize = 4 * 1024 * 1024;

/// A full field set by key, holding proto values so resolution never converts them.
pub type FieldMap = BTreeMap<String, Data>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
  Base,
  Full,
  Change,
}

impl EntryKind {
  fn key(self, version: u64) -> String {
    match self {
      Self::Base => BASE_KEY.to_string(),
      Self::Full => format!("f/{version}"),
      Self::Change => format!("v/{version}"),
    }
  }

  fn parse(key: &str) -> Option<(Self, Option<u64>)> {
    if key == BASE_KEY {
      return Some((Self::Base, None));
    }
    let (kind, version) = key.split_once('/')?;
    let version = version.parse().ok()?;
    match kind {
      "f" => Some((Self::Full, Some(version))),
      "v" => Some((Self::Change, Some(version))),
      _ => None,
    }
  }
}

/// Applies an entry to a running field set.
fn apply(state: &mut FieldMap, kind: EntryKind, entry: &FieldSet) {
  if kind != EntryKind::Change {
    state.clear();
  }
  for field in &entry.fields {
    match &field.value_type {
      Some(Value_type::Cleared(true)) => {
        state.remove(&field.key);
      },
      Some(Value_type::Value(value)) => {
        state.insert(field.key.clone(), value.clone());
      },
      Some(Value_type::Cleared(false)) | None => {},
    }
  }
}

/// The encoded size of a full set's fields, without building the set.
fn fields_size(fields: &FieldMap) -> usize {
  fields
    .iter()
    .map(|(key, value)| {
      let size = protobuf::rt::string_size(1, key) + {
        let value_size = value.compute_size();
        1 + protobuf::rt::compute_raw_varint64_size(value_size) + value_size
      };
      usize::try_from(1 + protobuf::rt::compute_raw_varint64_size(size) + size)
        .unwrap_or(usize::MAX)
    })
    .fold(0, usize::saturating_add)
}

/// Encodes a full field set, ordered by key.
#[must_use]
pub fn field_set(version: u64, fields: &FieldMap) -> FieldSet {
  FieldSet {
    version,
    fields: fields
      .iter()
      .map(|(key, value)| Field {
        key: key.clone(),
        value_type: Some(Value_type::Value(value.clone())),
        ..Default::default()
      })
      .collect(),
    ..Default::default()
  }
}

/// What the table knows about a version requested by an uploader.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
  Found(Arc<FieldSet>),
  /// Older than the table's `base`: folded away by a rotation.
  BeyondWindow,
  /// Not in the table, for example because the table or its newest entries were lost.
  Missing,
}

/// The outcome of [`VersionTable::write`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Written {
  /// The version is written. `rotate` is set when the journal passed its high-water mark.
  Done { rotate: bool },
  /// The journal is full and nothing was written: rotate with room for `bytes`, then write again.
  Full { bytes: usize },
}

/// A rotation planned under the table's lock, to be built without it.
pub struct Rotation {
  directory: PathBuf,
  size: usize,
  entries: Vec<(Scope, String, FieldSet, u64)>,
  /// The new `base` version, if the rotation folds versions away.
  base_version: Option<u64>,
  newest_version: u64,
  time_provider: Arc<dyn TimeProvider>,
}

impl Rotation {
  /// Writes the new journal next to the table's file and syncs it, so it is on disk before it
  /// replaces the old one. This blocks on file I/O: async callers run it on a blocking thread.
  pub fn build(self) -> anyhow::Result<Rotated> {
    let path = rotation_path(&self.directory);
    let journal = MemMappedVersionedJournal::new(
      &path,
      self.size,
      HIGH_WATER_MARK_RATIO,
      self.time_provider,
      self.entries,
    )
    .and_then(|journal| MemMappedVersionedJournal::sync(&journal).map(|()| journal))
    .inspect_err(|_| remove_file(&path))?;
    Ok(Rotated {
      journal,
      base_version: self.base_version,
      newest_version: self.newest_version,
    })
  }
}

/// A rotated journal, ready for [`VersionTable::install`].
pub struct Rotated {
  journal: MemMappedVersionedJournal<FieldSet>,
  base_version: Option<u64>,
  newest_version: u64,
}

/// The rename that replaces the table's file with an installed rotation. The rename is atomic, so
/// a crash leaves one complete journal.
pub struct Rename {
  pub from: PathBuf,
  pub to: PathBuf,
}

/// Recovered state of an opened table.
pub struct Recovered {
  /// The newest version in the table, or 0 for an empty table.
  pub newest_version: u64,
  /// The full field set at the newest version.
  pub newest_fields: FieldMap,
  /// Whether an existing table was discarded because it was unreadable or had lost entries.
  pub discarded: bool,
}

pub struct VersionTable {
  directory: PathBuf,
  journal: MemMappedVersionedJournal<FieldSet>,
  max_bytes: usize,
  time_provider: Arc<dyn TimeProvider>,
  /// The version of the `base` entry, or 0 if no rotation has folded versions into one. Older
  /// versions are beyond the table's window.
  base_version: u64,
  newest_version: u64,
  /// Set when a rotation could not bring the table below its high-water mark, so later writes do
  /// not rotate again until the table is actually full.
  rotation_ineffective: bool,
  /// Set when an installed rotation could not be renamed into place: the table only resolves.
  read_only: bool,
  /// Rotations started, for tests that check when the table rotates.
  #[cfg(test)]
  rotations_started: usize,
  /// Where recent resolutions stopped, most recently used last. Each uploader reads versions in
  /// order, so it walks the table forward from its own cursor instead of replaying it, and a
  /// repeated version is answered from the cursor.
  cursors: Vec<Cursor>,
}

/// The running field set after the entry ending at `offset`.
struct Cursor {
  offset: usize,
  version: u64,
  fields: FieldMap,
  /// The encoded set at `version`, once resolved.
  set: Option<Arc<FieldSet>>,
}

const CURSORS: usize = 4;

/// The table's journal file.
fn journal_path(directory: &Path) -> PathBuf {
  directory.join(format!("{JOURNAL_NAME}.jrn"))
}

/// Where a rotation writes the new journal before renaming it over the table's file, so a crash
/// leaves either the old journal or the new one.
fn rotation_path(directory: &Path) -> PathBuf {
  directory.join(format!("{JOURNAL_NAME}.jrn.tmp"))
}

fn remove_file(path: &Path) {
  match std::fs::remove_file(path) {
    Ok(()) => {},
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
    Err(error) => log::debug!("failed to remove {}: {error}", path.display()),
  }
}

impl VersionTable {
  /// Opens the table in `directory`, or creates an empty one.
  ///
  /// An intact journal is reopened. If it is unreadable or lost entries (for example on power
  /// loss), it is deleted and the table starts empty. This is an accepted loss: logs still buffered
  /// that refer to the discarded versions upload without their field sets, and the server marks
  /// them as missing their persistent fields. [`Recovered::discarded`] reports it so the loss is
  /// counted. The file I/O runs on a blocking thread.
  pub async fn open(
    directory: PathBuf,
    max_bytes: usize,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, Recovered)> {
    tokio::task::spawn_blocking(move || Self::open_blocking(&directory, max_bytes, time_provider))
      .await?
  }

  fn open_blocking(
    directory: &Path,
    max_bytes: usize,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, Recovered)> {
    let max_bytes = max_bytes.clamp(MIN_TABLE_BYTES, MAX_TABLE_BYTES);
    std::fs::create_dir_all(directory)?;
    // Left only by a rotation interrupted before its rename.
    remove_file(&rotation_path(directory));

    let path = journal_path(directory);
    let exists = std::fs::exists(&path).unwrap_or(false);
    if exists {
      match Self::recover(&path, time_provider.clone()) {
        Ok((journal, recovered, base_version)) => {
          let table = Self {
            directory: directory.to_path_buf(),
            journal,
            max_bytes,
            time_provider,
            base_version,
            newest_version: recovered.newest_version,
            rotation_ineffective: false,
            read_only: false,
            #[cfg(test)]
            rotations_started: 0,
            cursors: Vec::new(),
          };
          return Ok((table, recovered));
        },
        Err(error) => log::debug!(
          "field-set table at {} is unusable, starting empty: {error}",
          path.display()
        ),
      }
      remove_file(&path);
    }

    Self::new_empty(directory, max_bytes, time_provider, exists)
  }

  fn new_empty(
    directory: &Path,
    max_bytes: usize,
    time_provider: Arc<dyn TimeProvider>,
    discarded: bool,
  ) -> anyhow::Result<(Self, Recovered)> {
    let journal = MemMappedVersionedJournal::new(
      journal_path(directory),
      INITIAL_TABLE_BYTES.min(max_bytes),
      HIGH_WATER_MARK_RATIO,
      time_provider.clone(),
      std::iter::empty(),
    )?;
    Ok((
      Self {
        directory: directory.to_path_buf(),
        journal,
        max_bytes,
        time_provider,
        base_version: 0,
        newest_version: 0,
        rotation_ineffective: false,
        read_only: false,
        #[cfg(test)]
        rotations_started: 0,
        cursors: Vec::new(),
      },
      Recovered {
        newest_version: 0,
        newest_fields: FieldMap::new(),
        discarded,
      },
    ))
  }

  /// Replays a journal file, checking that each key matches its entry and that versions increase.
  /// A journal that lost entries is an error: its chain of changes is broken.
  fn recover(
    path: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(MemMappedVersionedJournal<FieldSet>, Recovered, u64)> {
    let mut state = FieldMap::new();
    let mut newest_version = 0;
    let mut base_version = 0;
    let mut error = None;
    let mut first = true;
    let (journal, partial_data_loss) = MemMappedVersionedJournal::<FieldSet>::from_file(
      path,
      HIGH_WATER_MARK_RATIO,
      time_provider,
      |_scope, key, entry, _timestamp| {
        if error.is_some() {
          return;
        }
        match EntryKind::parse(key) {
          Some((kind, key_version))
            if key_version.is_none_or(|version| version == entry.version)
              && entry.version > newest_version
              && (kind != EntryKind::Base || first) =>
          {
            apply(&mut state, kind, entry);
            if kind == EntryKind::Base {
              base_version = entry.version;
            }
            newest_version = entry.version;
            first = false;
          },
          _ => error = Some(anyhow::anyhow!("unexpected field-set entry {key:?}")),
        }
      },
    )?;
    if let Some(error) = error {
      return Err(error);
    }
    anyhow::ensure!(
      !matches!(partial_data_loss, PartialDataLoss::Yes),
      "lost entries"
    );
    Ok((
      journal,
      Recovered {
        newest_version,
        newest_fields: state,
        discarded: false,
      },
      base_version,
    ))
  }

  #[cfg(test)]
  #[must_use]
  pub const fn newest_version(&self) -> u64 {
    self.newest_version
  }

  #[must_use]
  pub const fn is_empty(&self) -> bool {
    self.newest_version == 0
  }

  /// The number of bytes the journal holds.
  #[cfg(test)]
  #[must_use]
  pub fn size_bytes(&self) -> usize {
    self.journal.position()
  }

  /// Appends a version. A full version replaces the field set; a change sets `set` and removes
  /// `removed`. The caller rotates the table when the result asks for it.
  pub fn write(
    &mut self,
    version: u64,
    full: bool,
    set: &FieldMap,
    removed: &[String],
  ) -> anyhow::Result<Written> {
    anyhow::ensure!(
      !self.read_only,
      "field-set table is read-only after a failed rotation"
    );
    anyhow::ensure!(
      version > self.newest_version,
      "field-set version {version} does not follow {}",
      self.newest_version
    );
    // An empty table has nothing to change.
    let kind = if full || self.is_empty() {
      EntryKind::Full
    } else {
      EntryKind::Change
    };
    let mut entry = field_set(version, set);
    if kind == EntryKind::Change {
      entry.fields.extend(removed.iter().map(|key| Field {
        key: key.clone(),
        value_type: Some(Value_type::Cleared(true)),
        ..Default::default()
      }));
    }
    let key = kind.key(version);

    match self.journal.insert_entry_ref(SCOPE, &key, &entry) {
      Ok(_) => {},
      Err(UpdateError::CapacityExceeded) => {
        let bytes =
          usize::try_from(entry.compute_size()).unwrap_or(usize::MAX) + FRAME_OVERHEAD_BYTES;
        return Ok(Written::Full { bytes });
      },
      Err(UpdateError::System(error)) => return Err(error),
    }
    self.newest_version = version;

    // A rotation that could not get below the high-water mark, because `base` alone is that large,
    // is not repeated on every write; the next one happens when the table is full.
    Ok(Written::Done {
      rotate: !self.rotation_ineffective && self.journal.is_high_water_mark_triggered(),
    })
  }

  /// Plans a rotation: the entries of a new journal, built by [`Rotation::build`] without the
  /// table's lock and swapped in by [`Self::install`]. Below its cap the table grows: the entries
  /// are kept and the new file is the next power of two that holds them under the high-water mark.
  /// At its cap, the oldest changes are folded into `base` until the rest fit in half of it.
  /// `additional` is room for an entry that did not fit.
  pub fn plan_rotation(&mut self, additional: usize) -> anyhow::Result<Rotation> {
    #[cfg(test)]
    {
      self.rotations_started += 1;
    }
    let mut entries = Vec::new();
    let partial = self
      .journal
      .for_each_entry(|_scope, key, entry, timestamp| {
        if let Some((kind, _)) = EntryKind::parse(key) {
          entries.push((kind, entry.clone(), timestamp));
        }
      });
    // A table that lost an entry after it was opened cannot be rewritten without breaking the
    // chain of changes.
    anyhow::ensure!(
      matches!(partial, PartialDataLoss::None)
        || entries.last().map(|(_, entry, _)| entry.version) == Some(self.newest_version),
      "field-set table has undecodable entries"
    );
    anyhow::ensure!(!entries.is_empty(), "nothing to rotate");

    let entry_size = |entry: &FieldSet| {
      usize::try_from(entry.compute_size()).unwrap_or(usize::MAX) + FRAME_OVERHEAD_BYTES + 8
    };
    let mut remaining: usize = entries.iter().map(|(_, entry, _)| entry_size(entry)).sum();

    // Grow while the entries, plus room for the next one, stay under the high-water mark of a file
    // within the cap. The file never shrinks below its current size unless the cap was lowered.
    #[allow(
      clippy::cast_precision_loss,
      clippy::cast_possible_truncation,
      clippy::cast_sign_loss
    )]
    let needed = ((remaining + additional) as f32 / HIGH_WATER_MARK_RATIO).ceil() as usize;
    if needed <= self.max_bytes {
      let size = needed
        .next_power_of_two()
        .max(self.journal.buffer_len())
        .min(self.max_bytes);
      let entries = entries
        .into_iter()
        .map(|(kind, entry, timestamp)| (SCOPE, kind.key(entry.version), entry, timestamp))
        .collect();
      return Ok(self.rotation(size, entries, None));
    }

    let target = self.max_bytes / 2;

    // Fold from the front until the rest, plus the new base, fit in the target. Folding every
    // entry leaves a base at the newest version.
    let mut base = FieldMap::new();
    let mut base_size = 0;
    let mut base_version = 0;
    let mut base_timestamp = 0;
    let mut folded = 0;
    while folded < entries.len() {
      if folded > 0 && remaining + base_size + FRAME_OVERHEAD_BYTES <= target {
        break;
      }
      let (kind, entry, timestamp) = &entries[folded];
      apply(&mut base, *kind, entry);
      // Folding is rare, so the base is measured again rather than tracked as it changes.
      base_size = fields_size(&base);
      base_version = entry.version;
      base_timestamp = *timestamp;
      remaining -= entry_size(entry);
      folded += 1;
    }

    let entries = std::iter::once((
      SCOPE,
      EntryKind::Base.key(base_version),
      field_set(base_version, &base),
      base_timestamp,
    ))
    .chain(
      entries
        .into_iter()
        .skip(folded)
        .map(|(kind, entry, timestamp)| (SCOPE, kind.key(entry.version), entry, timestamp)),
    )
    .collect();
    Ok(self.rotation(self.max_bytes, entries, Some(base_version)))
  }

  fn rotation(
    &self,
    size: usize,
    entries: Vec<(Scope, String, FieldSet, u64)>,
    base_version: Option<u64>,
  ) -> Rotation {
    Rotation {
      directory: self.directory.clone(),
      size,
      entries,
      base_version,
      newest_version: self.newest_version,
      time_provider: self.time_provider.clone(),
    }
  }

  /// Swaps in a journal built by [`Rotation::build`]. The caller then renames the new file over
  /// the table's file with the returned [`Rename`]; the journal stays mapped across the rename.
  /// Fails if the table was written since the rotation was planned.
  pub fn install(&mut self, rotated: Rotated) -> anyhow::Result<Rename> {
    anyhow::ensure!(
      rotated.newest_version == self.newest_version,
      "field-set table was written during its rotation"
    );
    self.journal = rotated.journal;
    if let Some(base_version) = rotated.base_version {
      self.base_version = base_version;
    }
    // A journal created with entries does not report its high-water mark until the next write,
    // so check the position directly.
    self.rotation_ineffective = self.above_high_water();
    self.cursors.clear();
    Ok(Rename {
      from: rotation_path(&self.directory),
      to: journal_path(&self.directory),
    })
  }

  /// Records a rotation that failed before it was installed. The table is unchanged; it is not
  /// rotated again until it is full.
  pub fn rotation_failed(&mut self) {
    self.rotation_ineffective = true;
  }

  /// Records a failed rename of an installed journal. The table still resolves versions, but its
  /// file was not replaced, so it takes no more writes: they would be lost on the next start.
  pub fn rename_failed(&mut self) {
    self.read_only = true;
  }

  /// Resolves each of `versions`.
  #[cfg(test)]
  pub fn resolve(&mut self, versions: &[u64]) -> Vec<(u64, Resolution)> {
    versions
      .iter()
      .map(|&version| (version, self.resolve_one(version)))
      .collect()
  }

  /// Returns the full field set at `version`. Resolving versions in increasing order, as uploaders
  /// reading a buffer oldest first do, walks the table forward once.
  pub fn resolve_one(&mut self, version: u64) -> Resolution {
    if version < self.base_version {
      return Resolution::BeyondWindow;
    }
    // A version in a gap, such as the rest of a block reserved by an earlier process, stops the
    // walk at the next entry and is reported missing.
    if version == 0 || version > self.newest_version {
      return Resolution::Missing;
    }

    // Continue from the cursor furthest along that has not passed the version.
    let mut cursor = self
      .cursors
      .iter()
      .enumerate()
      .filter(|(_, cursor)| cursor.version <= version)
      .max_by_key(|(_, cursor)| cursor.version)
      .map(|(index, _)| index)
      .map_or_else(
        || Cursor {
          offset: HEADER_SIZE,
          version: 0,
          fields: FieldMap::new(),
          set: None,
        },
        |index| self.cursors.remove(index),
      );
    if cursor.version < version {
      self
        .journal
        .for_each_entry_from(cursor.offset, |offset, _scope, key, entry, _timestamp| {
          let Some((kind, _)) = EntryKind::parse(key) else {
            return false;
          };
          apply(&mut cursor.fields, kind, entry);
          cursor.offset = offset;
          cursor.version = entry.version;
          cursor.set = None;
          entry.version < version
        });
    }

    let resolution = if cursor.version == version {
      let set = cursor
        .set
        .get_or_insert_with(|| Arc::new(field_set(version, &cursor.fields)))
        .clone();
      Resolution::Found(set)
    } else {
      Resolution::Missing
    };
    self.cursors.push(cursor);
    if self.cursors.len() > CURSORS {
      self.cursors.remove(0);
    }
    resolution
  }

  fn above_high_water(&self) -> bool {
    #[allow(clippy::cast_precision_loss)]
    let high_water = self.journal.buffer_len() as f32 * HIGH_WATER_MARK_RATIO;
    #[allow(clippy::cast_precision_loss)]
    let position = self.journal.position() as f32;
    position >= high_water
  }

  /// Applies a new size cap at the next rotation.
  pub fn set_max_bytes(&mut self, max_bytes: usize) {
    self.max_bytes = max_bytes.clamp(MIN_TABLE_BYTES, MAX_TABLE_BYTES);
  }
}
