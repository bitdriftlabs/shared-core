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
//! * `f/<version>`: a full field set, written for a new table and after a failed write so that the
//!   chain of changes is never broken.
//! * `v/<version>`: the fields set (with values) and removed (as `cleared` fields) since the
//!   previous version.
//!
//! A version's full set is found by replaying the entries in order up to it. When the journal
//! reaches its high-water mark, the oldest changes are folded into `base` until the rest fit in
//! half of the table, and a new journal file is swapped in. The table never leaves the device.

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
  let mut size = 0;
  apply_sized(state, &mut size, kind, entry);
}

/// The encoded size of one field of a `FieldSet`, as an entry of its repeated field.
fn field_size(key: &str, value: &Data) -> usize {
  let size = protobuf::rt::string_size(1, key) + {
    let value_size = value.compute_size();
    1 + protobuf::rt::compute_raw_varint64_size(value_size) + value_size
  };
  usize::try_from(1 + protobuf::rt::compute_raw_varint64_size(size) + size).unwrap_or(usize::MAX)
}

/// Applies an entry and keeps `size` equal to the encoded size of the full set's fields.
fn apply_sized(state: &mut FieldMap, size: &mut usize, kind: EntryKind, entry: &FieldSet) {
  if kind != EntryKind::Change {
    state.clear();
    *size = 0;
  }
  for field in &entry.fields {
    match &field.value_type {
      Some(Value_type::Cleared(true)) => {
        if let Some(old) = state.remove(&field.key) {
          *size = size.saturating_sub(field_size(&field.key, &old));
        }
      },
      Some(Value_type::Value(value)) => {
        *size = size.saturating_add(field_size(&field.key, value));
        if let Some(old) = state.insert(field.key.clone(), value.clone()) {
          *size = size.saturating_sub(field_size(&field.key, &old));
        }
      },
      Some(Value_type::Cleared(false)) | None => {},
    }
  }
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

/// Recovered state of an opened table.
pub struct Recovered {
  /// The newest version in the table, or 0 for an empty table.
  pub newest_version: u64,
  /// The full field set at the newest version.
  pub newest_fields: FieldMap,
  /// Whether the table was found corrupt or unreadable and replaced with an empty one.
  pub reset: bool,
}

/// Outcome of a successful rotation, for metrics.
#[derive(Debug, PartialEq, Eq)]
pub struct Rotation {
  /// Microseconds between the new base's creation and the rotation.
  pub window_span_micros: u64,
}

pub struct VersionTable {
  directory: PathBuf,
  generation: u64,
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
  /// The versions in the table, as ranges of consecutive numbers, so a version that is not in the
  /// table is answered without walking it.
  versions: Vec<(u64, u64)>,
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

fn journal_path(directory: &Path, generation: u64) -> PathBuf {
  directory.join(format!("{JOURNAL_NAME}.jrn.{generation}"))
}

/// The generations of journal files in `directory`, newest first.
fn generations(directory: &Path) -> Vec<u64> {
  let prefix = format!("{JOURNAL_NAME}.jrn.");
  let mut generations: Vec<u64> = std::fs::read_dir(directory)
    .into_iter()
    .flatten()
    .filter_map(|entry| {
      entry
        .ok()?
        .file_name()
        .to_str()?
        .strip_prefix(&prefix)?
        .parse()
        .ok()
    })
    .collect();
  generations.sort_unstable_by(|a, b| b.cmp(a));
  generations
}

impl VersionTable {
  /// Opens the table in `directory`, recovering the entries that survive, or creates an empty
  /// table if there is none or it is unreadable. If the newest journal file is unreadable, for
  /// example after power loss during a rotation, the previous one is used.
  pub fn open(
    directory: &Path,
    max_bytes: usize,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, Recovered)> {
    let max_bytes = max_bytes.clamp(MIN_TABLE_BYTES, MAX_TABLE_BYTES);
    std::fs::create_dir_all(directory)?;
    let generations = generations(directory);
    let newest_generation = generations.first().copied().unwrap_or(0);

    // Use the readable generation with the newest version. A rotation's output can be shorter
    // than its input if power was lost before it reached the disk; then the older file wins.
    let mut candidates = Vec::new();
    for &generation in &generations {
      let path = journal_path(directory, generation);
      match Self::recover(&path, time_provider.clone()) {
        Ok((journal, recovered, base_version, partial_data_loss))
          if recovered.newest_version != 0 =>
        {
          candidates.push((
            generation,
            journal,
            recovered,
            base_version,
            partial_data_loss,
          ));
        },
        Ok(_) => log::debug!("field-set table at {} has no entries", path.display()),
        Err(error) => log::debug!(
          "field-set table at {} is unreadable: {error}",
          path.display()
        ),
      }
    }
    // Generations are newest first, so the newest file wins a tie.
    let best = candidates
      .iter()
      .enumerate()
      .max_by_key(|(index, candidate)| (candidate.2.newest_version, std::cmp::Reverse(*index)))
      .map(|(index, _)| index);

    let mut opened = None;
    if let Some(index) = best {
      let (generation, journal, recovered, base_version, partial_data_loss) =
        candidates.swap_remove(index);
      drop(candidates);
      let mut table = Self {
        directory: directory.to_path_buf(),
        generation,
        journal,
        max_bytes,
        time_provider: time_provider.clone(),
        base_version,
        newest_version: recovered.newest_version,
        rotation_ineffective: false,
        versions: Vec::new(),
        cursors: Vec::new(),
      };
      table.rebuild_versions();
      // Entries appended after an undecodable frame could never be read back, and a file other
      // than the newest must not be appended to, so rewrite the surviving entries into a new
      // generation.
      if partial_data_loss || generation != newest_generation {
        match table.rotate_from(newest_generation) {
          Ok(_) => opened = Some((table, recovered)),
          Err(error) => log::debug!("failed to rewrite recovered field-set table: {error}"),
        }
      } else {
        opened = Some((table, recovered));
      }
    } else {
      drop(candidates);
    }

    let (table, recovered) = opened.map_or_else(
      || Self::new_empty(directory, newest_generation + 1, max_bytes, time_provider),
      Ok,
    )?;

    // Only the active generation is kept.
    for generation in generations {
      if generation != table.generation
        && let Err(error) = std::fs::remove_file(journal_path(directory, generation))
      {
        log::debug!("failed to remove old field-set table: {error}");
      }
    }
    Ok((table, recovered))
  }

  fn new_empty(
    directory: &Path,
    generation: u64,
    max_bytes: usize,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(Self, Recovered)> {
    let journal = MemMappedVersionedJournal::new(
      journal_path(directory, generation),
      max_bytes,
      HIGH_WATER_MARK_RATIO,
      time_provider.clone(),
      std::iter::empty(),
    )?;
    Ok((
      Self {
        directory: directory.to_path_buf(),
        generation,
        journal,
        max_bytes,
        time_provider,
        base_version: 0,
        newest_version: 0,
        rotation_ineffective: false,
        versions: Vec::new(),
        cursors: Vec::new(),
      },
      Recovered {
        newest_version: 0,
        newest_fields: FieldMap::new(),
        reset: true,
      },
    ))
  }

  /// Replays a journal file, checking that each key matches its entry and that versions increase.
  fn recover(
    path: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> anyhow::Result<(MemMappedVersionedJournal<FieldSet>, Recovered, u64, bool)> {
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
    let partial_data_loss = matches!(partial_data_loss, PartialDataLoss::Yes);
    if partial_data_loss {
      // The newest entries were lost, for example on power loss. The surviving prefix is
      // consistent; version numbers are reserved separately, so none of the lost ones is reused.
      log::debug!("field-set table lost its newest entries; continuing from {newest_version}");
    }
    Ok((
      journal,
      Recovered {
        newest_version,
        newest_fields: state,
        reset: false,
      },
      base_version,
      partial_data_loss,
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
  /// `removed`. On a full journal the table is rotated once before giving up.
  pub fn write(
    &mut self,
    version: u64,
    full: bool,
    set: &FieldMap,
    removed: &[String],
  ) -> anyhow::Result<Option<Rotation>> {
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

    let mut rotation = None;
    match self.journal.insert_entry_ref(SCOPE, &key, &entry) {
      Ok(_) => {},
      Err(UpdateError::CapacityExceeded) => {
        rotation = Some(self.rotate()?);
        self
          .journal
          .insert_entry_ref(SCOPE, &key, &entry)
          .map_err(|error| anyhow::anyhow!("field-set table full after rotation: {error}"))?;
      },
      Err(UpdateError::System(error)) => return Err(error),
    }
    self.newest_version = version;
    match self.versions.last_mut() {
      Some((_, end)) if *end + 1 == version => *end = version,
      _ => self.versions.push((version, version)),
    }

    // A rotation that could not get below the high-water mark, because `base` alone is that large,
    // is not repeated on every write; the next one happens when the table is full.
    if rotation.is_none()
      && !self.rotation_ineffective
      && self.journal.is_high_water_mark_triggered()
    {
      match self.rotate() {
        Ok(completed) => rotation = Some(completed),
        // The version is written. The rotation is retried when the table is full rather than on
        // every write.
        Err(error) => {
          log::debug!("failed to rotate field-set table: {error}");
          self.rotation_ineffective = true;
        },
      }
    }
    Ok(rotation)
  }

  /// Folds the oldest changes into `base` until the rest fit in half the table, and swaps in a
  /// new journal holding `base` and the remaining changes.
  pub fn rotate(&mut self) -> anyhow::Result<Rotation> {
    self.rotate_from(self.generation)
  }

  fn rotate_from(&mut self, newest_generation: u64) -> anyhow::Result<Rotation> {
    let mut entries = Vec::new();
    let partial = self
      .journal
      .for_each_entry(|_scope, key, entry, timestamp| {
        if let Some((kind, _)) = EntryKind::parse(key) {
          entries.push((kind, entry.clone(), timestamp));
        }
      });
    // A table that lost an entry after it was opened cannot be rewritten without breaking the
    // chain of changes. Recovery at open rewrites only the readable prefix, which is consistent.
    anyhow::ensure!(
      matches!(partial, PartialDataLoss::None)
        || entries.last().map(|(_, entry, _)| entry.version) == Some(self.newest_version),
      "field-set table has undecodable entries"
    );
    anyhow::ensure!(!entries.is_empty(), "nothing to rotate");

    let entry_size = |entry: &FieldSet| {
      usize::try_from(entry.compute_size()).unwrap_or(usize::MAX) + FRAME_OVERHEAD_BYTES + 8
    };
    let target = self.max_bytes / 2;
    let mut remaining: usize = entries.iter().map(|(_, entry, _)| entry_size(entry)).sum();

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
      apply_sized(&mut base, &mut base_size, *kind, entry);
      base_version = entry.version;
      base_timestamp = *timestamp;
      remaining -= entry_size(entry);
      folded += 1;
    }

    let rebuilt =
      std::iter::once((
        SCOPE,
        EntryKind::Base.key(base_version),
        field_set(base_version, &base),
        base_timestamp,
      ))
      .chain(entries[folded ..].iter().map(|(kind, entry, timestamp)| {
        (SCOPE, kind.key(entry.version), entry.clone(), *timestamp)
      }));

    let next_generation = self.generation.max(newest_generation) + 1;
    let path = journal_path(&self.directory, next_generation);
    let journal = MemMappedVersionedJournal::new(
      &path,
      self.max_bytes,
      HIGH_WATER_MARK_RATIO,
      self.time_provider.clone(),
      rebuilt,
    )
    .inspect_err(|_| {
      let _ignored = std::fs::remove_file(&path);
    })?;
    // The new file must be on disk before the old one is removed.
    MemMappedVersionedJournal::sync(&journal).inspect_err(|_| {
      let _ignored = std::fs::remove_file(&path);
    })?;

    let old_path = journal_path(&self.directory, self.generation);
    self.journal = journal;
    self.generation = next_generation;
    self.base_version = base_version;
    // A journal created with entries does not report its high-water mark until the next write,
    // so check the position directly.
    self.rotation_ineffective = self.above_high_water();
    self.cursors.clear();
    self.rebuild_versions();
    if let Err(error) = std::fs::remove_file(&old_path) {
      log::debug!("failed to remove rotated field-set table: {error}");
    }

    let now = u64::try_from(self.time_provider.now().unix_timestamp_nanos() / 1_000).unwrap_or(0);
    Ok(Rotation {
      window_span_micros: now.saturating_sub(base_timestamp),
    })
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
    if version == 0 || version > self.newest_version || !self.contains(version) {
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

  /// Whether `version` was written to the table and is at or after its `base`.
  fn contains(&self, version: u64) -> bool {
    let index = self.versions.partition_point(|(_, end)| *end < version);
    self
      .versions
      .get(index)
      .is_some_and(|(start, _)| *start <= version)
  }

  fn rebuild_versions(&mut self) {
    let mut versions: Vec<(u64, u64)> = Vec::new();
    self.journal.for_each_entry(
      |_scope, _key, entry, _timestamp| match versions.last_mut() {
        Some((_, end)) if *end + 1 == entry.version => *end = entry.version,
        _ => versions.push((entry.version, entry.version)),
      },
    );
    self.versions = versions;
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
