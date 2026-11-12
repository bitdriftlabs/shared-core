// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::{FieldMap, INITIAL_TABLE_BYTES, MIN_TABLE_BYTES, Resolution, VersionTable, field_set};
use bd_proto::protos::client::api::FieldSet;
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_time::{SystemTimeProvider, TimeProvider};
use std::sync::Arc;
use tempfile::TempDir;

fn string(value: &str) -> Data {
  Data {
    data_type: Some(Data_type::StringData(value.to_string())),
    ..Default::default()
  }
}

fn map(entries: &[(&str, &str)]) -> FieldMap {
  entries
    .iter()
    .map(|(key, value)| ((*key).to_string(), string(value)))
    .collect()
}

fn time() -> Arc<dyn TimeProvider> {
  Arc::new(SystemTimeProvider)
}

fn found(resolution: &Resolution) -> FieldSet {
  match resolution {
    Resolution::Found(set) => (**set).clone(),
    other => panic!("expected a field set, got {other:?}"),
  }
}

#[test]
fn versions_resolve_to_their_full_sets_and_survive_reopening() {
  let directory = TempDir::new().unwrap();
  let (mut table, recovered) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  assert!(!recovered.discarded);
  assert_eq!(recovered.newest_version, 0);

  table
    .write(1, true, &map(&[("a", "1"), ("b", "2")]), &[])
    .unwrap();
  table.write(2, false, &map(&[("c", "3")]), &[]).unwrap();
  table
    .write(5, false, &map(&[("a", "changed")]), &["b".to_string()])
    .unwrap();

  let resolved = table.resolve(&[5, 1, 2]);
  assert_eq!(
    found(&resolved.iter().find(|(v, _)| *v == 1).unwrap().1),
    field_set(1, &map(&[("a", "1"), ("b", "2")]))
  );
  assert_eq!(
    found(&resolved.iter().find(|(v, _)| *v == 2).unwrap().1),
    field_set(2, &map(&[("a", "1"), ("b", "2"), ("c", "3")]))
  );
  assert_eq!(
    found(&resolved.iter().find(|(v, _)| *v == 5).unwrap().1),
    field_set(5, &map(&[("a", "changed"), ("c", "3")]))
  );
  // Never written, and newer than the table.
  assert_eq!(table.resolve(&[3]), vec![(3, Resolution::Missing)]);
  assert_eq!(table.resolve(&[6]), vec![(6, Resolution::Missing)]);

  drop(table);
  let (mut table, recovered) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  assert!(!recovered.discarded);
  assert_eq!(recovered.newest_version, 5);
  assert_eq!(
    recovered.newest_fields,
    map(&[("a", "changed"), ("c", "3")])
  );
  assert_eq!(
    found(&table.resolve(&[2])[0].1),
    field_set(2, &map(&[("a", "1"), ("b", "2"), ("c", "3")]))
  );
  // Versions must increase.
  assert!(table.write(5, false, &map(&[]), &[]).is_err());
}

#[test]
fn rotation_folds_the_oldest_changes_into_base() {
  let directory = TempDir::new().unwrap();
  let (mut table, _) = VersionTable::open(directory.path(), MIN_TABLE_BYTES, time()).unwrap();

  let value = "x".repeat(100);
  for version in 1 ..= 200u64 {
    let key = format!("key{}", version % 7);
    table
      .write(
        version,
        version == 1,
        &map(&[(&key, &format!("{value}{version}"))]),
        &[],
      )
      .unwrap();
  }
  assert!(table.size_bytes() <= MIN_TABLE_BYTES);
  assert!(table.base_version > 1);
  assert_eq!(table.newest_version(), 200);

  // Folded versions are beyond the window; retained ones resolve to the same set as before.
  assert_eq!(table.resolve(&[1]), vec![(1, Resolution::BeyondWindow)]);
  let expected: FieldMap = (194 ..= 200u64)
    .map(|version| {
      (
        format!("key{}", version % 7),
        string(&format!("{value}{version}")),
      )
    })
    .collect();
  assert_eq!(
    found(&table.resolve(&[200])[0].1),
    field_set(200, &expected)
  );
  let base_version = table.base_version;
  assert!(matches!(
    table.resolve(&[base_version])[0].1,
    Resolution::Found(_)
  ));

  // Only the table's file is left on disk, and it reopens to the same state.
  let files: Vec<_> = std::fs::read_dir(directory.path())
    .unwrap()
    .map(|entry| entry.unwrap().file_name())
    .collect();
  assert_eq!(files.len(), 1, "{files:?}");
  drop(table);
  let (mut table, recovered) =
    VersionTable::open(directory.path(), MIN_TABLE_BYTES, time()).unwrap();
  assert_eq!(recovered.newest_version, 200);
  assert_eq!(recovered.newest_fields, expected);
  assert_eq!(table.resolve(&[1]), vec![(1, Resolution::BeyondWindow)]);
}

#[test]
fn corrupt_table_is_replaced() {
  let directory = TempDir::new().unwrap();
  let (mut table, _) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  table.write(1, true, &map(&[("a", "1")]), &[]).unwrap();
  drop(table);

  // Overwrite the first frame's bytes after the header.
  let path = std::fs::read_dir(directory.path())
    .unwrap()
    .next()
    .unwrap()
    .unwrap()
    .path();
  let mut bytes = std::fs::read(&path).unwrap();
  for byte in &mut bytes[bd_versioned_kv::versioned_kv_journal::HEADER_SIZE ..][.. 16] {
    *byte = 0xff;
  }
  std::fs::write(&path, bytes).unwrap();

  let (mut table, recovered) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  // A table that lost entries is discarded, and its next version is written in full.
  assert!(recovered.discarded);
  assert_eq!(recovered.newest_version, 0);
  assert_eq!(table.resolve(&[1]), vec![(1, Resolution::Missing)]);
  table.write(2, false, &map(&[("b", "2")]), &[]).unwrap();
  drop(table);
  let (_, recovered) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  assert_eq!(recovered.newest_version, 2);
  assert_eq!(recovered.newest_fields, map(&[("b", "2")]));
}

#[test]
fn an_interrupted_rotation_leaves_the_table_intact() {
  let directory = TempDir::new().unwrap();
  let (mut table, _) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  table.write(1, true, &map(&[("a", "1")]), &[]).unwrap();
  table.write(2, false, &map(&[("b", "2")]), &[]).unwrap();
  drop(table);

  // A rotation that died before renaming its output over the table.
  std::fs::write(
    directory.path().join("field_sets.jrn.tmp"),
    vec![0xff; MIN_TABLE_BYTES],
  )
  .unwrap();

  let (mut table, recovered) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  assert!(!recovered.discarded);
  assert_eq!(recovered.newest_version, 2);
  assert_eq!(
    found(&table.resolve(&[2])[0].1),
    field_set(2, &map(&[("a", "1"), ("b", "2")]))
  );
  let files: Vec<_> = std::fs::read_dir(directory.path())
    .unwrap()
    .map(|entry| entry.unwrap().file_name())
    .collect();
  assert_eq!(files.len(), 1, "{files:?}");
}

#[test]
fn interleaved_resolutions_and_gaps_resolve_correctly() {
  let directory = TempDir::new().unwrap();
  let (mut table, _) = VersionTable::open(directory.path(), 64 * 1024, time()).unwrap();
  table.write(1, true, &map(&[("a", "1")]), &[]).unwrap();
  for version in 2 ..= 20u64 {
    table
      .write(version, false, &map(&[("a", &version.to_string())]), &[])
      .unwrap();
  }
  // A gap, as after a restart that reserved a new block of numbers.
  for version in 65 ..= 70u64 {
    table
      .write(version, false, &map(&[("a", &version.to_string())]), &[])
      .unwrap();
  }
  // Two readers alternating between old and new versions, as a trigger upload and the continuous
  // uploader do, plus versions in the gap.
  // 66 repeats, so a cursor answers it again without walking.
  for version in [2u64, 66, 66, 3, 67, 30, 4, 68, 64, 19, 70, 21] {
    let resolved = table.resolve(&[version]).pop().unwrap().1;
    if (1 ..= 20).contains(&version) || (65 ..= 70).contains(&version) {
      assert_eq!(
        found(&resolved),
        field_set(version, &map(&[("a", &version.to_string())])),
        "{version}"
      );
    } else {
      assert_eq!(resolved, Resolution::Missing, "{version}");
    }
  }
  assert!(table.cursors.len() <= super::CURSORS);
}

#[test]
fn a_base_above_the_high_water_mark_is_not_rotated_on_every_write() {
  let directory = TempDir::new().unwrap();
  let (mut table, _) = VersionTable::open(directory.path(), MIN_TABLE_BYTES, time()).unwrap();
  // A set that alone fills most of the table: the rotation after it cannot get below the mark.
  table
    .write(1, true, &map(&[("large", &"x".repeat(3400))]), &[])
    .unwrap();
  assert!(table.rotation_ineffective);
  let rotations_started = table.rotations_started;

  for version in 2 ..= 5u64 {
    table
      .write(
        version,
        false,
        &map(&[("small", &version.to_string())]),
        &[],
      )
      .unwrap();
  }
  assert_eq!(table.rotations_started, rotations_started);
}

#[test]
fn the_table_grows_to_its_cap_before_folding_versions_away() {
  let directory = TempDir::new().unwrap();
  let cap = 64 * 1024;
  let (mut table, _) = VersionTable::open(directory.path(), cap, time()).unwrap();
  assert_eq!(table.journal.buffer_len(), INITIAL_TABLE_BYTES);

  let value = "x".repeat(200);
  let write = |table: &mut VersionTable, version: u64| {
    table
      .write(
        version,
        version == 1,
        &map(&[("a", &format!("{value}{version}"))]),
        &[],
      )
      .unwrap();
  };

  // Below the cap, rotations grow the file and keep every version.
  let mut version = 0;
  while table.journal.buffer_len() < cap {
    version += 1;
    write(&mut table, version);
    assert_eq!(table.base_version, 0);
    assert!(matches!(table.resolve(&[1])[0].1, Resolution::Found(_)));
  }
  assert!(table.rotations_started > 0);

  // At the cap, the next rotation folds the oldest versions into `base`.
  while table.base_version == 0 {
    version += 1;
    write(&mut table, version);
  }
  assert_eq!(table.resolve(&[1]), vec![(1, Resolution::BeyondWindow)]);
  assert_eq!(table.journal.buffer_len(), cap);

  // The table reopens at its grown size.
  drop(table);
  let (table, _) = VersionTable::open(directory.path(), cap, time()).unwrap();
  assert_eq!(table.journal.buffer_len(), cap);
}
