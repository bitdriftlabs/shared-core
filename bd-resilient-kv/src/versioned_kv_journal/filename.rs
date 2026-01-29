// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Parsing for archived journal snapshot filenames: `{name}.jrn.g{generation}.t{timestamp}.zz`

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFilename {
  pub generation: u64,
  pub timestamp_micros: u64,
}

impl SnapshotFilename {
  /// Parses `{name}.jrn.g{generation}.t{timestamp}.zz` format.
  #[must_use]
  pub fn parse(filename: &str) -> Option<Self> {
    if !std::path::Path::new(filename)
      .extension()
      .is_some_and(|ext| ext.eq_ignore_ascii_case("zz"))
    {
      return None;
    }

    let generation = filename
      .split('.')
      .find(|part| {
        part.starts_with('g') && part.len() > 1 && part[1 ..].chars().all(|c| c.is_ascii_digit())
      })
      .and_then(|part| part.strip_prefix('g'))
      .and_then(|g| g.parse::<u64>().ok())?;

    let timestamp_micros = filename
      .split('.')
      .find(|part| {
        part.starts_with('t') && part.len() > 1 && part[1 ..].chars().all(|c| c.is_ascii_digit())
      })
      .and_then(|part| part.strip_prefix('t'))
      .and_then(|ts| ts.parse::<u64>().ok())?;

    Some(Self {
      generation,
      timestamp_micros,
    })
  }

  #[must_use]
  pub fn extract_timestamp(filename: &str) -> Option<u64> {
    Self::parse(filename).map(|sf| sf.timestamp_micros)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_valid_filename() {
    let result = SnapshotFilename::parse("state.jrn.g1.t1234567890.zz");
    assert_eq!(
      result,
      Some(SnapshotFilename {
        generation: 1,
        timestamp_micros: 1_234_567_890
      })
    );
  }

  #[test]
  fn parse_large_values() {
    let result = SnapshotFilename::parse("state.jrn.g999.t1737933600000000.zz");
    assert_eq!(
      result,
      Some(SnapshotFilename {
        generation: 999,
        timestamp_micros: 1_737_933_600_000_000
      })
    );
  }

  #[test]
  fn parse_different_journal_names() {
    let result = SnapshotFilename::parse("other.jrn.g5.t999999.zz");
    assert_eq!(
      result,
      Some(SnapshotFilename {
        generation: 5,
        timestamp_micros: 999_999
      })
    );
  }

  #[test]
  fn parse_invalid_missing_zz_extension() {
    assert_eq!(SnapshotFilename::parse("state.jrn.g1.t1234567890"), None);
  }

  #[test]
  fn parse_invalid_missing_generation() {
    assert_eq!(SnapshotFilename::parse("state.jrn.t1234567890.zz"), None);
  }

  #[test]
  fn parse_invalid_missing_timestamp() {
    assert_eq!(SnapshotFilename::parse("state.jrn.g1.zz"), None);
  }

  #[test]
  fn parse_invalid_non_numeric_generation() {
    assert_eq!(
      SnapshotFilename::parse("state.jrn.gabc.t1234567890.zz"),
      None
    );
  }

  #[test]
  fn parse_invalid_non_numeric_timestamp() {
    assert_eq!(SnapshotFilename::parse("state.jrn.g1.tabc.zz"), None);
  }

  #[test]
  fn parse_invalid_completely_wrong_format() {
    assert_eq!(SnapshotFilename::parse("random_file.txt"), None);
    assert_eq!(SnapshotFilename::parse(""), None);
    assert_eq!(SnapshotFilename::parse(".zz"), None);
  }

  #[test]
  fn extract_timestamp_works() {
    assert_eq!(
      SnapshotFilename::extract_timestamp("state.jrn.g1.t1234567890.zz"),
      Some(1_234_567_890)
    );
    assert_eq!(SnapshotFilename::extract_timestamp("invalid.txt"), None);
  }
}
