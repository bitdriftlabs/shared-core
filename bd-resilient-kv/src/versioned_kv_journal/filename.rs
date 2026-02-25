// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Parsing for archived journal snapshot filenames: `{name}.jrn.g{generation}.t{timestamp}.zz`

#[cfg(test)]
#[path = "./filename_test.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFilename {
  pub timestamp_micros: u64,
}

impl SnapshotFilename {
  /// Parses `{name}.jrn.g{generation}.t{timestamp}.zz` format, returning only the timestamp.
  #[must_use]
  pub fn parse(filename: &str) -> Option<Self> {
    if !std::path::Path::new(filename)
      .extension()
      .is_some_and(|ext| ext.eq_ignore_ascii_case("zz"))
    {
      return None;
    }

    let timestamp_micros = filename
      .split('.')
      .find(|part| {
        part.starts_with('t') && part.len() > 1 && part[1 ..].chars().all(|c| c.is_ascii_digit())
      })
      .and_then(|part| part.strip_prefix('t'))
      .and_then(|ts| ts.parse::<u64>().ok())?;

    Some(Self { timestamp_micros })
  }
}
