// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fixtures_test.rs"]
mod tests;

use anyhow::{Result, bail};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

const FIXTURES_DIRECTORY: &str = "fixtures";
const DEFAULT_CORPORA: &[&str] = &["small", "large"];

//
// FixturePaths
//

/// Resolved paths and display name for one checked-in benchmark corpus.
#[derive(Debug)]
pub struct FixturePaths {
  pub name: String,
  pub config_path: PathBuf,
  pub logs_path: PathBuf,
}

//
// FixtureManifest
//

#[derive(Deserialize)]
struct FixtureManifest {
  format_version: u32,
  name: String,
  config: FixtureFile,
  logs: FixtureFile,
}

//
// FixtureFile
//

#[derive(Deserialize)]
struct FixtureFile {
  path: PathBuf,
}

/// Resolve every checked-in corpus. Default benchmarks intentionally run all of them.
pub fn default_fixture_paths() -> Result<Vec<FixturePaths>> {
  DEFAULT_CORPORA
    .iter()
    .map(|name| fixture_paths(name))
    .collect()
}

fn fixtures_root() -> PathBuf {
  std::env::var_os("TEST_SRCDIR")
    .map_or_else(
      || PathBuf::from(env!("CARGO_MANIFEST_DIR")),
      |runfiles_dir| {
        PathBuf::from(runfiles_dir)
          .join(std::env::var("TEST_WORKSPACE").unwrap_or_else(|_| "_main".to_owned()))
          .join("shared-core/bd-workflow-bench")
      },
    )
    .join(FIXTURES_DIRECTORY)
}

fn fixture_paths(name: &str) -> Result<FixturePaths> {
  let root = fixtures_root().join(name);
  let manifest: FixtureManifest = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
  if manifest.format_version != 1 {
    bail!(
      "unsupported fixture manifest version {}",
      manifest.format_version
    );
  }
  if manifest.name != name {
    bail!(
      "fixture manifest name {:?} does not match directory {name:?}",
      manifest.name
    );
  }

  Ok(FixturePaths {
    name: manifest.name,
    config_path: root.join(manifest.config.path),
    logs_path: root.join(manifest.logs.path),
  })
}

pub fn read_to_string(path: &Path) -> Result<String> {
  let mut content = String::new();
  open_reader(path)?.read_to_string(&mut content)?;
  Ok(content)
}

pub fn open_reader(path: &Path) -> Result<Box<dyn BufRead>> {
  let file = File::open(path)?;
  if path.extension().is_some_and(|extension| extension == "zst") {
    return Ok(Box::new(BufReader::new(zstd::stream::read::Decoder::new(
      file,
    )?)));
  }

  Ok(Box::new(BufReader::new(file)))
}
