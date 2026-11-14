// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::chunked::{CHUNK_BYTES, MAX_INDEX_BYTES, file_identity, read_index, safe_id};
use super::{ARTIFACT_UPLOAD_DIRECTORY, REPORT_INDEX_FILE};
use bd_client_common::artifact::MAX_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file_system::FileSystem;
use bd_runtime::runtime::{ConfigLoader, IntWatch, chunked_artifact_upload};
use std::collections::{HashMap, HashSet};
use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

const MAX_FILES: usize = 64;
const MAX_OWNED_BYTES: u64 = 640 * 1024 * 1024;
pub(super) const FREE_SPACE_HEADROOM: u64 = MAX_INDEX_BYTES + 1024 * 1024;

// Retained-file admission and uploader preparation must not race their filesystem inventories.
// The permit carries no ownership state; durable files and the existing index remain authoritative.
static PREPARATION: Semaphore = Semaphore::const_new(1);

//
// ArtifactStore
//

pub struct ArtifactStore {
  max_logical_bytes: IntWatch<chunked_artifact_upload::MaxLogicalBytes>,
}

impl ArtifactStore {
  #[must_use]
  pub fn new(runtime: &ConfigLoader) -> Self {
    Self {
      max_logical_bytes: runtime.register_int_watch(),
    }
  }

  #[must_use]
  pub fn whole_file_limit(&self) -> u64 {
    CHUNK_BYTES.min(self.logical_limit())
  }

  /// New captures use the whole-file representation when one bounded upload can carry them.
  pub fn should_chunk(&self, logical_bytes: u64) -> anyhow::Result<bool> {
    self.check_logical_bytes(logical_bytes)?;
    Ok(logical_bytes > CHUNK_BYTES)
  }

  pub(crate) fn logical_limit(&self) -> u64 {
    u64::from((*self.max_logical_bytes.read()).min(MAX_ARTIFACT_LOGICAL_BYTES))
  }

  fn check_logical_bytes(&self, logical_bytes: u64) -> anyhow::Result<()> {
    if logical_bytes > self.logical_limit() {
      anyhow::bail!("artifact logical byte admission exhausted");
    }
    Ok(())
  }

  pub(crate) async fn lock() -> anyhow::Result<SemaphorePermit<'static>> {
    Ok(PREPARATION.acquire().await?)
  }

  pub(super) async fn admit_capture(
    &self,
    filesystem: &dyn FileSystem,
    metadata: &Metadata,
  ) -> anyhow::Result<()> {
    self
      .check_capacity(
        filesystem,
        metadata.len(),
        metadata.len(),
        file_identity(metadata),
        metadata.len(),
      )
      .await
  }

  pub(crate) async fn check_capacity(
    &self,
    filesystem: &dyn FileSystem,
    logical_bytes: u64,
    source_bytes: u64,
    source_identity: Option<(u64, u64)>,
    output_bytes: u64,
  ) -> anyhow::Result<()> {
    self.check_logical_bytes(logical_bytes)?;
    let mut ids = HashSet::new();
    let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    match filesystem.open_regular_file(&index_path).await {
      Ok(mut file) => {
        let index = read_index(&mut file).await?;
        ids.extend(index.artifact.into_iter().map(|entry| entry.name));
        for entry in index.chunked_artifact {
          let request = entry
            .request
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing chunk request"))?;
          ids.insert(safe_id(&request.artifact_id)?.to_string());
        }
      },
      Err(error) if is_not_found(&error) => {},
      Err(error) => return Err(error),
    }
    let mut identities = HashSet::new();
    let mut used = 0_u64;
    let mut unmaterialized = 0_u64;
    let mut staged = HashMap::<Uuid, u64>::new();
    for path in list_files(filesystem, &ARTIFACT_UPLOAD_DIRECTORY).await? {
      let path = Path::new(&path);
      let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        continue;
      };
      if name == REPORT_INDEX_FILE.to_string_lossy().as_ref() {
        continue;
      }
      let Some(metadata) = file_metadata(filesystem, path).await? else {
        continue;
      };
      if file_identity(&metadata).is_none_or(|key| identities.insert(key)) {
        used = used.saturating_add(metadata.len());
        if let Some(id) = name
          .strip_prefix("chunk-")
          .and_then(|name| name.get(.. 36))
          .and_then(|id| safe_id(id).ok())
        {
          *staged.entry(id).or_default() += metadata.len();
        }
      }
    }
    for path in list_files(filesystem, Path::new("workflow-attachments")).await? {
      let path = Path::new(&path);
      let Some(metadata) = file_metadata(filesystem, path).await? else {
        continue;
      };
      if file_identity(&metadata).is_none_or(|key| identities.insert(key)) {
        used = used.saturating_add(metadata.len());
      }
      if path
        .extension()
        .is_none_or(|extension| extension != "raw" && extension != "payload")
      {
        continue;
      }
      let Some(id) = path
        .file_stem()
        .and_then(|id| id.to_str())
        .and_then(|id| safe_id(id).ok())
      else {
        continue;
      };
      if path.extension().is_some_and(|extension| extension == "raw")
        && !ids.contains(&id.to_string())
      {
        let logical = metadata
          .len()
          .checked_sub(4)
          .ok_or_else(|| anyhow::anyhow!("invalid retained workflow file"))?;
        // Before handoff, reserve RAW output as well as the retained source. Partial chunks from
        // failed preparation consume that reservation, not a second copy of the same headroom.
        let reservation = logical.saturating_sub(*staged.get(&id).unwrap_or(&0));
        used = used.saturating_add(reservation);
        unmaterialized = unmaterialized.saturating_add(reservation);
      }
      ids.insert(id.to_string());
    }
    // TODO: Account for pre-attribution private profile storage in the shared byte budget.
    // It is separately bounded today and only enters this inventory at uploader handoff.
    let source_charge = if source_identity.is_some_and(|key| identities.contains(&key)) {
      0
    } else {
      source_bytes
    };
    let requested_bytes = used
      .saturating_add(source_charge)
      .saturating_add(output_bytes);
    if ids.len() >= MAX_FILES || requested_bytes > MAX_OWNED_BYTES {
      log::debug!(
        "artifact admission rejected: ids={}, owned_bytes={used}, source_bytes={source_charge}, \
         output_bytes={output_bytes}",
        ids.len()
      );
      anyhow::bail!("artifact slots or physical byte admission exhausted");
    }
    if filesystem.available_space(Path::new(".")).await?
      < unmaterialized
        .saturating_add(output_bytes)
        .saturating_add(FREE_SPACE_HEADROOM)
    {
      anyhow::bail!("insufficient free space for staged payloads and metadata");
    }
    Ok(())
  }
}

async fn list_files(filesystem: &dyn FileSystem, directory: &Path) -> anyhow::Result<Vec<PathBuf>> {
  match filesystem.list_files(directory).await {
    // Listings include the SDK root, which may itself be relative. Filesystem operations need
    // SDK-relative paths so that root is joined exactly once.
    Ok(files) => files
      .into_iter()
      .map(|file| {
        let name = Path::new(&file)
          .file_name()
          .ok_or_else(|| anyhow::anyhow!("artifact store file has no filename"))?;
        Ok(directory.join(name))
      })
      .collect(),
    Err(error) if is_not_found(&error) => Ok(Vec::new()),
    Err(error) => Err(error),
  }
}

async fn file_metadata(
  filesystem: &dyn FileSystem,
  path: &Path,
) -> anyhow::Result<Option<Metadata>> {
  match filesystem.open_regular_file(path).await {
    Ok(file) => Ok(Some(file.metadata().await?)),
    // Workflow retention and completed-upload cleanup can only reduce the inventory.
    Err(error) if is_not_found(&error) => Ok(None),
    Err(error) => Err(error),
  }
}

fn is_not_found(error: &anyhow::Error) -> bool {
  error
    .downcast_ref::<io::Error>()
    .is_some_and(|error| error.kind() == io::ErrorKind::NotFound)
}
