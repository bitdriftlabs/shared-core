// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::chunked::{MAX_INDEX_BYTES, file_identity, read_index, safe_id};
use super::{ARTIFACT_UPLOAD_DIRECTORY, REPORT_INDEX_FILE};
use bd_client_common::artifact::MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file_system::FileSystem;
use bd_runtime::runtime::{BoolWatch, ConfigLoader, IntWatch, chunked_artifact_upload};
use std::collections::{HashMap, HashSet};
use std::fs::Metadata;
use std::io;
use std::path::Path;
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

const MAX_FILES: usize = 2;
const MAX_OWNED_BYTES: u64 = 640 * 1024 * 1024;
const FREE_SPACE_HEADROOM: u64 = MAX_INDEX_BYTES + 1024 * 1024;

// Retained-file admission and uploader preparation must not race their filesystem inventories.
// The permit carries no ownership state; durable files and the existing index remain authoritative.
static PREPARATION: Semaphore = Semaphore::const_new(1);

//
// ChunkedFileAdmission
//

pub struct ChunkedFileAdmission {
  enabled: BoolWatch<chunked_artifact_upload::Enabled>,
  workflow_enabled: BoolWatch<chunked_artifact_upload::WorkflowEnabled>,
  max_logical_bytes: IntWatch<chunked_artifact_upload::MaxLogicalBytes>,
  max_owned_bytes: IntWatch<chunked_artifact_upload::MaxOwnedPayloadBytes>,
  max_files: IntWatch<chunked_artifact_upload::MaxAdmittedFiles>,
}

impl ChunkedFileAdmission {
  #[must_use]
  pub fn new(runtime: &ConfigLoader) -> Self {
    Self {
      enabled: runtime.register_bool_watch(),
      workflow_enabled: runtime.register_bool_watch(),
      max_logical_bytes: runtime.register_int_watch(),
      max_owned_bytes: runtime.register_int_watch(),
      max_files: runtime.register_int_watch(),
    }
  }

  #[must_use]
  pub fn workflow_enabled(&self) -> bool {
    *self.enabled.read() && *self.workflow_enabled.read()
  }

  pub(super) async fn lock() -> anyhow::Result<SemaphorePermit<'static>> {
    Ok(PREPARATION.acquire().await?)
  }

  /// Holds shared admission until a checksummed retained file has been durably published.
  pub async fn admit_workflow(
    &self,
    filesystem: &dyn FileSystem,
    logical_bytes: u64,
  ) -> anyhow::Result<SemaphorePermit<'static>> {
    let permit = Self::lock().await?;
    if !self.workflow_enabled() || logical_bytes == 0 {
      anyhow::bail!("chunked workflow admission disabled or empty");
    }
    self
      .check_capacity(filesystem, logical_bytes, logical_bytes + 4, None)
      .await?;
    Ok(permit)
  }

  pub(super) async fn admit_capture(
    &self,
    filesystem: &dyn FileSystem,
    metadata: &Metadata,
    workflow: bool,
  ) -> anyhow::Result<()> {
    if !*self.enabled.read() || (workflow && !*self.workflow_enabled.read()) {
      anyhow::bail!("chunked artifact admission disabled");
    }
    self
      .check_capacity(
        filesystem,
        metadata.len(),
        metadata.len(),
        file_identity(metadata),
      )
      .await
  }

  async fn check_capacity(
    &self,
    filesystem: &dyn FileSystem,
    logical_bytes: u64,
    source_bytes: u64,
    source_identity: Option<(u64, u64)>,
  ) -> anyhow::Result<()> {
    let logical_limit =
      u64::from((*self.max_logical_bytes.read()).min(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES));
    let owned_limit = u64::from(*self.max_owned_bytes.read()).min(MAX_OWNED_BYTES);
    let files_limit = (*self.max_files.read() as usize).min(MAX_FILES);
    if logical_bytes > logical_limit {
      anyhow::bail!("chunked artifact logical byte admission exhausted");
    }
    let mut ids = HashSet::new();
    let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    match filesystem.open_regular_file(&index_path).await {
      Ok(mut file) => {
        for entry in read_index(&mut file).await?.chunked_artifact {
          let request = entry
            .request
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing chunk request"))?;
          ids.insert(safe_id(&request.artifact_id)?);
        }
      },
      Err(error) if is_not_found(&error) => {},
      Err(error) => return Err(error),
    }
    let mut identities = HashSet::new();
    let mut used = 0_u64;
    let mut staged = HashMap::<Uuid, u64>::new();
    for path in list_files(filesystem, &ARTIFACT_UPLOAD_DIRECTORY).await? {
      let path = Path::new(&path);
      let Some(name) = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("chunk-"))
      else {
        continue;
      };
      let Some(metadata) = file_metadata(filesystem, path).await? else {
        continue;
      };
      if file_identity(&metadata).is_none_or(|key| identities.insert(key)) {
        used = used.saturating_add(metadata.len());
        if let Some(id) = name.get(.. 36).and_then(|id| safe_id(id).ok()) {
          *staged.entry(id).or_default() += metadata.len();
        }
      }
    }
    for path in list_files(filesystem, Path::new("workflow-attachments")).await? {
      let path = Path::new(&path);
      if path.extension().is_none_or(|extension| extension != "raw") {
        continue;
      }
      let Some(id) = path
        .file_stem()
        .and_then(|id| id.to_str())
        .and_then(|id| safe_id(id).ok())
      else {
        continue;
      };
      let Some(metadata) = file_metadata(filesystem, path).await? else {
        continue;
      };
      let logical = metadata
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow::anyhow!("invalid retained workflow file"))?;
      if file_identity(&metadata).is_none_or(|key| identities.insert(key)) {
        used = used.saturating_add(metadata.len());
      }
      if !ids.contains(&id) {
        // Before handoff, reserve RAW output as well as the retained source. Partial chunks from
        // failed preparation consume that reservation, not a second copy of the same headroom.
        used = used.saturating_add(logical.saturating_sub(*staged.get(&id).unwrap_or(&0)));
      }
      ids.insert(id);
    }
    // TODO: Account for pre-attribution private profile storage in the shared byte budget.
    // It is separately bounded today and only enters this inventory at uploader handoff.
    let source_charge = if source_identity.is_some_and(|key| identities.contains(&key)) {
      0
    } else {
      source_bytes
    };
    if ids.len() >= files_limit
      || used
        .saturating_add(source_charge)
        .saturating_add(logical_bytes)
        > owned_limit
    {
      anyhow::bail!("chunked artifact slots or physical byte admission exhausted");
    }
    if filesystem.available_space(Path::new(".")).await?
      < logical_bytes.saturating_add(FREE_SPACE_HEADROOM)
    {
      anyhow::bail!("insufficient free space for staged payloads and metadata");
    }
    Ok(())
  }
}

async fn list_files(filesystem: &dyn FileSystem, directory: &Path) -> anyhow::Result<Vec<String>> {
  match filesystem.list_files(directory).await {
    Ok(files) => Ok(files),
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
