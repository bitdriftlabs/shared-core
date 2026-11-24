// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Filesystem ownership and cleanup for the chunk spool.
//!
//! Deletion is always downstream of index publication. If publishing/removing a lease fails, the
//! uploader stays dirty and suppresses orphan deletion until the shared index retry succeeds.

use super::chunked::{chunk_path, source_path};
use super::{ARTIFACT_UPLOAD_DIRECTORY, REPORT_INDEX_FILE, Uploader};
use bd_proto::protos::client::api::UploadArtifactRequest;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub(super) fn owned_paths(request: &UploadArtifactRequest) -> impl Iterator<Item = PathBuf> + '_ {
  (0 .. request.chunks.len())
    .map(|index| chunk_path(&request.artifact_id, index))
    .chain(std::iter::once(source_path(&request.artifact_id)))
}

//
// IndexWriteError
//

/// A successful rename is an ownership boundary even when the subsequent durability check fails.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub(super) struct IndexWriteError {
  pub(super) published: bool,
  #[source]
  pub(super) error: anyhow::Error,
}

impl Uploader {
  pub(super) async fn delete_paths(&mut self, paths: &[PathBuf]) -> anyhow::Result<()> {
    // Try every known path rather than stopping at the first unlink failure. A failed unlink is
    // still visible to usage accounting and the next orphan scan; no synthetic credit is released.
    let mut first_error = None;
    for path in paths {
      if let Err(error) = self.file_system.delete_file(path).await {
        log::warn!(
          "failed to remove artifact spool file {}: {error}",
          path.display()
        );
        self.cleanup_pending = true;
        first_error.get_or_insert(error);
      }
    }
    first_error.map_or(Ok(()), Err)
  }

  pub(super) async fn clean_sources(&mut self) {
    // Only callers that know the index is durable may release moved capture sources. Payload
    // chunks remain owned until whole-artifact retirement, independently of per-chunk progress.
    let sources: Vec<_> = self
      .chunked
      .entries
      .iter()
      .map(|entry| source_path(&entry.artifact_id))
      .collect();
    if let Err(error) = self.delete_paths(&sources).await {
      log::warn!("failed to remove committed artifact sources; will retry: {error}");
    }
  }

  pub(super) async fn reconcile_files(&mut self) {
    self.cleanup_pending = false;
    self.clean_sources().await;
    if let Err(error) = self.clean_orphan_files().await {
      self.cleanup_pending = true;
      log::warn!("failed to remove orphaned artifact payloads; will retry: {error}");
    }
  }

  async fn clean_orphan_files(&mut self) -> anyhow::Result<()> {
    let mut retained: HashSet<_> = self.chunked.entries.iter().flat_map(owned_paths).collect();
    retained.insert(ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE));
    let mut orphaned = Vec::new();
    for file in self
      .file_system
      .list_files(&ARTIFACT_UPLOAD_DIRECTORY)
      .await?
    {
      let name = Path::new(&file)
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("spool file has no filename"))?;
      let path = ARTIFACT_UPLOAD_DIRECTORY.join(name);
      if !retained.contains(&path) {
        orphaned.push(path);
      }
    }
    self.delete_paths(&orphaned).await
  }

  pub(super) async fn retire_files(&mut self, paths: &[PathBuf]) -> anyhow::Result<()> {
    // The caller has removed the entry from its in-memory collection. Persist that removal first;
    // on failure keep every byte, and let index_retry + orphan reconciliation finish retirement.
    // This same ordering covers success, admission denial and explicit chunk rejection.
    if let Err(error) = self.write_index().await {
      self.cleanup_pending = true;
      return Err(error.into());
    }
    self.delete_paths(paths).await
  }
}
