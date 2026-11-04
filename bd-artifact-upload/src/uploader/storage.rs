// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The filesystem projection of the two persisted artifact layouts.
//!
//! This is deliberately not another index or uploader. A whole-file entry owns one spool file,
//! except for workflow entries, which only borrow their canonical payload. A chunked entry owns its
//! descriptor-addressed files and, temporarily, a moved capture source. Defining those paths here
//! lets startup, periodic orphan reconciliation and retirement use the same ownership rules.
//!
//! Deletion is always downstream of index publication. If publishing/removing a lease fails, the
//! uploader stays dirty and suppresses orphan deletion until the shared index retry succeeds.

use super::chunked::{chunk_path, source_path};
use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  REPORT_INDEX_FILE,
  Uploader,
  WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID,
};
use bd_proto::protos::client::artifact::artifact_upload_index::{Artifact, ChunkedArtifact};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

//
// StoredArtifact
//

/// A borrowed view, not a third in-memory or persisted representation of artifact metadata.
pub(super) enum StoredArtifact<'a> {
  WholeFile(&'a Artifact),
  Chunked(&'a ChunkedArtifact),
}

impl StoredArtifact<'_> {
  pub(super) fn owned_paths(&self) -> anyhow::Result<Vec<PathBuf>> {
    match self {
      Self::WholeFile(entry) => {
        let path = Uploader::payload_path(entry)?;
        // A workflow lease protects storage owned by AttachmentStore. Removing the lease must not
        // unlink that payload: the store also tracks independent log references and retention.
        Ok(
          if entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID) {
            Vec::new()
          } else {
            vec![path]
          },
        )
      },
      Self::Chunked(entry) => {
        let request = entry
          .request
          .as_ref()
          .ok_or_else(|| anyhow::anyhow!("missing chunk request"))?;
        Ok(
          (0 .. request.chunks.len())
            .map(|index| chunk_path(&request.artifact_id, index))
            .chain(self.source_path())
            .collect(),
        )
      },
    }
  }

  fn source_path(&self) -> Option<PathBuf> {
    match self {
      Self::WholeFile(_) => None,
      Self::Chunked(entry) => entry
        .request
        .as_ref()
        .map(|request| source_path(&request.artifact_id)),
    }
  }
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
  fn stored_artifacts(&self) -> impl Iterator<Item = StoredArtifact<'_>> {
    self
      .index
      .iter()
      .map(StoredArtifact::WholeFile)
      .chain(self.chunked.entries.iter().map(StoredArtifact::Chunked))
  }

  pub(super) async fn delete_paths(&self, paths: &[PathBuf]) -> anyhow::Result<()> {
    // Try every known path rather than stopping at the first unlink failure. A failed unlink is
    // still visible to usage accounting and the next orphan scan; no synthetic credit is released.
    let mut first_error = None;
    for path in paths {
      if let Err(error) = self.file_system.delete_file(path).await {
        log::warn!(
          "failed to remove artifact spool file {}: {error}",
          path.display()
        );
        first_error.get_or_insert(error);
      }
    }
    first_error.map_or(Ok(()), Err)
  }

  pub(super) async fn clean_sources(&self) {
    // Only callers that know the index is durable may release moved capture sources. Payload
    // chunks remain owned until whole-artifact retirement, independently of per-chunk progress.
    let sources: Vec<_> = self
      .stored_artifacts()
      .filter_map(|entry| entry.source_path())
      .collect();
    if let Err(error) = self.delete_paths(&sources).await {
      log::warn!("failed to remove committed artifact sources; will retry: {error}");
    }
  }

  pub(super) async fn clean_orphan_files(&self) -> anyhow::Result<()> {
    let mut retained = HashSet::new();
    for entry in self.stored_artifacts() {
      retained.extend(entry.owned_paths()?);
    }
    retained.insert(ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE));
    let mut orphaned = Vec::new();
    for file in self
      .file_system
      .list_files(&ARTIFACT_UPLOAD_DIRECTORY)
      .await?
    {
      let path = Path::new(&file);
      let name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("spool file has no filename"))?;
      if !retained.contains(&ARTIFACT_UPLOAD_DIRECTORY.join(name)) {
        orphaned.push(path.to_owned());
      }
    }
    self.delete_paths(&orphaned).await
  }

  pub(super) async fn retire_files(&mut self, paths: &[PathBuf]) -> anyhow::Result<()> {
    // The caller has removed the entry from its in-memory collection. Persist that removal first;
    // on failure keep every byte, and let index_retry + orphan reconciliation finish retirement.
    // This same ordering covers success, intent rejection, eviction and chunk deadline expiry.
    self.write_index().await?;
    self.delete_paths(paths).await
  }
}
