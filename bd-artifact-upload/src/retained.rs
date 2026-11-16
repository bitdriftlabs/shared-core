// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Payload retention uses the same admission owner as upload staging. The logger supplies
//! lifecycle metadata and retirement decisions, but does not prepare or account for payloads.

use crate::input::Input;
use crate::{ArtifactStore, UploadSource};
use bd_client_common::file::{
  async_write_checksummed_data,
  read_and_compress_limited_with_size,
  write_checksummed_data,
};
use bd_client_common::file_system::FileSystem;
use bd_proto::protos::client::artifact::WorkflowAttachmentState;
use protobuf::Message;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

impl ArtifactStore {
  pub async fn retain(
    &self,
    filesystem: &dyn FileSystem,
    source: UploadSource,
    id: Uuid,
    metadata: &WorkflowAttachmentState,
  ) -> anyhow::Result<u64> {
    let _admission = Self::lock().await?;
    let mut input = Input::open(source, filesystem).await?;
    let size = input.size();
    if size == 0 {
      anyhow::bail!("empty workflow artifact");
    }
    let chunked = self.should_chunk(size)?;
    let reservation = if chunked {
      size.saturating_mul(2) + 4
    } else {
      size + 4096
    };
    self
      .check_capacity(filesystem, size, 0, None, reservation + 1028)
      .await?;
    let directory = Path::new("workflow-attachments");
    let pending = directory.join(format!(".{id}.partial"));
    let target = directory.join(format!("{id}.{}", if chunked { "raw" } else { "payload" }));
    let marker = directory.join(format!("{id}.metadata"));
    let marker_partial = marker.with_extension("metadata.partial");
    let bytes = metadata.write_to_bytes()?;
    if bytes.len() + 13 > 1024 {
      anyhow::bail!("workflow attachment metadata exceeds size limit");
    }
    let result = async {
      let mut output = filesystem.create_file(&pending).await?;
      let logical_bytes = if chunked {
        async_write_checksummed_data(input.reader().take(size + 1), &mut output).await?;
        if output.metadata().await?.len() != size + 4 {
          anyhow::bail!("attachment changed during retention");
        }
        size
      } else {
        let (encoded, size) =
          read_and_compress_limited_with_size(input.reader(), self.whole_file_limit()).await?;
        output.write_all(&encoded).await?;
        size
      };
      input.validate().await?;
      output.flush().await?;
      filesystem.sync_file_and_parent(&pending).await?;
      filesystem
        .write_file(&marker_partial, &write_checksummed_data(&bytes))
        .await?;
      filesystem.sync_file_and_parent(&marker_partial).await?;
      filesystem.rename_file(&marker_partial, &marker).await?;
      filesystem.sync_file_and_parent(&marker).await?;
      filesystem.rename_file(&pending, &target).await?;
      filesystem.sync_file_and_parent(&target).await?;
      log::debug!("retained artifact {id}: logical_bytes={logical_bytes}, chunked={chunked}");
      Ok(logical_bytes)
    }
    .await;
    if result.is_err() {
      for path in [&pending, &target, &marker, &marker_partial] {
        if let Err(error) = filesystem.delete_file(path).await {
          log::warn!(
            "failed to roll back retained artifact {}: {error}",
            path.display()
          );
        }
      }
    }
    result
  }

  pub async fn remove_retained_payload(
    &self,
    filesystem: &dyn FileSystem,
    id: Uuid,
    allow_missing: bool,
  ) -> anyhow::Result<()> {
    let mut removed = false;
    for extension in ["payload", "raw"] {
      let path = PathBuf::from(format!("workflow-attachments/{id}.{extension}"));
      if filesystem.exists(&path).await? {
        filesystem.open_regular_file(&path).await?;
        filesystem.delete_file(&path).await?;
        removed = true;
      }
    }
    if !removed && !allow_missing {
      return Err(
        std::io::Error::new(
          std::io::ErrorKind::NotFound,
          "missing workflow attachment payload",
        )
        .into(),
      );
    }
    filesystem
      .sync_file_and_parent(Path::new("workflow-attachments"))
      .await?;
    Ok(())
  }
}
