// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::UploadSource;
use bd_client_common::file_system::FileSystem;
use std::fs::Metadata;
use std::io::Cursor;
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncSeekExt};

//
// Input
//

pub enum Input {
  Bytes(Cursor<Vec<u8>>),
  File {
    file: tokio::fs::File,
    original_len: u64,
    modified: SystemTime,
    size: u64,
  },
}

impl Input {
  pub(crate) fn from_file(file: tokio::fs::File, original: &Metadata) -> anyhow::Result<Self> {
    Ok(Self::File {
      file,
      original_len: original.len(),
      modified: original.modified()?,
      size: original.len(),
    })
  }

  pub(crate) async fn open(
    source: UploadSource,
    filesystem: &dyn FileSystem,
  ) -> anyhow::Result<Self> {
    let mut file = match source {
      UploadSource::Bytes(bytes) => return Ok(Self::Bytes(Cursor::new(bytes))),
      UploadSource::File(file) => tokio::fs::File::from_std(file),
      UploadSource::Path(path) => filesystem.open_regular_file(&path).await?,
      UploadSource::Retained(_) => {
        anyhow::bail!("retained payload already has a stored representation")
      },
    };
    let original = file.metadata().await?;
    if !original.is_file() {
      anyhow::bail!("attachment must be a regular file");
    }
    let size = original
      .len()
      .checked_sub(file.stream_position().await?)
      .ok_or_else(|| anyhow::anyhow!("attachment position exceeds file size"))?;
    Ok(Self::File {
      file,
      original_len: original.len(),
      modified: original.modified()?,
      size,
    })
  }

  pub(crate) fn size(&self) -> u64 {
    match self {
      Self::Bytes(bytes) => bytes.get_ref().len() as u64,
      Self::File { size, .. } => *size,
    }
  }

  pub(crate) fn reader(&mut self) -> &mut (dyn AsyncRead + Unpin + Send) {
    match self {
      Self::Bytes(bytes) => bytes,
      Self::File { file, .. } => file,
    }
  }

  pub(crate) async fn validate(&self) -> anyhow::Result<()> {
    if let Self::File {
      file,
      original_len,
      modified,
      ..
    } = self
    {
      let current = file.metadata().await?;
      if current.len() != *original_len || current.modified()? != *modified {
        anyhow::bail!("attachment changed during preparation");
      }
    }
    Ok(())
  }
}
