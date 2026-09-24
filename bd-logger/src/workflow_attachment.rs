// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./workflow_attachment_test.rs"]
mod tests;

use bd_artifact_upload::UploadSource;
use bd_client_common::file::read_and_compress_limited;
use bd_client_common::file_system::delete_file_if_exists_async;
use bd_runtime::runtime::{ConfigLoader, IntWatch, attachment, workflow_attachment};
use bd_shutdown::ComponentShutdown;
use bd_time::OffsetDateTimeExt;
use parking_lot::Mutex;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::{OnceCell, Semaphore};
use tokio::time::sleep;
use uuid::Uuid;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

//
// Capacity
//

#[derive(Default)]
struct Capacity {
  bytes: u64,
  files: usize,
}

//
// AttachmentStoreHandle
//

#[derive(Clone)]
pub struct AttachmentStoreHandle {
  sdk_directory: PathBuf,
  runtime: Arc<ConfigLoader>,
  store: Arc<OnceCell<Arc<AttachmentStore>>>,
}

impl AttachmentStoreHandle {
  pub fn new(sdk_directory: PathBuf, runtime: Arc<ConfigLoader>) -> Self {
    Self {
      sdk_directory,
      runtime,
      store: Arc::new(OnceCell::new()),
    }
  }

  pub async fn get(&self) -> io::Result<Arc<AttachmentStore>> {
    self
      .store
      .get_or_try_init(|| async {
        AttachmentStore::new(&self.sdk_directory, &self.runtime)
          .await
          .map(Arc::new)
      })
      .await
      .map(Arc::clone)
  }
}

//
// WorkflowAttachmentCleanupWorker
//

pub struct WorkflowAttachmentCleanupWorker {
  store_handle: AttachmentStoreHandle,
  retention_registry: Arc<bd_state::RetentionRegistry>,
  previous_cleanup_key: Option<(Option<u64>, u64)>,
}

impl WorkflowAttachmentCleanupWorker {
  pub fn new(
    store_handle: AttachmentStoreHandle,
    retention_registry: Arc<bd_state::RetentionRegistry>,
  ) -> Self {
    Self {
      store_handle,
      retention_registry,
      previous_cleanup_key: None,
    }
  }

  pub async fn run(mut self, mut shutdown: ComponentShutdown) {
    loop {
      self.cleanup_once().await;
      tokio::select! {
        () = sleep(CLEANUP_INTERVAL) => {},
        () = shutdown.cancelled() => return,
      }
    }
  }

  async fn cleanup_once(&mut self) -> bool {
    let retention = self.retention_registry.min_retention_timestamp().await;
    let store = match self.store_handle.get().await {
      Ok(store) => store,
      Err(error) => {
        log::warn!("failed to open workflow attachment store for cleanup: {error}");
        return false;
      },
    };
    let cleanup_key = (retention, store.cleanup_generation());
    if self.previous_cleanup_key == Some(cleanup_key) {
      return false;
    }
    let cleanup_result = match retention {
      Some(cutoff_micros) => store.cleanup_before(cutoff_micros).await,
      None => store.cleanup_all().await,
    };
    if let Err(error) = cleanup_result {
      log::warn!("failed to clean up expired workflow attachments: {error}");
      return false;
    }
    self.previous_cleanup_key = Some(cleanup_key);
    true
  }
}

//
// AttachmentStore
//

pub struct AttachmentStore {
  sdk_directory: PathBuf,
  directory: PathBuf,
  capacity: Mutex<Capacity>,
  oldest_timestamp: Mutex<Option<u64>>,
  cleanup_generation: AtomicU64,
  admissions: Semaphore,
  max_attachment_bytes: IntWatch<attachment::MaxBytes>,
  max_owned_bytes: IntWatch<workflow_attachment::MaxOwnedBytes>,
  max_owned_files: IntWatch<workflow_attachment::MaxOwnedFiles>,
}

//
// AdmittedAttachment
//

pub struct AdmittedAttachment {
  pub id: Uuid,
}

impl AttachmentStore {
  pub async fn new(sdk_directory: &Path, runtime: &ConfigLoader) -> io::Result<Self> {
    let directory = sdk_directory.join("workflow-attachments");
    fs::create_dir_all(&directory).await?;
    let mut capacity = Capacity::default();
    let mut oldest_timestamp: Option<u64> = None;
    let mut recovered_sidecar = false;
    let mut pending_ownership = Vec::new();
    let mut entries = fs::read_dir(&directory).await?;
    while let Some(entry) = entries.next_entry().await? {
      let entry_path = entry.path();
      if entry
        .path()
        .extension()
        .is_some_and(|extension| extension == "partial")
        && entry
          .path()
          .file_stem()
          .and_then(|stem| stem.to_str())
          .and_then(|stem| stem.strip_prefix('.'))
          .is_some_and(|stem| Uuid::parse_str(stem).is_ok())
      {
        fs::remove_file(entry_path).await?;
        log::debug!("removed interrupted workflow attachment admission");
        continue;
      }
      if let Some(sidecar_path) = sidecar_staging_target(&entry_path) {
        if !fs::symlink_metadata(&entry_path).await?.is_file() {
          return Err(io::Error::other("invalid workflow attachment sidecar"));
        }
        if fs::try_exists(&sidecar_path).await? {
          fs::remove_file(entry_path).await?;
        } else {
          fs::rename(entry_path, &sidecar_path).await?;
        }
        recovered_sidecar = true;
        log::debug!("recovered interrupted workflow attachment sidecar write");
        continue;
      }
      if entry_path
        .extension()
        .is_some_and(|extension| extension == "pending")
        && entry_path
          .file_stem()
          .and_then(|stem| stem.to_str())
          .is_some_and(|stem| Uuid::parse_str(stem).is_ok())
      {
        if !fs::symlink_metadata(&entry_path).await?.is_file() {
          return Err(io::Error::other(
            "invalid workflow attachment ownership marker",
          ));
        }
        pending_ownership.push(entry_path);
        continue;
      }
      if entry_path
        .extension()
        .is_some_and(|extension| extension == "timestamp")
      {
        let timestamp = read_timestamp(&entry_path).await?;
        oldest_timestamp = Some(oldest_timestamp.map_or(timestamp, |oldest| oldest.min(timestamp)));
      }
      if entry_path
        .extension()
        .is_none_or(|extension| extension != "payload")
      {
        continue;
      }
      if entry
        .path()
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_none_or(|stem| Uuid::parse_str(stem).is_err())
      {
        return Err(io::Error::new(
          io::ErrorKind::InvalidData,
          "invalid attachment name",
        ));
      }
      let metadata = fs::symlink_metadata(entry_path).await?;
      if !metadata.is_file() {
        return Err(io::Error::new(
          io::ErrorKind::InvalidData,
          "invalid attachment file",
        ));
      }
      capacity.bytes = capacity.bytes.saturating_add(metadata.len());
      capacity.files = capacity.files.saturating_add(1);
    }
    for marker_path in pending_ownership {
      let id = marker_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| Uuid::parse_str(stem).ok())
        .ok_or_else(|| io::Error::other("invalid workflow attachment ownership marker"))?;
      let timestamp_path = directory.join(format!("{id}.timestamp"));
      if fs::try_exists(&timestamp_path).await? {
        fs::remove_file(marker_path).await?;
      } else if fs::try_exists(directory.join(format!("{id}.payload"))).await? {
        // An admitted attachment may outlive a crash before its outcome log timestamp is recorded.
        // Use its durable admission time as a best-effort retirement fallback after restart.
        let timestamp = read_timestamp(&marker_path).await?;
        fs::rename(marker_path, &timestamp_path).await?;
        oldest_timestamp = Some(oldest_timestamp.map_or(timestamp, |oldest| oldest.min(timestamp)));
        log::warn!("recovered workflow attachment {id} with its admission timestamp");
      } else {
        fs::remove_file(marker_path).await?;
      }
      recovered_sidecar = true;
    }
    if recovered_sidecar {
      File::open(&directory).await?.sync_all().await?;
    }
    Ok(Self {
      sdk_directory: sdk_directory.to_owned(),
      directory,
      capacity: Mutex::new(capacity),
      oldest_timestamp: Mutex::new(oldest_timestamp),
      cleanup_generation: AtomicU64::new(0),
      admissions: Semaphore::new(1),
      max_attachment_bytes: runtime.register_int_watch(),
      max_owned_bytes: runtime.register_int_watch(),
      max_owned_files: runtime.register_int_watch(),
    })
  }

  pub async fn admit(self: &Arc<Self>, source: UploadSource) -> io::Result<AdmittedAttachment> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    self.admit_async(source).await
  }

  pub async fn release(self: &Arc<Self>, id: Uuid) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    self.release_async(id).await
  }

  async fn release_async(&self, id: Uuid) -> io::Result<()> {
    self.remove_payload_async(id, false).await?;
    delete_file_if_exists_async(&self.timestamp_path(id))
      .await
      .map_err(io::Error::other)?;
    delete_file_if_exists_async(&self.uploaded_path(id))
      .await
      .map_err(io::Error::other)?;
    delete_file_if_exists_async(&self.pending_path(id))
      .await
      .map_err(io::Error::other)?;
    File::open(&self.directory).await?.sync_all().await?;
    log::debug!("released workflow attachment {id}");
    Ok(())
  }

  /// Records the event timestamp used by the first-version, best-effort retirement policy.
  pub async fn record_timestamp(
    self: &Arc<Self>,
    id: Uuid,
    occurred_at: OffsetDateTime,
  ) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    let micros = u64::try_from(occurred_at.unix_timestamp_micros()).map_err(io::Error::other)?;
    write_sidecar(&self.timestamp_path(id), micros.to_string().as_bytes()).await?;
    delete_file_if_exists_async(&self.pending_path(id))
      .await
      .map_err(io::Error::other)?;
    File::open(&self.directory).await?.sync_all().await?;
    let mut oldest_timestamp = self.oldest_timestamp.lock();
    *oldest_timestamp = Some(oldest_timestamp.map_or(micros, |oldest| oldest.min(micros)));
    self.cleanup_generation.fetch_add(1, Ordering::Release);
    Ok(())
  }

  /// Marks an artifact uploaded and drops the retained payload while preserving timestamp metadata.
  pub async fn complete_upload(self: &Arc<Self>, id: Uuid) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    write_sidecar(&self.uploaded_path(id), b"uploaded").await?;
    self.remove_payload_async(id, true).await?;
    log::debug!("released uploaded workflow attachment payload {id}");
    Ok(())
  }

  pub async fn is_uploaded(&self, id: Uuid) -> io::Result<bool> {
    fs::try_exists(self.uploaded_path(id)).await
  }

  fn cleanup_generation(&self) -> u64 {
    self.cleanup_generation.load(Ordering::Acquire)
  }

  /// Retires timestamped attachment state older than the ring-buffer retention watermark.
  ///
  /// This first version assumes log timestamps advance with ring-buffer append and retirement
  /// order. Out-of-order or replayed timestamps may therefore retain data too long or delete it
  /// too early. TODO: replace timestamp retirement with exact per-buffer append ownership.
  pub async fn cleanup_before(&self, cutoff_micros: u64) -> io::Result<()> {
    if self
      .oldest_timestamp
      .lock()
      .is_none_or(|oldest| oldest >= cutoff_micros)
    {
      return Ok(());
    }
    self.cleanup(Some(cutoff_micros)).await
  }

  /// Retires all timestamped attachment state when no log buffers still retain it.
  pub async fn cleanup_all(&self) -> io::Result<()> {
    if self.oldest_timestamp.lock().is_none() {
      return Ok(());
    }
    self.cleanup(None).await
  }

  async fn cleanup(&self, cutoff_micros: Option<u64>) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    let mut entries = fs::read_dir(&self.directory).await?;
    let mut oldest_timestamp: Option<u64> = None;
    while let Some(entry) = entries.next_entry().await? {
      let path = entry.path();
      if path
        .extension()
        .is_none_or(|extension| extension != "timestamp")
      {
        continue;
      }
      let Some(id) = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| Uuid::parse_str(stem).ok())
      else {
        log::warn!(
          "invalid workflow attachment timestamp sidecar {}",
          path.display()
        );
        continue;
      };
      let timestamp = read_timestamp(&path).await?;
      if cutoff_micros.is_some_and(|cutoff_micros| timestamp >= cutoff_micros) {
        oldest_timestamp = Some(oldest_timestamp.map_or(timestamp, |oldest| oldest.min(timestamp)));
        continue;
      }
      self.remove_payload_async(id, true).await?;
      delete_file_if_exists_async(&self.uploaded_path(id))
        .await
        .map_err(io::Error::other)?;
      delete_file_if_exists_async(&path)
        .await
        .map_err(io::Error::other)?;
      if let Some(cutoff_micros) = cutoff_micros {
        log::debug!("retired workflow attachment {id} before timestamp {cutoff_micros}");
      } else {
        log::debug!("retired workflow attachment {id} with no retention requirement");
      }
    }
    File::open(&self.directory).await?.sync_all().await?;
    *self.oldest_timestamp.lock() = oldest_timestamp;
    Ok(())
  }

  fn payload_path(&self, id: Uuid) -> PathBuf {
    self.directory.join(format!("{id}.payload"))
  }

  fn timestamp_path(&self, id: Uuid) -> PathBuf {
    self.directory.join(format!("{id}.timestamp"))
  }

  fn uploaded_path(&self, id: Uuid) -> PathBuf {
    self.directory.join(format!("{id}.uploaded"))
  }

  fn pending_path(&self, id: Uuid) -> PathBuf {
    self.directory.join(format!("{id}.pending"))
  }

  async fn remove_payload_async(&self, id: Uuid, allow_missing: bool) -> io::Result<()> {
    let path = self.payload_path(id);
    let metadata = match fs::symlink_metadata(&path).await {
      Ok(metadata) => metadata,
      Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => return Ok(()),
      Err(error) => return Err(error),
    };
    if !metadata.is_file() {
      return Err(io::Error::other(
        "workflow attachment is not a regular file",
      ));
    }
    let (remaining_bytes, remaining_files) = {
      let capacity = self.capacity.lock();
      let bytes = capacity.bytes.checked_sub(metadata.len());
      let files = capacity.files.checked_sub(1);
      match (bytes, files) {
        (Some(bytes), Some(files)) => (bytes, files),
        _ => {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "workflow attachment capacity mismatch",
          ));
        },
      }
    };
    fs::remove_file(path).await?;
    {
      let mut capacity = self.capacity.lock();
      capacity.bytes = remaining_bytes;
      capacity.files = remaining_files;
    }
    File::open(&self.directory).await?.sync_all().await?;
    Ok(())
  }

  async fn admit_async(&self, source: UploadSource) -> io::Result<AdmittedAttachment> {
    let (current_bytes, current_files) = {
      let capacity = self.capacity.lock();
      (capacity.bytes, capacity.files)
    };
    let max_attachment_bytes = u64::from(*self.max_attachment_bytes.read());
    let max_owned_bytes = u64::from(*self.max_owned_bytes.read());
    let max_owned_files = usize::try_from(*self.max_owned_files.read()).unwrap_or(usize::MAX);
    if current_files >= max_owned_files {
      return Err(io::Error::other("workflow attachment capacity exhausted"));
    }

    let reader: Box<dyn AsyncRead + Unpin + Send> = match source {
      UploadSource::Bytes(bytes) => {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_attachment_bytes {
          return Err(io::Error::other("workflow attachment exceeds size limit"));
        }
        Box::new(io::Cursor::new(bytes))
      },
      UploadSource::File(file) => {
        let file = tokio::fs::File::from_std(file);
        validate_file(&file, max_attachment_bytes).await?;
        Box::new(file)
      },
      UploadSource::Path(path) => {
        let path = if path.is_absolute() {
          path
        } else {
          self.sdk_directory.join(path)
        };
        let file = open_regular_file_async(&path).await?;
        validate_file(&file, max_attachment_bytes).await?;
        Box::new(file)
      },
      UploadSource::Retained(_) => {
        return Err(io::Error::other(
          "cannot admit an already retained attachment",
        ));
      },
    };

    let id = Uuid::new_v4();
    let pending = self.directory.join(format!(".{id}.partial"));
    let path = self.directory.join(format!("{id}.payload"));
    let ownership_marker = self.pending_path(id);
    let result: io::Result<AdmittedAttachment> = async {
      let compressed = read_and_compress_limited(reader, max_attachment_bytes)
        .await
        .map_err(io::Error::other)?;
      if compressed.len() as u64 > max_owned_bytes.saturating_sub(current_bytes) {
        return Err(io::Error::other("workflow attachment capacity exhausted"));
      }
      let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .await?;
      output.write_all(&compressed).await?;
      output.sync_all().await?;
      let admission_micros = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_micros())
        .map_err(io::Error::other)?;
      write_sidecar(&ownership_marker, admission_micros.to_string().as_bytes()).await?;
      // Publish only after verifying the staged bytes; a crash before rename leaves no visible ID.
      tokio::fs::rename(&pending, &path).await?;
      tokio::fs::File::open(&self.directory)
        .await?
        .sync_all()
        .await?;
      let mut capacity = self.capacity.lock();
      capacity.bytes += compressed.len() as u64;
      capacity.files += 1;
      log::debug!(
        "admitted workflow attachment {id} ({} bytes)",
        compressed.len()
      );
      Ok(AdmittedAttachment { id })
    }
    .await;
    if result.is_err() {
      let _ = tokio::fs::remove_file(&pending).await;
      let _ = tokio::fs::remove_file(&path).await;
      let _ = tokio::fs::remove_file(&ownership_marker).await;
    }
    result
  }
}

async fn write_sidecar(path: &Path, contents: &[u8]) -> io::Result<()> {
  let staging_path = sidecar_staging_path(path)?;
  fs::write(&staging_path, contents).await?;
  File::open(&staging_path).await?.sync_all().await?;
  fs::rename(staging_path, path).await?;
  let directory = path
    .parent()
    .ok_or_else(|| io::Error::other("sidecar has no parent"))?;
  File::open(directory).await?.sync_all().await
}

fn sidecar_staging_path(path: &Path) -> io::Result<PathBuf> {
  let extension = path
    .extension()
    .and_then(|extension| extension.to_str())
    .ok_or_else(|| io::Error::other("workflow attachment sidecar has no extension"))?;
  Ok(path.with_extension(format!("{extension}.partial")))
}

fn sidecar_staging_target(path: &Path) -> Option<PathBuf> {
  let file_name = path.file_name()?.to_str()?;
  let (stem, extension) = file_name.strip_suffix(".partial")?.rsplit_once('.')?;
  if !matches!(extension, "pending" | "timestamp" | "uploaded") || Uuid::parse_str(stem).is_err() {
    return None;
  }
  Some(path.with_file_name(format!("{stem}.{extension}")))
}

async fn read_timestamp(path: &Path) -> io::Result<u64> {
  fs::read_to_string(path)
    .await?
    .trim()
    .parse::<u64>()
    .map_err(|error| {
      io::Error::other(format!(
        "invalid workflow attachment timestamp {}: {error}",
        path.display()
      ))
    })
}

async fn open_regular_file_async(path: &Path) -> io::Result<tokio::fs::File> {
  if !tokio::fs::symlink_metadata(path).await?.is_file() {
    return Err(io::Error::other(
      "workflow attachment must be a regular file",
    ));
  }
  let mut options = tokio::fs::OpenOptions::new();
  options.read(true);
  #[cfg(unix)]
  options.custom_flags(libc::O_NOFOLLOW);
  let file = options.open(path).await?;
  if !file.metadata().await?.is_file() {
    return Err(io::Error::other(
      "workflow attachment must be a regular file",
    ));
  }
  Ok(file)
}

async fn validate_file(file: &tokio::fs::File, max_attachment_bytes: u64) -> io::Result<()> {
  let metadata = file.metadata().await?;
  if !metadata.is_file() || metadata.len() > max_attachment_bytes {
    return Err(io::Error::other(
      "workflow attachment must be a regular file within size limit",
    ));
  }
  Ok(())
}
