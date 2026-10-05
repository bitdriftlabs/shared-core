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
use bd_client_common::file::{
  read_and_compress_limited_with_size,
  read_checksummed_data,
  write_checksummed_data,
};
use bd_client_common::file_system::{delete_file_if_exists_async, remove_dir_if_exists_async};
use bd_proto::protos::client::artifact::WorkflowAttachmentState;
use bd_runtime::runtime::{ConfigLoader, IntWatch, attachment, workflow_attachment};
use bd_shutdown::ComponentShutdown;
use bd_time::OffsetDateTimeExt;
use bd_workflows::workflow::CommandArtifactMetadata;
use parking_lot::Mutex;
use protobuf::Message;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OnceCell, Semaphore};
use tokio::time::sleep;
use uuid::Uuid;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SIDECAR_BYTES: u64 = 1024;
// The timestamp needs at most 11 bytes (tag + uint64 varint), and the upload flag needs 2.
const MAX_LIFECYCLE_METADATA_BYTES: u64 = 13;

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
  cleanup_ready: Arc<AtomicBool>,
  previous_cleanup_key: Option<(Option<u64>, u64)>,
}

impl WorkflowAttachmentCleanupWorker {
  pub fn new(
    store_handle: AttachmentStoreHandle,
    retention_registry: Arc<bd_state::RetentionRegistry>,
    cleanup_ready: Arc<AtomicBool>,
  ) -> Self {
    Self {
      store_handle,
      retention_registry,
      cleanup_ready,
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
    if !self.cleanup_ready.load(Ordering::Acquire) {
      return false;
    }
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
  pub artifact_metadata: CommandArtifactMetadata,
}

impl AttachmentStore {
  pub async fn new(sdk_directory: &Path, runtime: &ConfigLoader) -> io::Result<Self> {
    let directory = sdk_directory.join("workflow-attachments");
    fs::create_dir_all(&directory).await?;
    let mut capacity = Capacity::default();
    let mut oldest_timestamp: Option<u64> = None;
    let mut recovered_sidecar = false;
    let mut metadata_paths = Vec::new();
    let mut payload_ids = HashSet::new();
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
        remove_owned_path_if_exists(&entry_path).await?;
        recovered_sidecar = true;
        log::debug!("removed interrupted workflow attachment admission");
        continue;
      }
      if let Some(sidecar_path) = sidecar_staging_target(&entry_path) {
        match fs::symlink_metadata(&sidecar_path).await {
          Ok(_) => remove_owned_path_if_exists(&entry_path).await?,
          Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Err(error) = read_metadata(&entry_path).await {
              discard_corrupt_sidecar(&entry_path, error).await?;
              recovered_sidecar = true;
              continue;
            }
            fs::rename(entry_path, &sidecar_path).await?;
          },
          Err(error) => return Err(error),
        }
        metadata_paths.push(sidecar_path);
        recovered_sidecar = true;
        log::debug!("recovered interrupted workflow attachment sidecar write");
        continue;
      }
      if entry_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension == "metadata")
        && owned_attachment_id(&entry_path).is_some()
      {
        metadata_paths.push(entry_path);
        continue;
      }
      if entry_path
        .extension()
        .is_none_or(|extension| extension != "payload")
      {
        continue;
      }
      let Some(id) = owned_attachment_id(&entry_path) else {
        remove_owned_path_if_exists(&entry_path).await?;
        recovered_sidecar = true;
        log::warn!("discarded workflow attachment with an invalid name");
        continue;
      };
      let metadata = fs::symlink_metadata(&entry_path).await?;
      if !metadata.is_file() {
        remove_owned_path_if_exists(&entry_path).await?;
        recovered_sidecar = true;
        log::warn!("discarded nonregular workflow attachment payload {id}");
        continue;
      }
      payload_ids.insert(id);
      capacity.bytes = capacity.bytes.saturating_add(metadata.len());
      capacity.files = capacity.files.saturating_add(1);
    }
    metadata_paths.sort();
    metadata_paths.dedup();
    let store = Self {
      sdk_directory: sdk_directory.to_owned(),
      directory,
      capacity: Mutex::new(capacity),
      oldest_timestamp: Mutex::new(None),
      cleanup_generation: AtomicU64::new(0),
      admissions: Semaphore::new(1),
      max_attachment_bytes: runtime.register_int_watch(),
      max_owned_bytes: runtime.register_int_watch(),
      max_owned_files: runtime.register_int_watch(),
    };
    for metadata_path in metadata_paths {
      let id = owned_attachment_id(&metadata_path)
        .ok_or_else(|| io::Error::other("invalid workflow attachment ownership marker"))?;
      let has_payload = payload_ids.remove(&id);
      let mut metadata = match read_metadata(&metadata_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
          store.discard_corrupt_attachment(id, &error).await?;
          continue;
        },
        Err(error) => return Err(error),
      };
      if !metadata.uploaded && !fs::try_exists(store.payload_path(id)).await? {
        fs::remove_file(metadata_path).await?;
        recovered_sidecar = true;
        continue;
      }
      if metadata.uploaded && has_payload {
        store.remove_payload_async(id, true).await?;
        recovered_sidecar = true;
        log::debug!("released workflow attachment payload {id} after interrupted upload cleanup");
      }
      let timestamp = metadata
        .occurred_at_micros
        .unwrap_or(metadata.admitted_at_micros);
      if metadata.occurred_at_micros.is_none() {
        // An admitted or uploaded attachment may outlive a crash before its outcome log timestamp
        // is recorded. Use its durable admission time as a best-effort retirement fallback after
        // restart.
        metadata.occurred_at_micros = Some(timestamp);
        write_metadata(&metadata_path, &metadata).await?;
        recovered_sidecar = true;
        log::warn!("recovered workflow attachment {id} with its admission timestamp");
      }
      oldest_timestamp = Some(oldest_timestamp.map_or(timestamp, |oldest| oldest.min(timestamp)));
    }
    for id in payload_ids {
      store.remove_payload_async(id, true).await?;
      recovered_sidecar = true;
      log::warn!("discarded orphan workflow attachment payload {id}");
    }
    if recovered_sidecar {
      File::open(&store.directory).await?.sync_all().await?;
    }
    *store.oldest_timestamp.lock() = oldest_timestamp;
    Ok(store)
  }

  pub async fn admit(
    self: &Arc<Self>,
    source: UploadSource,
    content_type: Option<String>,
  ) -> io::Result<AdmittedAttachment> {
    if content_type.as_ref().is_some_and(|content_type| {
      content_type.len() > 255 || content_type.bytes().any(|byte| byte.is_ascii_control())
    }) {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "attachment content type exceeds 255 bytes or contains control characters",
      ));
    }
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    self.admit_async(source, content_type).await
  }

  pub async fn release(self: &Arc<Self>, id: Uuid) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    self.release_async(id).await
  }

  async fn release_async(&self, id: Uuid) -> io::Result<()> {
    self.remove_payload_async(id, false).await?;
    self.remove_metadata(id).await?;
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
    let Some(mut metadata) = self.metadata(id).await? else {
      log::debug!("ignoring timestamp for retired workflow attachment {id}");
      return Ok(());
    };
    metadata.occurred_at_micros = Some(micros);
    write_metadata(&self.metadata_path(id), &metadata).await?;
    let mut oldest_timestamp = self.oldest_timestamp.lock();
    *oldest_timestamp = Some(oldest_timestamp.map_or(micros, |oldest| oldest.min(micros)));
    self.cleanup_generation.fetch_add(1, Ordering::Release);
    Ok(())
  }

  /// Marks an artifact uploaded and drops the retained payload while preserving timestamp metadata.
  pub async fn complete_upload(self: &Arc<Self>, id: Uuid) -> io::Result<()> {
    let _permit = self.admissions.acquire().await.map_err(io::Error::other)?;
    let Some(mut metadata) = self.metadata(id).await? else {
      log::debug!("ignoring upload completion for retired workflow attachment {id}");
      return Ok(());
    };
    if !metadata.uploaded {
      metadata.uploaded = true;
      write_metadata(&self.metadata_path(id), &metadata).await?;
    }
    self.remove_payload_async(id, true).await?;
    log::debug!("released uploaded workflow attachment payload {id}");
    Ok(())
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
        .is_none_or(|extension| extension != "metadata")
      {
        continue;
      }
      let Some(id) = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| Uuid::parse_str(stem).ok())
      else {
        log::warn!(
          "invalid workflow attachment metadata sidecar {}",
          path.display()
        );
        continue;
      };
      let metadata = match read_metadata(&path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
          self.discard_corrupt_attachment(id, &error).await?;
          continue;
        },
        Err(error) => return Err(error),
      };
      let Some(timestamp) = metadata.occurred_at_micros else {
        continue;
      };
      if cutoff_micros.is_some_and(|cutoff_micros| timestamp >= cutoff_micros) {
        oldest_timestamp = Some(oldest_timestamp.map_or(timestamp, |oldest| oldest.min(timestamp)));
        continue;
      }
      self.remove_payload_async(id, true).await?;
      self.remove_metadata(id).await?;
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

  fn metadata_path(&self, id: Uuid) -> PathBuf {
    self.directory.join(format!("{id}.metadata"))
  }

  pub(crate) async fn metadata(&self, id: Uuid) -> io::Result<Option<WorkflowAttachmentState>> {
    match read_metadata(&self.metadata_path(id)).await {
      Ok(metadata) => Ok(Some(metadata)),
      Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
      Err(error) => Err(error),
    }
  }

  async fn remove_metadata(&self, id: Uuid) -> io::Result<()> {
    let path = self.metadata_path(id);
    remove_owned_path_if_exists(&path).await?;
    remove_owned_path_if_exists(&sidecar_staging_path(&path)?).await
  }

  async fn discard_corrupt_attachment(&self, id: Uuid, error: &io::Error) -> io::Result<()> {
    self.remove_payload_async(id, true).await?;
    self.remove_metadata(id).await?;
    File::open(&self.directory).await?.sync_all().await?;
    log::warn!("discarded corrupt workflow attachment {id}: {error}");
    Ok(())
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

  async fn admit_async(
    &self,
    source: UploadSource,
    content_type: Option<String>,
  ) -> io::Result<AdmittedAttachment> {
    let metadata = WorkflowAttachmentState {
      admitted_at_micros: u64::try_from(OffsetDateTime::now_utc().unix_timestamp_micros())
        .map_err(io::Error::other)?,
      content_type: content_type.clone(),
      ..Default::default()
    }
    .write_to_bytes()
    .map_err(io::Error::other)?;
    if metadata.len() as u64 + MAX_LIFECYCLE_METADATA_BYTES > MAX_SIDECAR_BYTES {
      return Err(io::Error::other(
        "workflow attachment metadata exceeds size limit",
      ));
    }
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
    let ownership_marker = self.metadata_path(id);
    let result: io::Result<AdmittedAttachment> = async {
      let (compressed, size_bytes) =
        read_and_compress_limited_with_size(reader, max_attachment_bytes)
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
      write_sidecar(&ownership_marker, &metadata).await?;
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
      Ok(AdmittedAttachment {
        id,
        artifact_metadata: CommandArtifactMetadata::new(content_type, size_bytes),
      })
    }
    .await;
    if result.is_err() {
      let _ = tokio::fs::remove_file(&pending).await;
      let _ = tokio::fs::remove_file(&path).await;
      let _ = tokio::fs::remove_file(&ownership_marker).await;
      let _ = tokio::fs::remove_file(sidecar_staging_path(&ownership_marker)?).await;
    }
    result
  }
}

async fn read_metadata(path: &Path) -> io::Result<WorkflowAttachmentState> {
  let file = open_regular_file_async(path).await?;
  let max_bytes = MAX_SIDECAR_BYTES + size_of::<u32>() as u64;
  let mut bytes = Vec::new();
  file.take(max_bytes + 1).read_to_end(&mut bytes).await?;
  if bytes.len() as u64 > max_bytes {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "workflow attachment metadata exceeds size limit",
    ));
  }
  let bytes = read_checksummed_data(&bytes)
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
  WorkflowAttachmentState::parse_from_bytes(&bytes)
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_metadata(path: &Path, metadata: &WorkflowAttachmentState) -> io::Result<()> {
  write_sidecar(path, &metadata.write_to_bytes().map_err(io::Error::other)?).await
}

async fn write_sidecar(path: &Path, contents: &[u8]) -> io::Result<()> {
  if contents.len() as u64 > MAX_SIDECAR_BYTES {
    return Err(io::Error::other(
      "workflow attachment metadata exceeds size limit",
    ));
  }
  let staging_path = sidecar_staging_path(path)?;
  fs::write(&staging_path, write_checksummed_data(contents)).await?;
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
  if extension != "metadata" {
    return None;
  }
  let target = path.with_file_name(format!("{stem}.{extension}"));
  owned_attachment_id(&target)?;
  Some(target)
}

fn owned_attachment_id(path: &Path) -> Option<Uuid> {
  let stem = path.file_stem()?.to_str()?;
  let id = Uuid::parse_str(stem).ok()?;
  (id.to_string() == stem).then_some(id)
}

async fn discard_corrupt_sidecar(path: &Path, error: io::Error) -> io::Result<()> {
  if error.kind() != io::ErrorKind::InvalidData {
    return Err(error);
  }
  remove_owned_path_if_exists(path).await?;
  log::warn!("discarded interrupted workflow attachment metadata: {error}");
  Ok(())
}

async fn remove_owned_path_if_exists(path: &Path) -> io::Result<()> {
  let metadata = match fs::symlink_metadata(path).await {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
    Err(error) => return Err(error),
  };
  let result = if metadata.is_dir() {
    remove_dir_if_exists_async(path).await
  } else {
    delete_file_if_exists_async(path).await
  };
  result.map_err(io::Error::other)
}

async fn open_regular_file_async(path: &Path) -> io::Result<tokio::fs::File> {
  if !tokio::fs::symlink_metadata(path).await?.is_file() {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "workflow attachment must be a regular file",
    ));
  }
  let mut options = tokio::fs::OpenOptions::new();
  options.read(true);
  #[cfg(unix)]
  options.custom_flags(libc::O_NOFOLLOW);
  let file = options.open(path).await?;
  if !file.metadata().await?.is_file() {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
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
