// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Chunk-format mechanics inside the shared artifact uploader.
//!
//! This module owns bounded geometry, compression/digests, physical-byte admission and validation
//! of recovered manifests. It does not own another submission channel, index writer,
//! acknowledgement path or network retry loop. `staging` supplies the transaction and its
//! created-file ledger; `storage` supplies the filesystem inventory used by recovery and terminal
//! cleanup.
//!
//! Preparation holds at most one logical chunk and one awaited compression job at a time. Files
//! become durable before their descriptors are published in the common index. Recovery ignores
//! current admission flags/limits: accepted bytes survive runtime reductions until their fixed
//! deadline, and only malformed or missing data—not an unexpected I/O error—may invalidate them.
//!
//! The code follows the ownership lifecycle: validate capture/index metadata, bound index decoding,
//! encode or verify individual chunks, then admit/recover/retire entries through the shared
//! uploader. Index decoding uses the generated protobuf parser after bounded reads/decompression;
//! recovery then validates artifact metadata and the corresponding staged files.

use super::staging::StagedFiles;
use super::storage::StoredArtifact;
use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  UploadSource,
  Uploader,
  WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID,
};
use bd_client_common::artifact::MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file::{read_compressed_limited, read_limited, write_compressed};
use bd_client_common::file_system::{FileSystem, OwnedFile};
use bd_proto::protos::client::api::{
  ArtifactChunkDescriptor,
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::artifact::artifact_upload_index::ChunkedArtifact;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::{BoolWatch, ConfigLoader, IntWatch, chunked_artifact_upload};
use bd_time::OffsetDateTimeExt;
use protobuf::Message;
use protobuf::well_known_types::timestamp::Timestamp;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::Metadata;
use std::io;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

pub(super) const CHUNK_BYTES: u64 = 1024 * 1024;
pub(super) const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
// Reserve room for request framing and an eventual accepted deadline before staging new ownership.
const RESERVED_INDEX_ENTRY_BYTES: u64 = MAX_MANIFEST_BYTES + 64;
const MAX_CAPTURE_METADATA_BYTES: u64 = 8 * 1024;
const MAX_FILES: usize = 2;
const MAX_OWNED_BYTES: u64 = 640 * 1024 * 1024;
const FREE_SPACE_HEADROOM: u64 = MAX_INDEX_BYTES + 1024 * 1024;

//
// Spool
//

pub(super) struct Spool {
  pub(super) entries: Vec<ChunkedArtifact>,
  enabled: BoolWatch<chunked_artifact_upload::Enabled>,
  workflow_enabled: BoolWatch<chunked_artifact_upload::WorkflowEnabled>,
  max_logical_bytes: IntWatch<chunked_artifact_upload::MaxLogicalBytes>,
  max_owned_bytes: IntWatch<chunked_artifact_upload::MaxOwnedPayloadBytes>,
  max_files: IntWatch<chunked_artifact_upload::MaxAdmittedFiles>,
}

impl Spool {
  pub(super) fn new(runtime: &ConfigLoader) -> Self {
    Self {
      entries: Vec::new(),
      enabled: runtime.register_bool_watch(),
      workflow_enabled: runtime.register_bool_watch(),
      max_logical_bytes: runtime.register_int_watch(),
      max_owned_bytes: runtime.register_int_watch(),
      max_files: runtime.register_int_watch(),
    }
  }
}

// Local metadata and manifest validation.

pub(super) fn safe_id(value: &str) -> anyhow::Result<Uuid> {
  // IDs become spool filenames, so accept only one safe spelling of a non-nil UUID.
  let id = Uuid::parse_str(value)?;
  if id.is_nil() || id.to_string() != value {
    anyhow::bail!("artifact identifiers must be non-nil canonical UUIDs");
  }
  Ok(id)
}

fn timestamp(value: Option<&Timestamp>) -> anyhow::Result<OffsetDateTime> {
  let value = value.ok_or_else(|| anyhow::anyhow!("missing artifact timestamp"))?;
  Ok(
    OffsetDateTime::from_unix_timestamp(value.seconds)?
      .replace_nanosecond(u32::try_from(value.nanos)?)?,
  )
}

fn validate_metadata(request: &UploadArtifactRequest, limit: u64) -> anyhow::Result<Uuid> {
  let id = safe_id(&request.artifact_id)?;

  // Bound producer-controlled metadata before queueing it or retaining a recovered template.
  // MIME checks deliberately match whole-file SDK admission; parsing belongs to the backend.
  if request.compute_size() > limit
    || request.type_id.is_empty()
    || request.type_id.len() > 255
    || request.session_id.is_empty()
    || request.session_id.len() > 255
    || request.state_metadata.len() > 32
    || request
      .content_type
      .as_ref()
      .is_some_and(|mime| mime.len() > 255 || mime.bytes().any(|byte| byte.is_ascii_control()))
  {
    anyhow::bail!("invalid local chunked artifact metadata");
  }

  // The retained request is a payload-free Begin/resume template, not a network attempt or a
  // whole-file report. Chunks carry their own encoding; the outer encoding must remain RAW.
  if !request.upload_uuid.is_empty()
    || !request.contents.is_empty()
    || request.chunk_index.is_some()
    || !request.feature_flags.is_empty()
    || request.workflow_report_handoff.is_some()
    || request.payload_encoding.enum_value()
      != Ok(ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW)
  {
    anyhow::bail!("invalid local chunked artifact metadata");
  }

  // Remote results require command provenance; workflow attachments use session provenance only.
  match &request.command_id {
    Some(command_id) if request.type_id != WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID => {
      safe_id(command_id)?;
    },
    None if request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID => {},
    _ => anyhow::bail!("invalid local chunked artifact provenance"),
  }

  // Origin fields share one aggregate budget as well as individual key/value limits.
  let mut origin_bytes = 0;
  for (key, data) in &request.state_metadata {
    let Some(Data_type::StringData(value)) = &data.data_type else {
      anyhow::bail!("chunked origin fields must be bounded strings");
    };
    if key.is_empty() || key.len() > 255 || value.len() > 255 {
      anyhow::bail!("invalid local chunked origin field");
    }
    origin_bytes += key.len() + value.len();
  }
  if u64::try_from(origin_bytes)? > MAX_CAPTURE_METADATA_BYTES {
    anyhow::bail!("chunked origin metadata exceeds its size limit");
  }
  timestamp(request.time.as_ref())?;
  timestamp(request.upload_deadline.as_ref())?;
  Ok(id)
}

pub(super) fn validate_capture_request(request: &UploadArtifactRequest) -> anyhow::Result<Uuid> {
  let id = validate_metadata(request, MAX_CAPTURE_METADATA_BYTES)?;
  // Producers supply capture identity and timestamps. Only staging may add file geometry/hashes
  // or start the durable admission clock.
  if request.logical_bytes.is_some()
    || !request.logical_sha256.is_empty()
    || !request.chunks.is_empty()
    || request.admitted_at.is_some()
  {
    anyhow::bail!("capture metadata contains staging-only fields");
  }
  Ok(id)
}

fn validate_entry(entry: &ChunkedArtifact) -> anyhow::Result<&UploadArtifactRequest> {
  let request = entry
    .request
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("missing local artifact request"))?;
  validate_metadata(request, MAX_MANIFEST_BYTES)?;

  // Establish geometry before deriving any chunk offset. Empty remote files have no descriptors
  // and still carry SHA-256(empty); empty workflow attachments are not admissible.
  let size = request
    .logical_bytes
    .ok_or_else(|| anyhow::anyhow!("missing artifact logical size"))?;
  let admitted = timestamp(request.admitted_at.as_ref())?;
  let deadline = timestamp(request.upload_deadline.as_ref())?;
  if size > u64::from(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES)
    || (size == 0 && request.command_id.is_none())
    || request.logical_sha256.len() != 32
    || request.chunks.len() as u64 != size.div_ceil(CHUNK_BYTES)
  {
    anyhow::bail!("inconsistent local chunked artifact geometry");
  }

  // Persisted admission/deadline values do not move on retry or after runtime policy reductions.
  if timestamp(request.time.as_ref())? > admitted
    || admitted >= deadline
    || deadline - admitted > time::Duration::hours(24)
  {
    anyhow::bail!("inconsistent local chunked artifact deadline");
  }
  if let Some(accepted) = entry.accepted_deadline.as_ref() {
    let accepted = timestamp(Some(accepted))?;
    if accepted <= admitted || accepted > deadline {
      anyhow::bail!("invalid persisted accepted deadline");
    }
  }
  // Descriptor position determines logical offset/length. Compression is valid only when smaller
  // than RAW; verify these bounds before recovery allocates or decompresses any payload.
  for (index, descriptor) in request.chunks.iter().enumerate() {
    let logical = (size - index as u64 * CHUNK_BYTES).min(CHUNK_BYTES);
    let encoded = u64::from(descriptor.encoded_bytes);
    if descriptor.logical_sha256.len() != 32
      || descriptor.encoded_sha256.len() != 32
      || encoded == 0
      || match descriptor.encoding.enum_value() {
        Ok(ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW) => encoded != logical,
        Ok(ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB) => encoded >= logical,
        _ => true,
      }
    {
      anyhow::bail!("inconsistent local chunk descriptor");
    }
  }
  Ok(request)
}

// Shared spool names and retirement policy.

pub(super) fn chunk_path(id: &str, index: usize) -> PathBuf {
  ARTIFACT_UPLOAD_DIRECTORY.join(format!("chunk-{id}-{index}"))
}

pub(super) fn source_path(id: &str) -> PathBuf {
  ARTIFACT_UPLOAD_DIRECTORY.join(format!("chunk-{id}.source"))
}

fn retirement_deadline(entry: &ChunkedArtifact) -> anyhow::Result<OffsetDateTime> {
  // Once admitted by the server, its fixed deadline takes precedence over the SDK proposal.
  let request = entry
    .request
    .as_ref()
    .ok_or_else(|| anyhow::anyhow!("missing local artifact request"))?;
  timestamp(
    entry
      .accepted_deadline
      .as_ref()
      .or(request.upload_deadline.as_ref()),
  )
}

// Bounded index decoding.

pub(super) async fn read_index(
  file: &mut (impl AsyncRead + Unpin),
) -> io::Result<ArtifactUploadIndex> {
  // Bound compressed I/O and decompression, then use normal generated protobuf decoding. Recovery
  // validates the resulting metadata and payloads; there is no separate wire-format parser here.
  let compressed = read_limited(file, MAX_INDEX_BYTES).await?;
  let decoded = read_compressed_limited(&compressed, MAX_INDEX_BYTES)?;
  ArtifactUploadIndex::parse_from_bytes(&decoded)
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

// Capture identity and per-chunk encoding/integrity.

#[cfg(unix)]
// Other platforms have no portable inode identity and conservatively charge each path.
#[allow(clippy::unnecessary_wraps)]
fn file_identity(metadata: &Metadata) -> Option<(u64, u64)> {
  use std::os::unix::fs::MetadataExt;
  Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn file_identity(_: &Metadata) -> Option<(u64, u64)> {
  None
}

fn encode_chunk(raw: Vec<u8>) -> anyhow::Result<(Vec<u8>, ArtifactChunkDescriptor)> {
  // Try zlib once and retain RAW when compression would grow the payload. This function owns only
  // bounded CPU buffers; the async owner remains responsible for all filesystem/index changes.
  let logical_sha256 = Sha256::digest(&raw).to_vec();
  let compressed = write_compressed(&raw)?;
  let (encoded, encoding) = if compressed.len() < raw.len() {
    (
      compressed,
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB,
    )
  } else {
    (raw, ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW)
  };

  // RAW did not change the bytes, so reuse its digest instead of hashing the same chunk again.
  let encoded_sha256 = if encoding == ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW {
    logical_sha256.clone()
  } else {
    Sha256::digest(&encoded).to_vec()
  };
  let descriptor = ArtifactChunkDescriptor {
    encoded_bytes: u32::try_from(encoded.len())?,
    encoding: encoding.into(),
    logical_sha256,
    encoded_sha256,
    ..Default::default()
  };
  Ok((encoded, descriptor))
}

async fn stage(
  fs: &dyn FileSystem,
  mut file: tokio::fs::File,
  original: &Metadata,
  mut request: UploadArtifactRequest,
  files: &mut StagedFiles,
) -> anyhow::Result<ChunkedArtifact> {
  let size = original.len();
  let mut whole = Sha256::new();
  let mut remaining = size;
  let mut chunks = Vec::new();

  // Read and encode one logical segment at a time. Await each CPU job before reading the next
  // segment, bounding both live buffers and compression concurrency independently of file size.
  while remaining != 0 {
    let logical_bytes = remaining.min(CHUNK_BYTES);
    let mut raw = vec![0; usize::try_from(logical_bytes)?];
    file.read_exact(&mut raw).await?;
    whole.update(&raw);
    let (encoded, descriptor) = tokio::task::spawn_blocking(move || encode_chunk(raw)).await??;

    // Make each payload durable before its descriptor can reach the index. Include a partially
    // written current chunk in rollback, not only completed descriptors.
    let path = chunk_path(&request.artifact_id, chunks.len());
    files.created.push(path.clone());
    let mut target = fs.create_file(&path).await?;
    target.write_all(&encoded).await?;
    target.flush().await?;
    fs.sync_file_and_parent(&path).await?;
    remaining -= logical_bytes;
    chunks.push(descriptor);
  }

  // Detect observed growth, truncation or modification of the pinned input. This is not a snapshot:
  // the capture owner must stop writing when it hands over the file.
  let mut trailing = [0; 1];
  let current = file.metadata().await?;
  if file.read(&mut trailing).await? != 0
    || current.len() != size
    || current.modified()? != original.modified()?
  {
    anyhow::bail!("capture file changed during staging");
  }

  // Publish only a complete, self-consistent manifest to the shared staging transaction. The
  // transaction, not this encoder, commits index ownership and releases the moved capture source.
  request.logical_bytes = Some(size);
  request.logical_sha256 = whole.finalize().to_vec();
  request.chunks = chunks;
  let entry = ChunkedArtifact {
    request: Some(request).into(),
    ..Default::default()
  };
  validate_entry(&entry)?;
  Ok(entry)
}

async fn validate_files(
  fs: &dyn FileSystem,
  request: &UploadArtifactRequest,
) -> anyhow::Result<()> {
  // validate_entry has already established geometry and allowed encodings. This pass verifies the
  // corresponding disk bytes without loading more than one encoded/decoded chunk at a time.
  let mut whole = Sha256::new();
  let size = request
    .logical_bytes
    .ok_or_else(|| anyhow::anyhow!("missing logical size"))?;
  for (index, descriptor) in request.chunks.iter().enumerate() {
    // Check encoded size/hash before attempting decompression.
    let mut file = fs
      .open_regular_file(&chunk_path(&request.artifact_id, index))
      .await?;
    let encoded = read_limited(&mut file, u64::from(descriptor.encoded_bytes)).await?;
    if encoded.len() != descriptor.encoded_bytes as usize
      || Sha256::digest(&encoded).as_slice() != descriptor.encoded_sha256
    {
      anyhow::bail!("staged chunk does not match encoded descriptor");
    }
    // Bound the decoded segment to its exact logical length and verify its independent digest.
    let expected = (size - index as u64 * CHUNK_BYTES).min(CHUNK_BYTES);
    let raw = if descriptor.encoding.enum_value_or_default()
      == ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    {
      read_compressed_limited(&encoded, expected)?
    } else {
      encoded
    };
    if raw.len() as u64 != expected || Sha256::digest(&raw).as_slice() != descriptor.logical_sha256
    {
      anyhow::bail!("staged chunk does not match logical descriptor");
    }
    // Chunk hashes alone do not prove the manifest's whole-file digest or ordering.
    whole.update(&raw);
  }
  if whole.finalize().as_slice() != request.logical_sha256 {
    anyhow::bail!("staged artifact does not match whole logical digest");
  }
  Ok(())
}

impl Uploader {
  pub(super) async fn prepare_chunked(
    &self,
    source: UploadSource,
    mut request: UploadArtifactRequest,
    source_cleanup: &mut Option<OwnedFile>,
    files: &mut StagedFiles,
  ) -> anyhow::Result<ChunkedArtifact> {
    // Runtime policy gates new ownership only. Reserve both a chunk slot and enough bounded index
    // capacity before touching the producer's input; recovery deliberately ignores these knobs.
    let enabled = *self.chunked.enabled.read()
      && (request.type_id != WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        || *self.chunked.workflow_enabled.read());
    let max_files = (*self.chunked.max_files.read() as usize).min(MAX_FILES);
    if !enabled || self.chunked.entries.len() >= max_files {
      anyhow::bail!("chunked artifact admission disabled or slots exhausted");
    }
    if self.index_proto().compute_size() + RESERVED_INDEX_ENTRY_BYTES > MAX_INDEX_BYTES {
      anyhow::bail!("insufficient bounded index space for chunked artifact metadata");
    }
    // Pin a regular capture file. Charge its existing bytes plus worst-case RAW chunk output;
    // aliases of already-owned inodes must not be charged twice.
    let UploadSource::Path(path) = source else {
      anyhow::bail!("chunked staging requires an owned capture path");
    };
    let file = self.file_system.open_regular_file(&path).await?;
    let metadata = file.metadata().await?;
    let size = metadata.len();
    let logical_limit =
      u64::from((*self.chunked.max_logical_bytes.read()).min(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES));
    let (used, identities) = self.chunked_usage().await?;
    let source_charge = if file_identity(&metadata).is_some_and(|key| identities.contains(&key)) {
      0
    } else {
      size
    };
    let budget = u64::from(*self.chunked.max_owned_bytes.read()).min(MAX_OWNED_BYTES);
    if size > logical_limit
      || (size == 0 && request.command_id.is_none())
      || used.saturating_add(source_charge).saturating_add(size) > budget
    {
      anyhow::bail!("chunked artifact logical or physical byte admission exhausted");
    }
    // Disk headroom also covers an atomic index replacement. Source bytes already exist on disk,
    // so only new chunk output and metadata headroom need to fit in currently available space.
    let available = self
      .file_system
      .available_space(&ARTIFACT_UPLOAD_DIRECTORY)
      .await?;
    if available < size.saturating_add(FREE_SPACE_HEADROOM) {
      anyhow::bail!("insufficient free space for staged chunks and metadata");
    }
    // Fix admission time and clamp the requested deadline before moving ownership. Neither retries
    // nor later policy changes may extend this window.
    let now = self.time_provider.now();
    let deadline = timestamp(request.upload_deadline.as_ref())?;
    if timestamp(request.time.as_ref())? > now || deadline <= now {
      anyhow::bail!("capture timestamp/deadline outside admission window");
    }
    request.admitted_at = now.into_proto();
    request.upload_deadline = deadline.min(now + time::Duration::hours(24)).into_proto();
    // Move first: interrupted preparations have only SDK-local, safely named orphan files.
    // Cross-filesystem rename errors reject admission rather than creating an untracked copy.
    let retained_source = source_path(&request.artifact_id);
    self
      .file_system
      .rename_file(&path, &retained_source)
      .await?;
    files.created.push(retained_source.clone());
    if let Some(guard) = source_cleanup.take() {
      guard.disarm();
    }
    // The pinned handle must still name the file that was moved. A pathname replacement is not an
    // alternative capture; reject it and leave rollback to the shared transaction.
    let moved = self
      .file_system
      .open_regular_file(&retained_source)
      .await?
      .metadata()
      .await?;
    if file_identity(&moved) != file_identity(&metadata) || moved.len() != size {
      anyhow::bail!("capture path changed during ownership transfer");
    }
    self
      .file_system
      .sync_file_and_parent(&retained_source)
      .await?;
    // Encoding can outlast the remaining deadline. Recheck before index publication so expired
    // preparation never receives a successful persistence acknowledgement.
    let entry = stage(self.file_system.as_ref(), file, &metadata, request, files).await?;
    if retirement_deadline(&entry)? <= self.time_provider.now() {
      anyhow::bail!("artifact expired during staging");
    }
    Ok(entry)
  }

  async fn chunked_usage(&self) -> anyhow::Result<(u64, HashSet<(u64, u64)>)> {
    let mut identities = HashSet::new();
    let mut used = 0_u64;
    // Count physical spool files, including interrupted/retired data whose unlink failed. The
    // index alone cannot account for those bytes; inode deduplication handles hard-link aliases.
    for path in self
      .file_system
      .list_files(&ARTIFACT_UPLOAD_DIRECTORY)
      .await?
    {
      let path = Path::new(&path);
      if !path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("chunk-"))
      {
        continue;
      }
      let file = self.file_system.open_regular_file(path).await?;
      let metadata = file.metadata().await?;
      if file_identity(&metadata).is_none_or(|key| identities.insert(key)) {
        used = used.saturating_add(metadata.len());
      }
    }
    Ok((used, identities))
  }

  pub(super) async fn recover_chunks(&mut self, modified: &mut bool) -> anyhow::Result<()> {
    // Rebuild ownership from committed entries, not current runtime admission limits. Whole-file
    // IDs participate in collision detection so no two modes can claim the same artifact.
    let mut ids: HashSet<String> = self.index.iter().map(|entry| entry.name.clone()).collect();
    let mut retained = Vec::new();
    for entry in std::mem::take(&mut self.chunked.entries) {
      // Reject malformed templates before deriving paths, allocating chunk buffers or decoding
      // data.
      let request = match validate_entry(&entry) {
        Ok(request) => request,
        Err(error) => {
          log::warn!("discarding invalid chunked artifact index entry: {error}");
          *modified = true;
          continue;
        },
      };
      let id = &request.artifact_id;
      if !ids.insert(id.clone()) {
        // Remove both owners of a collision; choosing one could hide conflicting payload identity.
        log::warn!("discarding duplicate/cross-mode chunked artifact {id}");
        retained.retain(|entry: &ChunkedArtifact| {
          entry
            .request
            .as_ref()
            .is_some_and(|request| request.artifact_id != *id)
        });
        self.index.retain(|entry| entry.name != *id);
        *modified = true;
        continue;
      }
      // Expired or corrupt payloads retire normally. Unexpected filesystem errors must stop
      // initialization without publishing a reduced index that would lose accepted ownership.
      if retirement_deadline(&entry)? <= self.time_provider.now() {
        log::debug!("retiring expired recovered chunked artifact {id}");
        *modified = true;
        continue;
      }
      // The shared initializer persists any removals before orphan cleanup may delete payload
      // files.
      if let Err(error) = validate_files(self.file_system.as_ref(), request).await {
        if error.downcast_ref::<io::Error>().is_some_and(|error| {
          !matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData
          )
        }) {
          return Err(error);
        }
        log::warn!("discarding inconsistent staged artifact {id}: {error}");
        *modified = true;
        continue;
      }
      retained.push(entry);
    }
    self.chunked.entries = retained;
    self.index_sync_required |= *modified;
    Ok(())
  }

  pub(super) async fn retire_expired_chunks(&mut self) -> anyhow::Result<()> {
    let now = self.time_provider.now();
    let mut expired = Vec::new();
    let mut ids = HashSet::new();
    // Collect ownership removals and their exact payload paths before mutating the live index.
    for entry in &self.chunked.entries {
      let request = entry
        .request
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing request"))?;
      if retirement_deadline(entry)? <= now {
        ids.insert(request.artifact_id.clone());
        expired.extend(StoredArtifact::Chunked(entry).owned_paths()?);
      }
    }
    if ids.is_empty() {
      return Ok(());
    }
    // Publish index retirement before unlinking. Failed commits leave the payloads intact for the
    // common index retry/orphan-cleanup path; failed unlinks remain charged to physical usage.
    self.chunked.entries.retain(|entry| {
      entry
        .request
        .as_ref()
        .is_none_or(|request| !ids.contains(&request.artifact_id))
    });
    self.index_sync_required = true;
    log::debug!("retiring expired chunked artifact files");
    self.retire_files(&expired).await
  }
}
