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
//! Preparation compresses one continuous stream through bounded CPU jobs and encoded fragments.
//! Files become durable before their descriptors are published in the common index. Recovery
//! ignores current admission flags/limits: accepted bytes survive runtime reductions and offline
//! periods, and only malformed or missing data—not an unexpected I/O error—may invalidate them.
//!
//! The code follows the ownership lifecycle: validate capture/index metadata, bound index decoding,
//! encode or verify individual chunks, then admit/recover/retire entries through the shared
//! uploader. Index decoding uses the generated protobuf parser after bounded reads/decompression;
//! recovery then validates artifact metadata and the corresponding staged files.

use super::staging::StagedFiles;
use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  ArtifactStore,
  UploadSource,
  Uploader,
  WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID,
};
use crate::input::Input;
use bd_client_common::artifact::{
  CLIENT_REPORT_ARTIFACT_TYPE_ID,
  CRASH_PROFILE_ARTIFACT_TYPE_ID,
  CRASH_PROFILE_MAX_REPORT_IDS,
  MAX_ARTIFACT_LOGICAL_BYTES,
  MAX_RAW_CLIENT_REPORT_BYTES,
  STATE_SNAPSHOT_ARTIFACT_TYPE_ID,
};
use bd_client_common::file::{read_compressed_limited, read_limited};
use bd_client_common::file_system::{FileSystem, OwnedFile, open_regular_file_async};
use bd_log_primitives::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use bd_metadata::artifact::{
  ARTIFACT_BEGIN_METADATA_KEYS,
  ARTIFACT_CHUNK_BYTES,
  MAX_ARTIFACT_ATTACHED_METADATA_BYTES,
  MAX_ARTIFACT_MANIFEST_BYTES,
  artifact_encoded_bound,
};
use bd_proto::protos::client::api::{
  ArtifactChunkDescriptor,
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::ConfigLoader;
use bd_time::TimestampExt as _;
use flate2::write::ZlibEncoder;
use flate2::{Compression, Decompress, FlushDecompress, Status};
use protobuf::Message;
use protobuf::well_known_types::timestamp::Timestamp;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::Metadata;
use std::io;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

pub(super) const CHUNK_BYTES: u64 = ARTIFACT_CHUNK_BYTES as u64;
pub(super) const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = MAX_ARTIFACT_MANIFEST_BYTES as u64;
const MAX_ATTACHED_METADATA_BYTES: u64 = MAX_ARTIFACT_ATTACHED_METADATA_BYTES as u64;
// Reserve room for up to 257 chunk descriptors and index entry framing before staging.
pub(super) const CHUNK_DESCRIPTOR_RESERVE_BYTES: u64 = 16 * 1024;

//
// Spool
//

pub(super) struct Spool {
  pub(super) entries: Vec<UploadArtifactRequest>,
  pub(super) admission: ArtifactStore,
}

impl Spool {
  pub(super) fn new(runtime: &ConfigLoader) -> Self {
    Self {
      entries: Vec::new(),
      admission: ArtifactStore::new(runtime),
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
  value
    .to_offset_date_time_checked()
    .ok_or_else(|| anyhow::anyhow!("invalid artifact timestamp"))
}

fn validate_metadata(request: &UploadArtifactRequest, limit: u64) -> anyhow::Result<Uuid> {
  let id = safe_id(&request.artifact_id)?;

  // Bound producer-controlled metadata before queueing it or retaining a recovered template.
  // MIME checks deliberately match whole-file SDK admission; parsing belongs to the backend.
  if request.compute_size() > limit
    || request.type_id.is_empty()
    || request.type_id.len() > 255
    || request.session_id.len() > 255
    || request
      .content_type
      .as_ref()
      .is_some_and(|mime| mime.len() > 255 || mime.bytes().any(|byte| byte.is_ascii_control()))
  {
    anyhow::bail!("invalid local chunked artifact metadata");
  }

  // The retained request is a payload-free template, not a network attempt or a whole-file upload.
  if !request.upload_uuid.is_empty()
    || !request.contents.is_empty()
    || request.chunk_index.is_some()
    || request.workflow_report_handoff.is_some()
  {
    anyhow::bail!("invalid local chunked artifact metadata");
  }

  for flag in &request.feature_flags {
    if flag.name.is_empty() {
      anyhow::bail!("missing artifact feature flag name");
    }
    timestamp(flag.last_updated.as_ref())?;
  }

  // Parent report IDs are immutable crash-profile associations, never command authority.
  if request.type_id == CRASH_PROFILE_ARTIFACT_TYPE_ID {
    let parents = request
      .crash_profile
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("missing profile parent metadata"))?;
    let mut ids = HashSet::new();
    if parents.report_ids.is_empty() || parents.report_ids.len() > CRASH_PROFILE_MAX_REPORT_IDS {
      anyhow::bail!("invalid profile parent count");
    }
    for parent in &parents.report_ids {
      if !ids.insert(safe_id(parent)?) {
        anyhow::bail!("duplicate profile parent");
      }
    }
  } else if request.crash_profile.is_some() {
    anyhow::bail!("profile metadata on a non-profile artifact");
  }
  match &request.command_id {
    Some(command_id)
      if request.type_id != WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        && request.type_id != CRASH_PROFILE_ARTIFACT_TYPE_ID =>
    {
      safe_id(command_id)?;
    },
    None
      if request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        || request.type_id == CRASH_PROFILE_ARTIFACT_TYPE_ID
        || request.type_id == CLIENT_REPORT_ARTIFACT_TYPE_ID
        || request.type_id == STATE_SNAPSHOT_ARTIFACT_TYPE_ID => {},
    _ => anyhow::bail!("invalid local chunked artifact provenance"),
  }

  // Keys must be bounded for every artifact. Identity keys sent on Begin must be bounded strings;
  // full state_metadata and feature_flags travel on the final chunk within the overall size cap.
  for (key, data) in &request.state_metadata {
    if key.is_empty() || key.len() > 255 {
      anyhow::bail!("invalid local chunked metadata key");
    }
    if ARTIFACT_BEGIN_METADATA_KEYS.contains(&key.as_str()) {
      match &data.data_type {
        Some(Data_type::StringData(value)) if value.len() <= 255 => {},
        _ => anyhow::bail!("Begin identity metadata fields must be bounded strings"),
      }
    }
  }
  timestamp(request.time.as_ref())?;
  Ok(id)
}

pub(super) fn validate_capture_request(request: &UploadArtifactRequest) -> anyhow::Result<Uuid> {
  let id = validate_metadata(request, MAX_ATTACHED_METADATA_BYTES)?;
  // Producers supply capture identity and timestamp. Only staging may add file geometry/hashes.
  if request.logical_bytes.is_some()
    || request.encoded_bytes.is_some()
    || !request.logical_sha256.is_empty()
    || !request.chunks.is_empty()
    || request.payload_encoding.enum_value()
      != Ok(ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW)
  {
    anyhow::bail!("capture metadata contains staging-only fields");
  }
  Ok(id)
}

fn logical_size_limit(type_id: &str) -> u64 {
  if type_id == CLIENT_REPORT_ARTIFACT_TYPE_ID {
    MAX_RAW_CLIENT_REPORT_BYTES as u64
  } else {
    u64::from(MAX_ARTIFACT_LOGICAL_BYTES)
  }
}

fn validate_entry(request: &UploadArtifactRequest) -> anyhow::Result<()> {
  validate_metadata(request, MAX_ATTACHED_METADATA_BYTES + MAX_MANIFEST_BYTES)?;

  // Establish geometry before deriving any chunk offset. Empty remote files have no descriptors
  // and still carry SHA-256(empty); empty workflow attachments are not admissible.
  let size = request
    .logical_bytes
    .ok_or_else(|| anyhow::anyhow!("missing artifact logical size"))?;
  let encoded = request
    .encoded_bytes
    .ok_or_else(|| anyhow::anyhow!("missing artifact encoded size"))?;
  if size > logical_size_limit(&request.type_id)
    || encoded > artifact_encoded_bound(size)
    || (size != 0 && encoded == 0)
    || (size == 0 && request.command_id.is_none())
    || request.logical_sha256.len() != 32
    || request.chunks.len() as u64 != encoded.div_ceil(CHUNK_BYTES)
    || request.payload_encoding.enum_value()
      != Ok(
        if size == 0 {
          ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
        } else {
          ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
        },
      )
  {
    anyhow::bail!("inconsistent local chunked artifact geometry");
  }

  // Descriptor position determines encoded offset/length, not logical output boundaries.
  for (index, descriptor) in request.chunks.iter().enumerate() {
    if descriptor.encoded_sha256.len() != 32
      || u64::from(descriptor.encoded_bytes)
        != (encoded - index as u64 * CHUNK_BYTES).min(CHUNK_BYTES)
    {
      anyhow::bail!("inconsistent local chunk descriptor");
    }
  }
  Ok(())
}

// Shared spool names.

pub(super) fn chunk_path(id: &str, index: usize) -> PathBuf {
  ARTIFACT_UPLOAD_DIRECTORY.join(format!("chunk-{id}-{index}"))
}

pub(super) fn source_path(id: &str) -> PathBuf {
  ARTIFACT_UPLOAD_DIRECTORY.join(format!("chunk-{id}.source"))
}

// Bounded index decoding.

pub(super) async fn read_index(
  file: &mut (impl AsyncRead + Unpin),
) -> io::Result<ArtifactUploadIndex> {
  // Bound compressed I/O and decompression, then use normal generated protobuf decoding. Recovery
  // validates the resulting metadata and payloads; there is no separate wire-format parser here.
  let compressed = read_limited(file, MAX_INDEX_BYTES).await?;
  tokio::task::spawn_blocking(move || {
    let decoded = read_compressed_limited(&compressed, MAX_INDEX_BYTES)?;
    ArtifactUploadIndex::parse_from_bytes(&decoded)
      .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
  })
  .await
  .map_err(io::Error::other)?
}

// Capture identity and per-chunk encoding/integrity.

#[cfg(unix)]
// Other platforms have no portable inode identity and conservatively charge each path.
#[allow(clippy::unnecessary_wraps)]
pub(super) fn file_identity(metadata: &Metadata) -> Option<(u64, u64)> {
  use std::os::unix::fs::MetadataExt;
  Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
pub(super) fn file_identity(_: &Metadata) -> Option<(u64, u64)> {
  None
}

pub(super) async fn stage(
  fs: &dyn FileSystem,
  mut input: Input,
  mut request: UploadArtifactRequest,
  files: &mut StagedFiles,
  checksummed: bool,
) -> anyhow::Result<UploadArtifactRequest> {
  let size = input
    .size()
    .checked_sub(if checksummed { 4 } else { 0 })
    .ok_or_else(|| anyhow::anyhow!("invalid checksummed workflow input"))?;
  anyhow::ensure!(
    size <= logical_size_limit(&request.type_id),
    "artifact exceeds logical size limit"
  );
  if size == 0 && request.command_id.is_none() {
    anyhow::bail!("empty non-command artifact");
  }
  let mut whole = Sha256::new();
  let mut crc = checksummed.then(crc32fast::Hasher::new);
  let mut remaining = size;
  let mut chunks = Vec::new();
  // State snapshots are already zlib-compressed journals; stored blocks preserve the uniform
  // ZLIB wire encoding without paying double-compression CPU on mobile or server.
  let compression = if request.type_id == STATE_SNAPSHOT_ARTIFACT_TYPE_ID {
    Compression::none()
  } else {
    Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL)
  };
  let mut encoder = ZlibEncoder::new(Vec::new(), compression);
  let mut pending = Vec::new();
  let chunk_bytes = usize::try_from(CHUNK_BYTES)?;
  let mut encoded_bytes = 0_u64;

  // One encoder spans all input reads. Draining its sink does not flush/reset the codec.
  while remaining != 0 {
    let logical_bytes = remaining.min(64 * 1024);
    let mut raw = vec![0; usize::try_from(logical_bytes)?];
    input.reader().read_exact(&mut raw).await?;
    let encoded = tokio::task::spawn_blocking(move || {
      whole.update(&raw);
      if let Some(crc) = &mut crc {
        crc.update(&raw);
      }
      encoder.write_all(&raw)?;
      let encoded = std::mem::take(encoder.get_mut());
      Ok::<_, anyhow::Error>((encoded, encoder, whole, crc))
    })
    .await??;
    let (encoded, next_encoder, next_whole, next_crc) = encoded;
    encoder = next_encoder;
    whole = next_whole;
    crc = next_crc;
    encoded_bytes += encoded.len() as u64;
    anyhow::ensure!(
      encoded_bytes <= artifact_encoded_bound(size),
      "compressed artifact exceeds bound"
    );
    pending.extend(encoded);
    while pending.len() >= chunk_bytes {
      let tail = pending.split_off(chunk_bytes);
      chunks.push(stage_fragment(fs, &request.artifact_id, chunks.len(), pending, files).await?);
      pending = tail;
    }
    remaining -= logical_bytes;
  }
  if size != 0 {
    let trailer = tokio::task::spawn_blocking(move || encoder.finish()).await??;
    encoded_bytes += trailer.len() as u64;
    anyhow::ensure!(
      encoded_bytes <= artifact_encoded_bound(size),
      "compressed artifact exceeds bound"
    );
    pending.extend(trailer);
    while !pending.is_empty() {
      let tail = pending.split_off(pending.len().min(chunk_bytes));
      chunks.push(stage_fragment(fs, &request.artifact_id, chunks.len(), pending, files).await?);
      pending = tail;
    }
  }

  // Detect observed growth, truncation or modification of the pinned input. This is not a snapshot:
  // the capture owner must stop writing when it hands over the file.
  if let Some(crc) = crc
    && input.reader().read_u32_le().await? != crc.finalize()
  {
    anyhow::bail!("retained workflow checksum mismatch");
  }
  let mut trailing = [0; 1];
  if input.reader().read(&mut trailing).await? != 0 {
    anyhow::bail!("capture file changed during staging");
  }
  input.validate().await?;
  fs.sync_directory(&ARTIFACT_UPLOAD_DIRECTORY).await?;

  // Publish only a complete, self-consistent manifest to the shared staging transaction. The
  // transaction, not this encoder, commits index ownership and releases the moved capture source.
  request.logical_bytes = Some(size);
  request.encoded_bytes = Some(encoded_bytes);
  request.payload_encoding = if size == 0 {
    ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
  } else {
    ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
  }
  .into();
  request.logical_sha256 = whole.finalize().to_vec();
  request.chunks = chunks;
  validate_entry(&request)?;
  Ok(request)
}

async fn stage_fragment(
  fs: &dyn FileSystem,
  id: &str,
  index: usize,
  encoded: Vec<u8>,
  files: &mut StagedFiles,
) -> anyhow::Result<ArtifactChunkDescriptor> {
  let (encoded, descriptor) = tokio::task::spawn_blocking(move || {
    let descriptor = ArtifactChunkDescriptor {
      encoded_bytes: u32::try_from(encoded.len())?,
      encoded_sha256: Sha256::digest(&encoded).to_vec(),
      ..Default::default()
    };
    Ok::<_, anyhow::Error>((encoded, descriptor))
  })
  .await??;
  write_chunk(fs, chunk_path(id, index), encoded, files).await?;
  Ok(descriptor)
}

async fn write_chunk(
  fs: &dyn FileSystem,
  path: PathBuf,
  encoded: Vec<u8>,
  files: &mut StagedFiles,
) -> anyhow::Result<()> {
  files.created.push(path.clone());
  let mut target = fs.create_file(&path).await?;
  target.write_all(&encoded).await?;
  target.flush().await?;
  fs.sync_file(&path).await
}

//
// StreamVerifier
//

struct StreamVerifier {
  whole: Sha256,
  decoder: Decompress,
  ended: bool,
  output: Option<Vec<u8>>,
}

impl StreamVerifier {
  fn new(output: Option<Vec<u8>>) -> Self {
    Self {
      whole: Sha256::new(),
      decoder: Decompress::new(true),
      ended: false,
      output,
    }
  }

  fn feed_fragment(
    &mut self,
    encoded: &[u8],
    descriptor: &ArtifactChunkDescriptor,
    expected_encoded: u64,
    size: u64,
  ) -> anyhow::Result<()> {
    if encoded.len() != descriptor.encoded_bytes as usize
      || Sha256::digest(encoded).as_slice() != descriptor.encoded_sha256
    {
      anyhow::bail!("staged chunk does not match encoded descriptor");
    }

    let mut offset = 0;
    let mut output = vec![0; 64 * 1024];
    loop {
      let before_in = self.decoder.total_in();
      let before_out = self.decoder.total_out();
      let status = self
        .decoder
        .decompress(&encoded[offset ..], &mut output, FlushDecompress::None)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
      offset += usize::try_from(self.decoder.total_in() - before_in)?;
      let produced = usize::try_from(self.decoder.total_out() - before_out)?;
      anyhow::ensure!(
        self.decoder.total_out() <= size,
        "staged stream exceeds logical size"
      );
      self.whole.update(&output[.. produced]);
      if let Some(decoded) = &mut self.output {
        decoded.extend_from_slice(&output[.. produced]);
      }
      if status == Status::StreamEnd {
        anyhow::ensure!(
          self.decoder.total_in() == expected_encoded,
          "trailing staged stream bytes"
        );
        self.ended = true;
        break;
      }
      if offset == encoded.len() && produced < output.len() {
        break;
      }
      anyhow::ensure!(
        self.decoder.total_in() != before_in || produced != 0,
        "stalled staged decoder"
      );
    }
    Ok(())
  }

  fn finish(self, size: u64, expected_digest: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    if (size != 0 && (!self.ended || self.decoder.total_out() != size))
      || self.whole.finalize().as_slice() != expected_digest
    {
      anyhow::bail!("staged artifact does not match whole logical digest");
    }
    Ok(self.output)
  }
}

async fn validate_files(
  fs: &dyn FileSystem,
  request: &UploadArtifactRequest,
) -> anyhow::Result<()> {
  // validate_entry has already established geometry and allowed encodings. This pass verifies the
  // corresponding disk bytes without loading more than one encoded/decoded chunk at a time.
  let size = request
    .logical_bytes
    .ok_or_else(|| anyhow::anyhow!("missing logical size"))?;
  let expected_encoded = request.encoded_bytes.unwrap_or_default();
  let mut verifier = StreamVerifier::new(None);
  for (index, descriptor) in request.chunks.iter().enumerate() {
    let mut file = fs
      .open_regular_file(&chunk_path(&request.artifact_id, index))
      .await?;
    let encoded = read_limited(&mut file, u64::from(descriptor.encoded_bytes)).await?;
    let descriptor = descriptor.clone();
    verifier = tokio::task::spawn_blocking(move || {
      verifier.feed_fragment(&encoded, &descriptor, expected_encoded, size)?;
      Ok::<_, anyhow::Error>(verifier)
    })
    .await??;
  }
  verifier.finish(size, &request.logical_sha256)?;
  Ok(())
}

pub async fn read_pending_report(
  sdk_directory: &Path,
  request: &UploadArtifactRequest,
) -> anyhow::Result<Vec<u8>> {
  if request.type_id != CLIENT_REPORT_ARTIFACT_TYPE_ID {
    anyhow::bail!("queued artifact is not a client report");
  }
  validate_entry(request)?;
  let size = request
    .logical_bytes
    .ok_or_else(|| anyhow::anyhow!("missing logical size"))?;
  let expected_encoded = request.encoded_bytes.unwrap_or_default();
  let mut verifier = StreamVerifier::new(Some(Vec::with_capacity(usize::try_from(size)?)));
  for (index, descriptor) in request.chunks.iter().enumerate() {
    let path = sdk_directory.join(chunk_path(&request.artifact_id, index));
    let mut file = open_regular_file_async(&path).await?;
    let fragment = read_limited(&mut file, u64::from(descriptor.encoded_bytes)).await?;
    let descriptor = descriptor.clone();
    verifier = tokio::task::spawn_blocking(move || {
      verifier.feed_fragment(&fragment, &descriptor, expected_encoded, size)?;
      Ok::<_, anyhow::Error>(verifier)
    })
    .await??;
  }
  let decoded = verifier
    .finish(size, &request.logical_sha256)?
    .ok_or_else(|| anyhow::anyhow!("missing decoded queued report buffer"))?;
  Ok(decoded)
}

impl Uploader {
  pub(super) async fn prepare_chunked(
    &self,
    source: UploadSource,
    request: UploadArtifactRequest,
    source_cleanup: &mut Option<OwnedFile>,
    files: &mut StagedFiles,
  ) -> anyhow::Result<UploadArtifactRequest> {
    let source = if matches!(source, UploadSource::Bytes(_) | UploadSource::File(_)) {
      let input = Input::open(source, self.file_system.as_ref()).await?;
      let size = input.size();
      if size == 0 && request.command_id.is_none() {
        anyhow::bail!("empty non-command artifact");
      }
      self
        .chunked
        .admission
        .check_capacity(
          self.file_system.as_ref(),
          size,
          0,
          None,
          artifact_encoded_bound(size),
        )
        .await?;
      return stage(self.file_system.as_ref(), input, request, files, false).await;
    } else {
      source
    };
    let (path, retained) = match source {
      UploadSource::Path(path) => (path, false),
      UploadSource::Retained(path)
        if request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
          && path == Path::new(&format!("workflow-attachments/{}.raw", request.artifact_id)) =>
      {
        (path, true)
      },
      _ => anyhow::bail!("chunked staging requires an owned capture or retained workflow file"),
    };
    let file = self.file_system.open_regular_file(&path).await?;
    let metadata = file.metadata().await?;
    if retained {
      // The retained store already admitted these bytes and reserved RAW staging headroom.
      // Runtime reductions cannot strand an accepted workflow attachment before its first flush.
      if metadata.len() <= 4 || metadata.len() - 4 > u64::from(MAX_ARTIFACT_LOGICAL_BYTES) {
        anyhow::bail!("retained workflow logical byte limit exceeded");
      }
      return stage(
        self.file_system.as_ref(),
        Input::from_file(file, &metadata)?,
        request,
        files,
        true,
      )
      .await;
    }
    if metadata.len() == 0 && request.command_id.is_none() {
      anyhow::bail!("empty workflow artifact");
    }
    self
      .chunked
      .admission
      .admit_capture(self.file_system.as_ref(), &metadata)
      .await?;
    if request.type_id == STATE_SNAPSHOT_ARTIFACT_TYPE_ID {
      // The snapshot producer still owns its source until the new manifest is durable.
      files.remove_source = Some(path);
      return stage(
        self.file_system.as_ref(),
        Input::from_file(file, &metadata)?,
        request,
        files,
        false,
      )
      .await;
    }
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
    if file_identity(&moved) != file_identity(&metadata) || moved.len() != metadata.len() {
      anyhow::bail!("capture path changed during ownership transfer");
    }
    self
      .file_system
      .sync_file_and_parent(&retained_source)
      .await?;
    stage(
      self.file_system.as_ref(),
      Input::from_file(file, &metadata)?,
      request,
      files,
      false,
    )
    .await
  }

  pub(super) async fn recover_chunks(&mut self, modified: &mut bool) -> anyhow::Result<()> {
    // TODO: Recover entries incrementally through the index owner so the first valid artifact can
    // transfer before the entire logical backlog is checked. Preserve collision/corruption fencing
    // and gate preparation/admission until recovery has established every durable ownership lease.
    // Rebuild ownership from committed entries, not current runtime admission limits.
    let mut ids = HashSet::new();
    let mut retained = Vec::new();
    for request in std::mem::take(&mut self.chunked.entries) {
      // Reject malformed templates before deriving paths, allocating chunk buffers or decoding
      // data.
      if let Err(error) = validate_entry(&request) {
        log::warn!("discarding invalid chunked artifact index entry: {error}");
        *modified = true;
        continue;
      }
      let id = &request.artifact_id;
      if !ids.insert(id.clone()) {
        // Remove both owners of a collision; choosing one could hide conflicting payload identity.
        log::warn!("discarding duplicate chunked artifact {id}");
        retained.retain(|entry: &UploadArtifactRequest| entry.artifact_id != *id);
        *modified = true;
        continue;
      }
      // The shared initializer persists any removals before orphan cleanup may delete payload
      // files. Unexpected I/O errors must not discard accepted ownership.
      if let Err(error) = validate_files(self.file_system.as_ref(), &request).await {
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
      log::debug!(
        "recovered chunked artifact: artifact_id={id} chunks={} logical_bytes={:?}",
        request.chunks.len(),
        request.logical_bytes,
      );
      retained.push(request);
    }
    self.chunked.entries = retained;
    Ok(())
  }
}
