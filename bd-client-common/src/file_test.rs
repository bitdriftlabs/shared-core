// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_macros::proto_serializable;
use fs2::FileExt;
use std::fs::File;
use std::io::Cursor;
use tempfile::TempDir;

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TestState {
  #[field(id = 1)]
  value: String,
}

#[test]
fn prepare_file_for_mmap_resizes_and_preallocates() -> anyhow::Result<()> {
  let temp_directory = TempDir::with_prefix("file-test")?;
  let path = temp_directory.path().join("mmap");
  let file = File::create(&path)?;
  let size = fs2::allocation_granularity(temp_directory.path())? * 2;

  assert!(super::prepare_file_for_mmap(&file, &path, Some(size))?);
  assert_eq!(file.metadata()?.len(), size);
  assert!(file.allocated_size()? >= size);
  Ok(())
}

#[test]
fn disk_space_check_skips_unsized_filesystems() {
  assert!(super::has_disk_space(10, 10, 100));
  assert!(!super::has_disk_space(11, 10, 100));
  assert!(!super::has_disk_space(1, 0, 100));
  assert!(super::has_disk_space(1, 0, 0));
}

#[test]
fn write_checksummed_data() {
  let data = b"100";

  let checksummed_data = super::write_checksummed_data(data);

  assert_eq!(
    data,
    super::read_checksummed_data(&checksummed_data)
      .unwrap()
      .as_slice()
  );
}

#[tokio::test]
async fn bounded_attachment_reads_report_uncompressed_size() {
  let bytes = vec![42; 1000];
  let (compressed, size_bytes) =
    super::read_and_compress_limited_with_size(Cursor::new(&bytes), 1000)
      .await
      .unwrap();
  assert_eq!(size_bytes, 1000);
  assert!(compressed.len() < bytes.len());
  assert_eq!(super::read_compressed(&compressed).unwrap(), bytes);
  assert_eq!(
    super::read_limited(Cursor::new(&bytes), 1000)
      .await
      .unwrap(),
    bytes
  );
  assert!(super::read_limited(Cursor::new(&bytes), 999).await.is_err());
}

#[test]
fn bounded_decode_checks_expansion_limit_and_integrity() {
  let bytes = vec![42; 1000];
  let compressed = super::write_compressed(&bytes).unwrap();
  assert_eq!(
    super::read_compressed_limited(&compressed, 1000).unwrap(),
    bytes
  );
  assert_eq!(
    super::read_compressed_limited(&compressed, 999)
      .unwrap_err()
      .kind(),
    std::io::ErrorKind::InvalidData
  );
  assert!(super::read_compressed_limited(&[], 1000).is_err());
  assert!(super::read_compressed_limited(&compressed[.. compressed.len() - 1], 1000).is_err());
  let mut corrupt = compressed;
  corrupt[0] ^= 0xff;
  assert_eq!(
    super::read_compressed_limited(&corrupt, 1000)
      .unwrap_err()
      .kind(),
    std::io::ErrorKind::InvalidData
  );
}

// Supplies one readable prefix then an actual I/O failure, rather than a short but valid EOF.
struct FailingReader(bool);

impl tokio::io::AsyncRead for FailingReader {
  fn poll_read(
    mut self: std::pin::Pin<&mut Self>,
    _: &mut std::task::Context<'_>,
    buffer: &mut tokio::io::ReadBuf<'_>,
  ) -> std::task::Poll<std::io::Result<()>> {
    if self.0 {
      std::task::Poll::Ready(Err(std::io::Error::from(
        std::io::ErrorKind::PermissionDenied,
      )))
    } else {
      self.0 = true;
      buffer.put_slice(b"prefix");
      std::task::Poll::Ready(Ok(()))
    }
  }
}

#[tokio::test]
async fn bounded_reads_and_checksummed_copy_preserve_input_errors() {
  assert_eq!(
    super::read_limited(FailingReader(false), 100)
      .await
      .unwrap_err()
      .kind(),
    std::io::ErrorKind::PermissionDenied
  );
  let mut output = Vec::new();
  let error = super::async_write_checksummed_data(FailingReader(false), &mut output)
    .await
    .unwrap_err();
  assert_eq!(
    error.downcast_ref::<std::io::Error>().unwrap().kind(),
    std::io::ErrorKind::PermissionDenied
  );
  // A read error must not be followed by a valid checksum for the truncated prefix.
  assert_eq!(output, b"prefix");
}

#[test]
fn owned_file_cleanup_and_transfer_do_not_remove_replacements() {
  use crate::file_system::OwnedFile;
  let directory = TempDir::with_prefix("file-test").unwrap();
  let path = directory.path().join("capture");
  std::fs::write(&path, b"owned").unwrap();
  drop(OwnedFile::new(&path).unwrap());
  assert!(!path.exists());
  assert!(OwnedFile::new(std::path::Path::new("relative")).is_none());

  std::fs::write(&path, b"transferred").unwrap();
  let guard = OwnedFile::new(&path).unwrap();
  let moved = directory.path().join("staged");
  std::fs::rename(&path, &moved).unwrap();
  guard.disarm();
  std::fs::write(&path, b"replacement").unwrap();
  assert_eq!(std::fs::read(path).unwrap(), b"replacement");
  assert_eq!(std::fs::read(moved).unwrap(), b"transferred");
}

#[test]
fn invalid_checksum_too_small() {
  let data = b"1";

  assert_eq!(
    super::read_checksummed_data(data).unwrap_err().to_string(),
    "data too small to contain CRC checksum"
  );
}

#[test]
fn invalid_checksum() {
  let data = b"111111";

  assert_eq!(
    super::read_checksummed_data(data).unwrap_err().to_string(),
    "crc mismatch"
  );
}

#[tokio::test]
async fn compressed_protobuf_file_round_trips() {
  let temp_directory = TempDir::with_prefix("file-test").unwrap();
  let path = temp_directory.path().join("state.pb");
  let state = TestState {
    value: "abc".to_string(),
  };

  super::write_compressed_protobuf_file(&path, &state)
    .await
    .unwrap();

  assert_eq!(
    super::read_compressed_protobuf_file_if_exists::<TestState>(&path)
      .await
      .unwrap(),
    Some(state)
  );
}

#[tokio::test]
async fn missing_compressed_protobuf_file_returns_none() {
  let temp_directory = TempDir::with_prefix("file-test").unwrap();
  let path = temp_directory.path().join("missing.pb");

  assert_eq!(
    super::read_compressed_protobuf_file_if_exists::<TestState>(&path)
      .await
      .unwrap(),
    None
  );
}
