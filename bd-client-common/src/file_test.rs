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
use std::pin::Pin;
use std::task::{Context, Poll};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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

struct CountingIo<T> {
  inner: T,
  calls: usize,
}

impl<T: AsyncRead + Unpin> AsyncRead for CountingIo<T> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    context: &mut Context<'_>,
    buffer: &mut ReadBuf<'_>,
  ) -> Poll<std::io::Result<()>> {
    self.calls += 1;
    Pin::new(&mut self.inner).poll_read(context, buffer)
  }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for CountingIo<T> {
  fn poll_write(
    mut self: Pin<&mut Self>,
    context: &mut Context<'_>,
    buffer: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    self.calls += 1;
    Pin::new(&mut self.inner).poll_write(context, buffer)
  }

  fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Pin::new(&mut self.inner).poll_flush(context)
  }

  fn poll_shutdown(
    mut self: Pin<&mut Self>,
    context: &mut Context<'_>,
  ) -> Poll<std::io::Result<()>> {
    Pin::new(&mut self.inner).poll_shutdown(context)
  }
}

#[tokio::test]
async fn checksummed_copy_batches_io_without_changing_bytes() {
  let data: Vec<u8> = (0 ..= 255).cycle().take(256 * 1024 + 17).collect();
  let mut reader = CountingIo {
    inner: Cursor::new(&data),
    calls: 0,
  };
  let mut writer = CountingIo {
    inner: Vec::new(),
    calls: 0,
  };

  super::async_write_checksummed_data(&mut reader, &mut writer)
    .await
    .unwrap();

  assert_eq!(writer.inner.len(), data.len() + 4);
  assert_eq!(super::read_checksummed_data(&writer.inner).unwrap(), data);
  assert!(reader.calls <= 8, "read calls: {}", reader.calls);
  assert!(writer.calls <= 8, "write calls: {}", writer.calls);
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

#[tokio::test]
async fn owned_file_remove_is_awaitable_and_reports_errors() {
  use crate::file_system::OwnedFile;

  let directory = TempDir::with_prefix("file-test").unwrap();
  let path = directory.path().join("capture");
  tokio::fs::write(&path, b"owned").await.unwrap();
  OwnedFile::new(&path).unwrap().remove().await.unwrap();
  assert!(!path.exists());
  OwnedFile::new(&path).unwrap().remove().await.unwrap();

  assert!(
    OwnedFile::new(directory.path())
      .unwrap()
      .remove()
      .await
      .is_err()
  );
}

#[test]
fn cancelled_owned_file_remove_does_not_unlink_a_replacement() {
  use crate::file_system::OwnedFile;
  use std::future::Future as _;
  use std::sync::{Arc, mpsc};
  use std::task::{Wake, Waker};

  struct CompletionSignal(mpsc::Sender<()>);

  impl Wake for CompletionSignal {
    fn wake(self: Arc<Self>) {
      let _ = self.0.send(());
    }
  }

  let directory = TempDir::with_prefix("file-test").unwrap();
  let path = directory.path().join("capture");
  std::fs::write(&path, b"owned").unwrap();
  let runtime = tokio::runtime::Builder::new_current_thread()
    .max_blocking_threads(1)
    .build()
    .unwrap();
  let (entered_tx, entered_rx) = mpsc::channel();
  let (release_tx, release_rx) = mpsc::channel();
  let blocker = runtime.spawn_blocking(move || {
    entered_tx.send(()).unwrap();
    release_rx.recv().unwrap();
  });
  entered_rx.recv().unwrap();

  let (completed_tx, completed_rx) = mpsc::channel();
  let waker = Waker::from(Arc::new(CompletionSignal(completed_tx)));
  let mut context = Context::from_waker(&waker);
  let mut removal = Box::pin(OwnedFile::new(&path).unwrap().remove());
  // Hold the only blocking worker until the removal has been submitted and its waker registered.
  let result = {
    let _runtime = runtime.enter();
    removal.as_mut().poll(&mut context)
  };
  release_tx.send(()).unwrap();
  assert!(result.is_pending());
  completed_rx.recv().unwrap();
  runtime.block_on(blocker).unwrap();

  std::fs::write(&path, b"replacement").unwrap();
  // Cancel after unlink completes but before the awaiting future observes completion.
  drop(removal);
  assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

#[cfg(unix)]
#[tokio::test]
async fn real_filesystem_lists_files_and_follows_symlinks() {
  use crate::file_system::{FileSystem, RealFileSystem};

  let directory = TempDir::with_prefix("file-test").unwrap();
  let file = directory.path().join("file");
  let subdirectory = directory.path().join("directory");
  let file_link = directory.path().join("file-link");
  tokio::fs::write(&file, b"data").await.unwrap();
  tokio::fs::create_dir(&subdirectory).await.unwrap();
  tokio::fs::symlink(&file, &file_link).await.unwrap();
  tokio::fs::symlink(&subdirectory, directory.path().join("directory-link"))
    .await
    .unwrap();
  let filesystem = RealFileSystem::new(directory.path().to_owned());

  let mut files = filesystem
    .list_files(std::path::Path::new(""))
    .await
    .unwrap();
  files.sort();
  let mut expected = vec![
    file.to_string_lossy().to_string(),
    file_link.to_string_lossy().to_string(),
  ];
  expected.sort();
  assert_eq!(files, expected);

  tokio::fs::symlink(
    directory.path().join("missing"),
    directory.path().join("dangling"),
  )
  .await
  .unwrap();
  let error = filesystem
    .list_files(std::path::Path::new(""))
    .await
    .unwrap_err();
  assert_eq!(
    error.downcast_ref::<std::io::Error>().unwrap().kind(),
    std::io::ErrorKind::NotFound
  );
}

#[tokio::test]
async fn real_filesystem_available_space_preserves_io_errors() {
  use crate::file_system::{FileSystem, RealFileSystem};

  let directory = TempDir::with_prefix("file-test").unwrap();
  let filesystem = RealFileSystem::new(directory.path().to_owned());
  assert!(
    filesystem
      .available_space(std::path::Path::new(""))
      .await
      .is_ok()
  );
  let error = filesystem
    .available_space(std::path::Path::new("missing"))
    .await
    .unwrap_err();
  assert_eq!(
    error.downcast_ref::<std::io::Error>().unwrap().kind(),
    std::io::ErrorKind::NotFound
  );
}

#[tokio::test]
async fn filesystem_syncs_preserve_faults_gates_and_count_attempts() {
  use crate::file_system::FileSystem;
  use crate::test::TestFileSystem;
  use std::path::Path;
  use std::sync::Arc;
  use std::sync::atomic::Ordering;

  let filesystem = Arc::new(TestFileSystem::new());
  let directory = Path::new("staged");
  let chunk = Path::new("staged/chunk");
  filesystem.create_dir(directory).await.unwrap();
  filesystem.write_file(chunk, b"data").await.unwrap();

  let gate = filesystem.gate_next_sync(chunk.to_owned());
  let sync_filesystem = filesystem.clone();
  let sync = tokio::spawn(async move { sync_filesystem.sync_file(chunk).await });
  let resume = gate.await.unwrap();
  assert_eq!(filesystem.file_sync_calls.load(Ordering::Relaxed), 1);
  assert_eq!(filesystem.directory_sync_calls.load(Ordering::Relaxed), 0);
  resume.send(()).unwrap();
  sync.await.unwrap().unwrap();

  filesystem.fail_next_sync.store(true, Ordering::Relaxed);
  assert!(filesystem.sync_file(chunk).await.is_err());
  filesystem.fail_next_sync_at(chunk.to_owned());
  assert!(filesystem.sync_file_and_parent(chunk).await.is_err());
  assert_eq!(filesystem.file_sync_calls.load(Ordering::Relaxed), 3);
  assert_eq!(filesystem.directory_sync_calls.load(Ordering::Relaxed), 0);

  filesystem.sync_file_and_parent(chunk).await.unwrap();
  assert_eq!(filesystem.file_sync_calls.load(Ordering::Relaxed), 4);
  assert_eq!(filesystem.directory_sync_calls.load(Ordering::Relaxed), 1);
  filesystem.fail_next_sync_at(directory.to_owned());
  assert!(filesystem.sync_directory(directory).await.is_err());
  filesystem.sync_directory(directory).await.unwrap();
  assert_eq!(filesystem.directory_sync_calls.load(Ordering::Relaxed), 3);

  filesystem.list_files(directory).await.unwrap();
  assert_eq!(filesystem.list_calls.load(Ordering::Relaxed), 1);
  filesystem.fail_deletes.store(true, Ordering::Relaxed);
  filesystem.delete_file(Path::new("absent")).await.unwrap();
  assert!(filesystem.delete_file(chunk).await.is_err());
  filesystem.fail_deletes.store(false, Ordering::Relaxed);
  filesystem.delete_file(chunk).await.unwrap();
  assert_eq!(filesystem.delete_calls.load(Ordering::Relaxed), 3);
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
