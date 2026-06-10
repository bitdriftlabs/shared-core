// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_macros::proto_serializable;
use tempfile::TempDir;

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TestState {
  #[field(id = 1)]
  value: String,
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
