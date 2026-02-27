// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
