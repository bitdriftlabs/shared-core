#[test]
fn write_checksummed_data() {
  let data = b"100";

  let checksummed_data = super::write_checksummed_data(data);

  assert_eq!(
    data,
    super::read_checksummed_data(&checksummed_data)
      .unwrap()
      .as_slice(),
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
