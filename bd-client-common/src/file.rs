// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use std::io::Read;

pub fn write_compressed_protobuf<T: protobuf::Message>(message: &T) -> Vec<u8> {
  let bytes = message.write_to_bytes().unwrap();
  write_compressed(&bytes)
}

#[must_use]
pub fn write_compressed(bytes: &[u8]) -> Vec<u8> {
  let mut encoder = ZlibEncoder::new(
    bytes,
    Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL),
  );
  let mut compressed_bytes = Vec::new();
  encoder.read_to_end(&mut compressed_bytes).unwrap();
  compressed_bytes
}

pub fn read_compressed(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
  // We should never write empty files. If there is no data this was a partial write, full disk
  // issue, or some other problem. We use zlib for compression which includes a CRC at the end
  // so as long as the file is not empty we can be sure that the data is not corrupted.
  if bytes.is_empty() {
    anyhow::bail!("unexpected empty file");
  }

  // The files are likely not large enough to deal with streaming decompression on top of flate2.
  // For now we just read the entire thing and then decompress it in memory. We can consider
  // streaming later. Generally these are small files so we use a small buffer to avoid needless
  // allocation.
  // TODO(mattklein123): Zero-initializing is not necessary here but we will defer this for now.
  let mut decoder = ZlibDecoder::new_with_buf(bytes, vec![0; 1024]);
  let mut decompressed_bytes = Vec::new();
  decoder.read_to_end(&mut decompressed_bytes)?;
  Ok(decompressed_bytes)
}

pub fn read_compressed_protobuf<T: protobuf::Message>(
  compressed_bytes: &[u8],
) -> anyhow::Result<T> {
  let decompressed_bytes = read_compressed(compressed_bytes)?;
  Ok(T::parse_from_tokio_bytes(&decompressed_bytes.into())?)
}
