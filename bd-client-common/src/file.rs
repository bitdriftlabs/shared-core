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
  let mut encoder = ZlibEncoder::new(
    &bytes[..],
    Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL),
  );
  let mut compressed_bytes = Vec::new();
  encoder.read_to_end(&mut compressed_bytes).unwrap();
  compressed_bytes
}

pub fn read_compressed_protobuf<T: protobuf::Message>(
  compressed_bytes: &[u8],
) -> anyhow::Result<T> {
  // We should never write empty files. If there is no data this was a partial write, full disk
  // issue, or some other problem. We use zlib for compression which includes a CRC at the end
  // so as long as the file is not empty we can be sure that the data is not corrupted.
  if compressed_bytes.is_empty() {
    anyhow::bail!("unexpected empty file");
  }

  // The files are likely not large enough to deal with streaming decompression on top of flate2.
  // For now we just read the entire thing and then decompress it in memory. We can consider
  // streaming later. Generally these are small files so we use a small buffer to avoid needless
  // allocation.
  // TODO(mattklein123): Zero-initializing is not necessary here but we will defer this for now.
  let mut decoder = ZlibDecoder::new_with_buf(compressed_bytes, vec![0; 1024]);
  // In the future if/when we switch to using Bytes/Chars in the compiled proto it may be more
  // efficient to read out the uncompressed into Bytes and then parse from that.
  Ok(T::parse_from_reader(&mut decoder)?)
}
