// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./file_test.rs"]
mod tests;

use bd_log_primitives::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use flate2::Compression;
use flate2::read::{ZlibDecoder, ZlibEncoder};
use std::io::Read;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub fn write_compressed_protobuf<T: protobuf::Message>(message: &T) -> anyhow::Result<Vec<u8>> {
  let bytes = message.write_to_bytes()?;
  write_compressed(&bytes)
}

pub fn write_compressed(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
  let mut encoder = ZlibEncoder::new(
    bytes,
    Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL),
  );
  let mut compressed_bytes = Vec::new();
  encoder.read_to_end(&mut compressed_bytes)?;
  Ok(compressed_bytes)
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
  let mut decoder = compressed_reader(bytes);
  let mut decompressed_bytes = Vec::new();
  decoder.read_to_end(&mut decompressed_bytes)?;
  Ok(decompressed_bytes)
}

pub fn compressed_reader(reader: impl Read) -> impl Read {
  // TODO(mattklein123): Zero-initializing is not necessary here but we will defer this for now.
  ZlibDecoder::new_with_buf(reader, vec![0; 1024])
}

pub fn read_compressed_protobuf<T: protobuf::Message>(
  compressed_bytes: &[u8],
) -> anyhow::Result<T> {
  let decompressed_bytes = read_compressed(compressed_bytes)?;
  Ok(T::parse_from_tokio_bytes(&decompressed_bytes.into())?)
}

/// Writes the data and appends a CRC checksum at the end of the slice. The checksum is a 4-byte
/// little-endian CRC32 checksum of the data.
#[must_use]
pub fn write_checksummed_data(bytes: &[u8]) -> Vec<u8> {
  let crc = crc32fast::hash(bytes);

  let mut result = Vec::with_capacity(bytes.len() + 4);
  result.extend_from_slice(bytes);
  result.extend_from_slice(&crc.to_le_bytes());
  result
}

/// Writes the data from the reader to the writer and appends a CRC checksum at the end of the
/// slice. The checksum is a 4-byte little-endian CRC32 checksum of the data.
pub async fn async_write_checksummed_data(
  mut reader: impl AsyncReadExt + Unpin,
  mut writer: impl AsyncWriteExt + Unpin,
) -> anyhow::Result<()> {
  let mut crc = crc32fast::Hasher::new();
  let mut buffer = vec![0; 1024];
  while let Ok(bytes_read) = reader.read(&mut buffer).await
    && bytes_read > 0
  {
    crc.update(&buffer[.. bytes_read]);
    writer.write_all(&buffer[.. bytes_read]).await?;
  }

  let crc = crc.finalize();
  // Write the CRC at the end of the data.
  let crc_bytes = crc.to_le_bytes();
  writer.write_all(&crc_bytes).await?;

  Ok(())
}

/// Reads the data and checks the CRC checksum at the end of the slice. If the checksum is valid, it
/// returns the data.
pub fn read_checksummed_data(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
  if bytes.len() < 4 {
    anyhow::bail!("data too small to contain CRC checksum");
  }

  let (data, crc_bytes) = bytes.split_at(bytes.len() - 4);
  let crc = u32::from_le_bytes(crc_bytes.try_into()?);
  let expected_crc = crc32fast::hash(data);

  if expected_crc != crc {
    anyhow::bail!("crc mismatch");
  }

  Ok(data.to_vec())
}
