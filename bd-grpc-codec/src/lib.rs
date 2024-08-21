// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./coding_test.rs"]
mod coding_test;

pub mod stats;

use crate::stats::DeferredCounter;
use bd_client_common::error::handle_unexpected_error_with_details;
use bd_stats_common::DynCounter;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::write::ZlibEncoder;
use protobuf::{CodedOutputStream, Message};
use std::io::{Read, Write};
use std::marker::PhantomData;

// Compression algorithms supported by crate's codes.
#[derive(Debug)]
pub enum Compression {
  Zlib,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("protobuf error: {0}")]
  Protobuf(#[from] protobuf::Error),
  #[error("An io error ocurred: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

// Compression byte + 4 message size bytes.
const GRPC_MESSAGE_PREFIX_LEN: usize = 5;
// Expressed in bytes, the minimum size of the message for it to be considered
// compressable. Used to avoid compression of small messages whose compressed
// version is often greater in size than orginal.
const GRPC_MIN_MESSAGE_SIZE_COMPRESSION_THRESHOLD: usize = 100;
// zlib has 10 compression levels (0-9). We use level 5 as this is what Apple says about
// this particular compression level: "This compression level provides a good balance
// between compression speed and compression ratio.".
// Source: https://developer.apple.com/documentation/compression/algorithm/zlib
const ZLIB_COMPRESSION_LEVEL: u32 = 5;

//
// Encoder
//

#[derive(Debug)]
pub struct Encoder<MessageType: protobuf::Message> {
  compressor: Option<flate2::write::ZlibEncoder<bytes::buf::Writer<BytesMut>>>,
  tx_bytes: DeferredCounter,
  tx_bytes_uncompressed: DeferredCounter,
  _type: PhantomData<MessageType>,
}

impl<MessageType: protobuf::Message> Encoder<MessageType> {
  #[must_use]
  pub fn new(compression: Option<Compression>) -> Self {
    let compressor = compression.map(|_| {
      flate2::write::ZlibEncoder::new(
        BytesMut::new().writer(),
        flate2::Compression::new(ZLIB_COMPRESSION_LEVEL),
      )
    });

    Self {
      compressor,
      tx_bytes: DeferredCounter::default(),
      tx_bytes_uncompressed: DeferredCounter::default(),
      _type: PhantomData,
    }
  }

  // Converts a Protobuf message into a gRPC frame, potentially compressing the message.
  pub fn encode(&mut self, message: &MessageType) -> Bytes {
    // Serialize the Protobuf message then prefix it with the compression byte and the length in big
    // endian (the default for BytesMut).
    // See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests for an
    // explanation of the gRPC wire format.

    #[allow(clippy::cast_possible_truncation)]
    let message_size = message.compute_size() as usize;
    self
      .tx_bytes_uncompressed
      .inc_by(message_size + GRPC_MESSAGE_PREFIX_LEN);

    let bytes = match (
      &mut self.compressor,
      message_size >= GRPC_MIN_MESSAGE_SIZE_COMPRESSION_THRESHOLD,
    ) {
      (Some(compressor), true) => match Self::encode_compressed(compressor, message) {
        Ok(compressed) => compressed,
        Err(e) => {
          handle_unexpected_error_with_details(
            e,
            "gRPC compression failed, falling back to uncompressed stream and disabling \
             compression",
            || None,
          );
          // Compression failed, fallback to uncompressed and nullify compressor so that
          // the encoder doesn't make further attempt to compress incoming messages. This is to
          // avoid compressing with the use of compressor that's potentially in a bad state.
          self.compressor = None;
          Self::encode_uncompressed(message)
        },
      },
      _ => Self::encode_uncompressed(message),
    };

    self.tx_bytes.inc_by(bytes.len());

    bytes
  }

  #[must_use]
  pub const fn bandwidth_stats(&self) -> (u64, u64) {
    (self.tx_bytes.count(), self.tx_bytes_uncompressed.count())
  }

  pub fn inc_stats(&mut self, tx: usize, tx_uncompressed: usize) {
    self.tx_bytes.inc_by(tx);
    self.tx_bytes_uncompressed.inc_by(tx_uncompressed);
  }

  pub fn initialize_stats(&mut self, tx_bytes: DynCounter, tx_bytes_uncompressed: DynCounter) {
    self.tx_bytes.initialize(tx_bytes);
    self.tx_bytes_uncompressed.initialize(tx_bytes_uncompressed);
  }

  fn encode_compressed(
    compressor: &mut ZlibEncoder<bytes::buf::Writer<BytesMut>>,
    message: &MessageType,
  ) -> Result<Bytes> {
    message.write_to_writer(compressor)?;

    compressor.flush()?;

    #[allow(clippy::cast_possible_truncation)]
    let compressed_message_size = compressor.get_ref().get_ref().len() as u32;

    let mut buffer = BytesMut::new();
    buffer.put_u8(1); // Compression byte, message compressed.
    buffer.put_u32(compressed_message_size);

    let mut buffer_writter = buffer.writer();
    std::io::copy(
      &mut compressor.get_mut().get_mut().reader(),
      &mut buffer_writter,
    )?;

    // This assumes that `compute_size()` was called first. It's called as part
    // of the `write_to_writer` method call.
    let message_size = message.cached_size();
    #[allow(clippy::cast_precision_loss, clippy::cast_lossless)]
    let ratio = compressed_message_size as f64 * 1.0 / message_size as f64;
    log::trace!(
      "compression completed; {} bytes compressed to {} bytes, compression ratio: {:.2}",
      message_size,
      compressed_message_size,
      ratio
    );

    Ok(buffer_writter.into_inner().freeze())
  }

  fn encode_uncompressed(message: &MessageType) -> Bytes {
    #[allow(clippy::cast_possible_truncation)]
    let message_size: usize = message.compute_size() as usize;
    // Create a buffer with enough size to serialize the message as well as the 5 byte prefix.
    let total_size = message_size + GRPC_MESSAGE_PREFIX_LEN;

    let mut buffer = BytesMut::with_capacity(total_size);
    buffer.put_u8(0); // Compression byte, message uncompressed.
    #[allow(clippy::cast_possible_truncation)]
    buffer.put_u32(message_size as u32);

    // The data is not yet initialized. We must set the length to be able to get a mutable
    // slice. This is safe as we guarantee the capacity above.
    unsafe { buffer.set_len(total_size) };
    {
      let message_slice = buffer.get_mut(GRPC_MESSAGE_PREFIX_LEN ..).unwrap();
      let mut output_stream = CodedOutputStream::bytes(message_slice);
      message
        .write_to_with_cached_sizes(&mut output_stream)
        .unwrap();
      log::trace!("writing message len={}", message_size);
    }

    buffer.freeze()
  }
}

//
// Decoder
//

// A stateful gRPC decoder. As data is added for decoding, the decoder will attempt to decode as
// many messages as possible. If the data contains a partial message, the remaining partial data
// will be retained combined with the data added when decode is next called. This allows for online
// processing of a data stream which might does not align with gRPC message boundaries (e.g. a
// single gRPC message split between multiple DATA frames).
#[derive(Debug)]
pub struct Decoder<MessageType: Message> {
  decompressor: flate2::read::ZlibDecoder<bytes::buf::Reader<BytesMut>>,
  current_message_compressed: bool,
  current_message_size: Option<usize>,
  _type: PhantomData<MessageType>,
  rx: DeferredCounter,
  rx_decompressed: DeferredCounter,
}

impl<MessageType: Message> Decoder<MessageType> {
  #[must_use]
  pub const fn bandwidth_stats(&self) -> (u64, u64) {
    (self.rx.count(), self.rx_decompressed.count())
  }

  // Decodes data, returning all complete messages parsed from the incoming data + any leftover
  // data from a previous chunk of data.
  pub fn decode_data(&mut self, data: &[u8]) -> Result<Vec<MessageType>> {
    self.buffer().extend_from_slice(data);

    self.rx.inc_by(data.len());

    let mut messages: Vec<MessageType> = Vec::new();

    // To parse the incoming data, we use a simple state machine:
    // - At the start, we attempt to read enough data to pase the gRPC message prefix (1 byte for
    //   compression, 4 for message size) and use this to determine how large the current message
    //   is.
    // - Once we know the message size, we attempt to read the data for the entire message. At this
    //   point we record the parsed message and go back to step 1.
    // - We end parsing once there is not enough data available to parse either the message prefix
    //   or the message, depending on which stage of the state machine we're at, returning all
    //   messages parsed and keeping track of any partial data remaining for further decode_data
    //   calls.
    //
    // See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests for an
    // explanation of the gRPC wire format.
    loop {
      match self.current_message_size {
        None => {
          if self.buffer().len() >= GRPC_MESSAGE_PREFIX_LEN {
            // Read compression byte. `1` means compressed, `0` uncompressed.
            self.current_message_compressed = self.buffer().get_u8() == 1;
            // Read the message size as big endian.
            self.current_message_size = Some(self.buffer().get_u32().try_into().unwrap());
            log::trace!("next message len={}", self.current_message_size.unwrap());

            continue;
          }

          return Ok(messages);
        },
        Some(message_size) => {
          if self.buffer().len() >= message_size {
            let message_buffer = if self.current_message_compressed {
              self.decompress(message_size)?
            } else {
              self.buffer().split_to(message_size).freeze()
            };

            self.rx_decompressed.inc_by(message_buffer.len());

            self.current_message_size = None;
            messages.push(MessageType::parse_from_tokio_bytes(&message_buffer)?);
          } else {
            return Ok(messages);
          }
        },
      }
    }
  }

  fn decompress(&mut self, message_size: usize) -> Result<Bytes> {
    // Leave in a buffer only these bytes that belong to the currently processed message.
    // O(1) operation.
    let remaining_bytes_buffer = self.buffer().split_off(message_size);

    let mut bytes = Vec::<u8>::new();
    self.decompressor.read_to_end(&mut bytes)?;

    #[allow(clippy::cast_precision_loss)]
    let ratio = message_size as f64 * 1.0 / bytes.len() as f64;
    log::trace!(
      "decompression completed; decompresed {} bytes to {} bytes, compression ratio: {:.2}",
      message_size,
      bytes.len(),
      ratio
    );

    // Put into the buffer all of the bytes removed from it at the beginning of the
    // method. This data (if any) belongs to next message(s) to be processed by the decoder.
    // O(1) operation.
    self.buffer().unsplit(remaining_bytes_buffer);

    Ok(Bytes::from(bytes))
  }

  pub fn initialize_stats(&mut self, rx: DynCounter, rx_decompressed: DynCounter) {
    self.rx.initialize(rx);
    self.rx_decompressed.initialize(rx_decompressed);
  }

  fn buffer(&mut self) -> &mut BytesMut {
    self.decompressor.get_mut().get_mut()
  }
}

impl<MessageType: Message> Default for Decoder<MessageType> {
  fn default() -> Self {
    Self {
      decompressor: flate2::read::ZlibDecoder::new(BytesMut::new().reader()),
      current_message_compressed: false,
      current_message_size: None,
      rx: DeferredCounter::default(),
      rx_decompressed: DeferredCounter::default(),
      _type: PhantomData,
    }
  }
}
