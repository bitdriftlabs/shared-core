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
use flate2::write::{ZlibDecoder, ZlibEncoder};
use protobuf::{CodedOutputStream, MessageFull};
use std::cell::RefCell;
use std::io::Write;
use std::marker::PhantomData;

// Compression algorithms supported by crate's code.
#[derive(Debug, Clone, Copy)]
pub enum Compression {
  // Parameter is the compression level in the range of 0-9.
  // Note as of 9/24 this was switched from new type format to struct format as new type seemed to
  // break RA occasionally.
  StatelessZlib { level: u32 },
  // Parameter is the compression level in the range of 0-9.
  // Note: This is only included for testing legacy clients. New clients always use stateless
  // which requires less RAM on the server.
  StatefulZlib { level: u32 },
}

// Decompression algorithms supported by crate's code.
pub enum Decompression {
  StatelessZlib,
  // Supported for legacy clients.
  StatefulZlib,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("protobuf error: {0}")]
  Protobuf(#[from] protobuf::Error),
  #[error("gRPC protocol error: {0}")]
  Protocol(&'static str),
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

pub const LEGACY_GRPC_ENCODING_HEADER: &str = "x-grpc-encoding";
pub const GRPC_ENCODING_HEADER: &str = "grpc-encoding";
pub const GRPC_ACCEPT_ENCODING_HEADER: &str = "grpc-accept-encoding";
pub const GRPC_ENCODING_DEFLATE: &str = "deflate";

//
// Compressor
//

enum Compressor {
  StatelessZlib { level: u32 },
  StatefulZlib(ZlibEncoder<Vec<u8>>),
}

//
// Encoder
//

pub struct Encoder<MessageType: protobuf::Message> {
  compressor: Option<Compressor>,
  tx_bytes: DeferredCounter,
  tx_bytes_uncompressed: DeferredCounter,
  _type: PhantomData<MessageType>,
}

impl<MessageType: protobuf::Message> Encoder<MessageType> {
  #[must_use]
  pub fn new(compression: Option<Compression>) -> Self {
    Self {
      compressor: compression.map(|compression| match compression {
        Compression::StatelessZlib { level } => Compressor::StatelessZlib { level },
        Compression::StatefulZlib { level } => Compressor::StatefulZlib(ZlibEncoder::new(
          Vec::new(),
          flate2::Compression::new(level),
        )),
      }),
      tx_bytes: DeferredCounter::default(),
      tx_bytes_uncompressed: DeferredCounter::default(),
      _type: PhantomData,
    }
  }

  // Converts a Protobuf message into a gRPC frame, potentially compressing the message.
  pub fn encode(&mut self, message: &MessageType) -> Bytes {
    // Serialize the Protobuf message then prefix it with the compression byte and the length in big
    // endian (the default for BufMut).
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

  fn encode_compressed(compression: &mut Compressor, message: &MessageType) -> Result<Bytes> {
    let mut buffer = match compression {
      Compressor::StatelessZlib { level } => {
        fn make_writer() -> Vec<u8> {
          let mut buffer = Vec::new();
          buffer.put_u8(1); // Compression byte, message compressed.
          buffer.put_u32(0); // We will fill this in later.
          buffer
        }

        thread_local! {
          static COMPRESSOR: RefCell<Option<ZlibEncoder<Vec<u8>>>> = const { RefCell::new(None) };
        }

        // TODO(mattklein123): For mobile we only ever use a single thread for communication, though
        // this will still keep the memory allocated. We could consider doing on demand allocation
        // for that case.
        // TODO(mattklein123): Using Compress here directly should remove some copies that are
        // required by using the writer interface.
        // Note that when using the thread local compressor the first level will win. We could
        // likely fix this if needed but it's not needed currently.
        COMPRESSOR.with_borrow_mut(|compressor| {
          let compressor = compressor.get_or_insert_with(|| {
            ZlibEncoder::new(make_writer(), flate2::Compression::new(*level))
          });

          message.write_to_writer(compressor)?;
          Ok::<_, Error>(compressor.reset(make_writer())?)
        })
      },
      Compressor::StatefulZlib(compressor) => {
        compressor.get_mut().put_u8(1);
        compressor.get_mut().put_u32(0); // We will fill this in later.
        message.write_to_writer(compressor)?;
        compressor.flush()?;
        Ok(std::mem::take(compressor.get_mut()))
      },
    }?;

    #[allow(clippy::cast_possible_truncation)]
    // Subtract off the 5 bytes of the prefix and then write it into the appropriate place.
    let compressed_message_size: u32 = (buffer.len() - GRPC_MESSAGE_PREFIX_LEN) as u32;
    buffer[1 .. 5].copy_from_slice(&compressed_message_size.to_be_bytes());

    // This assumes that `compute_size()` was called first. It's called as part
    // of the `write_to_writer` method call.
    let message_size = message.cached_size();
    #[allow(clippy::cast_precision_loss, clippy::cast_lossless)]
    let ratio = compressed_message_size as f64 * 1.0 / message_size as f64;
    log::trace!(
      "compression completed; {message_size} bytes compressed to {compressed_message_size} bytes, \
       compression ratio: {ratio:.2}"
    );

    Ok(buffer.into())
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
      log::trace!("writing message len={message_size}");
    }

    buffer.freeze()
  }
}

//
// Decompressor
//

enum Decompressor {
  StatelessZlib,
  StatefulZlib(ZlibDecoder<Vec<u8>>),
}

//
// OptimizeFor
//

pub enum OptimizeFor {
  // Will attempt to reduce CPU usage at the expense of memory usage.
  Cpu,
  // Will attempt to reduce memory usage at the expense of CPU usage.
  Memory,
}

//
// DecodingResult
//

pub trait DecodingResult {
  type Message: MessageFull;

  fn from_flags_and_bytes(flags: u8, bytes: Bytes) -> Result<Self>
  where
    Self: Sized;

  fn message(&self) -> Option<&Self::Message>;
}

impl<M: MessageFull> DecodingResult for M {
  type Message = M;

  fn from_flags_and_bytes(_flags: u8, bytes: Bytes) -> Result<Self> {
    Ok(M::parse_from_tokio_bytes(&bytes)?)
  }

  fn message(&self) -> Option<&Self::Message> {
    Some(self)
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
pub struct Decoder<MessageType: DecodingResult> {
  input_buffer: BytesMut,
  decompressor: Option<Decompressor>,
  current_message_flags: u8,
  current_message_size: Option<usize>,
  _type: PhantomData<MessageType>,
  rx: DeferredCounter,
  rx_decompressed: DeferredCounter,
  optimize_for: OptimizeFor,
}

impl<MessageType: DecodingResult> Decoder<MessageType> {
  #[must_use]
  pub fn new(decompression: Option<Decompression>, optimize_for: OptimizeFor) -> Self {
    Self {
      input_buffer: BytesMut::new(),
      decompressor: decompression.map(|decompression| match decompression {
        Decompression::StatefulZlib => Decompressor::StatefulZlib(ZlibDecoder::new(Vec::new())),
        Decompression::StatelessZlib => Decompressor::StatelessZlib,
      }),
      current_message_flags: 0,
      current_message_size: None,
      rx: DeferredCounter::default(),
      rx_decompressed: DeferredCounter::default(),
      _type: PhantomData,
      optimize_for,
    }
  }

  #[must_use]
  pub const fn bandwidth_stats(&self) -> (u64, u64) {
    (self.rx.count(), self.rx_decompressed.count())
  }

  // Decodes data, returning all complete messages parsed from the incoming data + any leftover
  // data from a previous chunk of data.
  pub fn decode_data(&mut self, data: &[u8]) -> Result<Vec<MessageType>> {
    self.input_buffer.extend_from_slice(data);
    log::trace!("have {} bytes", self.input_buffer.len());

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
    let messages = loop {
      match self.current_message_size {
        None => {
          if self.input_buffer.len() >= GRPC_MESSAGE_PREFIX_LEN {
            // Read flags byte.
            self.current_message_flags = self.input_buffer.get_u8();
            log::trace!("next message flags={}", self.current_message_flags);
            // Read the message size as big endian.
            self.current_message_size = Some(self.input_buffer.get_u32().try_into().unwrap());
            log::trace!("next message len={}", self.current_message_size.unwrap());

            continue;
          }

          break messages;
        },
        Some(message_size) => {
          if self.input_buffer.len() >= message_size {
            let message_buffer = if self.current_message_flags & 0x1 == 0x1 {
              self.decompress(message_size)?
            } else {
              self.input_buffer.split_to(message_size).freeze()
            };

            self.rx_decompressed.inc_by(message_buffer.len());
            let flags = std::mem::take(&mut self.current_message_flags);
            self.current_message_size = None;
            messages.push(MessageType::from_flags_and_bytes(flags, message_buffer)?);
          } else {
            break messages;
          }
        },
      }
    };

    if matches!(self.optimize_for, OptimizeFor::Memory) && self.input_buffer.is_empty() {
      // BytesMut will keep capacity around even if it's empty. If we are trying to reduce memory
      // usage (as in the case of many long lived low throughput connections) we will swap out
      // the buffer for an empty buffer with no backing allocations.
      std::mem::take(&mut self.input_buffer);
    }

    Ok(messages)
  }

  fn decompress(&mut self, message_size: usize) -> Result<Bytes> {
    let compressed = self.input_buffer.split_to(message_size);

    let bytes: Vec<u8> = match self.decompressor {
      None => return Err(Error::Protocol("compressed frame with no decompressor")),
      Some(Decompressor::StatefulZlib(ref mut decompressor)) => {
        decompressor.write_all(&compressed)?;
        decompressor.flush()?;
        std::mem::take(decompressor.get_mut())
      },
      Some(Decompressor::StatelessZlib) => {
        // TODO(mattklein123): At one point we tried to use a thread local to avoid re-allocating
        // the decompression buffer, but it's not clear that in some error cases the decompressor
        // can always be fully reset (inside calls finish()?). For now just allocate a fresh one
        // each time. This code could likely be optimized by using the raw interfaces directly
        // and moving back to thread local per the next TODO.
        // TODO(mattklein123): Using Decompress here directly should remove some copies that are
        // required by using the writer interface.
        let mut decompressor = ZlibDecoder::new(Vec::new());
        decompressor.write_all(&compressed)?;
        decompressor.flush()?;
        std::mem::take(decompressor.get_mut())
      },
    };

    #[allow(clippy::cast_precision_loss)]
    let ratio = message_size as f64 * 1.0 / bytes.len() as f64;
    log::trace!(
      "decompression completed; decompresed {} bytes to {} bytes, compression ratio: {:.2}",
      message_size,
      bytes.len(),
      ratio
    );

    Ok(bytes.into())
  }

  pub fn initialize_stats(&mut self, rx: DynCounter, rx_decompressed: DynCounter) {
    self.rx.initialize(rx);
    self.rx_decompressed.initialize(rx_decompressed);
  }
}
