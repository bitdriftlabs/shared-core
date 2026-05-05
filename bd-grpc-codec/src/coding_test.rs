// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Compression, DEFAULT_MAX_MESSAGE_BYTES, Decoder, Decompression, Encoder, OptimizeFor};
use protobuf::Message;
use protobuf::well_known_types::any::Any;
use protobuf::well_known_types::struct_::{Struct, Value};
use rstest::rstest;

#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

#[rstest]
#[case((Compression::StatefulZlib {
  level: 3,
}, Decompression::StatefulZlib))]
#[case((Compression::StatelessZlib {
  level: 3,
}, Decompression::StatelessZlib))]
fn decoder_does_not_panic_on_invalid_input_data(
  #[case] (compression, decompression): (Compression, Decompression),
) {
  let mut encoder = Encoder::<Struct>::new(None);
  let mut compressing_encoder = Encoder::<Struct>::new(Some(compression));

  let message = &create_compressable_message();
  let bytes = &encoder.encode(message).unwrap();
  let compressed_bytes = &compressing_encoder.encode(message).unwrap();

  let mut decoder = Decoder::<Any>::new(
    Some(decompression),
    Some(DEFAULT_MAX_MESSAGE_BYTES),
    OptimizeFor::Cpu,
  );

  assert!(decoder.decode_data(bytes).is_err());
  assert!(decoder.decode_data(compressed_bytes).is_err());
}

#[test]
fn frame_length_over_limit_returns_message_too_large() {
  let max_message_bytes = 32;
  let mut decoder = Decoder::<Struct>::new(None, Some(max_message_bytes), OptimizeFor::Cpu);
  let mut bytes = Vec::from([0]);
  bytes.extend_from_slice(&u32::try_from(max_message_bytes + 1).unwrap().to_be_bytes());

  assert!(matches!(
    decoder.decode_data(&bytes),
    Err(crate::Error::MessageTooLarge {
      message_bytes,
      max_bytes,
    }) if message_bytes == max_message_bytes + 1 && max_bytes == max_message_bytes
  ));
}

#[test]
fn decompressed_frame_over_limit_returns_message_too_large() {
  let max_message_bytes = 1024;
  let message = create_compressable_message();
  let mut encoder = Encoder::<Struct>::new(Some(Compression::StatelessZlib { level: 3 }));
  let compressed = encoder.encode(&message).unwrap();
  let uncompressed = message.write_to_bytes().unwrap();

  assert!(uncompressed.len() > max_message_bytes);
  assert!(compressed.len() - 5 <= max_message_bytes);

  let mut decoder = Decoder::<Struct>::new(
    Some(Decompression::StatelessZlib),
    Some(max_message_bytes),
    OptimizeFor::Cpu,
  );

  assert!(matches!(
    decoder.decode_data(&compressed),
    Err(crate::Error::MessageTooLarge {
      message_bytes,
      max_bytes,
    }) if message_bytes > max_message_bytes && max_bytes == max_message_bytes
  ));
}

#[rstest]
#[case((Compression::StatefulZlib {
  level: 3,
}, Decompression::StatefulZlib, OptimizeFor::Cpu))]
#[case((Compression::StatelessZlib {
  level: 3,
}, Decompression::StatelessZlib, OptimizeFor::Memory))]
fn encoding_decoding_flow(
  #[case] (compression, decompression, optimize_for): (Compression, Decompression, OptimizeFor),
) {
  let mut encoder = Encoder::<Struct>::new(Some(compression));
  let mut decoder = Decoder::<Struct>::new(
    Some(decompression),
    Some(DEFAULT_MAX_MESSAGE_BYTES),
    optimize_for,
  );

  // Check various message sizes to make sure that compressor and decompressor
  // work with diff message lengths. Verify that buffering done internally by
  // encoder and decoder works correctly.
  for i in 0 .. 100 {
    let mut message = Struct::new();
    message.fields.insert(
      "key1".to_string(),
      Value {
        kind: Some(
          protobuf::well_known_types::struct_::value::Kind::StringValue("abc".repeat(i * 100)),
        ),
        ..Default::default()
      },
    );

    let bytes = encoder.encode(&message).unwrap();
    let result = decoder.decode_data(&bytes);

    assert!(result.is_ok());
    assert_eq!(message, result.unwrap()[0]);
  }
}

// Only applies to Stateful.
#[test]
fn compression_decompression_is_stateful() {
  let mut encoder = Encoder::<Struct>::new(Some(Compression::StatefulZlib { level: 3 }));
  let mut decoder = Decoder::<Struct>::new(
    Some(Decompression::StatefulZlib),
    Some(DEFAULT_MAX_MESSAGE_BYTES),
    OptimizeFor::Cpu,
  );

  let message1 = create_compressable_message();
  let message2 = create_compressable_message();

  let _bytes1 = encoder.encode(&message1);
  let bytes2 = encoder.encode(&message2).unwrap();

  // `message2` cannot be decoded as decoder did not see encoded `message1`.
  // Inability to decode may surface in one of the two following ways:
  decoder.decode_data(&bytes2).unwrap_err();
}

// Only applies to Stateful.
#[test]
fn compression_gets_more_effective_as_streaming_progresses() {
  let mut encoder1 = Encoder::<Struct>::new(Some(Compression::StatefulZlib { level: 3 }));
  let mut encoder2 = Encoder::<Struct>::new(Some(Compression::StatefulZlib { level: 3 }));

  let message1 = create_compressable_message();
  let message2 = create_compressable_message();

  let encoder1_bytes2 = encoder1.encode(&message2).unwrap();

  _ = encoder2.encode(&message1);
  let encoder2_bytes2 = encoder2.encode(&message2).unwrap();

  assert!(encoder2_bytes2.len() < encoder1_bytes2.len());
}

fn create_compressable_message() -> Struct {
  // 'Big' message so that it's above min size eligible for compression
  // threshold.
  let mut message = Struct::new();
  message.fields.insert(
    "key2".to_string(),
    Value {
      kind: Some(
        protobuf::well_known_types::struct_::value::Kind::StringValue("foofoo".repeat(1000)),
      ),
      ..Default::default()
    },
  );
  message
}
