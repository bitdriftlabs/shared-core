// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use crate::tests::make_string_value;
use bd_proto::protos::state::payload::StateValue;

#[test]
fn varint_encoding() {
  let test_cases = vec![
    (0u64, vec![0x00]),
    (1u64, vec![0x01]),
    (127u64, vec![0x7F]),
    (128u64, vec![0x80, 0x01]),
    (300u64, vec![0xAC, 0x02]),
    (16_384u64, vec![0x80, 0x80, 0x01]),
    (
      u64::MAX,
      vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
    ),
  ];

  for (value, expected) in test_cases {
    let mut buf = [0u8; MAX_VARINT_SIZE];
    let len = encode_varint(value, &mut buf);
    assert_eq!(&buf[.. len], &expected[..], "Failed for value {value}");
  }
}

#[test]
fn varint_decoding() {
  let test_cases = vec![
    (vec![0x00], 0u64, 1),
    (vec![0x01], 1u64, 1),
    (vec![0x7F], 127u64, 1),
    (vec![0x80, 0x01], 128u64, 2),
    (vec![0xAC, 0x02], 300u64, 2),
    (vec![0x80, 0x80, 0x01], 16_384u64, 3),
    (
      vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
      u64::MAX,
      10,
    ),
  ];

  for (buf, expected_value, expected_len) in test_cases {
    let (value, len) = decode_varint(&buf).unwrap();
    assert_eq!(value, expected_value, "Failed for buffer {buf:?}");
    assert_eq!(len, expected_len, "Wrong length for buffer {buf:?}");
  }
}

#[test]
fn varint_roundtrip() {
  let values = vec![0, 1, 127, 128, 255, 256, 65535, 65536, 1_000_000, u64::MAX];

  for value in values {
    let mut buf = [0u8; MAX_VARINT_SIZE];
    let encoded_len = encode_varint(value, &mut buf);
    let (decoded_value, decoded_len) = decode_varint(&buf).unwrap();

    assert_eq!(decoded_value, value, "Roundtrip failed for {value}");
    assert_eq!(decoded_len, encoded_len, "Length mismatch for {value}");
  }
}

#[test]
fn varint_incomplete() {
  // Incomplete varint (has continuation bit but no next byte)
  let buf = vec![0x80];
  assert!(decode_varint(&buf).is_none());
}

#[test]
fn varint_too_long() {
  // 11 bytes (exceeds MAX_VARINT_SIZE)
  let buf = vec![
    0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
  ];
  assert!(decode_varint(&buf).is_none());
}

#[test]
fn frame_encode_decode() {
  let frame = Frame::new(1_700_000_000_000_000, make_string_value("value"));

  let mut buf = vec![0u8; 1024];
  let encoded_len = frame.encode(&mut buf).unwrap();

  let (decoded_frame, decoded_len) = Frame::<StateValue>::decode(&buf).unwrap();

  assert_eq!(decoded_frame, frame);
  assert_eq!(decoded_len, encoded_len);
}

#[test]
fn frame_with_delete() {
  let frame = Frame::new(1_700_000_000_000_000, make_string_value(""));

  let mut buf = vec![0u8; 1024];
  let encoded_len = frame.encode(&mut buf).unwrap();

  let (decoded_frame, decoded_len) = Frame::<StateValue>::decode(&buf).unwrap();

  assert_eq!(decoded_frame, frame);
  assert_eq!(decoded_len, encoded_len);
}

#[test]
fn frame_empty_payload() {
  let frame = Frame::new(1_700_000_000_000_000, StateValue::default());

  let mut buf = vec![0u8; 1024];
  let encoded_len = frame.encode(&mut buf).unwrap();

  let (decoded_frame, decoded_len) = Frame::<StateValue>::decode(&buf).unwrap();

  assert_eq!(decoded_frame, frame);
  assert_eq!(decoded_len, encoded_len);
}

#[test]
fn frame_various_timestamps() {
  let timestamps = vec![0, 1, 127, 128, 1_000_000, 1_700_000_000_000_000, u64::MAX];

  for timestamp in timestamps {
    let frame = Frame::new(timestamp, make_string_value("test"));
    let mut buf = vec![0u8; 1024];
    let encoded_len = frame.encode(&mut buf).unwrap();
    let (decoded_frame, decoded_len) = Frame::<StateValue>::decode(&buf).unwrap();

    assert_eq!(decoded_frame.timestamp_micros, timestamp);
    assert_eq!(decoded_frame.payload, make_string_value("test"));
    assert_eq!(decoded_len, encoded_len);
  }
}

#[test]
fn frame_buffer_too_small() {
  let frame = Frame::new(1_700_000_000_000_000, make_string_value("key:value"));
  let mut buf = vec![0u8; 5]; // Too small

  let result = frame.encode(&mut buf);
  assert!(result.is_err());
}

#[test]
fn frame_incomplete_length() {
  let buf = vec![0x01, 0x02]; // Only 2 bytes (need 4 for length)

  let result = Frame::<StateValue>::decode(&buf);
  assert!(result.is_err());
}

#[test]
fn frame_incomplete_data() {
  // Frame says it needs 100 bytes but we only provide 20
  let mut buf = vec![0u8; 20];
  buf[0 .. 4].copy_from_slice(&100u32.to_le_bytes());

  let result = Frame::<StateValue>::decode(&buf);
  assert!(result.is_err());
}

#[test]
fn frame_crc_mismatch() {
  let frame = Frame::new(1_700_000_000_000_000, make_string_value("key:value"));

  let mut buf = vec![0u8; 1024];
  let encoded_len = frame.encode(&mut buf).unwrap();

  // Corrupt the CRC
  buf[encoded_len - 1] ^= 0xFF;

  let result = Frame::<StateValue>::decode(&buf);
  assert!(result.is_err());
  assert!(result.unwrap_err().to_string().contains("CRC mismatch"));
}

#[test]
fn frame_multiple_frames() {
  let frame1 = Frame::new(1000, make_string_value("first"));
  let frame2 = Frame::new(2000, make_string_value("second"));
  let frame3 = Frame::new(3000, make_string_value("third"));

  let mut buf = vec![0u8; 1024];
  let len1 = frame1.encode(&mut buf).unwrap();
  let len2 = frame2.encode(&mut buf[len1 ..]).unwrap();
  let len3 = frame3.encode(&mut buf[len1 + len2 ..]).unwrap();

  // Decode all three
  let (decoded1, consumed1) = Frame::<StateValue>::decode(&buf).unwrap();
  let (decoded2, consumed2) = Frame::<StateValue>::decode(&buf[consumed1 ..]).unwrap();
  let (decoded3, consumed3) = Frame::<StateValue>::decode(&buf[consumed1 + consumed2 ..]).unwrap();

  assert_eq!(decoded1, frame1);
  assert_eq!(decoded2, frame2);
  assert_eq!(decoded3, frame3);
  assert_eq!(consumed1, len1);
  assert_eq!(consumed2, len2);
  assert_eq!(consumed3, len3);
}
