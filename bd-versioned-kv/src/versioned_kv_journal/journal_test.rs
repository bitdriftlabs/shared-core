// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use crate::tests::make_string_value;
use bd_proto::protos::state::state_payload::StateValue;
use bd_time::TestTimeProvider;
use crc32fast::Hasher;
use std::cell::Cell;
use std::sync::Arc;
use time::macros::datetime;

#[test]
fn unknown_scope_marks_partial_data_loss_without_startup_failure() {
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
  let mut buffer = vec![0_u8; 4096];

  VersionedJournal::new(
    &mut buffer,
    0.8,
    time_provider.clone(),
    [(
      Scope::CustomFields,
      "field".to_string(),
      make_string_value("value"),
      1,
    )],
  )
  .unwrap();

  // Older SDKs do not recognize the CustomFields scope byte. The test models their decoder with
  // an otherwise valid future scope frame, which must degrade recovery rather than fail startup.
  assert_eq!(
    buffer[HEADER_SIZE] & 0x80,
    0,
    "test frame length must use one byte"
  );
  let frame_start = HEADER_SIZE + 1;
  let frame_len = usize::from(buffer[HEADER_SIZE]);
  let crc_start = frame_start + frame_len - 4;
  buffer[frame_start] = u8::MAX;

  let mut hasher = Hasher::new();
  hasher.update(&buffer[frame_start .. crc_start]);
  buffer[crc_start .. crc_start + 4].copy_from_slice(&hasher.finalize().to_le_bytes());

  let callback_called = Cell::new(false);
  let (_journal, data_loss) = VersionedJournal::<StateValue>::from_buffer(
    &mut buffer,
    0.8,
    time_provider,
    |_scope, _key, _value, _timestamp| callback_called.set(true),
  )
  .unwrap();

  assert!(!callback_called.get());
  assert!(matches!(data_loss, PartialDataLoss::Yes));
}
