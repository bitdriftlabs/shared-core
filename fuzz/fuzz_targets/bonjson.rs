// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use bd_bonjson::decoder::decode_value;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
  match decode_value(data) {
    Ok(_) | Err(_) => {},
  }
});
