// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#![no_main]

use fuzz::json_path::{JsonPathFuzzTestCase, run};

libfuzzer_sys::fuzz_target!(|input: JsonPathFuzzTestCase| {
  run(&input);
});
