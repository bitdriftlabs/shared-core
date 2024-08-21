// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use fuzz::mpsc_buffer_fuzz_test::MpscBufferFuzzTestCase;
use fuzz::BufferFuzzTest;

libfuzzer_sys::fuzz_target!(|input: MpscBufferFuzzTestCase| {
  BufferFuzzTest::new(input.test_case).run(false);
});
