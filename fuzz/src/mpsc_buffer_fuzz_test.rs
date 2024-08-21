// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{process_test_case, Action, BufferFuzzTestCase, BufferType};
use arbitrary::Arbitrary;

#[derive(Debug)]
pub struct MpscBufferFuzzTestCase {
  pub test_case: BufferFuzzTestCase,
}

impl<'a> Arbitrary<'a> for MpscBufferFuzzTestCase {
  fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
    let mut test_case = BufferFuzzTestCase::arbitrary(u).unwrap();

    // Volatile does not support cursor consumer.
    test_case.cursor_consumer = false;

    // Set range of 1-5 producers and turn on interleaved processing.
    if test_case.num_producers == 0 || test_case.num_producers > 5 {
      test_case.num_producers = (test_case.num_producers % 5) + 1;
    }
    test_case.interleaved_operations = true;

    // For now only test the volatile buffer.
    test_case.buffer_type = BufferType::Volatile;

    Ok(Self {
      test_case: process_test_case(
        test_case,
        |action| !matches!(action, Action::ReopenBuffer(_)),
        |reserve_and_commit| {
          // Clamp to index 0-4 to reduce the fuzz space for this parameter. The max producers are
          // 5 per above.
          reserve_and_commit.producer_index %= 5;
        },
      ),
    })
  }
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus(
    "corpus/mpsc_buffer_fuzz_test",
    |input: MpscBufferFuzzTestCase| {
      crate::BufferFuzzTest::new(input.test_case).run(false);
    },
  );
}
