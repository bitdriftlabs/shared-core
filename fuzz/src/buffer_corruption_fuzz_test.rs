// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{process_test_case, BufferFuzzTestCase, BufferType};
use arbitrary::Arbitrary;

#[derive(Debug)]
pub struct BufferCorruptionFuzzTestCase {
  pub test_case: BufferFuzzTestCase,
}

impl<'a> Arbitrary<'a> for BufferCorruptionFuzzTestCase {
  fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
    let mut test_case = BufferFuzzTestCase::arbitrary(u).unwrap();

    // Single producer and turn on interleaved processing.
    test_case.num_producers = 1;
    test_case.interleaved_operations = true;

    // For now only test the aggregate buffer.
    test_case.buffer_type = BufferType::Aggregate;

    Ok(Self {
      test_case: process_test_case(
        test_case,
        |_| true,
        |reserve_and_commit| {
          // Clamp to index 0 to reduce the fuzz space for this parameter.
          reserve_and_commit.producer_index = 0;
        },
      ),
    })
  }
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus(
    "corpus/buffer_corruption_fuzz_test",
    |input: BufferCorruptionFuzzTestCase| {
      crate::BufferFuzzTest::new(input.test_case).run(true);
    },
  );
}
