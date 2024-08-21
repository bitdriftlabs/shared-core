// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::buffer::common_ring_buffer::Cursor;
use crate::buffer::test::{reserve_no_commit, Helper};
use crate::buffer::volatile_ring_buffer::RingBufferImpl;
use crate::buffer::{to_u32, RingBufferProducer, RingBufferStats};
use crate::{AbslCode, Error};
use assert_matches::assert_matches;
use itertools::Itertools;

fn make_helper(size: u32) -> Helper {
  Helper::new(
    RingBufferImpl::new("test".to_string(), size, RingBufferStats::default().into()),
    Cursor::No,
  )
}

// No cursor consumer
#[test]
fn no_cursor_consumer() {
  let helper = make_helper(30);
  assert_matches!(
    helper.buffer.clone().register_cursor_consumer(),
    Err(Error::AbslStatus(AbslCode::Unimplemented, message)) if message == "not supported"
  );
}

// 2 producers that are used to write both before and after underlying ring buffer is destroyed
// at different parts of their reserve/commit lifecycle.
#[test]
fn write_after_buffer_destroyed() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  reserve_no_commit(helper.producer(), "abcdef");
  reserve_no_commit(producer2.as_mut(), "abcdef");

  helper.producer().commit().unwrap();
  producer2.commit().unwrap();

  reserve_no_commit(producer2.as_mut(), "testtest");
  let mut moved_producer = helper.producer.take().unwrap();
  std::mem::drop(helper);

  assert_matches!(
    moved_producer.reserve(1, false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::FailedPrecondition && message == "shutdown"
  );
  assert_matches!(
    producer2.commit(),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::FailedPrecondition && message == "shutdown"
  );
}

// 2 producers that reserve and then commit in order without wrapping.
#[test]
fn concurrent_reservations_in_order() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve 0-9 on producer 1.
  reserve_no_commit(helper.producer(), "abcdef");

  // Reserve 0-9 on producer 2.
  reserve_no_commit(producer2.as_mut(), "ghijkl");

  // Commit in order.
  helper.producer().commit().unwrap();
  producer2.commit().unwrap();

  // Verify reads.
  helper.read_and_verify("abcdef");
  helper.read_and_verify("ghijkl");
}

// 2 producers that reserve and then commit out of order without wrapping.
#[test]
fn concurrent_reservations_out_of_order() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve 0-9 on producer 1.
  reserve_no_commit(helper.producer(), "abcdef");

  // Reserve 0-9 on producer 2.
  reserve_no_commit(producer2.as_mut(), "ghijkl");

  // Commit out of order.
  producer2.commit().unwrap();
  helper.producer().commit().unwrap();

  // Verify reads.
  helper.read_and_verify("abcdef");
  helper.read_and_verify("ghijkl");
}

// 2 producers that reserve over a wrap, and then commit in order, using aligned writes.
#[test]
fn concurrent_reservations_in_order_wrap_aligned() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve 10-29
  let test_string = "a".repeat(15);
  reserve_no_commit(producer2.as_mut(), &test_string);

  // Reserve 0-9
  reserve_no_commit(helper.producer(), "abcdef");

  // Commit in order.
  producer2.commit().unwrap();
  helper.producer().commit().unwrap();

  // Verify reads.
  helper.read_and_verify(&test_string);
  helper.read_and_verify("abcdef");
}

// 2 producers that reserve over a wrap, and then commit in order, using unaligned writes.
#[test]
fn concurrent_reservations_in_order_wrap_unaligned() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve 10-29
  let test_string = "a".repeat(15);
  reserve_no_commit(producer2.as_mut(), &test_string);

  // Reserve 0-8
  reserve_no_commit(helper.producer(), "abcde");

  // Commit in order.
  producer2.commit().unwrap();
  helper.producer().commit().unwrap();

  // Verify reads.
  helper.read_and_verify(&test_string);
  helper.read_and_verify("abcde");
}

// 2 producers that reserve over a wrap, and then commit out of order, using aligned writes.
#[test]
fn concurrent_reservations_out_of_order_wrap_aligned() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve 10-29
  let test_string = "a".repeat(15);
  reserve_no_commit(producer2.as_mut(), &test_string);

  // Reserve 0-9
  reserve_no_commit(helper.producer(), "abcdef");

  // Commit out of order.
  helper.producer().commit().unwrap();
  producer2.commit().unwrap();

  // Verify reads.
  helper.read_and_verify(&test_string);
  helper.read_and_verify("abcdef");
}

// 2 producers that reserve over a wrap, and then commit out of order, using unaligned writes.
#[test]
fn concurrent_reservations_out_of_order_wrap_unaligned() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  // Reserve and write 0-9.
  helper.reserve_and_commit("123456");

  // Read 0-9.
  helper.read_and_verify("123456");

  // Reserve 10-29
  let test_string = "a".repeat(15);
  reserve_no_commit(producer2.as_mut(), &test_string);

  // Reserve 0-8
  reserve_no_commit(helper.producer(), "abcde");

  // Commit out of order.
  helper.producer().commit().unwrap();
  producer2.commit().unwrap();

  // Verify reads.
  helper.read_and_verify(&test_string);
  helper.read_and_verify("abcde");
}

// Test out of order straight merges with various
#[test]
fn concurrent_reservations_out_of_order_straight_merge() {
  let mut helper = make_helper(50);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();
  let mut producer3 = helper.buffer.clone().register_producer().unwrap();

  reserve_no_commit(helper.producer(), "a");
  reserve_no_commit(producer2.as_mut(), "bb");
  reserve_no_commit(producer3.as_mut(), "cccc");
  producer3.commit().unwrap();
  producer2.commit().unwrap();
  helper.producer().commit().unwrap();
  helper.read_and_verify("a");
  helper.read_and_verify("bb");
  helper.read_and_verify("cccc");

  reserve_no_commit(helper.producer(), "d");
  reserve_no_commit(producer2.as_mut(), "ee");
  reserve_no_commit(producer3.as_mut(), "fff");
  producer2.commit().unwrap();
  producer3.commit().unwrap();
  helper.producer().commit().unwrap();
  helper.read_and_verify("d");
  helper.read_and_verify("ee");
  helper.read_and_verify("fff");
}

// Three producers doing aligned writes, out of order commits, and then reads, in every permutation.
#[test]
fn three_producers_aligned() {
  let mut helper = make_helper(30);
  let mut producers = vec![
    helper.producer.take().unwrap(),
    helper.buffer.clone().register_producer().unwrap(),
    helper.buffer.clone().register_producer().unwrap(),
  ];

  // In order interleaved.
  reserve_no_commit(producers[0].as_mut(), "aaaaaa");
  reserve_no_commit(producers[1].as_mut(), "bbbbbb");
  reserve_no_commit(producers[2].as_mut(), "cccccc");
  producers[0].commit().unwrap();
  helper.read_and_verify("aaaaaa");
  producers[1].commit().unwrap();
  helper.read_and_verify("bbbbbb");
  producers[2].commit().unwrap();
  helper.read_and_verify("cccccc");

  let do_commit_and_verify =
    |helper: &mut Helper, producers: &mut [Box<dyn RingBufferProducer>], indexes: Vec<usize>| {
      reserve_no_commit(producers[0].as_mut(), "dddddd");
      reserve_no_commit(producers[1].as_mut(), "eeeeee");
      reserve_no_commit(producers[2].as_mut(), "ffffff");
      producers[indexes[0]].commit().unwrap();
      producers[indexes[1]].commit().unwrap();
      producers[indexes[2]].commit().unwrap();
      helper.read_and_verify("dddddd");
      helper.read_and_verify("eeeeee");
      helper.read_and_verify("ffffff");
    };

  // All commit permutations at index 0.
  for indexes in vec![0, 1, 2].into_iter().permutations(3).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }

  // Reserve and write 0-9. Then read it.
  reserve_no_commit(producers[0].as_mut(), "123456");
  producers[0].commit().unwrap();
  helper.read_and_verify("123456");

  // All commit permutations at index 10 (2 before the wrap and 1 after).
  for indexes in vec![0, 1, 2].into_iter().permutations(3).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }

  // Reserve and write 10-19. Then read it.
  reserve_no_commit(producers[0].as_mut(), "123456");
  producers[0].commit().unwrap();
  helper.read_and_verify("123456");

  // All commit permutations at index 20 (1 before the wrap and 2 after).
  for indexes in vec![0, 1, 2].into_iter().permutations(3).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }
}

// Four producers doing aligned writes, out of order commits, and then reads, in every permutation.
#[test]
fn four_producers() {
  let mut helper = make_helper(40);
  let mut producers = vec![
    helper.producer.take().unwrap(),
    helper.buffer.clone().register_producer().unwrap(),
    helper.buffer.clone().register_producer().unwrap(),
    helper.buffer.clone().register_producer().unwrap(),
  ];

  let do_commit_and_verify =
    |helper: &mut Helper, producers: &mut [Box<dyn RingBufferProducer>], indexes: Vec<usize>| {
      reserve_no_commit(producers[0].as_mut(), "dddddd");
      reserve_no_commit(producers[1].as_mut(), "eeeeee");
      reserve_no_commit(producers[2].as_mut(), "ffffff");
      reserve_no_commit(producers[3].as_mut(), "gggggg");
      producers[indexes[0]].commit().unwrap();
      producers[indexes[1]].commit().unwrap();
      producers[indexes[2]].commit().unwrap();
      producers[indexes[3]].commit().unwrap();
      helper.read_and_verify("dddddd");
      helper.read_and_verify("eeeeee");
      helper.read_and_verify("ffffff");
      helper.read_and_verify("gggggg");
    };

  // All commit permutations at index 0.
  for indexes in vec![0, 1, 2, 3].into_iter().permutations(4).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }

  // Advance 0-9.
  reserve_no_commit(producers[0].as_mut(), "123456");
  producers[0].commit().unwrap();
  helper.read_and_verify("123456");

  // All commit permutations at index 10 (3 before the wrap and 1 after).
  for indexes in vec![0, 1, 2, 3].into_iter().permutations(4).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }

  // Advance 10-19.
  reserve_no_commit(producers[0].as_mut(), "123456");
  producers[0].commit().unwrap();
  helper.read_and_verify("123456");

  // All commit permutations at index 20 (2 before the wrap and 2 after).
  for indexes in vec![0, 1, 2, 3].into_iter().permutations(4).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }

  // Advance 20-29.
  reserve_no_commit(producers[0].as_mut(), "123456");
  producers[0].commit().unwrap();
  helper.read_and_verify("123456");

  // All commit permutations at index 30 (1 before the wrap and 3 after).
  for indexes in vec![0, 1, 2, 3].into_iter().permutations(4).unique() {
    do_commit_and_verify(&mut helper, &mut producers, indexes);
  }
}

// Verify that writing into a concurrent reader is prevented.
#[test]
fn write_into_concurrent_reader() {
  let mut helper = make_helper(30);

  // Fill 0-29.
  helper.reserve_and_commit("aaaaaa");
  helper.reserve_and_commit("bbbbbb");
  helper.reserve_and_commit("cccccc");

  // Start reading 0-9 and then reserve and commit what should go into 10-19.
  let reserved = helper.start_read_and_verify("aaaaaa");
  assert_matches!(
    helper.producer().reserve(to_u32("dddddd".len()), true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::ResourceExhausted && message == "writing into concurrent read"
  );
  reserved.verify_and_finish(helper.consumer(), "aaaaaa");
}

// Verify that writing into a concurrent writer causes the write to be dropped. Also verify that no
// state was updated during the drop.
#[test]
fn write_onto_concurrent_writer() {
  let mut helper = make_helper(30);
  let mut producer2 = helper.buffer.clone().register_producer().unwrap();

  reserve_no_commit(helper.producer(), &"a".repeat(20));
  assert_matches!(
    producer2.as_mut().reserve(10, true),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::ResourceExhausted && message == "writing into concurrent write"
  );

  reserve_no_commit(producer2.as_mut(), "bb");
  producer2.as_mut().commit().unwrap();
  helper.producer().commit().unwrap();
  helper.read_and_verify(&"a".repeat(20));
  helper.read_and_verify("bb");

  assert_matches!(
    helper.consumer().start_read(false),
    Err(Error::AbslStatus(code, message))
      if code == AbslCode::Unavailable && message == "no data to read"
  );
}
