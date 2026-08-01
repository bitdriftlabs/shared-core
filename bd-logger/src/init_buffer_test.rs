// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::init_buffer::{self, InitBuffer, Prioritizable};
use bd_log_primitives::size::MemorySized;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SimulatedSizeLog {
  size: usize,
}

impl MemorySized for SimulatedSizeLog {
  fn size(&self) -> usize {
    self.size
  }
}

impl Prioritizable for SimulatedSizeLog {
  fn is_prioritized(&self) -> bool {
    self.size.is_multiple_of(2)
  }
}

#[test]
fn buffer_accepts_one_item_beyond_its_soft_limit() {
  let mut buffer = InitBuffer::new(1024);

  let logs = [
    SimulatedSizeLog { size: 1 },
    SimulatedSizeLog { size: 999 },
    SimulatedSizeLog { size: 101 },
    SimulatedSizeLog { size: 3 },
  ];

  assert_eq!(Ok(()), buffer.push(logs[0]));
  assert_eq!(Ok(()), buffer.push(logs[1]));
  assert_eq!(Ok(()), buffer.push(logs[2]));
  assert_eq!(
    Err(init_buffer::Error::FullSizeOverflow),
    buffer.push(logs[3])
  );

  let items: Vec<_> = buffer.drain().collect();
  assert_eq!(vec![logs[0], logs[1], logs[2]], items);
}

#[test]
fn buffer_can_prioritize_items_without_reordering_each_group() {
  let mut buffer = InitBuffer::new(1024);
  for size in [1, 2, 3, 4] {
    buffer.push(SimulatedSizeLog { size }).unwrap();
  }

  let items: Vec<_> = buffer.drain().collect();

  assert_eq!(
    vec![
      SimulatedSizeLog { size: 2 },
      SimulatedSizeLog { size: 4 },
      SimulatedSizeLog { size: 1 },
      SimulatedSizeLog { size: 3 },
    ],
    items
  );
}
