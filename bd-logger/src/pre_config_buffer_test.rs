// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::bounded_buffer::MemorySized;
use crate::pre_config_buffer::{self, PreConfigBuffer};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SimulatedSizeLog {
  size: usize,
}

impl MemorySized for SimulatedSizeLog {
  fn size(&self) -> usize {
    self.size
  }
}

#[test]
fn buffer_acts_as_fifo_queue_with_limits() {
  let mut buffer = PreConfigBuffer::new(2, 1024);

  let logs = [
    SimulatedSizeLog { size: 1 },
    SimulatedSizeLog { size: 2000 },
    SimulatedSizeLog { size: 2 },
    SimulatedSizeLog { size: 3 },
  ];

  assert_eq!(Ok(()), buffer.push(logs[0]));
  // Too big to be accepted by the buffer.
  assert_eq!(
    Err(pre_config_buffer::Error::FullSizeOverflow),
    buffer.push(logs[1])
  );
  assert_eq!(Ok(()), buffer.push(logs[2]));
  // At this point buffer has 2 elements which means that it's full.
  assert_eq!(
    Err(pre_config_buffer::Error::FullCountOverflow),
    buffer.push(logs[3])
  );

  assert_eq!(
    vec![logs[0], logs[2]],
    buffer.pop_all().collect::<Vec<SimulatedSizeLog>>()
  );
}
