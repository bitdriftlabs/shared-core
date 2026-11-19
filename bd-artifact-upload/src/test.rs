// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use protobuf::CodedOutputStream;

fn message(fields: impl FnOnce(&mut CodedOutputStream<'_>)) -> Vec<u8> {
  let mut bytes = Vec::new();
  let mut output = CodedOutputStream::vec(&mut bytes);
  fields(&mut output);
  output.flush().unwrap();
  drop(output);
  bytes
}

/// Minimal raw Trace with a typed heap sample and a paired REALTIME/BOOTTIME snapshot.
#[must_use]
pub fn heap_profile(pid: u32, boot_epoch_ns: u64, sample_ns: u64) -> Vec<u8> {
  let heap = message(|output| {
    // HeapGraph.objects (skipped by the reader) and HeapGraph.pid.
    output
      .write_bytes(2, &message(|output| output.write_uint64(1, 1).unwrap()))
      .unwrap();
    output.write_uint32(1, pid).unwrap();
  });
  let heap_packet = message(|output| {
    // TracePacket.heap_graph (ART extension 56) and TracePacket.timestamp.
    output.write_bytes(56, &heap).unwrap();
    output.write_uint64(8, sample_ns).unwrap();
  });
  let clocks = message(|output| {
    for (id, timestamp) in [(1, boot_epoch_ns + sample_ns), (6, sample_ns)] {
      let clock = message(|output| {
        output.write_uint32(1, id).unwrap();
        output.write_uint64(2, timestamp).unwrap();
      });
      output.write_bytes(1, &clock).unwrap();
    }
  });
  let clock_packet = message(|output| output.write_bytes(6, &clocks).unwrap());
  message(|output| {
    output.write_bytes(1, &heap_packet).unwrap();
    output.write_bytes(1, &clock_packet).unwrap();
  })
}
