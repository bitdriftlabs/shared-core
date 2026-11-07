// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{BOOTTIME, CompressedReader, NEEDS_STATE, REALTIME, Reader, STATE_CLEARED};
use crate::crash_profile::CLOCK_TOLERANCE_NS;
use protobuf::CodedOutputStream;
use std::io::{self, Read, Write};

fn message(fields: impl FnOnce(&mut CodedOutputStream<'_>)) -> Vec<u8> {
  let mut bytes = Vec::new();
  let mut output = CodedOutputStream::vec(&mut bytes);
  fields(&mut output);
  output.flush().unwrap();
  drop(output);
  bytes
}

fn clocks() -> Vec<u8> {
  clock_snapshot(&[
    (REALTIME, 1_000_000_000_050, false, 1),
    (BOOTTIME, 50, false, 1),
  ])
}

fn clock_snapshot(clocks: &[(u32, u64, bool, u64)]) -> Vec<u8> {
  message(|output| {
    for &(id, timestamp, incremental, unit) in clocks {
      let clock = message(|output| {
        // ClockSnapshot.Clock timestamp / clock_id / is_incremental / unit_multiplier_ns.
        output.write_uint64(2, timestamp).unwrap();
        output.write_uint32(1, id).unwrap();
        output.write_bool(3, incremental).unwrap();
        output.write_uint64(4, unit).unwrap();
      });
      output.write_bytes(1, &clock).unwrap();
    }
  })
}

fn trace(packets: &[Vec<u8>]) -> Vec<u8> {
  message(|output| {
    for packet in packets {
      output.write_bytes(1, packet).unwrap();
    }
  })
}

fn clock_packet() -> Vec<u8> {
  message(|output| {
    output.write_bytes(6, &clocks()).unwrap();
  })
}

fn heap_packet() -> Vec<u8> {
  let heap = message(|output| {
    output
      .write_bytes(2, &message(|output| output.write_uint64(1, 1).unwrap()))
      .unwrap();
    output.write_uint32(1, 7).unwrap();
  });
  message(|output| {
    output.write_bytes(56, &heap).unwrap();
    output.write_uint64(8, 25).unwrap();
  })
}

fn heap_with_header(header: impl FnOnce(&mut CodedOutputStream<'_>)) -> Vec<u8> {
  let mut packet = heap_packet();
  packet.extend(message(header));
  packet
}

fn sequence_header(sequence: u32, flags: u32, default_clock: Option<u32>) -> Vec<u8> {
  message(|output| {
    // TracePacket.trusted_packet_sequence_id / sequence_flags / trace_packet_defaults.
    output.write_uint32(10, sequence).unwrap();
    output.write_uint32(13, flags).unwrap();
    if let Some(clock) = default_clock {
      let defaults = message(|output| output.write_uint32(58, clock).unwrap());
      output.write_bytes(59, &defaults).unwrap();
    }
  })
}

fn sequenced_heap(sequence: u32, flags: u32) -> Vec<u8> {
  let mut packet = heap_packet();
  packet.extend(sequence_header(sequence, flags, None));
  packet
}

fn process_tree(pid: u32, tid: u32, start: Option<u64>) -> Vec<u8> {
  message(|output| {
    let tree = message(|output| {
      let process = message(|output| {
        output.write_uint32(1, pid).unwrap();
        if let Some(start) = start {
          output.write_uint64(7, start).unwrap();
        }
      });
      let thread = message(|output| {
        output.write_uint32(1, tid).unwrap();
        output.write_uint32(3, pid).unwrap();
      });
      output.write_bytes(1, &process).unwrap();
      output.write_bytes(2, &thread).unwrap();
    });
    output.write_bytes(2, &tree).unwrap();
  })
}

fn sched_event(timestamp: u64, next_tid: u32, header_tid: u32) -> Vec<u8> {
  message(|output| {
    // FtraceEvent.sched_switch, then its next_pid.
    let switch = message(|output| output.write_uint32(6, next_tid).unwrap());
    output.write_bytes(4, &switch).unwrap();
    output.write_uint32(2, header_tid).unwrap();
    output.write_uint64(1, timestamp).unwrap();
  })
}

#[test]
fn heap_pid_is_typed_and_packet_field_order_does_not_matter() {
  let evidence = Reader::read(trace(&[heap_packet(), clock_packet()]).as_slice()).unwrap();
  assert_eq!(evidence.boot_epoch_ns, 1_000_000_000_000);
  assert_eq!(evidence.observations.len(), 1);
  assert_eq!(evidence.observations[0].pid, 7);
  assert_eq!(evidence.observations[0].first_ns, 25);
}

#[test]
fn process_tree_or_arbitrary_integer_without_pid_timing_is_not_evidence() {
  let tree = message(|output| {
    output
      .write_bytes(1, &message(|output| output.write_uint32(1, 7).unwrap()))
      .unwrap();
  });
  let packet = message(|output| {
    output.write_uint64(8, 25).unwrap();
    output.write_bytes(2, &tree).unwrap();
    output.write_uint32(999, 7).unwrap();
  });
  assert!(Reader::read(trace(&[clock_packet(), packet]).as_slice()).is_err());
}

#[test]
fn compact_scheduler_requires_typed_tid_mapping_and_consistent_arrays() {
  let thread = message(|output| {
    output.write_uint32(3, 7).unwrap();
    output.write_uint32(1, 8).unwrap();
  });
  let tree = message(|output| {
    output.write_bytes(2, &thread).unwrap();
    output
      .write_bytes(1, &message(|output| output.write_uint32(1, 7).unwrap()))
      .unwrap();
  });
  let tree = message(|output| output.write_bytes(2, &tree).unwrap());
  for consistent in [true, false] {
    let compact = message(|output| {
      // Both packed and unpacked repeated representations are accepted.
      output.write_uint64(1, 20).unwrap();
      output.write_bytes(1, &[5]).unwrap();
      output.write_uint32(3, 8).unwrap();
      if consistent {
        output.write_bytes(3, &[8]).unwrap();
      }
    });
    let ftrace = message(|output| output.write_bytes(4, &compact).unwrap());
    let packet = message(|output| output.write_bytes(1, &ftrace).unwrap());
    let result = Reader::read(trace(&[clock_packet(), packet, tree.clone()]).as_slice());
    if consistent {
      let evidence = result.unwrap();
      assert_eq!(evidence.observations[0].pid, 7);
      assert_eq!(evidence.observations[0].first_ns, 20);
      assert_eq!(evidence.observations[0].last_ns, 25);
    } else {
      assert!(result.is_err());
    }
  }
}

#[test]
fn malformed_truncated_unknown_and_clockless_inputs_fail_closed() {
  let valid = trace(&[clock_packet(), heap_packet()]);
  for len in 0 .. valid.len() {
    assert!(Reader::read(&valid[.. len]).is_err(), "truncation at {len}");
  }
  assert!(Reader::read(trace(&[heap_packet()]).as_slice()).is_err());
  let bad_type = message(|output| output.write_uint64(56, 7).unwrap());
  assert!(Reader::read(trace(&[clock_packet(), bad_type]).as_slice()).is_err());
  let unknown = message(|output| {
    output.write_bytes(1000, &vec![7; 100_000]).unwrap();
    output.write_bytes(6, &clocks()).unwrap();
  });
  assert!(Reader::read(trace(&[unknown, heap_packet()]).as_slice()).is_ok());
}

#[test]
fn compressed_packets_preserve_evidence_and_validate_complete_streams() {
  let original = trace(&[clock_packet(), heap_packet()]);
  let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
  encoder.write_all(&original).unwrap();
  let encoded = encoder.finish().unwrap();
  let compressed =
    |encoded: &[u8]| trace(&[message(|output| output.write_bytes(50, encoded).unwrap())]);
  assert_eq!(
    Reader::read(compressed(&encoded).as_slice()).unwrap(),
    Reader::read(original.as_slice()).unwrap()
  );
  for len in 0 .. encoded.len() {
    assert!(Reader::read(compressed(&encoded[.. len]).as_slice()).is_err());
  }
  let oversized = message(|output| {
    output
      .write_tag(50, protobuf::rt::WireType::LengthDelimited)
      .unwrap();
    output
      .write_raw_varint32(u32::try_from(encoded.len()).unwrap() + 1)
      .unwrap();
    output.write_raw_bytes(&encoded).unwrap();
  });
  assert!(Reader::read(trace(&[oversized]).as_slice()).is_err());
  let mut trailing = encoded.clone();
  trailing.push(0);
  assert!(Reader::read(compressed(&trailing).as_slice()).is_err());
  let mut concatenated = encoded.clone();
  concatenated.extend_from_slice(&encoded);
  assert!(Reader::read(compressed(&concatenated).as_slice()).is_err());
  let mut corrupt = encoded.clone();
  *corrupt.last_mut().unwrap() ^= 1;
  assert!(Reader::read(compressed(&corrupt).as_slice()).is_err());
  let mut nested = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
  nested.write_all(&compressed(&encoded)).unwrap();
  assert!(
    Reader::read(compressed(&nested.finish().unwrap()).as_slice())
      .unwrap_err()
      .to_string()
      .contains("nested compressed")
  );
}

//
// BoundedReads
//

struct BoundedReads<R> {
  input: R,
  total: usize,
  largest: usize,
}

impl<R: Read> Read for BoundedReads<R> {
  fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
    // Detect a parser asking to materialize the ignored body, not merely a decoder chunk cap.
    assert!(output.len() <= 8192, "unbounded protobuf read");
    self.largest = self.largest.max(output.len());
    let read = self.input.read(output)?;
    self.total += read;
    Ok(read)
  }
}

#[test]
fn compressed_ignored_bodies_are_consumed_with_bounded_scratch_space() {
  // Compress a 32 MiB heap object without allocating its expanded representation. The typed
  // PID and timestamp follow the ignored object, so selective parsing must reach its far end.
  let ignored_len = 32 * 1024 * 1024;
  let object_header = message(|output| {
    output
      .write_tag(2, protobuf::rt::WireType::LengthDelimited)
      .unwrap();
    output.write_raw_varint32(ignored_len).unwrap();
  });
  let heap_tail = message(|output| output.write_uint32(1, 7).unwrap());
  let heap_len = u32::try_from(object_header.len() + heap_tail.len()).unwrap() + ignored_len;
  let heap_header = message(|output| {
    output
      .write_tag(56, protobuf::rt::WireType::LengthDelimited)
      .unwrap();
    output.write_raw_varint32(heap_len).unwrap();
  });
  let packet_tail = message(|output| output.write_uint64(8, 25).unwrap());
  let packet_len = u32::try_from(heap_header.len() + packet_tail.len()).unwrap() + heap_len;
  let trace_header = message(|output| {
    output
      .write_tag(1, protobuf::rt::WireType::LengthDelimited)
      .unwrap();
    output.write_raw_varint32(packet_len).unwrap();
  });
  let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
  encoder.write_all(&trace_header).unwrap();
  encoder.write_all(&heap_header).unwrap();
  encoder.write_all(&object_header).unwrap();
  io::copy(
    &mut io::repeat(0).take(u64::from(ignored_len)),
    &mut encoder,
  )
  .unwrap();
  encoder.write_all(&heap_tail).unwrap();
  encoder.write_all(&packet_tail).unwrap();
  let selected = trace(&[clock_packet(), heap_packet()]);
  let clock = trace(&[clock_packet()]);
  encoder.write_all(&clock).unwrap();
  let encoded = encoder.finish().unwrap();
  assert!(encoded.len() < 40 * 1024);
  let mut decoded = BoundedReads {
    input: CompressedReader::new(encoded.as_slice()),
    total: 0,
    largest: 0,
  };
  assert_eq!(
    Reader::read(&mut decoded).unwrap(),
    Reader::read(selected.as_slice()).unwrap()
  );
  assert_eq!(
    decoded.total,
    trace_header.len() + usize::try_from(packet_len).unwrap() + clock.len()
  );
  assert!(decoded.largest > 0);
  assert!(decoded.input.finished);
  // Exercise the production compressed-field adapter too (including its encoded-length bound).
  let packet = message(|output| output.write_bytes(50, &encoded).unwrap());
  assert_eq!(
    Reader::read(trace(&[packet]).as_slice()).unwrap(),
    Reader::read(selected.as_slice()).unwrap()
  );
}

#[test]
fn only_boottime_heap_timestamps_are_supported() {
  for clock in [BOOTTIME, REALTIME, 3, 64, 128] {
    let packet = heap_with_header(|output| output.write_uint32(58, clock).unwrap());
    let result = Reader::read(trace(&[clock_packet(), packet]).as_slice());
    if clock == BOOTTIME {
      assert_eq!(result.unwrap().observations[0].first_ns, 25);
    } else {
      assert!(
        result
          .unwrap_err()
          .to_string()
          .contains("unsupported heap packet clock")
      );
    }
  }
}

#[test]
fn unrelated_custom_clock_timestamps_do_not_need_conversion() {
  let packet = message(|output| {
    output.write_uint32(58, 64).unwrap();
    output.write_uint64(8, 20).unwrap();
    output.write_bytes(1000, b"ignored").unwrap();
  });
  let snapshot = message(|output| {
    output
      .write_bytes(6, &clock_snapshot(&[(64, 12, true, 1000)]))
      .unwrap();
  });
  let result = Reader::read(trace(&[packet, snapshot, heap_packet(), clock_packet()]).as_slice());
  assert_eq!(result.unwrap().observations[0].first_ns, 25);
}

#[test]
fn heap_clock_defaults_are_sequence_scoped_and_explicit_clocks_override_them() {
  let defaults = sequence_header(1, STATE_CLEARED, Some(64));
  let heap = sequenced_heap(1, NEEDS_STATE);
  assert!(Reader::read(trace(&[clock_packet(), defaults.clone(), heap]).as_slice()).is_err());
  let heap = sequenced_heap(2, 0);
  assert_eq!(
    Reader::read(trace(&[clock_packet(), defaults.clone(), heap]).as_slice())
      .unwrap()
      .observations[0]
      .first_ns,
    25
  );
  let mut heap = sequenced_heap(1, NEEDS_STATE);
  heap.extend(message(|output| output.write_uint32(58, BOOTTIME).unwrap()));
  assert_eq!(
    Reader::read(trace(&[clock_packet(), defaults, heap]).as_slice())
      .unwrap()
      .observations[0]
      .first_ns,
    25
  );
}

#[test]
fn state_resets_clear_defaults_and_packet_loss_requires_a_new_reset() {
  let defaults = sequence_header(1, STATE_CLEARED, Some(64));
  for reset in [
    sequence_header(1, STATE_CLEARED, None),
    message(|output| {
      output.write_bool(41, true).unwrap(); // TracePacket.incremental_state_cleared (legacy).
      output.write_uint32(10, 1).unwrap();
    }),
  ] {
    let heap = sequenced_heap(1, NEEDS_STATE);
    assert_eq!(
      Reader::read(trace(&[clock_packet(), defaults.clone(), reset, heap]).as_slice())
        .unwrap()
        .observations[0]
        .first_ns,
      25
    );
  }
  let dropped = message(|output| {
    output.write_uint32(42, 1).unwrap(); // TracePacket.previous_packet_dropped.
    output.write_uint32(10, 1).unwrap();
  });
  assert!(
    Reader::read(
      trace(&[
        clock_packet(),
        sequence_header(1, STATE_CLEARED, Some(BOOTTIME)),
        dropped.clone(),
        sequenced_heap(1, NEEDS_STATE),
      ])
      .as_slice()
    )
    .unwrap_err()
    .to_string()
    .contains("invalid incremental sequence")
  );
  assert!(
    Reader::read(
      trace(&[
        clock_packet(),
        dropped,
        sequenced_heap(1, STATE_CLEARED | NEEDS_STATE),
      ])
      .as_slice()
    )
    .is_ok()
  );
  assert!(
    Reader::read(trace(&[clock_packet(), sequenced_heap(1, NEEDS_STATE)]).as_slice()).is_err()
  );
}

#[test]
fn unrelated_lost_sequence_is_skipped_but_selected_state_still_fails_closed() {
  let unrelated = message(|output| {
    output
      .write_bytes(1000, b"unusable interned payload")
      .unwrap();
    output.write_uint32(10, 9).unwrap();
    output.write_bool(42, true).unwrap();
    output.write_uint32(13, NEEDS_STATE).unwrap();
  });
  let healthy = [
    clock_packet(),
    sequenced_heap(1, STATE_CLEARED | NEEDS_STATE),
  ];
  assert!(
    Reader::read(trace(&[healthy[0].clone(), unrelated.clone(), healthy[1].clone()]).as_slice())
      .is_ok()
  );
  let scheduler = message(|output| {
    let bundle = message(|output| output.write_bytes(2, &sched_event(25, 7, 8)).unwrap());
    output.write_bytes(1, &bundle).unwrap();
  });
  for selected in [
    clock_packet(),
    heap_packet(),
    process_tree(7, 8, None),
    scheduler,
  ] {
    let mut invalid = selected;
    invalid.extend(sequence_header(9, NEEDS_STATE, None));
    assert!(
      Reader::read(
        trace(&[
          healthy[0].clone(),
          unrelated.clone(),
          invalid,
          healthy[1].clone()
        ])
        .as_slice()
      )
      .unwrap_err()
      .to_string()
      .contains("invalid incremental sequence")
    );
  }
  // An unrelated dropped packet must still invalidate defaults needed by a later heap packet.
  assert!(
    Reader::read(
      trace(&[
        clock_packet(),
        sequence_header(9, STATE_CLEARED, Some(BOOTTIME)),
        unrelated,
        sequenced_heap(9, NEEDS_STATE),
      ])
      .as_slice()
    )
    .is_err()
  );
}
#[test]
fn reordered_and_repeated_scalar_headers_use_the_final_timestamp_and_clock() {
  let packet = heap_with_header(|output| {
    output.write_uint32(58, REALTIME).unwrap();
    output.write_uint64(8, 30).unwrap();
    output.write_uint32(58, BOOTTIME).unwrap();
    output.write_uint32(10, 2).unwrap();
  });
  assert_eq!(
    Reader::read(trace(&[packet, clock_packet()]).as_slice())
      .unwrap()
      .observations[0]
      .first_ns,
    30
  );
}

#[test]
fn snapshot_extrema_bound_clock_skew_without_retaining_snapshot_history() {
  for delta in [CLOCK_TOLERANCE_NS, CLOCK_TOLERANCE_NS + 1] {
    let snapshot = message(|output| {
      output
        .write_bytes(
          6,
          &clock_snapshot(&[
            (
              REALTIME,
              1_000_000_000_050 + u64::try_from(delta).unwrap(),
              false,
              1,
            ),
            (BOOTTIME, 50, false, 1),
          ]),
        )
        .unwrap();
    });
    let result = Reader::read(trace(&[clock_packet(), snapshot, heap_packet()]).as_slice());
    if delta == CLOCK_TOLERANCE_NS {
      assert_eq!(result.unwrap().boot_epoch_ns, 1_000_000_000_000);
    } else {
      assert!(
        result
          .unwrap_err()
          .to_string()
          .contains("unstable trace clocks")
      );
    }
  }
  let mut packets = vec![clock_packet(); 256];
  packets.push(heap_packet());
  assert_eq!(
    Reader::read(trace(&packets).as_slice())
      .unwrap()
      .observations
      .len(),
    1
  );
}

#[test]
fn altered_units_incremental_builtin_clocks_and_incomplete_snapshots_fail_explicitly() {
  for clocks in [
    vec![(REALTIME, 1_000_000_000_050, false, 1)],
    vec![
      (REALTIME, 1_000_000_000_050, false, 1),
      (BOOTTIME, 50, false, 1000),
    ],
    vec![
      (REALTIME, 1_000_000_000_050, true, 1),
      (BOOTTIME, 50, false, 1),
    ],
    vec![
      (REALTIME, 1_000_000_000_050, false, 0),
      (BOOTTIME, 50, false, 1),
    ],
    vec![
      (REALTIME, 1_000_000_000_050, false, 1),
      (BOOTTIME, 50, false, 1),
      (BOOTTIME, 50, false, 1),
    ],
  ] {
    let snapshot = message(|output| output.write_bytes(6, &clock_snapshot(&clocks)).unwrap());
    assert!(Reader::read(trace(&[clock_packet(), snapshot, heap_packet()]).as_slice()).is_err());
  }
}

#[test]
fn normal_scheduler_uses_next_tid_not_the_arbitrary_event_header_pid() {
  for next_tid in [8, 99] {
    let bundle = message(|output| {
      output
        .write_bytes(2, &sched_event(20, next_tid, 7))
        .unwrap();
    });
    let packet = message(|output| output.write_bytes(1, &bundle).unwrap());
    let result =
      Reader::read(trace(&[clock_packet(), packet, process_tree(7, 8, Some(10))]).as_slice());
    if next_tid == 8 {
      let observation = &result.unwrap().observations[0];
      assert_eq!(observation.pid, 7);
      assert_eq!(observation.process_start_ns, Some(10));
      assert_eq!(observation.first_ns, 20);
    } else {
      assert!(result.is_err());
    }
  }
}

#[test]
fn ftrace_clock_anchors_can_follow_events_and_are_independent_of_packet_clock() {
  for include_boot_anchor in [true, false] {
    let bundle = message(|output| {
      output.write_bytes(2, &sched_event(80, 8, 99)).unwrap();
      output.write_uint64(6, 100).unwrap(); // FtraceEventBundle.ftrace_timestamp.
      output.write_uint32(5, 2).unwrap(); // FtraceClock.FTRACE_CLOCK_GLOBAL.
      if include_boot_anchor {
        output.write_uint64(7, 50).unwrap(); // FtraceEventBundle.boot_timestamp.
      }
    });
    let packet = message(|output| {
      output.write_bytes(1, &bundle).unwrap();
      output.write_uint32(58, 64).unwrap(); // Unrelated TracePacket.timestamp_clock_id.
    });
    let result =
      Reader::read(trace(&[clock_packet(), packet, process_tree(7, 8, None)]).as_slice());
    if include_boot_anchor {
      assert_eq!(result.unwrap().observations[0].first_ns, 30);
    } else {
      assert!(
        result
          .unwrap_err()
          .to_string()
          .contains("missing ftrace boot anchor")
      );
    }
  }
}

#[test]
fn changed_process_or_thread_identity_is_not_silently_merged() {
  for changed in [process_tree(7, 8, Some(11)), process_tree(9, 8, Some(10))] {
    assert!(
      Reader::read(
        trace(&[
          clock_packet(),
          heap_packet(),
          process_tree(7, 8, Some(10)),
          changed,
        ])
        .as_slice()
      )
      .is_err()
    );
  }
}

#[test]
fn valid_raw_and_compressed_profiles_have_no_reader_byte_or_work_cap() {
  let mut bytes = trace(&[clock_packet(), heap_packet()]);
  {
    let mut output = CodedOutputStream::vec(&mut bytes);
    // Exercise inputs above the removed 32 MiB input/expansion and two-million-field caps.
    output
      .write_bytes(1000, &vec![0; 32 * 1024 * 1024 + 1])
      .unwrap();
    for _ in 0 .. 2_000_001 {
      output.write_uint64(1001, 0).unwrap();
    }
    output.flush().unwrap();
  }
  let raw = Reader::read(bytes.as_slice()).unwrap();
  assert_eq!(raw.observations.len(), 1);
  assert_eq!(raw.observations[0].first_ns, 25);
  let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
  encoder.write_all(&bytes).unwrap();
  let encoded = encoder.finish().unwrap();
  let packet = message(|output| output.write_bytes(50, &encoded).unwrap());
  assert_eq!(Reader::read(trace(&[packet]).as_slice()).unwrap(), raw);
}

#[test]
fn valid_metadata_and_compact_scheduler_have_no_count_caps() {
  // Exceed the removed process, thread/activity, producer-sequence and compact-event budgets.
  let tree = message(|output| {
    for pid in 1 ..= 129 {
      output
        .write_bytes(1, &message(|output| output.write_uint32(1, pid).unwrap()))
        .unwrap();
    }
    for index in 0 .. 4097 {
      let thread = message(|output| {
        output.write_uint32(1, 1000 + index).unwrap();
        output.write_uint32(3, 1 + index % 129).unwrap();
      });
      output.write_bytes(2, &thread).unwrap();
    }
  });
  let mut packets = vec![
    clock_packet(),
    message(|output| output.write_bytes(2, &tree).unwrap()),
  ];
  for sequence in 1 ..= 257 {
    packets.push(sequence_header(sequence, STATE_CLEARED, None));
  }
  let compact = message(|output| {
    for index in 0 .. 32_769 {
      output.write_uint64(1, 1).unwrap();
      output.write_uint32(3, 1000 + index % 4097).unwrap();
    }
  });
  let bundle = message(|output| output.write_bytes(4, &compact).unwrap());
  packets.push(message(|output| output.write_bytes(1, &bundle).unwrap()));
  let evidence = Reader::read(trace(&packets).as_slice()).unwrap();
  assert_eq!(evidence.observations.len(), 129);
  assert_eq!(evidence.observations[0].pid, 1);
  assert_eq!(evidence.observations[128].pid, 129);
}

#[test]
fn nested_zlib_and_zstd_wrappers_fail_explicitly() {
  let nested = trace(&[message(|output| output.write_bytes(50, b"unused").unwrap())]);
  let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
  encoder.write_all(&nested).unwrap();
  let encoded = encoder.finish().unwrap();
  let packet = message(|output| output.write_bytes(50, &encoded).unwrap());
  assert!(
    Reader::read(trace(&[clock_packet(), heap_packet(), packet]).as_slice())
      .unwrap_err()
      .to_string()
      .contains("nested compressed packets")
  );
  let packet = message(|output| output.write_bytes(133, b"unused").unwrap());
  assert!(
    Reader::read(trace(&[clock_packet(), heap_packet(), packet]).as_slice())
      .unwrap_err()
      .to_string()
      .contains("zstd-compressed")
  );
}
