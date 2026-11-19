// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! A selective Perfetto wire reader, not an on-device trace processor.
//!
//! Attribution needs only (1) REALTIME/BOOTTIME alignment, (2) typed PID/TID relationships and
//! optional process-start ages, and (3) PID-specific scheduler-switch or heap-sample times. Process
//! names, symbols, heap objects/references and arbitrary event PIDs are skipped without copying.
//! We retain min/max activity per identity, matching the native collector's evidence shape.
//!
//! This initial Android subset supports BOOTTIME heap packet timestamps and ftrace timestamps
//! aligned by their own bundle anchors. Per-sequence defaults/reset state is still required to
//! distinguish an omitted BOOTTIME clock from an omitted *unsupported* clock override. We do not
//! implement a general clock graph or incremental/custom packet-clock conversion: a heap sample
//! requiring either fails explicitly. Unrelated packet timestamps need no interpretation.
//!
//! Numeric field comments name the pinned schemas under
//! loop-api/loop-api-perfetto/src/proto/perfetto/protos. No generated Perfetto types or descriptors
//! are linked into the SDK. Protobuf field order is arbitrary, so packet headers and bundle anchors
//! are applied only after their complete messages have been read. Metadata may also follow activity
//! in later packets; TID-to-PID resolution happens after the trace has been scanned.
//!
//! For this POC, profiles are assumed valid and file-size policy belongs to the existing file
//! admission path. This reader adds no byte/work/history budgets. Parsing or attribution errors
//! reject the whole candidate instead of returning partial evidence; the platform caller logs
//! the rejection and continues ordinary report delivery.

#[cfg(test)]
#[path = "./reader_test.rs"]
mod tests;

use super::{CLOCK_TOLERANCE_NS, Evidence, PidObservation};
use anyhow::{bail, ensure};
use bd_proto_util::serialization::runtime::Tag;
use protobuf::CodedInputStream;
use protobuf::rt::WireType;
use std::collections::BTreeMap;
use std::io::{self, BufRead, Read};

// TODO: Before production, consider parser/decompression and candidate-set safety limits,
// coordinated with existing file limits and measured Android memory/work budgets.

// perfetto.protos.BuiltinClock: built-in clock IDs, not protobuf field numbers.
const REALTIME: u32 = 1;
const BOOTTIME: u32 = 6;
// TracePacket.SequenceFlags; defaults become invalid after loss until a reset is seen.
const STATE_CLEARED: u32 = 1;
const NEEDS_STATE: u32 = 2;

//
// Sequence
//

// This small state is necessary even without custom clock conversion: silently dropping a
// non-BOOTTIME default or reusing one across packet loss can assign the wrong wall/capture time.
#[derive(Default)]
struct Sequence {
  clock: Option<u32>,
  valid: bool,
}

//
// Packet
//

// Heap data may precede its timestamp/defaults, and repeated ftrace messages merge within a packet.
// Buffer selected timing fields, not payload bodies. Snapshots/process metadata is consumed
// directly.
#[derive(Default)]
struct Packet {
  timestamp: Option<u64>,
  clock: Option<u32>,
  sequence: u32,
  flags: u32,
  cleared: bool,
  dropped: bool,
  default_clock: Option<u32>,
  heap_pid: Option<u32>,
  heap_has_objects: bool,
  ftrace: FtraceBundle,
}

//
// FtraceBundle
//

// Singular bundle/compact messages can span wire occurrences. Merge arrays and anchors before
// decoding deltas or converting clocks; normal events need only one min/max pair per TID.
#[derive(Default)]
struct FtraceBundle {
  events: BTreeMap<u32, (u64, u64)>,
  clock: u64,
  ftrace_anchor: Option<u64>,
  boot_anchor: Option<u64>,
  switch_timestamps: Vec<u64>,
  switch_tids: Vec<u64>,
}

//
// ClockSample
//

#[derive(Clone, Copy)]
struct ClockSample {
  snapshot_index: u64,
  realtime_ns: u64,
  boottime_ns: u64,
}

impl ClockSample {
  fn boot_epoch_ns(self) -> i128 {
    i128::from(self.realtime_ns) - i128::from(self.boottime_ns)
  }
}

//
// Reader
//

// These are cross-packet facts, not decoded protobufs. Separate process/TID namespaces prevent
// an untyped scheduler TID from masquerading as a heap PID. Keep clock pairs so historical
// corrections can be separated from mappings that apply to the retained activity window.
#[derive(Default)]
pub struct Reader {
  sequences: BTreeMap<u32, Sequence>,
  clock_snapshots: u64,
  clock_samples: Vec<ClockSample>,
  processes: BTreeMap<u32, Option<u64>>,
  threads: BTreeMap<u32, u32>,
  activity: BTreeMap<(bool, u32), (u64, u64)>,
}

fn read_tag(input: &mut CodedInputStream<'_>) -> anyhow::Result<Option<Tag>> {
  input.read_raw_tag_or_eof()?.map(Tag::new).transpose()
}

// Bound nested messages in the streaming input; a malformed length must not consume later packets.
// Restore the parent limit even on error, and require the selected parser to consume its message.
fn nested<T>(
  input: &mut CodedInputStream<'_>,
  parse: impl FnOnce(&mut CodedInputStream<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
  let len = input.read_uint32()?;
  let old_limit = input.push_limit(u64::from(len))?;
  let result = parse(input);
  let remaining = input.bytes_until_limit();
  input.pop_limit(old_limit);
  let value = result?;
  ensure!(remaining == 0, "incomplete selected protobuf message");
  Ok(value)
}

// All selected integer/bool/enum fields are varints. Wrong wire types are errors, not unknown data.
fn scalar(input: &mut CodedInputStream<'_>, wire: WireType) -> anyhow::Result<u64> {
  ensure!(
    wire == WireType::Varint,
    "incorrect metadata scalar wire type"
  );
  Ok(input.read_uint64()?)
}

// The pinned proto3/2 schemas use length-delimited messages, bytes and packed scheduler arrays.
fn message_wire(wire: WireType) -> anyhow::Result<()> {
  ensure!(
    wire == WireType::LengthDelimited,
    "incorrect metadata message wire type"
  );
  Ok(())
}

// Unknown fields are intentionally not retained. Groups are unused by these schemas and would
// require recursive unknown-message parsing, so fail explicitly rather than expanding the reader.
fn skip(input: &mut CodedInputStream<'_>, wire: WireType) -> anyhow::Result<()> {
  ensure!(
    wire != WireType::StartGroup && wire != WireType::EndGroup,
    "unsupported protobuf group"
  );
  input.skip_field(wire)?;
  Ok(())
}

impl Reader {
  /// Stream a raw Trace; transport decompression is separate from `compressed_packets` inside it.
  ///
  /// Return no evidence until clock stability and typed process timing have been checked.
  /// The caller must not enqueue a profile from a partial/failed read.
  pub fn read(mut source: impl Read) -> anyhow::Result<Evidence> {
    let mut input = CodedInputStream::new(&mut source);
    let mut reader = Self::default();
    reader.trace(&mut input, false)?;
    // Linux shares the numeric PID/TID namespace, including process IDs known only through thread
    // ownership. Check after all metadata so packet order cannot hide conflicting identities.
    for pid in reader.processes.keys().chain(reader.threads.values()) {
      ensure!(
        reader.threads.get(pid).is_none_or(|parent| parent == pid),
        "conflicting trace PID/TID identity"
      );
    }
    let mut observations = BTreeMap::<u32, PidObservation>::new();
    for (&(process, tid), &(first, last)) in &reader.activity {
      let pid = if process {
        Some(tid)
      } else {
        reader
          .threads
          .get(&tid)
          .copied()
          .or_else(|| reader.processes.contains_key(&tid).then_some(tid))
      };
      let Some(pid) = pid else { continue };
      let start = reader.processes.get(&pid).copied().flatten();
      observations
        .entry(pid)
        .and_modify(|observation| {
          observation.first_ns = observation.first_ns.min(first);
          observation.last_ns = observation.last_ns.max(last);
        })
        .or_insert(PidObservation {
          pid,
          first_ns: first,
          last_ns: last,
          process_start_ns: start,
        });
    }
    ensure!(
      !observations.is_empty(),
      "missing typed PID-specific timing"
    );
    let (first_ns, last_ns) = observations
      .values()
      .fold((u64::MAX, 0), |range, observation| {
        (
          range.0.min(observation.first_ns),
          range.1.max(observation.last_ns),
        )
      });
    let boot_epoch_ns = reader.activity_boot_epoch(first_ns, last_ns)?;
    log::debug!(
      "Extracted crash profile timing (processes={})",
      observations.len()
    );
    let evidence = Evidence {
      boot_epoch_ns,
      observations: observations.into_values().collect(),
    };
    evidence.validate()?;
    Ok(evidence)
  }

  fn activity_boot_epoch(&mut self, first_ns: u64, last_ns: u64) -> anyhow::Result<i128> {
    self
      .clock_samples
      .sort_unstable_by_key(|sample| (sample.boottime_ns, sample.boot_epoch_ns()));
    // Perfetto uses the preceding snapshot, or the earliest snapshot for activity before all
    // snapshots. Include every mapping through the last observation, including duplicate times.
    let initial = self
      .clock_samples
      .iter()
      .rev()
      .find(|sample| sample.boottime_ns <= first_ns)
      .or_else(|| self.clock_samples.first())
      .ok_or_else(|| anyhow::anyhow!("missing REALTIME/BOOTTIME mapping"))?;
    let mut min_sample = *initial;
    let mut max_sample = *initial;
    let mut selected = *initial;
    let mut applicable_snapshots = 0;
    for sample in self.clock_samples.iter().filter(|sample| {
      sample.boottime_ns >= initial.boottime_ns
        && sample.boottime_ns <= last_ns.max(initial.boottime_ns)
    }) {
      let epoch = sample.boot_epoch_ns();
      if epoch < min_sample.boot_epoch_ns() {
        min_sample = *sample;
      }
      if epoch > max_sample.boot_epoch_ns() {
        max_sample = *sample;
      }
      selected = *sample;
      applicable_snapshots += 1;
    }
    let min_epoch = min_sample.boot_epoch_ns();
    let max_epoch = max_sample.boot_epoch_ns();
    let spread_ns = max_epoch - min_epoch;
    let clock_summary = format!(
      "snapshots={}, applicable_snapshots={applicable_snapshots}, first_activity_ns={first_ns}, \
       last_activity_ns={last_ns}, selected_snapshot_index={}, selected_epoch_ns={}, \
       min_epoch_ns={min_epoch}, max_epoch_ns={max_epoch}, spread_ns={spread_ns}, \
       tolerance_ns={CLOCK_TOLERANCE_NS}, min_snapshot_index={}, min_realtime_ns={}, \
       min_boottime_ns={}, max_snapshot_index={}, max_realtime_ns={}, max_boottime_ns={}",
      self.clock_snapshots,
      selected.snapshot_index,
      selected.boot_epoch_ns(),
      min_sample.snapshot_index,
      min_sample.realtime_ns,
      min_sample.boottime_ns,
      max_sample.snapshot_index,
      max_sample.realtime_ns,
      max_sample.boottime_ns
    );
    log::debug!("crash profile clock summary: {clock_summary}");
    ensure!(
      spread_ns <= CLOCK_TOLERANCE_NS,
      "unstable trace clocks: {clock_summary}"
    );
    Ok(selected.boot_epoch_ns())
  }

  fn trace(&mut self, input: &mut CodedInputStream<'_>, compressed: bool) -> anyhow::Result<()> {
    while let Some(tag) = read_tag(input)? {
      // Trace.packet = 1. No other top-level fields contribute attribution.
      if tag.field_number == 1 {
        message_wire(tag.wire_type)?;
        nested(input, |input| self.packet(input, compressed))?;
      } else {
        skip(input, tag.wire_type)?;
      }
    }
    Ok(())
  }

  fn packet(&mut self, input: &mut CodedInputStream<'_>, compressed: bool) -> anyhow::Result<()> {
    let mut packet = Packet::default();
    let mut requires_state = false;
    while let Some(tag) = read_tag(input)? {
      match tag.field_number {
        // TracePacket.timestamp / trusted_packet_sequence_id / sequence_flags.
        8 => packet.timestamp = Some(scalar(input, tag.wire_type)?),
        10 => packet.sequence = u32::try_from(scalar(input, tag.wire_type)?)?,
        13 => packet.flags = u32::try_from(scalar(input, tag.wire_type)?)?,
        // TracePacket.incremental_state_cleared (legacy) / previous_packet_dropped.
        41 => packet.cleared = scalar(input, tag.wire_type)? != 0,
        42 => packet.dropped = scalar(input, tag.wire_type)? != 0,
        // TracePacket.timestamp_clock_id.
        58 => packet.clock = Some(u32::try_from(scalar(input, tag.wire_type)?)?),
        // TracePacket.trace_packet_defaults; only its timestamp clock affects heap timing.
        59 => {
          message_wire(tag.wire_type)?;
          packet.default_clock = nested(input, |input| {
            let mut clock = packet.default_clock;
            while let Some(tag) = read_tag(input)? {
              // TracePacketDefaults.timestamp_clock_id = 58.
              if tag.field_number == 58 {
                clock = Some(u32::try_from(scalar(input, tag.wire_type)?)?);
              } else {
                skip(input, tag.wire_type)?;
              }
            }
            Ok(clock)
          })?;
        },
        // TracePacket.process_tree.
        2 => {
          requires_state = true;
          message_wire(tag.wire_type)?;
          nested(input, |input| self.process_tree(input))?;
        },
        // TracePacket.clock_snapshot: retain paired clocks, not their surrounding packet.
        6 => {
          requires_state = true;
          message_wire(tag.wire_type)?;
          nested(input, |input| self.clock_snapshot(input))?;
        },
        // TracePacket extension 56: com.android.art.tracing.HeapGraph.
        56 => {
          requires_state = true;
          message_wire(tag.wire_type)?;
          nested(input, |input| {
            while let Some(tag) = read_tag(input)? {
              match tag.field_number {
                // HeapGraph.pid identifies a process, unlike FtraceEvent.pid (a TID).
                1 => packet.heap_pid = Some(u32::try_from(scalar(input, tag.wire_type)?)?),
                // HeapGraph.objects. Presence supplies a graph sample time; skip object bodies.
                2 => {
                  message_wire(tag.wire_type)?;
                  packet.heap_has_objects = true;
                  skip(input, tag.wire_type)?;
                },
                _ => skip(input, tag.wire_type)?,
              }
            }
            Ok(())
          })?;
        },
        // TracePacket.ftrace_events; event timestamps use the bundle's clock, not this packet's.
        1 => {
          requires_state = true;
          message_wire(tag.wire_type)?;
          nested(input, |input| Self::ftrace(input, &mut packet.ftrace))?;
        },
        // TracePacket.compressed_packets wraps a zlib-compressed Trace packet stream.
        50 => {
          message_wire(tag.wire_type)?;
          ensure!(!compressed, "nested compressed packets are unsupported");
          let len = input.read_uint32()?;
          let mut encoded = (&mut *input).take(u64::from(len));
          let mut decoded = CompressedReader::new(&mut encoded);
          self.trace(&mut CodedInputStream::new(&mut decoded), true)?;
          ensure!(encoded.limit() == 0, "incomplete compressed packet field");
        },
        // TracePacket.zstd_compressed_packets: do not add a mobile zstd dependency for this subset.
        133 => bail!("zstd-compressed metadata packets are unsupported"),
        _ => skip(input, tag.wire_type)?,
      }
    }
    let sequence = self.sequences.entry(packet.sequence).or_default();
    if packet.dropped {
      sequence.valid = false;
      sequence.clock = None;
    }
    if packet.cleared || packet.flags & STATE_CLEARED != 0 {
      sequence.valid = true;
      sequence.clock = None;
    }
    if let Some(clock) = packet.default_clock {
      sequence.clock = Some(clock);
    }
    let clock_id = packet.clock.or(sequence.clock).unwrap_or(BOOTTIME);
    ensure!(
      !requires_state || packet.flags & NEEDS_STATE == 0 || sequence.valid,
      "invalid incremental sequence"
    );
    if packet.heap_has_objects {
      let pid = packet
        .heap_pid
        .ok_or_else(|| anyhow::anyhow!("heap graph has no source PID"))?;
      ensure!(pid > 0, "invalid heap source PID");
      let timestamp = packet
        .timestamp
        .ok_or_else(|| anyhow::anyhow!("heap graph has no timestamp"))?;
      // BOOTTIME is Android's documented default. Do not guess a conversion for another clock,
      // including sequence-local incremental clocks. Unrelated timestamps are simply skipped.
      ensure!(clock_id == BOOTTIME, "unsupported heap packet clock");
      self.add_process(pid, None)?;
      self.observe(true, pid, timestamp, timestamp);
    }
    self.observe_ftrace(packet.ftrace)?;
    Ok(())
  }

  fn process_tree(&mut self, input: &mut CodedInputStream<'_>) -> anyhow::Result<()> {
    while let Some(tag) = read_tag(input)? {
      // ProcessTree.processes = 1 / threads = 2 are incremental, not complete snapshots.
      if tag.field_number == 1 || tag.field_number == 2 {
        message_wire(tag.wire_type)?;
        let thread = tag.field_number == 2;
        let (mut id, mut parent, mut start) = (None, None, None);
        nested(input, |input| {
          while let Some(tag) = read_tag(input)? {
            match tag.field_number {
              // ProcessTree.Process.pid / ProcessTree.Thread.tid.
              1 => id = Some(u32::try_from(scalar(input, tag.wire_type)?)?),
              // ProcessTree.Thread.tgid is the owning process's PID.
              3 if thread => parent = Some(u32::try_from(scalar(input, tag.wire_type)?)?),
              // ProcessTree.Process.process_start_from_boot is already BOOTTIME ns.
              7 if !thread => start = Some(scalar(input, tag.wire_type)?),
              _ => skip(input, tag.wire_type)?,
            }
          }
          Ok(())
        })?;
        if let Some(id) = id.filter(|id| *id > 0) {
          if thread {
            if let Some(parent) = parent.filter(|parent| *parent > 0)
              && let Some(previous) = self.threads.insert(id, parent)
            {
              ensure!(previous == parent, "ambiguous reused trace TID");
            }
          } else {
            self.add_process(id, start)?;
          }
        }
      } else {
        skip(input, tag.wire_type)?;
      }
    }
    Ok(())
  }

  fn add_process(&mut self, pid: u32, start: Option<u64>) -> anyhow::Result<()> {
    let previous = self.processes.entry(pid).or_default();
    if let (Some(previous), Some(start)) = (*previous, start) {
      ensure!(previous == start, "ambiguous reused trace PID");
    }
    if start.is_some() {
      *previous = start;
    }
    Ok(())
  }

  // Only simultaneous REALTIME/BOOTTIME samples establish wall capture time and reboot identity.
  // Ignore other domains rather than constructing Perfetto's general clock-conversion graph.
  fn clock_snapshot(&mut self, input: &mut CodedInputStream<'_>) -> anyhow::Result<()> {
    self.clock_snapshots += 1;
    let (mut realtime, mut boottime) = (None, None);
    while let Some(tag) = read_tag(input)? {
      // ClockSnapshot.clocks = 1.
      if tag.field_number == 1 {
        message_wire(tag.wire_type)?;
        let (id, timestamp, incremental, unit) = nested(input, |input| {
          let (mut id, mut timestamp, mut incremental, mut unit) = (None, None, false, 1);
          while let Some(tag) = read_tag(input)? {
            match tag.field_number {
              // ClockSnapshot.Clock.clock_id / timestamp / is_incremental / unit_multiplier_ns.
              1 => id = Some(u32::try_from(scalar(input, tag.wire_type)?)?),
              2 => timestamp = Some(scalar(input, tag.wire_type)?),
              3 => incremental = scalar(input, tag.wire_type)? != 0,
              4 => unit = scalar(input, tag.wire_type)?,
              _ => skip(input, tag.wire_type)?,
            }
          }
          Ok((
            id.ok_or_else(|| anyhow::anyhow!("clock has no ID"))?,
            timestamp.ok_or_else(|| anyhow::anyhow!("clock has no timestamp"))?,
            incremental,
            unit,
          ))
        })?;
        if id == REALTIME || id == BOOTTIME {
          // Heap timestamps below are interpreted directly as BOOTTIME ns. Reject altered
          // units/incremental built-in clocks instead of accepting a correct-looking epoch
          // while interpreting the actual activity timestamp in the wrong unit.
          ensure!(
            !incremental && unit == 1,
            "unsupported attribution clock encoding"
          );
          let slot = if id == REALTIME {
            &mut realtime
          } else {
            &mut boottime
          };
          ensure!(slot.is_none(), "duplicate attribution clock");
          *slot = Some(timestamp);
        }
      } else {
        skip(input, tag.wire_type)?;
      }
    }
    match (realtime, boottime) {
      (Some(realtime), Some(boottime)) => {
        self.clock_samples.push(ClockSample {
          snapshot_index: self.clock_snapshots,
          realtime_ns: realtime,
          boottime_ns: boottime,
        });
      },
      (None, None) => {},
      // Do not hide a possible later wall-clock jump behind an earlier valid alignment.
      _ => bail!("incomplete REALTIME/BOOTTIME snapshot"),
    }
    Ok(())
  }

  // Coalesce only typed activity, never a process-table or arbitrary packet timestamp. The bool
  // separates heap PIDs from scheduler TIDs until all incremental process trees have been read.
  fn observe(&mut self, process: bool, tid: u32, first: u64, last: u64) {
    if tid == 0 {
      return;
    }
    self
      .activity
      .entry((process, tid))
      .and_modify(|range| {
        range.0 = range.0.min(first);
        range.1 = range.1.max(last);
      })
      .or_insert((first, last));
  }

  fn ftrace(input: &mut CodedInputStream<'_>, bundle: &mut FtraceBundle) -> anyhow::Result<()> {
    while let Some(tag) = read_tag(input)? {
      match tag.field_number {
        // FtraceEventBundle.event.
        2 => {
          message_wire(tag.wire_type)?;
          let (mut timestamp, mut next_tid) = (None, None);
          nested(input, |input| {
            while let Some(tag) = read_tag(input)? {
              match tag.field_number {
                // FtraceEvent.timestamp.
                1 => timestamp = Some(scalar(input, tag.wire_type)?),
                // FtraceEvent.sched_switch. FtraceEvent.pid is an unrelated event-header TID.
                4 => {
                  message_wire(tag.wire_type)?;
                  nested(input, |input| {
                    while let Some(tag) = read_tag(input)? {
                      // SchedSwitchFtraceEvent.next_pid is the scheduled-in TID.
                      if tag.field_number == 6 {
                        next_tid = Some(u32::try_from(scalar(input, tag.wire_type)?)?);
                      } else {
                        skip(input, tag.wire_type)?;
                      }
                    }
                    Ok(())
                  })?;
                },
                _ => skip(input, tag.wire_type)?,
              }
            }
            Ok(())
          })?;
          if let Some(tid) = next_tid.filter(|tid| *tid > 0) {
            let timestamp =
              timestamp.ok_or_else(|| anyhow::anyhow!("scheduler event has no timestamp"))?;
            Self::bundle_observe(&mut bundle.events, tid, timestamp);
          }
        },
        // FtraceEventBundle.compact_sched.
        4 => {
          message_wire(tag.wire_type)?;
          nested(input, |input| {
            while let Some(tag) = read_tag(input)? {
              match tag.field_number {
                // CompactSched.switch_timestamp = 1 / switch_next_pid = 3.
                1 | 3 => {
                  let values = if tag.field_number == 1 {
                    &mut bundle.switch_timestamps
                  } else {
                    &mut bundle.switch_tids
                  };
                  // Accept packed and unpacked repeated varints; protobuf permits either.
                  if tag.wire_type == WireType::LengthDelimited {
                    nested(input, |input| {
                      while !input.eof()? {
                        values.push(input.read_uint64()?);
                      }
                      Ok(())
                    })?;
                  } else {
                    values.push(scalar(input, tag.wire_type)?);
                  }
                },
                _ => skip(input, tag.wire_type)?,
              }
            }
            Ok(())
          })?;
        },
        // FtraceEventBundle.ftrace_clock / ftrace_timestamp / boot_timestamp.
        5 => bundle.clock = scalar(input, tag.wire_type)?,
        6 => bundle.ftrace_anchor = Some(scalar(input, tag.wire_type)?),
        7 => bundle.boot_anchor = Some(scalar(input, tag.wire_type)?),
        _ => skip(input, tag.wire_type)?,
      }
    }
    Ok(())
  }

  fn observe_ftrace(&mut self, mut bundle: FtraceBundle) -> anyhow::Result<()> {
    ensure!(
      bundle.switch_timestamps.len() == bundle.switch_tids.len(),
      "inconsistent compact scheduler arrays"
    );
    let mut timestamp = 0u64;
    // CompactSched's first switch_timestamp is absolute; the rest are deltas. Waking timestamps
    // do not establish the collector's sched_slice activity and are not consumed.
    for (delta, tid) in bundle.switch_timestamps.into_iter().zip(bundle.switch_tids) {
      timestamp = timestamp
        .checked_add(delta)
        .ok_or_else(|| anyhow::anyhow!("compact scheduler timestamp overflow"))?;
      Self::bundle_observe(&mut bundle.events, u32::try_from(tid)?, timestamp);
    }
    // FtraceClock.FTRACE_CLOCK_UNSPECIFIED = 0 means BOOTTIME. Other ftrace clocks require a
    // simultaneous anchor pair; TracePacket.timestamp_clock_id does not apply to these events.
    let offset = if bundle.clock == 0 {
      0
    } else {
      i128::from(
        bundle
          .boot_anchor
          .ok_or_else(|| anyhow::anyhow!("missing ftrace boot anchor"))?,
      ) - i128::from(
        bundle
          .ftrace_anchor
          .ok_or_else(|| anyhow::anyhow!("missing ftrace clock anchor"))?,
      )
    };
    for (tid, (first, last)) in bundle.events {
      self.observe(
        false,
        tid,
        u64::try_from(i128::from(first) + offset)?,
        u64::try_from(i128::from(last) + offset)?,
      );
    }

    Ok(())
  }

  fn bundle_observe(events: &mut BTreeMap<u32, (u64, u64)>, tid: u32, timestamp: u64) {
    if tid == 0 {
      return;
    }
    events
      .entry(tid)
      .and_modify(|range| {
        range.0 = range.0.min(timestamp);
        range.1 = range.1.max(timestamp);
      })
      .or_insert((timestamp, timestamp));
  }
}

//
// CompressedReader
//

// Neither the encoded field nor its expansion is materialized. Protobuf skips ignored bodies
// through its fixed-size input buffer. Require the zlib trailer/checksum and exact consumption;
// convenience ZlibDecoder readers alone can accept a truncated stream.
struct CompressedReader<R> {
  input: R,
  decoder: flate2::Decompress,
  finished: bool,
}

impl<R: BufRead> CompressedReader<R> {
  fn new(input: R) -> Self {
    Self {
      input,
      decoder: flate2::Decompress::new(true),
      finished: false,
    }
  }
}

impl<R: BufRead> Read for CompressedReader<R> {
  fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
    if output.is_empty() || self.finished {
      return Ok(0);
    }
    loop {
      let before_in = self.decoder.total_in();
      let before_out = self.decoder.total_out();
      let status = self
        .decoder
        .decompress(
          self.input.fill_buf()?,
          output,
          flate2::FlushDecompress::None,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
      let consumed =
        usize::try_from(self.decoder.total_in() - before_in).map_err(io::Error::other)?;
      let produced =
        usize::try_from(self.decoder.total_out() - before_out).map_err(io::Error::other)?;
      self.input.consume(consumed);
      if status == flate2::Status::StreamEnd {
        if !self.input.fill_buf()?.is_empty() {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing compressed packet bytes",
          ));
        }
        self.finished = true;
        return Ok(produced);
      }
      if produced != 0 {
        return Ok(produced);
      }
      if consumed == 0 {
        return Err(io::Error::new(
          io::ErrorKind::UnexpectedEof,
          "incomplete compressed packet stream",
        ));
      }
    }
  }
}
