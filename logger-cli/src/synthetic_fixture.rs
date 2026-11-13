// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./synthetic_fixture_test.rs"]
mod tests;

use anyhow::{Result, anyhow, bail, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

const MAX_BYTES: u64 = 256 * 1024 * 1024 + 1;
const BUFFER_BYTES: usize = 64 * 1024;
// Leave space for protobuf envelopes and Tokio's write buffer, including during final-packet
// sizing.
const HEAP_BATCH: u64 = 512;

// The same standalone, symbolized sample as fake_perfetto. Keep the 103-byte seed independent of
// backend schemas; only padding packets are added, so the expected sample count stays one.
const PERFETTO_TRACE: &[u8] = b"\x0a\x13\x40\x01\xe2\x03\x0e\x08\x01\x12\x0alogger-cli\
  \x0a\x1a\x40\x02\x50\x01\x5a\x14\x48\x03\x58\x01\xba\x01\x0dfake-perfetto\
  \x0a\x34\x40\x03\x50\x01\x68\x01\x62\x1f\
  \x2a\x11\x08\x01\x12\x0dfake_perfetto\
  \x32\x04\x08\x01\x10\x01\x3a\x04\x08\x01\x10\x01\
  \x92\x04\x0a\x08\x00\x10\x01\x18\x01\x20\x01\x30\x01";

//
// FixtureKind / Pattern / FixtureOptions / FixtureManifest
//

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
pub enum FixtureKind {
  Opaque,
  Perfetto,
  HeapGraph,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
pub enum Pattern {
  Zeros,
  Random,
}

#[derive(Clone, Copy, Debug)]
pub struct FixtureOptions {
  pub size_bytes: u64,
  pub kind: FixtureKind,
  pub pattern: Pattern,
  pub seed: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FixtureManifest {
  pub path: String,
  pub size_bytes: u64,
  pub sha256: String,
  pub expected_samples: u64,
  pub expected_heap_objects: u64,
  pub expected_heap_references: u64,
  /// Walker charges: object type IDs, object references, root IDs, and type field IDs.
  pub expected_walk_references: u64,
}

//
// PreparedFixture
//

struct PreparedFixture {
  manifest: FixtureManifest,
  commit: oneshot::Sender<()>,
}

/// Creates a new durable fixture without replacing an existing path.
///
/// Cancellation closes the result channel; an owned I/O task then removes the incomplete output.
/// Cleanup is asynchronous and requires the Tokio runtime to remain alive.
pub async fn generate(path: &Path, options: &FixtureOptions) -> Result<FixtureManifest> {
  ensure!(path.to_str().is_some(), "fixture path must be UTF-8");
  ensure!(
    options.size_bytes <= MAX_BYTES,
    "fixture exceeds 256 MiB + 1 byte"
  );
  let minimum = match options.kind {
    FixtureKind::Opaque => 0,
    FixtureKind::Perfetto => PERFETTO_TRACE.len() as u64,
    FixtureKind::HeapGraph => {
      (heap_definitions().len() + heap_packet(0, 1, 1, false, options).len()) as u64
    },
  };
  ensure!(
    options.size_bytes >= minimum,
    "fixture requires at least {minimum} bytes"
  );
  let (sender, receiver) = oneshot::channel();
  tokio::spawn(generate_owned(path.to_owned(), *options, sender));
  let prepared = receiver
    .await
    .map_err(|_| anyhow!("fixture generation task stopped unexpectedly"))??;
  // There is no await between accepting ownership and returning the manifest. A cancelled caller
  // cannot leave an unclaimed successful file behind at the completion boundary.
  prepared
    .commit
    .send(())
    .map_err(|()| anyhow!("fixture generation task stopped before commit"))?;
  Ok(prepared.manifest)
}

async fn generate_owned(
  path: PathBuf,
  options: FixtureOptions,
  mut sender: oneshot::Sender<Result<PreparedFixture>>,
) {
  // Do not cancel open: Tokio can finish creating the file on its blocking pool after an abandoned
  // open future. Owning the entire lifecycle lets us always observe and clean up that creation.
  let mut file = match OpenOptions::new()
    .write(true)
    .create_new(true)
    .open(&path)
    .await
  {
    Ok(file) => file,
    Err(error) => {
      let _ = sender.send(Err(error.into()));
      return;
    },
  };
  log::debug!(
    "generating fixture: kind={:?} bytes={}",
    options.kind,
    options.size_bytes
  );
  let result = tokio::select! {
    biased;
    () = sender.closed() => Err(anyhow!("fixture generation cancelled")),
    result = write_fixture(&mut file, &path, &options) => result,
  };
  drop(file);
  let error = match result {
    Ok(manifest) => {
      let (commit, accepted) = oneshot::channel();
      if sender
        .send(Ok(PreparedFixture { manifest, commit }))
        .is_ok()
        && accepted.await.is_ok()
      {
        log::debug!("fixture generation committed: bytes={}", options.size_bytes);
        return;
      }
      None
    },
    Err(error) => Some((sender, error)),
  };
  log::debug!("removing uncommitted fixture: bytes={}", options.size_bytes);
  let cleanup = fs::remove_file(&path).await;
  if let Some((sender, error)) = error {
    let error = match cleanup {
      Ok(()) => error,
      Err(cleanup) => anyhow!("{error}; failed to remove partial fixture: {cleanup}"),
    };
    let _ = sender.send(Err(error));
  } else if let Err(error) = cleanup {
    log::warn!(
      "failed to remove cancelled fixture {}: {error}",
      path.display()
    );
  }
}

async fn write_fixture(
  file: &mut File,
  path: &Path,
  options: &FixtureOptions,
) -> Result<FixtureManifest> {
  let mut output = Output {
    file,
    digest: Sha256::new(),
    written: 0,
  };
  let mut expected_samples = 0;
  let mut expected_heap_objects = 0;
  let mut expected_walk_references = 0;
  match options.kind {
    FixtureKind::Opaque => {
      output
        .payload(
          options.size_bytes,
          options.pattern,
          &mut Random(options.seed),
        )
        .await?;
    },
    FixtureKind::Perfetto => {
      if options.size_bytes == PERFETTO_TRACE.len() as u64 + 1 {
        // A protobuf length varint may legally have a redundant final zero byte. Widening the
        // first packet length handles the only boundary not expressible as an additional packet.
        let mut seed = PERFETTO_TRACE.to_vec();
        widen_packet_length(&mut seed);
        output.write(&seed).await?;
      } else {
        output.write(PERFETTO_TRACE).await?;
      }
      output.padding(options.size_bytes, options).await?;
      expected_samples = 1;
    },
    FixtureKind::HeapGraph => {
      output.write(&heap_definitions()).await?;
      let mut index = 1;
      loop {
        let remaining = options.size_bytes - output.written;
        let mut packet = heap_packet(expected_heap_objects, HEAP_BATCH, index, true, options);
        let next_minimum = heap_packet(
          expected_heap_objects + HEAP_BATCH,
          1,
          index + 1,
          false,
          options,
        )
        .len();
        if packet.len() as u64 + next_minimum as u64 <= remaining {
          output.write(&packet).await?;
          expected_heap_objects += HEAP_BATCH;
          expected_walk_references += HEAP_BATCH * 5 + 1;
          index += 1;
          continue;
        }
        let mut lower = 1;
        let mut upper = HEAP_BATCH;
        while lower < upper {
          let middle = (lower + upper).div_ceil(2);
          if heap_packet(expected_heap_objects, middle, index, false, options).len() as u64
            <= remaining
          {
            lower = middle;
          } else {
            upper = middle - 1;
          }
        }
        packet = heap_packet(expected_heap_objects, lower, index, false, options);
        if remaining - packet.len() as u64 == 1 {
          widen_packet_length(&mut packet);
        }
        output.write(&packet).await?;
        expected_heap_objects += lower;
        expected_walk_references += lower * 5 + 1 + 4;
        output.padding(options.size_bytes, options).await?;
        break;
      }
    },
  }
  ensure!(
    output.written == options.size_bytes,
    "fixture size mismatch"
  );
  output.file.sync_all().await?;
  let mut sha256 = String::with_capacity(64);
  for byte in output.digest.finalize() {
    write!(sha256, "{byte:02x}")?;
  }
  Ok(FixtureManifest {
    path: path
      .to_str()
      .ok_or_else(|| anyhow!("fixture path must be UTF-8"))?
      .to_owned(),
    size_bytes: output.written,
    sha256,
    expected_samples,
    expected_heap_objects,
    expected_heap_references: expected_heap_objects * 4,
    expected_walk_references,
  })
}

//
// Output
//

struct Output<'a> {
  file: &'a mut File,
  digest: Sha256,
  written: u64,
}

impl Output<'_> {
  async fn write(&mut self, bytes: &[u8]) -> Result<()> {
    self.file.write_all(bytes).await?;
    self.digest.update(bytes);
    self.written += bytes.len() as u64;
    Ok(())
  }

  async fn payload(&mut self, mut size: u64, pattern: Pattern, random: &mut Random) -> Result<()> {
    let mut buffer = vec![0; BUFFER_BYTES];
    while size > 0 {
      let count = usize::try_from(size.min(BUFFER_BYTES as u64))?;
      if matches!(pattern, Pattern::Random) {
        random.fill(&mut buffer[.. count]);
      }
      self.write(&buffer[.. count]).await?;
      size -= count as u64;
    }
    Ok(())
  }

  async fn padding(&mut self, target: u64, options: &FixtureOptions) -> Result<()> {
    let mut random = Random(options.seed);
    while self.written < target {
      let remaining = target - self.written;
      match remaining {
        1 => bail!("internal fixture padding sizing error"),
        2 => self.write(&[0x0a, 0x00]).await?,
        3 => self.write(&[0x0a, 0x80, 0x00]).await?,
        4 => self.write(&[0x0a, 0x02, 0x40, 0x00]).await?,
        _ => {
          let mut payload = (remaining - 5).min(BUFFER_BYTES as u64);
          while padding_size(payload) > remaining {
            payload -= 1;
          }
          if remaining - padding_size(payload) == 1 {
            payload -= 1;
          }
          let mut header = Vec::with_capacity(16);
          varint(&mut header, 0x0a);
          varint(&mut header, 2 + varint_size(payload) + payload);
          // Unknown TracePacket field 1000, not a string or nested message: entropy stays valid.
          varint(&mut header, (1000 << 3) | 2);
          varint(&mut header, payload);
          self.write(&header).await?;
          self.payload(payload, options.pattern, &mut random).await?;
        },
      }
    }
    Ok(())
  }
}

fn padding_size(payload: u64) -> u64 {
  let packet = 2 + varint_size(payload) + payload;
  1 + varint_size(packet) + packet
}

fn widen_packet_length(packet: &mut Vec<u8>) {
  let end = (1 .. packet.len())
    .find(|&index| packet[index] < 128)
    .unwrap();
  packet[end] |= 128;
  packet.insert(end + 1, 0);
}

fn varint_size(mut value: u64) -> u64 {
  let mut size = 1;
  while value >= 128 {
    size += 1;
    value >>= 7;
  }
  size
}

fn varint(output: &mut Vec<u8>, mut value: u64) {
  while value >= 128 {
    output.push((value.to_le_bytes()[0] & 127) | 128);
    value >>= 7;
  }
  output.push(value.to_le_bytes()[0]);
}

fn number(output: &mut Vec<u8>, field: u64, value: u64) {
  varint(output, field << 3);
  varint(output, value);
}

fn bytes(output: &mut Vec<u8>, field: u64, value: &[u8]) {
  varint(output, (field << 3) | 2);
  varint(output, value.len() as u64);
  output.extend_from_slice(value);
}

fn heap_envelope(graph: &[u8]) -> Vec<u8> {
  let mut packet = Vec::with_capacity(graph.len() + 16);
  number(&mut packet, 8, 1);
  number(&mut packet, 10, 1);
  bytes(&mut packet, 56, graph);
  let mut trace = Vec::with_capacity(packet.len() + 8);
  bytes(&mut trace, 1, &packet);
  trace
}

fn heap_definitions() -> Vec<u8> {
  // Android S+ field layout: four reference field IDs on the type, not on each object.
  let mut graph = Vec::new();
  number(&mut graph, 1, 42);
  number(&mut graph, 5, 1);
  number(&mut graph, 6, 0);
  let mut ty = Vec::new();
  number(&mut ty, 1, 1);
  number(&mut ty, 2, 1);
  bytes(&mut ty, 3, b"com.example.a");
  bytes(&mut ty, 6, &[1, 2, 3, 4]);
  number(&mut ty, 7, 1);
  bytes(&mut graph, 9, &ty);
  let mut location = Vec::new();
  number(&mut location, 1, 1);
  bytes(
    &mut location,
    2,
    b"/data/app/com.example.app-token/base.apk",
  );
  bytes(&mut graph, 8, &location);
  for id in 1_u8 ..= 4 {
    let mut field = Vec::new();
    number(&mut field, 1, u64::from(id));
    bytes(&mut field, 2, &[b'a' + id - 1]);
    bytes(&mut graph, 4, &field);
  }
  heap_envelope(&graph)
}

fn heap_packet(
  start: u64,
  count: u64,
  index: u64,
  continued: bool,
  options: &FixtureOptions,
) -> Vec<u8> {
  let mut graph = Vec::new();
  number(&mut graph, 1, 42);
  number(&mut graph, 5, u64::from(continued));
  number(&mut graph, 6, index);
  let mut root_ids = Vec::new();
  varint(&mut root_ids, start + 1);
  let mut root = Vec::new();
  bytes(&mut root, 1, &root_ids);
  number(&mut root, 2, 1);
  bytes(&mut graph, 7, &root);
  let mut object = Vec::with_capacity(192);
  let mut references = Vec::with_capacity(40);
  for offset in 0 .. count {
    object.clear();
    references.clear();
    number(&mut object, 1, start + offset + 1);
    number(&mut object, 2, 1);
    number(&mut object, 3, 32);
    for distance in 1 ..= 4 {
      varint(&mut references, start + (offset + distance) % count + 1);
    }
    bytes(&mut object, 5, &references);
    if matches!(options.pattern, Pattern::Random) {
      // Unknown per-object bytes carry entropy without inventing bogus IDs, sizes, or references.
      // Objects and their four live edges remain semantic records, rather than a padding-only heap.
      let mut entropy = [0; 128];
      Random(options.seed.wrapping_add(start + offset)).fill(&mut entropy);
      bytes(&mut object, 1000, &entropy);
    }
    bytes(&mut graph, 2, &object);
  }
  heap_envelope(&graph)
}

//
// Random
//

// SplitMix64 specifies portable fixture bytes, including seed zero. Not for cryptographic use.
struct Random(u64);

impl Random {
  fn fill(&mut self, bytes: &mut [u8]) {
    for chunk in bytes.chunks_mut(8) {
      self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
      let mut word = self.0;
      word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
      word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
      word ^= word >> 31;
      chunk.copy_from_slice(&word.to_le_bytes()[.. chunk.len()]);
    }
  }
}
