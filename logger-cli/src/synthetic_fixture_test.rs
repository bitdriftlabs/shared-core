// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{
  FixtureKind,
  FixtureManifest,
  FixtureOptions,
  MAX_BYTES,
  PERFETTO_TRACE,
  Pattern,
  generate,
  generate_owned,
  heap_definitions,
  heap_packet,
};
use protobuf::{CodedInputStream, UnknownFields, UnknownValueRef, rt};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;
use tempfile::tempdir;
use tokio::fs;
use tokio::sync::oneshot;

fn options(kind: FixtureKind, pattern: Pattern, size_bytes: u64) -> FixtureOptions {
  FixtureOptions {
    size_bytes,
    kind,
    pattern,
    seed: 0,
  }
}

async fn fixture(path: &Path, options: &FixtureOptions) -> (FixtureManifest, Vec<u8>) {
  let manifest = generate(path, options).await.unwrap();
  let data = fs::read(path).await.unwrap();
  assert_eq!(manifest.path, path.to_str().unwrap());
  assert_eq!(manifest.size_bytes, options.size_bytes);
  assert_eq!(data.len() as u64, options.size_bytes);
  let expected = Sha256::digest(&data)
    .iter()
    .fold(String::new(), |mut output, byte| {
      write!(output, "{byte:02x}").unwrap();
      output
    });
  assert_eq!(manifest.sha256, expected);
  (manifest, data)
}

fn decode(bytes: &[u8]) -> UnknownFields {
  let mut fields = UnknownFields::new();
  let mut input = CodedInputStream::from_bytes(bytes);
  while let Some(tag) = input.read_raw_tag_or_eof().unwrap() {
    rt::read_unknown_or_skip_group(tag, &mut input, &mut fields).unwrap();
  }
  fields
}

fn messages(fields: &UnknownFields, number: u32) -> Vec<&[u8]> {
  fields
    .iter()
    .filter_map(|(field, value)| {
      if field != number {
        return None;
      }
      let UnknownValueRef::LengthDelimited(bytes) = value else {
        panic!("expected message field {number}");
      };
      Some(bytes)
    })
    .collect()
}

fn number(fields: &UnknownFields, field: u32) -> u64 {
  let Some(UnknownValueRef::Varint(number)) = fields.get(field) else {
    panic!("expected integer field {field}");
  };
  number
}

fn packed(fields: &UnknownFields, field: u32) -> Vec<u64> {
  let mut result = Vec::new();
  for bytes in messages(fields, field) {
    let mut input = CodedInputStream::from_bytes(bytes);
    while !input.eof().unwrap() {
      result.push(input.read_uint64().unwrap());
    }
  }
  result
}

#[tokio::test]
async fn opaque_is_exact_streamed_and_seeded_including_empty_files() {
  let directory = tempdir().unwrap();
  for size in [0, 1, 7, 8, 65_537] {
    for pattern in [Pattern::Zeros, Pattern::Random] {
      let options = options(FixtureKind::Opaque, pattern, size);
      let (manifest, first) = fixture(&directory.path().join("first"), &options).await;
      let (_, second) = fixture(&directory.path().join("second"), &options).await;
      assert_eq!(first, second);
      assert_eq!(manifest.expected_samples, 0);
      assert_eq!(manifest.expected_heap_objects, 0);
      assert_eq!(manifest.expected_heap_references, 0);
      if matches!(pattern, Pattern::Zeros) {
        assert!(first.iter().all(|&byte| byte == 0));
      } else if size >= 8 {
        let changed = FixtureOptions { seed: 1, ..options };
        let (_, other) = fixture(&directory.path().join("other"), &changed).await;
        assert_ne!(first, other);
        fs::remove_file(directory.path().join("other"))
          .await
          .unwrap();
      }
      fs::remove_file(directory.path().join("first"))
        .await
        .unwrap();
      fs::remove_file(directory.path().join("second"))
        .await
        .unwrap();
    }
  }
}

#[tokio::test]
async fn perfetto_padding_preserves_sample_and_valid_protobuf_at_size_boundaries() {
  let directory = tempdir().unwrap();
  // Includes single-byte growth, short packets, varint growth, and a multi-buffer random payload.
  for size in (103 ..= 140).chain([16_383, 16_384, 65_540, 131_100]) {
    let options = options(FixtureKind::Perfetto, Pattern::Random, size);
    let path = directory.path().join("trace");
    let (manifest, data) = fixture(&path, &options).await;
    let trace = decode(&data);
    let packets: Vec<_> = messages(&trace, 1).into_iter().map(decode).collect();
    let sample_packets: Vec<_> = packets
      .iter()
      .filter(|packet| packet.get(66).is_some())
      .collect();
    assert_eq!(sample_packets.len(), 1);
    let sample = decode(messages(sample_packets[0], 66)[0]);
    assert_eq!(number(&sample, 4), 1);
    let interned = decode(messages(sample_packets[0], 12)[0]);
    let frame = decode(messages(&interned, 6)[0]);
    let callstack = decode(messages(&interned, 7)[0]);
    assert_eq!(number(&sample, 4), number(&callstack, 1));
    assert_eq!(number(&callstack, 2), number(&frame, 1));
    assert_eq!(manifest.expected_samples, 1);
    assert_eq!(manifest.expected_heap_objects, 0);
    if size == 103 {
      assert_eq!(data, PERFETTO_TRACE);
    }
    if size == 131_100 {
      let (_, repeated) = fixture(&directory.path().join("repeated"), &options).await;
      assert_eq!(data, repeated);
      let entropy: Vec<_> = packets
        .iter()
        .flat_map(|packet| messages(packet, 1000))
        .collect();
      assert!(entropy.iter().map(|bytes| bytes.len()).sum::<usize>() > data.len() * 99 / 100);
      let mut histogram = [0usize; 256];
      for byte in entropy.into_iter().flatten() {
        histogram[usize::from(*byte)] += 1;
      }
      assert!(
        histogram
          .into_iter()
          .all(|count| (350 .. 700).contains(&count))
      );
    }
    fs::remove_file(path).await.unwrap();
  }
}

fn check_heap(data: &[u8], manifest: &FixtureManifest) {
  let trace = decode(data);
  let mut ids = BTreeSet::new();
  let mut references = Vec::new();
  let mut root_count = 0;
  let mut type_fields = Vec::new();
  let mut field_ids = BTreeSet::new();
  let mut location_ids = BTreeSet::new();
  let mut graph_index = 0;
  let mut continued = true;
  for packet in messages(&trace, 1) {
    let packet = decode(packet);
    for graph in messages(&packet, 56) {
      assert!(continued, "heap data after terminal packet");
      assert_eq!(number(&packet, 8), 1);
      assert_eq!(number(&packet, 10), 1);
      let graph = decode(graph);
      assert_eq!(number(&graph, 1), 42);
      assert_eq!(number(&graph, 6), graph_index);
      graph_index += 1;
      continued = number(&graph, 5) != 0;
      for location in messages(&graph, 8) {
        let location = decode(location);
        location_ids.insert(number(&location, 1));
        assert_eq!(
          messages(&location, 2)[0],
          b"/data/app/com.example.app-token/base.apk"
        );
      }
      for field in messages(&graph, 4) {
        let field = decode(field);
        field_ids.insert(number(&field, 1));
        assert_eq!(messages(&field, 2)[0].len(), 1);
      }
      for ty in messages(&graph, 9) {
        let ty = decode(ty);
        assert_eq!(number(&ty, 1), 1);
        assert_eq!(number(&ty, 2), 1);
        assert_eq!(messages(&ty, 3)[0], b"com.example.a");
        type_fields.extend(packed(&ty, 6));
      }
      for object in messages(&graph, 2) {
        let object = decode(object);
        assert!(ids.insert(number(&object, 1)), "duplicate object ID");
        assert_eq!(number(&object, 2), 1);
        assert_eq!(number(&object, 3), 32);
        assert_eq!(messages(&object, 4), [] as [&[u8]; 0]);
        let edges = packed(&object, 5);
        assert_eq!(edges.len(), 4);
        references.extend(edges);
      }
      for root in messages(&graph, 7) {
        let root = decode(root);
        let roots = packed(&root, 1);
        root_count += roots.len() as u64;
        references.extend(roots);
      }
    }
  }
  assert!(!continued, "unterminated heap dump");
  assert_eq!(type_fields, [1, 2, 3, 4]);
  assert_eq!(location_ids, BTreeSet::from([1]));
  assert_eq!(field_ids, BTreeSet::from([1, 2, 3, 4]));
  assert!(
    references.iter().all(|id| ids.contains(id)),
    "dangling heap edge/root"
  );
  assert_eq!(manifest.expected_samples, 0);
  assert_eq!(manifest.expected_heap_objects, ids.len() as u64);
  assert_eq!(
    manifest.expected_heap_references,
    references.len() as u64 - root_count
  );
  assert_eq!(
    manifest.expected_walk_references,
    ids.len() as u64 * 5 + root_count + type_fields.len() as u64
  );
  assert!(
    ids.len() > 1024,
    "fixture should exercise multiple semantic packets"
  );
}

#[tokio::test]
async fn heap_is_semantic_exact_and_deterministic_for_both_patterns() {
  let directory = tempdir().unwrap();
  for pattern in [Pattern::Zeros, Pattern::Random] {
    let options = options(FixtureKind::HeapGraph, pattern, 262_151);
    let (manifest, first) = fixture(&directory.path().join("first"), &options).await;
    check_heap(&first, &manifest);
    let (_, second) = fixture(&directory.path().join("second"), &options).await;
    assert_eq!(first, second);
    fs::remove_file(directory.path().join("first"))
      .await
      .unwrap();
    fs::remove_file(directory.path().join("second"))
      .await
      .unwrap();

    let minimum = (heap_definitions().len() + heap_packet(0, 1, 1, false, &options).len()) as u64;
    for size_bytes in minimum .. minimum + 40 {
      let options = FixtureOptions {
        size_bytes,
        ..options
      };
      let path = directory.path().join("small");
      let (manifest, data) = fixture(&path, &options).await;
      assert!(manifest.expected_heap_objects >= 1);
      let trace = decode(&data);
      for packet in messages(&trace, 1) {
        let packet = decode(packet);
        for graph in messages(&packet, 56) {
          decode(graph);
        }
      }
      fs::remove_file(path).await.unwrap();
    }
  }
}

#[tokio::test]
async fn invalid_sizes_and_existing_inputs_are_preserved() {
  let directory = tempdir().unwrap();
  let input = directory.path().join("existing");
  fs::write(&input, b"preserve me").await.unwrap();
  let options = options(FixtureKind::Opaque, Pattern::Zeros, 17);
  assert!(generate(&input, &options).await.is_err());
  assert_eq!(fs::read(&input).await.unwrap(), b"preserve me");
  let output = directory.path().join("invalid");
  for options in [
    FixtureOptions {
      size_bytes: MAX_BYTES + 1,
      ..options
    },
    FixtureOptions {
      kind: FixtureKind::Perfetto,
      size_bytes: 102,
      ..options
    },
    FixtureOptions {
      kind: FixtureKind::HeapGraph,
      size_bytes: 0,
      ..options
    },
  ] {
    assert!(generate(&output, &options).await.is_err());
    assert!(!fs::try_exists(&output).await.unwrap());
  }
}

#[tokio::test]
async fn cancellation_removes_created_and_unclaimed_completed_outputs() {
  let directory = tempdir().unwrap();
  let path = directory.path().join("cancelled");
  let options = options(FixtureKind::Opaque, Pattern::Random, 131_100);

  // A closed caller is observed after open completes, so cancellation cannot leak a late creation.
  let (sender, receiver) = oneshot::channel();
  drop(receiver);
  generate_owned(path.clone(), options, sender).await;
  assert!(!fs::try_exists(&path).await.unwrap());

  // Also cover cancellation after durable writes, before ownership is transferred to the caller.
  let (sender, receiver) = oneshot::channel();
  let worker = tokio::spawn(generate_owned(path.clone(), options, sender));
  let prepared = receiver.await.unwrap().unwrap();
  assert!(fs::try_exists(&path).await.unwrap());
  drop(prepared);
  worker.await.unwrap();
  assert!(!fs::try_exists(&path).await.unwrap());
}
