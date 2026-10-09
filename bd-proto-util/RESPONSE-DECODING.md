# Response Decoding

The SDK's entire `ApiResponse` receive path uses `proto_deserialize`. Retained protobuf values
are constructed directly by trait implementations in this trait-owning crate. Configuration
envelopes, buffer/filter/tail configuration, workflows, and matchers use borrowed schema views
and are converted into final runtime objects without constructing generated protobuf graphs.
The configuration cache stores raw wire bytes; the runtime cache uses the same retained decoder
as live responses.

Retained roots are response leaves consumed by the existing API, client-state values, device
commands, and generate-log actions. Their reachable payloads are used by existing consumers or
persistence/encoding APIs. The generator's root and retained-boundary lists document those
choices. The declarations marked `target` disappear during macro expansion; they are not staging
models. Unused view getters and schema branches are removed by release linking.

## Storage and Collection Decoding

The common single-fragment `Message` stores its borrowed slice inline, including when a view
clones it. Only merged singular-message fragments allocate a metadata vector; payload bytes
remain borrowed. This is not a fully decoded protobuf object or a zero-sized wrapper.

Repeated-message and map decoding uses `visit_messages`, a shared non-inlined wire loop with
a type-erased callback. Generated callbacks append directly to their output collections without
an intermediate `Vec<Message>`. Output vectors, maps, owned strings, and compiled runtime values
still allocate as needed. Sharing the loop adds an indirect callback per occurrence in exchange
for less specialized code; throughput has not been measured.

## Regeneration

From the enclosing monorepo, with `protoc` and Python's standard `protobuf` package available:

```sh
protoc -I shared-core/api/src -I shared-core/api/protoc-gen-validate \
  -I /opt/homebrew/include --include_imports \
  --descriptor_set_out=.tmp/response-tree/response.pb \
  bitdrift_public/protobuf/client/v1/api.proto
python3 shared-core/bd-proto-util/generate-response-decoders.py \
  .tmp/response-tree/response.pb \
  shared-core/bd-proto-util/src/serialization/inline/response
just rustfmt shared-core/bd-proto-util/src/serialization/inline/response/*.rs \
  shared-core/bd-proto-util/src/serialization/inline/views/*.rs
```

Use an isolated Python environment if necessary. Create the workspace-local scratch directory
before running `protoc`; replace the well-known-type include path on other platforms. The
generator reads schema descriptors, not generated Rust, and removes obsolete files only when
they bear its generated header. Review regenerated declarations and run the normal crate tests
and Clippy targets. Do not hand-edit the generated declarations.

## Compatibility Choices

- Scalar duplicates and duplicate map keys are last-wins. Singular message fragments merge;
  changing a oneof alternative clears its earlier fragments. Conversion hooks run after reads.
- Collection visitors stop on a wire or callback error. If several occurrences are invalid,
  the first reported error can differ from the previous eager intermediate-message collection.
- Unknown fields are skipped, not retained in protobuf `special_fields`.
- Borrowed getters validate fields that consumers read, not unused semantic subtrees. Framing,
  lengths, wire types for consumed fields, and recursion limits are checked by the common reader.
- Raw configuration deliberately defers inner decoding to configuration processing, preserving
  the NACK and cache boundary. Separately applied client-state updates are excluded from cache
  bytes so they are not replayed.
- Business validation remains explicit: invalid workflows, filters, and tail matchers retain
  their existing partial-tolerance behavior; command IDs and required graph references are
  checked before installation.
- This is not a general protobuf codec. Packed scalars, groups, zigzag/fixed integers, and float32
  are not implemented by this macro; the selected response schema uses supported wire forms.

Valid-value parity, duplicate/oneof behavior, cache restoration, and runtime execution have
focused tests. Arbitrary malformed-input equivalence and throughput are not claimed.
