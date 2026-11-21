# Chunked artifact uploads

This document covers SDK preparation, protocol exchanges, retries and local file ownership.

Chunked file artifacts use one continuous zlib stream, at the SDK's mobile compression level 5
(except already-compressed state snapshots, which use stored zlib blocks at level 0 to avoid paying
double-compression CPU while preserving the uniform zlib wire encoding). The SDK splits that
encoded stream into 1 MiB fragments, with a shorter final fragment. Individual fragments are not
independently decodable. Client reports, state snapshots, command attachments, workflow attachments
and crash profiles all use this transport. A nonempty file that compresses to at most 1 MiB uses
one fragment; only the final encoded size, including the zlib trailer, determines the fragment
count. Logical size still determines admission and resource safety.

## Intent and round trips

The payload-free Begin requests approval before any payload is sent. No separate intent RPC precedes
it. For the new non-command server path, a fresh approval is stream-local; only staged fragments and
the published artifact are durable. Direct-command admission is unchanged.

- Receiving permits missing fragments and returns durable receipts.
- Dropped declines the intent: retire local bytes and notify a waiting caller of failure, without
  sending any fragment.
- Retryable retains local bytes and backs off; Rejected is a permanent protocol/admission failure.
- Finalized reconciles an earlier completed attempt.

A retry or reconnect keeps the same artifact identity and prepared fragments. Receiving returns the
durable receipts needed to resume missing fragments, including after a lost Begin or fragment
response.

Admission policy can decline a previously accepted upload on its next Begin. An observed Dropped
response is terminal for the SDK; if that response is lost, a retried Begin can receive a different
decision. Command expiry and other admission checks can also reject a resume. Thin fragment requests
do not renegotiate intent.

For N fragments, the successful path takes N + 1 exchanges: Begin, then N fragment uploads. A
one-fragment file needs two; an empty command file or a Drop needs one. The final fragment completes
the upload without another finalize RPC. Preparation still precedes negotiation, so a dropped file
has already paid its local compression cost.

## Identity and geometry

Begin carries bounded capture/policy metadata, the logical size and SHA-256, the encoded size, the
file-level ZLIB encoding and ordered encoded-fragment sizes/hashes (never `feature_flags`).
Intermediate PutChunk requests carry only identity, logical size, index and exact fragment bytes.
The final PutChunk (`index == chunks.len() - 1`) also carries the full scrubbed `state_metadata`
and `feature_flags`, which the server persists inside the canonical completed artifact envelope so
downstream readers can reconstruct them from S3 rather than Kafka. Offset is index times 1 MiB in
the encoded file, not the logical file. The SDK reapplies current scrub rules just before each Begin
and final PutChunk is sent, including after offline waits. In the new non-command path, the payload
geometry determines the staging prefix and the first completed publication binds the owner and
bytes. `session_id` may be empty when not applicable (such as state snapshots or previous-run
reports without a persisted session ID).

Missing or empty MIME types resolve to `application/octet-stream`; valid supplied types are
normalized for the uploaded attachment. The SDK preserves the original optional value in its Begin
manifest.

Nonempty chunked files always use zlib, including when it expands input. SDK admission reserves the
codec's worst-case output in addition to an owned source. The logical limit remains 256 MiB; encoded
expansion can require 257 fragments and a 33-byte receipt bitmap. The empty remote-file special case
has zero logical/encoded bytes, SHA-256(empty), RAW encoding and no fragments. Workflow attachments
and crash profiles cannot be empty.

## SDK ownership

Staging reads the source once and keeps one encoder across bounded input reads. Draining encoded
output into fragment files does not flush or reset compression. The stream trailer is emitted only
at EOF. Logical hashing and retained-workflow CRC validation happen on the same input pass. No whole
compressed temporary copy is needed.

`Bytes`, `File` and `Path` describe how input is acquired, not its compression or transport. Both
inputs use the same bounded reader. File descriptors retain their current position and remain usable
after unlinking; no temporary pathname is required. Snapshot source paths remain intact until the
fragment index commits. Transport decoding yields the original journal bytes unchanged. Local
storage checksums are independent of transport encoding.

New workflow retention stores a checksummed raw source at every size. The uploader performs the
single streaming compression pass when staging its fragments; it does not pre-compress small
workflows or route them through the whole-file uploader. Retained raw sources and their staged
fragments share the same admission inventory and cleanup leases.

The shared admission inventory includes retained workflows, staged files, and reserved
unwritten output. The 64-artifact/640 MiB budget rejects new work rather than evicting accepted
work. Runtime reductions do not invalidate committed entries.

Owned capture paths must share the SDK filesystem: staging transfers ownership by rename and rejects
cross-filesystem moves. Fragment files become durable before index publication; ambiguous index
publication retains payload ownership. Recovery verifies fragment hashes and the entire decoded
stream without loading a whole file. Retries reuse persisted bytes, never recompress. Missing,
oversized or checksum-invalid staged fragments are terminal local failures: the uploader retires
their index ownership and payloads and notifies a waiting caller. Transient filesystem and transport
failures retain ownership and retry.

## Completion

Fragments can be acknowledged independently; a receipt means encoded bytes are durable, not that the
logical file is complete. The SDK retains every fragment until Finalized, Dropped or Rejected,
unless local integrity verification fails.

Only Finalized confirms successful file completion after encoded and logical integrity validation.
Receiving and per-fragment receipts indicate progress, not upload success. A resumed Begin can
return Finalized when an earlier completion response was lost.

## SDK upgrade

The SDK discards legacy whole-file queue entries and retained workflow `.payload` files instead
of migrating them. Already queued chunked uploads resume with their original IDs. At startup,
the SDK still reads pending chunked reports to restore Android crash-profile attribution.
Loop API continues to accept legacy uploads from older SDKs.

