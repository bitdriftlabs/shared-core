# Chunked artifact uploads

This document covers SDK preparation, protocol exchanges, retries and local file ownership.

Chunked file artifacts use one continuous zlib stream, at the SDK's mobile compression level 5. The
SDK splits that encoded stream into 1 MiB fragments, with a shorter final fragment. Individual
fragments are not independently decodable. Command attachments, workflow attachments and crash
profiles use this same transport at every logical size. A nonempty file that compresses to at most 1
MiB uses one fragment; only the final encoded size, including the zlib trailer, determines the
fragment count. Logical size still determines admission and resource safety. Unrelated client
reports, snapshots and existing whole-file producers keep their legacy transport.

## Intent and round trips

The payload-free Begin is both intent negotiation and durable admission. No separate intent RPC or
payload precedes it:

- Receiving permits missing fragments and returns durable receipts.
- Dropped declines the intent: retire local bytes and notify a waiting caller of failure, without
  sending any fragment.
- Retryable retains local bytes and backs off; Rejected is a permanent protocol/admission failure.
- Finalized reconciles an earlier completed attempt.

A retry or reconnect keeps the same artifact identity and prepared fragments. Receiving returns the
durable receipts needed to resume missing fragments, including after a lost Begin or fragment response.

Admission policy can decline a previously accepted upload on its next Begin. An observed Dropped
response is terminal for the SDK; if that response is lost, a retried Begin can receive a different
decision. Command expiry and other admission checks can also reject a resume. Thin fragment requests
do not renegotiate intent.

For N fragments, the successful path takes N + 1 exchanges: Begin, then N fragment uploads. A
one-fragment file needs two; an empty command file or a Drop needs one. The final fragment completes
the upload without another finalize RPC. Preparation still precedes negotiation, so a dropped file
has already paid its local compression cost.

## Identity and geometry

Begin carries immutable capture metadata, the logical size and SHA-256, the encoded size, the
file-level ZLIB encoding and ordered encoded-fragment sizes/hashes. PutChunk carries only its
identity, logical size, index and exact fragment bytes. Offset is index times 1 MiB in the encoded
file, not the logical file. The first accepted Begin binds the manifest to the artifact identity.
The SDK reapplies current scrub rules just before each Begin is sent, including after
offline waits. Re-scrubbed resume metadata cannot replace that stored context; all other manifest
fields remain bound to the first admission.

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
preparers normalize fresh input through the same bounded reader. File descriptors retain their
current position and remain usable after unlinking; no temporary pathname is required. Whole-file
Bytes/File inputs share one streaming CRC writer. Whole-file snapshot paths keep their already
self-validating bytes unchanged, including on copy fallback, and are restored on failed publication.
Local storage checksums are independent of transport encoding.

New workflow retention stores a checksummed raw source at every size. The uploader performs the
single streaming compression pass when staging its fragments; it does not pre-compress small
workflows or route them through the whole-file uploader. Retained raw sources and their staged
fragments share the same admission inventory and cleanup leases.

The shared admission inventory includes retained workflows, whole files, fragments and reserved
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
Receiving and per-fragment receipts indicate progress, not upload success. A resumed Begin can return
Finalized when an earlier completion response was lost.

## Deferred transport unification

TODO: Consider migrating client reports and state snapshots to the same prepared-artifact
representation and Begin/fragment engine. `PreparationMode` and the whole-file staging/transfer
branches retain their distinct transports, retry policies and persisted layouts.

That migration could remove the separate queue-entry formats, intent/upload retry state and recovery
dispatch, but must first address:

- Client-report feature flags and issue metadata, which the fragment protocol does not accept.
- Already-compressed snapshot bytes and their potentially empty session IDs, without adding another
  compression layer or changing their format.
- Workflow retention and log-reference leases, whose durability boundary differs from a direct
  command waiting for upload completion.
