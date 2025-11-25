# Versioned KV Journal Design

## Overview

This document describes the versioned k-v store, which enables point-in-time state recovery by using timestamps as version identifiers. Each write operation is tagged with a timestamp, allowing the system to reconstruct the key-value store state at any historical moment.

## Goals

1. Enable recovery of key-value store state at any historical point in time
2. Preserve accurate write timestamps for audit and historical analysis
3. Support (near) indefinite retention of historical data without unbounded growth of active storage

## Design Overview

The versioned journal format uses timestamps as version identifiers for each write operation. Each entry in the journal records the timestamp, key, and value (or deletion marker) for every operation. This allows the store to reconstruct state at any point in time by replaying entries up to a target timestamp.

To prevent unbounded growth, the system uses journal rotation: when the active journal reaches a size threshold, it is rotated out and replaced with a new journal containing only the current compacted state. The old journal is archived and compressed. Each archived journal preserves the original write timestamps of all entries, enabling point-in-time recovery across rotation boundaries.

The underlying journal uses Protobuf to serialize the payloads that are used to implement the key-value semantics.

## File Types

### 1. Active Journal (`my_store.jrn.0`)
The current active journal receiving new writes. Active journals are **not compressed** for performance reasons.

The number at the end of the active journal reflects the generation of the active journal, which allows us to safely rotate the journal while gracefully handling I/O errors. More on this below in the rotation section.

### 2. Archived Journals (`my_store.jrn.t1699564900000000.zz`, etc.)
Previous journals, archived during rotation. Each contains complete state at its creation time plus subsequent incremental writes. The timestamp in the filename indicates the rotation/snapshot timestamp.

**Archived journals are automatically compressed using zlib** (indicated by the `.zz` extension) to reduce storage space and bandwidth requirements for remote backup. Compression is mandatory and occurs automatically during rotation.

## Format Specification

### Binary Structure

The byte-level layout of a VERSION 1 journal file:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         JOURNAL FILE HEADER                             │
├──────────────────┬──────────────────┬───────────────────────────────────┤
│  Format Version  │    Position      │  Reserved                         │
│     (u64)        │     (u64)        │    (u8)                           │
│    8 bytes       │    8 bytes       │   1 byte                          │
└──────────────────┴──────────────────┴───────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    VERSIONED JOURNAL ENTRY                              │
│  (Protobuf-encoded StateKeyValuePair)                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  Frame Length (varint)      │  Variable length (1-10 bytes)             │
│  Scope (u8)                 │  1 byte (namespace identifier)            │
│  Key Length (varint)        │  Variable length (1-10 bytes)             │
│  Key (UTF-8 bytes)          │  Variable length                          │
│  Timestamp (varint)         │  Variable length (microseconds)           │
│  Protobuf Payload           │  Variable length                          │
│  CRC32                      │  4 bytes                                  │
│                                                                         │
│  Payload contains:                                                      │
│  StateKeyValuePair {                                                    │
│    key: String,              // The key being modified                  │
│    value: StateValue         // Value for SET, null for DELETE          │
│  }                                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```


### Header Structure (17 bytes total)

| Field | Offset | Size | Type | Value | Purpose |
|-------|--------|------|------|-------|---------|
| Format Version | 0 | 8 bytes | u64 (little-endian) | `1` | Allows future format evolution |
| Position | 8 | 8 bytes | u64 (little-endian) | Current write position | Tracks where next entry will be written |
| Reserved | 16 | 1 byte | u8 | `0` | Reserved for future use |

### Entry Framing Format

Each entry in the journal uses a length-prefixed framing format with CRC32 integrity checking:

| Component | Size | Type | Description |
|-----------|------|------|-------------|
| Frame Length | Variable | varint | Total size of scope + key_len + key + timestamp + protobuf payload + CRC32 (1-10 bytes) |
| Scope | 1 byte | u8 | Namespace identifier for the entry |
| Key Length | Variable | varint | Length of the key in bytes (1-10 bytes) |
| Key | Variable | bytes | UTF-8 encoded key string |
| Timestamp | Variable | varint | Entry timestamp in microseconds (serves as version) |
| Protobuf Payload | Variable | bytes | Serialized StateKeyValuePair message |
| CRC32 | 4 bytes | u32 (little-endian) | Checksum of scope + key_len + key + timestamp + payload |

### Versioned Journal Entry Schema

Each entry in the journal is a `StateKeyValuePair` protobuf message:

```protobuf
message StateKeyValuePair {
  string key = 1;         // The key being modified
  StateValue value = 2;   // Value for SET, null/empty for DELETE
}

message StateValue {
  oneof value {
    string string_value = 1;
  }
}
```

Fields:
- `key`: The key being written (string)
- `value`: The value being set (StateValue) or null/empty for DELETE operations

**Timestamp Semantics:**
- Timestamps are stored as varints in microseconds since UNIX epoch
- Timestamps are monotonically non-decreasing, not strictly increasing
- If the system clock doesn't advance between writes, multiple entries may share the same timestamp
- This is expected behavior and ensures proper ordering without clock skew

**Type Flexibility**: The `StateValue` message supports multiple value types:
- Primitives: strings, integers, doubles, booleans
- Complex types: lists, maps
- Binary data: bytes
- null value (indicates DELETE operation)

**Size Considerations:**
- **Header**: Fixed 17 bytes
- **Per Entry**: Varies based on key and value size
  - Frame length: 1-10 bytes (varint-encoded)
  - Scope: 1 byte
  - Key length: 1-10 bytes (varint-encoded)
  - Key: Variable (UTF-8 bytes)
  - Timestamp: 1-10 bytes (varint-encoded)
  - Protobuf payload: varies by content
  - CRC: Fixed 4 bytes
  - Typical small entries: 25-60 bytes total
  - Typical medium entries: 60-220 bytes total

## Buffer Sizing and Dynamic Growth

The persistent store uses memory-mapped files with dynamically sized buffers that grow as needed to accommodate data. This balances memory efficiency with performance by starting small and expanding only when necessary.

### Growth Strategy

- **Initial Size**: Configurable via `PersistentStoreConfig::initial_buffer_size` (default: 8KB, power-of-2 recommended)
- **Growth Pattern**: Power-of-2 doubling (8KB → 16KB → 32KB → 64KB → ...)
- **Growth Trigger**: During journal rotation, if the compacted state requires more space
- **Headroom**: After growth, buffer provides 50% extra capacity beyond compacted size
- **Maximum Capacity**: Configurable via `PersistentStoreConfig::max_capacity_bytes` (default: 1MB)

### Configuration

```rust
let config = PersistentStoreConfig {
  initial_buffer_size: Some(8192),      // Start at 8KB (any value accepted, rounded to power-of-2)
  max_capacity_bytes: Some(10_485_760), // Cap at 10MB
  high_water_mark_ratio: Some(0.7),     // Rotate at 70% full
};
```

### Growth Behavior

**Normal Growth Example:**
1. Start: 8KB buffer
2. Data grows to 5.6KB (70% of 8KB) → rotation triggered
3. Compacted state is 4KB → grow to 8KB (provides 50% headroom)
4. More data → reaches 70% of 8KB again → rotation
5. Compacted state is 7KB → grow to 16KB (provides 50% headroom)
6. Continues until `max_capacity_bytes` is reached

**Maximum Capacity:**
- Once `max_capacity_bytes` is reached, buffer stops growing
- Rotations continue normally, but buffer size remains constant
- This prevents unbounded memory usage while allowing continued operation

### Config Normalization

Invalid configuration values are automatically normalized to safe defaults:
- `initial_buffer_size`: Non-power-of-2 → rounded up; invalid → 8KB
- `max_capacity_bytes`: Invalid or missing → 1MB (prevents unbounded growth)
- `high_water_mark_ratio`: Out of range or NaN → 0.7 (70%)

### File Size Reconciliation

When reopening an existing store with different configuration:
- If existing file is larger than configured initial size → file size preserved
- If existing file is smaller → continues with existing size
- Growth continues from current size using new configuration parameters

This allows configuration changes without data loss or unnecessary resizing.

## Journal Structure

### Initial Journal
When first created, the journal contains versioned entries:
```
Entry 0: {"key": "key1", "value": "value1"} @ t=1699564801000000
Entry 1: {"key": "key2", "value": "value2"} @ t=1699564802000000
...
```

### Rotated Journal
After rotation at timestamp 1699564900000000, the new journal contains:
```
Entry 0: {"key": "key1", "value": "value1"} @ t=1699564800123456  // Compacted state (original timestamp preserved)
Entry 1: {"key": "key2", "value": "value2"} @ t=1699564850987654  // Compacted state (original timestamp preserved)
Entry 2: {"key": "key3", "value": "value3"} @ t=1699564875111222  // Compacted state (original timestamp preserved)
Entry 3: {"key": "key4", "value": "value4"} @ t=1699564901000000  // New write after rotation
Entry 4: {"key": "key1", "value": "updated1"} @ t=1699564902000000 // New write after rotation
...
```

Key observations:
- **Timestamps are preserved**: Each compacted entry retains its original write timestamp (not the rotation time)
    - This ensures that not only is the state at any given time recoverable from a given snapshot, we'll also be able to recover how long the current state values have been active for without looking at the previous snapshot.
- All entries use the same protobuf framing format
- New writes continue with later timestamps
- Each rotated journal is self-contained and can be read independently

## Rotation Process

When high water mark is reached:

1. **Determine Rotation Timestamp**: Calculate max timestamp T from the most recent entry
2. **Increment Generation**: Calculate next generation number (e.g., 0 → 1)
3. **Create New Journal**: Initialize fresh journal file with next generation (e.g., `my_store.jrn.1`)
4. **Write Compacted State**: Write all current key-value pairs as versioned entries using their original update timestamp
5. **Activate New Journal**: Switch to new journal in-memory, unmap old journal
6. **Compress Archive** (async, best-effort): Compress old generation → `my_store.jrn.t{T}.zz` using zlib
7. **Delete Original** (best-effort): Remove uncompressed old generation file

Example:
```
Before rotation (generation 0):
  my_store.jrn.0                            # Active journal (generation 0)

After rotation (generation 1):
  my_store.jrn.1                            # Active, contains compacted state (generation 1)
  my_store.jrn.t1699564900000000.zz        # Compressed archive of generation 0
```

### Rotation Timeline Visualization

```
TIME
  │
  ├─ t0: Normal Operation (Generation 0)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │
  │    │  ├─ Entry @ t=1699564795000000     │
  │    │  ├─ Entry @ t=1699564796000000     │
  │    │  ├─ Entry @ t=1699564797000000     │
  │    │  ├─ Entry @ t=1699564798000000     │
  │    │  └─ Entry @ t=1699564799000000     │
  │    └────────────────────────────────────┘
  │
  ├─ t1: High Water Mark Reached
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │
  │    │  └─ Entry @ t=1699564800000000     │ ← TRIGGER
  │    └────────────────────────────────────┘
  │    max_timestamp = 1699564800000000
  │
  ├─ t2: Create New Journal (Step 1)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │  (old, still active - generation 0)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.1                    │  (new, being written - generation 1)
  │    │  └─ [header]                       │
  │    └────────────────────────────────────┘
  │
  ├─ t3: Write Compacted State (Step 2)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │  (old, still active)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.1                    │  (new, being written)
  │    │  ├─ Entry {"key1", ...} @ t=1699564750000000│ ← Original timestamps
  │    │  ├─ Entry {"key2", ...} @ t=1699564780000000│ ← Original timestamps
  │    │  └─ Entry {"key3", ...} @ t=1699564799000000│ ← Original timestamps
  │    └────────────────────────────────────┘
  │
  ├─ t4: Activate New Journal (Step 3)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │  (old, unmapped - ready for archive)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.1                    │  ← NOW ACTIVE! (generation 1)
  │    │  (contains compacted state)        │
  │    └────────────────────────────────────┘
  │
  ├─ t5: Compress Archive (Step 4 - Async)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.1                    │  (active, accepting writes)
  │    │  └─ Entry @ t=1699564801000000     │ ← New writes
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.0                    │  (being compressed...)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.t1699564800000000.zz │ (compressed output)
  │    └────────────────────────────────────┘
  │
  ├─ t6: Delete Original (Step 5)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.1                    │  (active - generation 1)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.t1699564800000000.zz │ (compressed archive of gen 0)
  │    └────────────────────────────────────┘
  │
  └─ t7: Continue Normal Operation
       ┌────────────────────────────────────┐
       │  my_store.jrn.1                    │
       │  ├─ Entry @ t=1699564801000000     │
       │  ├─ Entry @ t=1699564802000000     │
       │  └─ Entry @ t=1699564803000000     │
       └────────────────────────────────────┘
       ┌────────────────────────────────────┐
       │  my_store.jrn.t1699564800000000.zz │ (ready for upload)
       └────────────────────────────────────┘
```

### Compression

Archived journals are automatically compressed using zlib (compression level 5) during rotation:
- **Format**: Standard zlib format (RFC 1950)
- **Extension**: `.zz` indicates zlib compression
- **Benefits**: Reduced storage space and bandwidth for remote backups

### Rotation Failure Modes and Recovery

The generation-based rotation process is designed to be resilient:

| Failure Point | State | Recovery |
|---------------|-------|----------|
| Before Step 3 | Old generation active, new generation partially written | Delete incomplete new generation, retry |
| During/After Step 5 | New generation active | Continue normally, old generation remains until compressed |
| During Step 6-7 | Compression fails | Uncompressed old generation may remain, but new journal is valid |


**What Can Fail:**
- I/O errors (disk full, permissions, etc.)
- Compression errors during async compression phase

**Key Design Feature**: The rotation switches journals in-memory without file renames, making the critical transition atomic from the application's perspective. Old generation files remain at their original paths until successfully archived.

## Recovery and Audit

### Current State Recovery
The active journal is identified by finding the highest generation number (e.g., `my_store.jrn.0`, `my_store.jrn.1`, etc.). Simply read the active journal and replay all entries to reconstruct the current state.

### Audit and Analysis
While `VersionedKVStore` does not support point-in-time recovery through its API, archived journals contain complete historical data.

The timestamps in each entry allow you to understand the exact sequence of operations and build custom tooling for analyzing historical data.

**Timestamp Accuracy**: All entries preserve their original write timestamps, even after rotation. This means you can accurately track when each write originally occurred.

### Point-in-Time Recovery with VersionedRecovery

While `VersionedKVStore` is designed for active operation and does not support point-in-time recovery through its API, the `VersionedRecovery` utility provides a way to reconstruct state at arbitrary historical timestamps from raw journal bytes.

#### Overview

`VersionedRecovery` is a separate utility that:
- Loads journals from file paths and automatically handles decompression of `.zz` archives
- Uses async I/O for efficient file loading
- Can process multiple journals for cross-rotation recovery
- Designed for offline analysis, server-side tooling, and audit systems
- Completely independent from `VersionedKVStore`
