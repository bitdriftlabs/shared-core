# Versioned Journal Format Design

## Overview

This document describes the versioned journal format (VERSION 2) that enables point-in-time state recovery by using timestamps as version identifiers for each write operation.

## Goals

1. **Timestamp-Based Versioning**: Each write operation records a monotonically non-decreasing timestamp (in nanoseconds since UNIX epoch) that serves as both a version identifier and a logical clock. This allows correlating entries with with time-based data.
2. **Journal Rotation**: Periodic compaction with self-contained state in each journal

## File Types

### 1. Active Journal (`my_store.jrn`)
The current active journal receiving new writes. Active journals are **not compressed** for performance reasons.

### 2. Archived Journals (`my_store.jrn.t1699564900000000000.zz`, etc.)
Previous journals, archived during rotation. Each contains complete state at its creation time plus subsequent incremental writes. The timestamp in the filename indicates the rotation/snapshot timestamp.

**Archived journals are automatically compressed using zlib** (indicated by the `.zz` extension) to reduce storage space and bandwidth requirements for remote backup. Compression is mandatory and occurs automatically during rotation.

## Format Specification

### Binary Structure

The byte-level layout of a VERSION 2 journal file:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         JOURNAL FILE HEADER                             │
├──────────────────┬──────────────────┬───────────────────────────────────┤
│  Format Version  │    Position      │  Array Start Type Code            │
│     (u64)        │     (u64)        │        (u8)                       │
│    8 bytes       │    8 bytes       │       1 byte                      │
└──────────────────┴──────────────────┴───────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                      BONJSON METADATA OBJECT                            │
│  (First entry in the array)                                             │
├─────────────────────────────────────────────────────────────────────────┤
│  {                                                                      │
│    "initialized": 1699564800000000000,    // u64 timestamp (ns)         │
│    "format_version": 2                    // Format identifier          │
│  }                                                                      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    VERSIONED JOURNAL ENTRY                              │
│  (BONJSON Object)                                                       │
├─────────────────────────────────────────────────────────────────────────┤
│  {                                                                      │
│    "t": 1699564801000000000,              // Timestamp in ns (u64)      │
│    "k": "key1",                           // Key (string)               │
│    "o": "value1"                          // Value or null (any type)   │
│  }                                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```


### Header Structure (17 bytes total)

1. **Format Version** (offset 0, 8 bytes)
   - Type: `u64` little-endian
   - Value: `2` for VERSION 2 format
   - Purpose: Allows future format evolution

2. **Position** (offset 8, 8 bytes)
   - Type: `u64` little-endian
   - Value: Current write position in the buffer
   - Purpose: Tracks where next entry will be written

3. **Array Start Type Code** (offset 16, 1 byte)
   - Type: `u8`
   - Value: BONJSON type code indicating array start
   - Purpose: Begins the BONJSON array containing all entries

### Metadata Object (Variable size)

The first entry in the array is always a metadata object:

```json
{
  "initialized": <u64>,      // Creation timestamp (nanoseconds since epoch)
  "format_version": 2        // Must be 2 for this format
}
```

### Versioned Journal Entry Schema (Variable size)

Each subsequent entry follows this uniform schema:

```json
{
  "t": <u64>,                // Timestamp in nanoseconds (monotonically non-decreasing, serves as version)
  "k": "<string>",           // Key being modified
  "o": <value or null>       // Value for SET, null for DELETE
}
```

Fields:
- `t` (timestamp): Monotonically non-decreasing timestamp (ns since UNIX epoch) that serves as both the write time and version identifier
- `k` (key): The key being written
- `o` (operation): The value (for SET) or null (for DELETE)

**Type Flexibility**: The `"o"` field can contain any BONJSON-compatible type:
- Primitives (strings, numbers, booleans)
- Complex objects
- Arrays
- `null` (indicates DELETE operation)

**Timestamp Semantics:**
Timestamps are monotonically non-decreasing, not strictly increasing. If the system clock doesn't advance between writes, multiple entries may share the same timestamp. This is expected behavior and ensures proper ordering without clock skew.

**Size Considerations:**
- **Header**: Fixed 17 bytes
- **Metadata**: ~80-100 bytes (depending on timestamp magnitude)
- **Per Entry**: Varies based on key and value size
  - Minimum: ~50 bytes (short key, small value)
  - Typical: 100-500 bytes
  - Maximum: Limited by buffer size

## Journal Structure

### Initial Journal
When first created:
```json
{"initialized": 1699564800000000000, "format_version": 2}
{"t": 1699564801000000000, "k": "key1", "o": "value1"}
{"t": 1699564802000000000, "k": "key2", "o": "value2"}
...
```

### Rotated Journal
After rotation at timestamp 1699564900000000000, the new journal contains:
```json
{"initialized": 1699564900000000000, "format_version": 2}
{"t": 1699564800123456789, "k": "key1", "o": "value1"}  // Compacted state (original timestamp preserved)
{"t": 1699564850987654321, "k": "key2", "o": "value2"}  // Compacted state (original timestamp preserved)
{"t": 1699564875111222333, "k": "key3", "o": "value3"}  // Compacted state (original timestamp preserved)
{"t": 1699564901000000000, "k": "key4", "o": "value4"}  // New write after rotation
{"t": 1699564902000000000, "k": "key1", "o": "updated1"} // New write after rotation
...
```

Key observations:
- **Timestamps are preserved**: Each compacted entry retains its original write timestamp (not the rotation time)
    - This ensures that not only is the state at any given time recoverably from a given snapshot, we'll also be able to recover how long the current state values have been active for without looking at the previous snapshot.
- These are regular journal entries, not a special format
- New writes continue with later timestamps
- Each rotated journal is self-contained and can be read independently

## Rotation Process

When high water mark is reached:

1. **Determine Rotation Timestamp**: Calculate max timestamp T from the most recent entry
2. **Create New Journal**: Initialize fresh journal file (e.g., `my_store.jrn.tmp`)
3. **Write Compacted State**: Write all current key-value pairs as versioned entries using their original update timestamp
4. **Archive Old Journal**: Rename `my_store.jrn` → `my_store.jrn.old` (temporary)
5. **Activate New Journal**: Rename `my_store.jrn.tmp` → `my_store.jrn`
6. **Compress Archive**: Compress `my_store.jrn.old` → `my_store.jrn.t{T}.zz` using zlib
7. **Delete Temporary**: Remove uncompressed `my_store.jrn.old`

Example:
```
Before rotation at t=1699564900000000000:
  my_store.jrn                              # Active journal

After rotation:
  my_store.jrn                              # Active, contains compacted state
  my_store.jrn.t1699564900000000000.zz     # Compressed archive
```

### Rotation Timeline Visualization

```
TIME
  │
  ├─ t0: Normal Operation
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │
  │    │  ├─ {"t": 1699564795000000000, ...}│
  │    │  ├─ {"t": 1699564796000000000, ...}│
  │    │  ├─ {"t": 1699564797000000000, ...}│
  │    │  ├─ {"t": 1699564798000000000, ...}│
  │    │  └─ {"t": 1699564799000000000, ...}│
  │    └────────────────────────────────────┘
  │
  ├─ t1: High Water Mark Reached
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │
  │    │  └─ {"t": 1699564800000000000, ...}│ ← TRIGGER
  │    └────────────────────────────────────┘
  │    max_timestamp = 1699564800000000000
  │
  ├─ t2: Create New Journal (Step 1)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │  (old, still active)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.tmp                  │  (new, being written)
  │    │  └─ [header + metadata]            │
  │    └────────────────────────────────────┘
  │
  ├─ t3: Write Compacted State (Step 2)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │  (old, still active)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.tmp                  │  (new, being written)
  │    │  ├─ {"t": 1699564750000000000, "k": "key1", ...}│ ← Original timestamps
  │    │  ├─ {"t": 1699564780000000000, "k": "key2", ...}│ ← Original timestamps
  │    │  └─ {"t": 1699564799000000000, "k": "key3", ...}│ ← Original timestamps
  │    └────────────────────────────────────┘
  │
  ├─ t4: Archive Old Journal (Step 3)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.old                  │  (renamed, temporary)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.tmp                  │  (new, ready)
  │    └────────────────────────────────────┘
  │
  ├─ t5: Activate New Journal (Step 4)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.old                  │  (archived, temporary)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │  ← NOW ACTIVE!
  │    │  (contains compacted state)        │
  │    └────────────────────────────────────┘
  │
  ├─ t6: Compress Archive (Step 5 - Async)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │  (active, accepting writes)
  │    │  └─ {"t": 1699564801000000000, ...}│ ← New writes
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.old                  │  (being compressed...)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.t1699564800000000000.zz│ (compressed output)
  │    └────────────────────────────────────┘
  │
  ├─ t7: Delete Temporary (Step 6)
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn                      │  (active)
  │    └────────────────────────────────────┘
  │    ┌────────────────────────────────────┐
  │    │  my_store.jrn.t1699564800000000000.zz│ (compressed archive)
  │    └────────────────────────────────────┘
  │
  └─ t8: Continue Normal Operation
       ┌────────────────────────────────────┐
       │  my_store.jrn                      │
       │  ├─ {"t": 1699564801000000000, ...}│
       │  ├─ {"t": 1699564802000000000, ...}│
       │  └─ {"t": 1699564803000000000, ...}│
       └────────────────────────────────────┘
       ┌────────────────────────────────────┐
       │  my_store.jrn.t1699564800000000000.zz│ (ready for upload)
       └────────────────────────────────────┘
```

### Compression

Archived journals are automatically compressed using zlib (compression level 5) during rotation:
- **Format**: Standard zlib format (RFC 1950)
- **Extension**: `.zz` indicates zlib compression
- **Benefits**: Reduced storage space and bandwidth for remote backups

### Rotation Failure Modes and Recovery

| Failure Point | State | Recovery |
|---------------|-------|----------|
| Before Step 3 | my_store.jrn + my_store.jrn.tmp exist | Delete .tmp, retry |
| After Step 3, before Step 4 | my_store.jrn.old exists, no active journal | Rename .old back to .jrn |
| After Step 4 | New journal active | Continue normally, cleanup may be incomplete |
| During Step 5-6 | Compression fails | .old file may remain, but new journal is valid |


**What Can Fail:**
- I/O errors (disk full, permissions, etc.)
- Compression errors during async compression phase

## Recovery and Audit

### Current State Recovery
Simply read the active journal (`my_store.jrn`) and replay all entries to reconstruct the current state.

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
