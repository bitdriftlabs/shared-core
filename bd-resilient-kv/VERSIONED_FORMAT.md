# Versioned Journal Format Design

## Overview

This document describes the versioned journal format (VERSION 2) that enables point-in-time recovery by tracking write versions for each operation.

## Goals

1. **Version Tracking**: Each write operation gets a unique, monotonically increasing version number
2. **Point-in-Time Recovery**: Ability to reconstruct exact state at any version
3. **Journal Rotation**: Periodic compaction with self-contained state in each journal
4. **Backward Compatible**: New format coexists with existing VERSION 1

## Design Philosophy

Unlike traditional journal systems that use separate snapshot files, this design uses a **unified format** where:
- Each journal is self-contained with complete state embedded as regular entries
- No special "snapshot entry" format needed
- First N entries in a rotated journal are just regular versioned entries (all at same version)
- Simpler file structure and uniform entry format throughout

## File Types

### 1. Active Journal (`my_store.jrn`)
The current active journal receiving new writes.

### 2. Archived Journals (`my_store.jrn.v00020000`, `my_store.jrn.v00030000`, etc.)
Previous journals, archived during rotation. Each contains complete state at rotation version plus subsequent incremental writes. The version number in the filename indicates the rotation/snapshot version.

## Format Specification

### Journal Format (VERSION 2)

```
| Position | Data                     | Type           | Size    |
|----------|--------------------------|----------------|---------|
| 0        | Format Version           | u64            | 8 bytes |
| 8        | Position                 | u64            | 8 bytes |
| 16       | Type Code: Array Start   | u8             | 1 byte  |
| 17       | Metadata Object          | BONJSON Object | varies  |
| ...      | Versioned Journal Entry  | BONJSON Object | varies  |
| ...      | Versioned Journal Entry  | BONJSON Object | varies  |
```

**Metadata Object** (first entry in array):
```json
{
  "initialized": <u64 timestamp in ns>,
  "format_version": 2,
  "base_version": <u64 starting version for this journal>
}
```

**Versioned Journal Entry**:
```json
{
  "v": <u64 write version>,
  "t": <u64 timestamp in ns>,
  "k": "<key>",
  "o": <value or null for delete>
}
```

Fields:
- `v` (version): Monotonic write version number
- `t` (timestamp): When the write occurred (ns since UNIX epoch)
- `k` (key): The key being written
- `o` (operation): The value (for SET) or null (for DELETE)

## Journal Structure

### Initial Journal
When first created with base version 1:
```json
{"initialized": 1699564800000000000, "format_version": 2, "base_version": 1}
{"v": 2, "t": 1699564801000000000, "k": "key1", "o": "value1"}
{"v": 3, "t": 1699564802000000000, "k": "key2", "o": "value2"}
...
```

### Rotated Journal
After rotation at version 30000, the new journal contains:
```json
{"initialized": 1699564900000000000, "format_version": 2, "base_version": 30000}
{"v": 30000, "t": 1699564900000000000, "k": "key1", "o": "value1"}  // Compacted state
{"v": 30000, "t": 1699564900000000000, "k": "key2", "o": "value2"}  // Compacted state
{"v": 30000, "t": 1699564900000000000, "k": "key3", "o": "value3"}  // Compacted state
{"v": 30001, "t": 1699564901000000000, "k": "key4", "o": "value4"}  // New write
{"v": 30002, "t": 1699564902000000000, "k": "key1", "o": "updated1"} // New write
...
```

Key observations:
- All compacted state entries have the same version (30000)
- These are regular journal entries, not a special format
- Incremental writes continue with version 30001+
- Each rotated journal is self-contained and can be read independently

## Rotation Process

When high water mark is reached at version N:

1. **Create New Journal**: Initialize fresh journal file (e.g., `my_store.jrn.tmp`)
2. **Write Compacted State**: Write all current key-value pairs as versioned entries at version N
3. **Archive Old Journal**: Rename `my_store.jrn` → `my_store.jrn.v{N}`
4. **Activate New Journal**: Rename `my_store.jrn.tmp` → `my_store.jrn`
5. **Callback**: Notify application for upload/cleanup of archived journal

Example:
```
Before rotation at v30000:
  my_store.jrn                    # Active, base_version=20000, contains v20000-v30000

After rotation:
  my_store.jrn                    # Active, base_version=30000, contains compacted state at v30000
  my_store.jrn.v30000            # Archived, contains v20000-v30000
```

## Recovery Process

### Current State Recovery
Simply read the active journal (`my_store.jrn`) and replay all entries.

### Point-in-Time Recovery

To recover state at target version T:

1. **Find Correct Journal**: 
   - Check active journal's base_version and current_version range
   - If T is in active journal range, use active journal
   - Otherwise, find archived journal with appropriate version range

2. **Replay Entries**:
   - Read all entries from the journal
   - Apply entries with version <= T
   - Stop when reaching entries with version > T

3. **Result**: Exact state at version T

### Example Recovery Scenarios

**File Structure:**
```
my_store.jrn                # Active, base_version=30000, current=35000
my_store.jrn.v30000        # Archived, contains v20000-v30000  
my_store.jrn.v20000        # Archived, contains v10000-v20000
```

**Recover at v25000:**
1. Load `my_store.jrn.v30000` (archived journal)
2. Replay entries with version <= 25000
3. Result: State at v25000

**Recover at v30000:**
1. Load `my_store.jrn.v30000` (archived journal)
2. Replay all entries up to v30000
3. Result: State at v30000

**Recover at v32000:**
1. Load `my_store.jrn` (active journal, base_version=30000)
2. Replay entries with version <= 32000
3. Result: State at v32000

## Storage Efficiency

**Space Requirements:**
- Active journal: Compacted state + recent writes since rotation
- Archived journals: Full history for their version ranges

**Benefits of Unified Format:**
- Simpler file management (no separate snapshot + journal pairs)
- Each archived journal is self-contained
- Uniform entry format reduces code complexity
- Easy to understand and debug

**Cleanup Strategy:**
- Keep N most recent archived journals for recovery
- Upload archived journals to remote storage
- Delete old archived journals after successful upload

## API Usage

### Basic Operations

```rust
use bd_resilient_kv::VersionedKVStore;
use bd_bonjson::Value;

// Create or open store
let mut store = VersionedKVStore::new("mystore.jrn", 1024 * 1024, None)?;

// Writes return version numbers
let v1 = store.insert("key1".to_string(), Value::from(42))?;
let v2 = store.insert("key2".to_string(), Value::from("hello"))?;

// Point-in-time recovery
let state_at_v1 = store.as_hashmap_at_version(v1)?;
```

### Rotation Callback

```rust
// Set callback for rotation events
store.set_rotation_callback(Box::new(|old_path, new_path, version| {
    println!("Rotated at version {}", version);
    println!("Archived journal: {:?}", old_path);
    println!("New active journal: {:?}", new_path);
    // Upload old_path to remote storage...
}));
```

### Manual Rotation

```rust
// Automatic rotation on high water mark
let version = store.insert("key".to_string(), Value::from("value"))?;
// Rotation happens automatically if high water mark exceeded

// Or manually trigger rotation
store.rotate_journal()?;
```

## Migration from VERSION 1

VERSION 1 journals (without versioning) can coexist with VERSION 2:
- Existing VERSION 1 files continue to work with current `KVStore`
- New `VersionedKVStore` creates VERSION 2 journals
- No automatic migration (opt-in by using `VersionedKVStore`)

## Implementation Notes

1. **Version Counter Persistence**: Stored in metadata, initialized from journal on restart
2. **Atomicity**: Version increments are atomic with writes
3. **Monotonicity**: Versions never decrease or skip
4. **Concurrency**: Not thread-safe by design (same as current implementation)
5. **Format Field Names**: Use short names (`v`, `t`, `k`, `o`) to minimize storage overhead
6. **Self-Contained Journals**: Each rotated journal can be read independently without dependencies
