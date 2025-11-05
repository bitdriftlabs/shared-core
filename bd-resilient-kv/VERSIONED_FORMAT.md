# Versioned Journal Format Design

## Overview

This document describes the versioned journal format (VERSION 2) that enables version tracking for audit logs and remote backup by tracking write versions for each operation.

## Goals

1. **Version Tracking**: Each write operation gets a unique, monotonically increasing version number
2. **Journal Rotation**: Periodic compaction with self-contained state in each journal
3. **Remote Backup**: Archived journals can be uploaded to remote storage
4. **Backward Compatible**: New format coexists with existing VERSION 1

## Design Philosophy

Unlike traditional journal systems that use separate snapshot files, this design uses a **unified format** where:
- Each journal is self-contained with complete state embedded as regular entries
- No special "snapshot entry" format needed
- First N entries in a rotated journal are just regular versioned entries (all at same version)
- Simpler file structure and uniform entry format throughout

## File Types

### 1. Active Journal (`my_store.jrn`)
The current active journal receiving new writes. Active journals are **not compressed** for performance reasons.

### 2. Archived Journals (`my_store.jrn.v00020000.zz`, `my_store.jrn.v00030000.zz`, etc.)
Previous journals, archived during rotation. Each contains complete state at rotation version plus subsequent incremental writes. The version number in the filename indicates the rotation/snapshot version.

**Archived journals are automatically compressed using zlib** (indicated by the `.zz` extension) to reduce storage space and bandwidth requirements for remote backup. Compression is mandatory and occurs automatically during rotation.

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
{"v": 30000, "t": 1699564800123456789, "k": "key1", "o": "value1"}  // Compacted state (original timestamp)
{"v": 30000, "t": 1699564850987654321, "k": "key2", "o": "value2"}  // Compacted state (original timestamp)
{"v": 30000, "t": 1699564875111222333, "k": "key3", "o": "value3"}  // Compacted state (original timestamp)
{"v": 30001, "t": 1699564901000000000, "k": "key4", "o": "value4"}  // New write
{"v": 30002, "t": 1699564902000000000, "k": "key1", "o": "updated1"} // New write
...
```

Key observations:
- All compacted state entries have the same version (30000)
- **Timestamps are preserved**: Each compacted entry retains its original write timestamp (not the rotation time)
- These are regular journal entries, not a special format
- Incremental writes continue with version 30001+
- Each rotated journal is self-contained and can be read independently

## Rotation Process

When high water mark is reached at version N:

1. **Create New Journal**: Initialize fresh journal file (e.g., `my_store.jrn.tmp`)
2. **Write Compacted State**: Write all current key-value pairs as versioned entries at version N
   - **Timestamp Preservation**: Each entry retains its original write timestamp, not the rotation timestamp
   - This preserves historical accuracy and allows proper temporal analysis of the data
3. **Archive Old Journal**: Rename `my_store.jrn` → `my_store.jrn.old` (temporary)
4. **Activate New Journal**: Rename `my_store.jrn.tmp` → `my_store.jrn`
5. **Compress Archive**: Compress `my_store.jrn.old` → `my_store.jrn.v{N}.zz` using zlib
6. **Delete Temporary**: Remove uncompressed `my_store.jrn.old`
7. **Callback**: Notify application for upload/cleanup of compressed archived journal

Example:
```
Before rotation at v30000:
  my_store.jrn                    # Active, base_version=20000, contains v20000-v30000

After rotation:
  my_store.jrn                    # Active, base_version=30000, contains compacted state at v30000
  my_store.jrn.v30000.zz         # Compressed archive, contains v20000-v30000
```

### Compression

Archived journals are automatically compressed using zlib (compression level 3) during rotation:
- **Format**: Standard zlib format (RFC 1950)
- **Extension**: `.zz` indicates zlib compression
- **Transparency**: `VersionedRecovery` automatically decompresses archives when reading
- **Benefits**: Reduced storage space and bandwidth for remote backups
- **Performance**: Compression level 3 provides good balance between speed and compression ratio

## Recovery and Audit

### Current State Recovery
Simply read the active journal (`my_store.jrn`) and replay all entries to reconstruct the current state.

### Audit and Analysis
While `VersionedKVStore` does not support point-in-time recovery through its API, archived journals contain complete historical data that can be used for:

- **Audit Logging**: Review what changes were made and when
- **Offline Analysis**: Process archived journals to understand historical patterns
- **Remote Backup**: Upload archived journals to remote storage for disaster recovery
- **Compliance**: Maintain immutable records of all changes with version tracking

The version numbers in each entry allow you to understand the exact sequence of operations and build custom tooling for analyzing historical data.

**Timestamp Accuracy**: All entries preserve their original write timestamps, even after rotation. This means you can accurately track when each write originally occurred, making the journals suitable for temporal analysis, compliance auditing, and debugging time-sensitive issues.

### Point-in-Time Recovery with VersionedRecovery

While `VersionedKVStore` is designed for active operation and does not support point-in-time recovery through its API, the `VersionedRecovery` utility provides a way to reconstruct state at arbitrary historical versions from raw journal bytes.

#### Overview

`VersionedRecovery` is a separate utility that:
- Loads journals from file paths and automatically handles decompression of `.zz` archives
- Uses async I/O for efficient file loading
- Can process multiple journals for cross-rotation recovery
- Designed for offline analysis, server-side tooling, and audit systems
- Completely independent from `VersionedKVStore`

#### Use Cases

- **Server-Side Analysis**: Reconstruct state at specific versions for debugging or investigation
- **Audit Tooling**: Build custom audit systems that analyze historical changes
- **Cross-Rotation Recovery**: Recover state spanning multiple archived journals
- **Compliance**: Extract state at specific points in time for regulatory requirements
- **Testing**: Validate that state at historical versions matches expectations

#### API Methods

```rust
// Create recovery utility from journal files (oldest to newest) - async
let recovery = VersionedRecovery::from_files(vec![
    "store.jrn.v20000.zz",
    "store.jrn.v30000.zz", 
    "store.jrn"
]).await?;

// Recover state at specific version
let state = recovery.recover_at_version(25000)?;

// Get current state (latest version)
let current = recovery.recover_current()?;

// Get available version range
if let Some((min, max)) = recovery.version_range() {
    println!("Can recover versions {min} to {max}");
}
```

#### Example: Cross-Rotation Recovery

```rust
use bd_resilient_kv::VersionedRecovery;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create recovery utility from files (automatically decompresses .zz archives)
    // Provide journal paths in chronological order (oldest to newest)
    let recovery = VersionedRecovery::from_files(vec![
        "store.jrn.v20000.zz",
        "store.jrn.v30000.zz",
        "store.jrn",
    ]).await?;

    // Recover state at version 25000 (in archived journal)
    let state_at_25000 = recovery.recover_at_version(25000)?;

    // Recover state at version 35000 (across rotation boundary)
    let state_at_35000 = recovery.recover_at_version(35000)?;

    // Process the recovered state
    for (key, value) in state_at_25000 {
        println!("{key} = {value:?}");
    }
    
    Ok(())
}
```

#### Implementation Details

- **Async File Loading**: Constructor uses async I/O to load journal files efficiently
- **Automatic Decompression**: Transparently decompresses `.zz` archives when loading
- **Chronological Order**: Journals should be provided oldest to newest
- **Efficient Replay**: Automatically skips journals outside the target version range
- **Cross-Rotation**: Seamlessly handles recovery across multiple archived journals
- **Version Tracking**: Replays all entries up to and including the target version

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create or open store (requires directory path and name)
    let mut store = VersionedKVStore::new("/path/to/dir", "mystore", 1024 * 1024, None)?;

    // Writes return version numbers (async operations)
    let v1 = store.insert("key1".to_string(), Value::from(42)).await?;
    let v2 = store.insert("key2".to_string(), Value::from("hello")).await?;

    // Read current values (synchronous)
    let value = store.get("key1")?;
    
    Ok(())
}
```

### Rotation Callback

```rust
// Set callback for rotation events
store.set_rotation_callback(Box::new(|old_path, new_path, version| {
    println!("Rotated at version {version}");
    println!("Archived journal (compressed): {old_path:?}");
    println!("New active journal: {new_path:?}");
    // Upload old_path to remote storage...
}));
```

### Manual Rotation

```rust
// Automatic rotation on high water mark
let version = store.insert("key".to_string(), Value::from("value")).await?;
// Rotation happens automatically if high water mark exceeded

// Or manually trigger rotation (async)
store.rotate_journal().await?;
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
