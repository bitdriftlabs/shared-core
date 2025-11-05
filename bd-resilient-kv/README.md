# bd-resilient-kv

A crash-resilient key-value store library for Rust with automatic persistence, compression, and high performance.

## Overview

`bd-resilient-kv` provides a HashMap-like persistent key-value store that automatically saves data to disk while maintaining excellent performance through in-memory caching and smart buffering strategies. The library is designed to be crash-resilient, ensuring data integrity even in the event of unexpected shutdowns.

## Key Features

- **🛡️ Crash Resilient**: Automatic persistence with crash recovery capabilities
- **⚡ High Performance**: O(1) reads via in-memory caching, optimized writes
- **🗜️ Automatic Compression**: Double-buffered journaling with automatic compaction
- **💾 Memory Mapped I/O**: Efficient file operations using memory-mapped files
- **🔄 Self-Managing**: Automatic high water mark detection and buffer switching
- **🎯 Simple API**: HashMap-like interface that's easy to use
- **🏗️ JSON-like Values**: Built on `bd-bonjson` for flexible value types
- **📊 Version Tracking**: Optional versioned store with point-in-time recovery and automatic journal rotation

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
bd-resilient-kv = { path = "path/to/bd-resilient-kv" }
bd-bonjson = { path = "path/to/bd-bonjson" }
```

### Choosing Between KVStore and VersionedKVStore

**KVStore**: Use for general key-value storage with automatic compaction
- Best for: Configuration storage, caches, general-purpose persistence
- Features: Double-buffered journaling, automatic compaction, high performance

**VersionedKVStore**: Use when you need version tracking
- Best for: Audit logs, state history, remote backup
- Features: Every write operation returns a version number, automatic rotation with callbacks
- See: [VERSIONED_FORMAT.md](./VERSIONED_FORMAT.md) for detailed format documentation

### Basic Usage

```rust
use bd_resilient_kv::KVStore;
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    // Create a new store with 1MB buffer size
    let mut store = KVStore::new("my_store", 1024 * 1024, None, None)?;

    // Insert some values
    store.insert("name".to_string(), Value::String("Alice".to_string()))?;
    store.insert("age".to_string(), Value::Integer(30))?;
    store.insert("active".to_string(), Value::Boolean(true))?;

    // Read values
    if let Some(name) = store.get("name") {
        println!("Name: {}", name);
    }

    // Check existence
    if store.contains_key("age") {
        println!("Age is stored");
    }

    // Remove a key
    let old_value = store.remove("active")?;
    println!("Removed: {:?}", old_value);

    // Get all keys via as_hashmap
    let keys: Vec<&String> = store.as_hashmap().keys().collect();
    println!("Keys: {:?}", keys);

    // Get all values
    let values: Vec<&Value> = store.as_hashmap().values().collect();
    println!("Values: {:?}", values);

    // Iterate over all key-value pairs
    for (key, value) in store.as_hashmap() {
        println!("{}: {:?}", key, value);
    }

    Ok(())
}
```

### Working with Different Value Types

```rust
use bd_resilient_kv::KVStore;
use bd_bonjson::Value;
use std::collections::HashMap;

fn main() -> anyhow::Result<()> {
    let mut store = KVStore::new("typed_store", 1024 * 1024, None, None)?;

    // Strings
    store.insert("greeting".to_string(), Value::String("Hello, World!".to_string()))?;

    // Numbers
    store.insert("count".to_string(), Value::Integer(42))?;
    store.insert("temperature".to_string(), Value::Float(98.6))?;

    // Booleans
    store.insert("enabled".to_string(), Value::Boolean(true))?;

    // Arrays
    let array = Value::Array(vec![
        Value::String("item1".to_string()),
        Value::String("item2".to_string()),
        Value::Integer(123),
    ]);
    store.insert("items".to_string(), array)?;

    // Objects
    let mut obj = HashMap::new();
    obj.insert("x".to_string(), Value::Integer(10));
    obj.insert("y".to_string(), Value::Integer(20));
    store.insert("coordinates".to_string(), Value::Object(obj))?;

    // Null (equivalent to deletion)
    store.insert("to_delete".to_string(), Value::Null)?; // This removes the key

    Ok(())
}
```

### Persistence and Recovery

The store automatically creates two journal files with `.jrna` and `.jrnb` extensions:

```rust
use bd_resilient_kv::KVStore;
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    {
        // Create store and add data
        let mut store = KVStore::new("persistent_store", 1024 * 1024, None, None)?;
        store.insert("persistent_key".to_string(), Value::String("This survives restarts".to_string()))?;
        // Store automatically syncs to disk
    } // store goes out of scope

    {
        // Open the same store later - data is recovered
        let store = KVStore::new("persistent_store", 1024 * 1024, None, None)?;
        let value = store.get("persistent_key");
        assert_eq!(value, Some(Value::String("This survives restarts".to_string())));
    }

    Ok(())
}
```

### High Water Mark Monitoring

Monitor buffer usage and get notified when it's time to compress:

```rust
use bd_resilient_kv::KVStore;
use bd_bonjson::Value;

fn high_water_mark_callback(current_pos: usize, buffer_size: usize, hwm_pos: usize) {
    println!("High water mark triggered!");
    println!("Current position: {}, Buffer size: {}, HWM position: {}",
             current_pos, buffer_size, hwm_pos);
}

fn main() -> anyhow::Result<()> {
    let mut store = KVStore::new(
        "monitored_store",
        1024 * 1024,                    // 1MB buffer
        Some(0.8),                      // Trigger at 80% full
        Some(high_water_mark_callback)  // Callback function
    )?;

    // Add lots of data to trigger the high water mark
    for i in 0..1000 {
        store.insert(
            format!("key_{}", i),
            Value::String(format!("value_{}", i))
        )?;
    }

    Ok(())
}
```

## Versioned Key-Value Store

For applications that require version tracking, audit logs, or point-in-time recovery, use `VersionedKVStore`:

```rust
use bd_resilient_kv::VersionedKVStore;
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    // Create a versioned store with automatic rotation at 1MB
    let mut store = VersionedKVStore::new(
        "versioned_store.jrn",
        1024 * 1024,  // Rotate when journal reaches 1MB
        None          // Optional rotation callback
    )?;

    // All write operations return version numbers
    let v1 = store.insert("config".to_string(), Value::String("v1".to_string()))?;
    println!("Inserted at version: {}", v1);

    let v2 = store.insert("config".to_string(), Value::String("v2".to_string()))?;
    println!("Updated at version: {}", v2);

    // Read current state (O(1) from cache)
    assert_eq!(store.get("config"), Some(&Value::String("v2".to_string())));

    // Removing a key also returns a version
    let v3 = store.remove("config")?;
    if let Some(version) = v3 {
        println!("Removed at version: {}", version);
    }

    Ok(())
}
```

### Versioned Store with Rotation Callback

Monitor journal rotation events for remote backup or cleanup:

```rust
use bd_resilient_kv::{VersionedKVStore, RotationCallback};
use bd_bonjson::Value;
use std::sync::Arc;

fn upload_to_remote(path: &str, version: u64) {
    println!("Uploading archived journal {} at version {}", path, version);
    // Upload to S3, backup server, etc.
}

fn main() -> anyhow::Result<()> {
    let callback: RotationCallback = Arc::new(|archived_path, version| {
        upload_to_remote(archived_path, version);
    });

    let mut store = VersionedKVStore::new(
        "my_store.jrn",
        512 * 1024,    // 512KB rotation threshold
        Some(callback)
    )?;

    // When high water mark is reached during insert/remove,
    // the callback will be invoked with archived journal path
    for i in 0..10000 {
        store.insert(format!("key_{}", i), Value::Integer(i as i64))?;
        // Automatic rotation happens when journal exceeds 512KB
    }

    // Manual rotation is also supported
    store.rotate()?;

    Ok(())
}
```

### Key Features of VersionedKVStore

- **Version Tracking**: Every `insert()` and `remove()` returns a monotonically increasing version number
- **Timestamp Preservation**: Write timestamps are internally tracked and preserved during journal rotation for recovery purposes
- **Automatic Rotation**: When the journal exceeds the high water mark, it automatically:
  - Creates a new journal with the current state as versioned entries (compaction)
  - Preserves original timestamps from the initial writes
  - Archives the old journal with `.v{version}.zz` suffix
  - Compresses the archived journal using zlib (RFC 1950, level 3)
  - Invokes the rotation callback (if provided) for upload/cleanup
- **Automatic Compression**: Archived journals are automatically compressed to save disk space
  - Active journals remain uncompressed for write performance
  - Typically achieves >50% size reduction for text-based data
  - Transparent decompression during recovery operations
- **O(1) Reads**: In-memory cache provides constant-time access to current state
- **Persistent**: Uses memory-mapped journals for crash-resilient storage

See [VERSIONED_FORMAT.md](./VERSIONED_FORMAT.md) for detailed format documentation and recovery scenarios.

## API Reference

### KVStore (Standard Key-Value Store)

The main interface for the key-value store.

#### Constructor

```rust
pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
) -> anyhow::Result<Self>
```

- `base_path`: Base path for journal files (`.jrna` and `.jrnb` extensions added automatically)
- `buffer_size`: Size in bytes for each journal buffer
- `high_water_mark_ratio`: Optional ratio (0.0 to 1.0) for triggering compression (default: 0.8)
- `callback`: Optional callback function called when high water mark is exceeded

#### Core Methods

```rust
// Read operations (O(1) from cache)
pub fn get(&self, key: &str) -> Option<Value>
pub fn contains_key(&self, key: &str) -> bool
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn as_hashmap(&self) -> &HashMap<String, Value>

// Write operations
pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<Option<Value>>
pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<Value>>
pub fn clear(&mut self) -> anyhow::Result<()>
```

### VersionedKVStore (Version-Tracked Key-Value Store)

A higher-level store that tracks versions for every write operation and supports point-in-time recovery.

#### Constructor

```rust
pub fn new<P: AsRef<Path>>(
    journal_path: P,
    high_water_mark: usize,
    rotation_callback: Option<RotationCallback>
) -> anyhow::Result<Self>
```

- `journal_path`: Path to the journal file (e.g., "my_store.jrn")
- `high_water_mark`: Size threshold for automatic rotation (in bytes)
- `rotation_callback`: Optional callback invoked when journal is rotated
  - Signature: `Arc<dyn Fn(&str, u64) + Send + Sync>`
  - Parameters: `(archived_journal_path, version_at_rotation)`

#### Core Methods

```rust
// Read operations (O(1) from cache)
pub fn get(&self, key: &str) -> Option<&Value>
pub fn contains_key(&self, key: &str) -> bool
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn as_hashmap(&self) -> HashMap<String, &Value>

// Write operations (return version numbers)
pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<u64>
pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<u64>>

// Manual rotation
pub fn rotate(&mut self) -> anyhow::Result<()>

// Version information
pub fn current_version(&self) -> u64
```

**Internal Timestamp Tracking**: The store internally tracks timestamps for all writes and preserves them during journal rotation. These timestamps are used for recovery and point-in-time operations but are not exposed in the primary API. For advanced use cases requiring timestamp access, the `get_with_timestamp()` method is available.

#### Type Aliases

```rust
pub type RotationCallback = Arc<dyn Fn(&str, u64) + Send + Sync>;
```

## Architecture

### Storage Models

The library provides two storage architectures:

#### 1. Double-Buffered Journaling (KVStore)

The standard store uses a double-buffered approach with two journal files:

1. **Active Journal**: Receives new writes
2. **Inactive Journal**: Standby for compression
3. **Automatic Switching**: When the active journal reaches its high water mark, the system:
   - Compresses the current state into the inactive journal
   - Switches the inactive journal to become the new active journal
   - Resets the old active journal for future use

#### 2. Versioned Single-Journal (VersionedKVStore)

The versioned store uses a different architecture optimized for version tracking:

1. **Single Active Journal**: All writes go to one journal file
2. **Version Tracking**: Every entry includes a monotonically increasing version number
3. **Automatic Rotation**: When the journal reaches the high water mark:
   - Current state is serialized as versioned entries into a new journal
   - Old journal is archived with `.v{version}` suffix (e.g., `store.jrn.v123`)
   - Optional callback is invoked for remote upload/cleanup
4. **Point-in-Time Recovery**: Journal can be replayed up to any previous version

**Rotation Strategy**:
```
Before rotation:
  my_store.jrn (1MB, versions 1-1000)

After rotation:
  my_store.jrn (compacted, starts at version 1001)
  my_store.jrn.v1000.zz (archived, compressed, readonly)
```

**Compression**:
- Archived journals are automatically compressed using zlib (RFC 1950, level 3)
- Active journals remain uncompressed for optimal write performance
- Decompression is handled transparently during recovery
- File extension `.zz` indicates compressed archives

### Memory-Mapped I/O

- Uses `memmap2` for efficient file operations
- Changes are automatically synced to disk
- Crash recovery reads from the most recent valid journal

### Caching Strategy

Both `KVStore` and `VersionedKVStore` use the same caching approach:

- Maintains an in-memory `HashMap` cache of all key-value pairs
- Cache is always kept in sync with the persistent state
- Provides O(1) read performance
- Write operations update both cache and journal

**VersionedKVStore Additions**:
- Maintains current version counter
- Can reconstruct state at any historical version by replaying journal entries

## Performance Characteristics

### KVStore (Standard)

| Operation        | Time Complexity | Notes                           |
|------------------|-----------------|---------------------------------|
| `get()`          | O(1)            | Reads from in-memory cache      |
| `insert()`       | O(1) amortized  | Journal write + cache update    |
| `remove()`       | O(1) amortized  | Journal write + cache update    |
| `contains_key()` | O(1)            | Cache lookup                    |
| `len()`          | O(1)            | Cache size                      |
| `as_hashmap()`   | O(1)            | Returns reference to cache      |
| `clear()`        | O(1)            | Efficient journal clearing      |

### VersionedKVStore (With Version Tracking)

| Operation          | Time Complexity | Notes                               |
|--------------------|-----------------|-------------------------------------|
| `get()`            | O(1)            | Reads from in-memory cache          |
| `insert()`         | O(1) amortized  | Journal write + cache + version     |
| `remove()`         | O(1) amortized  | Journal write + cache + version     |
| `contains_key()`   | O(1)            | Cache lookup                        |
| `len()`            | O(1)            | Cache size                          |
| `as_hashmap()`     | O(n)            | Creates temporary map of values     |
| `rotate()`         | O(n)            | Serializes current state to new journal |
| `current_version()`| O(1)            | Returns version counter             |

## Error Handling

All write operations return `anyhow::Result<T>` for comprehensive error handling, while read operations return values directly from the cache:

```rust
use bd_resilient_kv::KVStore;
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    let mut store = KVStore::new("my_store", 1024 * 1024, None, None)?;

    match store.insert("key".to_string(), Value::String("value".to_string())) {
        Ok(old_value) => println!("Success: {:?}", old_value),
        Err(e) => eprintln!("Error: {}", e),
    }

    Ok(())
}
```

## File Management

### KVStore Files

The library automatically manages journal files:

- **Creation**: Files are created if they don't exist
- **Recovery**: Existing files are loaded and validated on startup
- **Resizing**: Files are resized if the specified buffer size differs
- **Corruption Handling**: Corrupted files are automatically recreated
- **Cleanup**: No manual cleanup required

Example file structure:
```
my_store.jrna  # Journal A
my_store.jrnb  # Journal B
```

### VersionedKVStore Files

The versioned store manages a single journal with archived versions:

- **Active Journal**: Current journal file (e.g., `my_store.jrn`)
- **Archived Journals**: Previous versions with `.v{version}` suffix
- **Automatic Archival**: Old journals are preserved during rotation
- **Callback Integration**: Application controls upload/cleanup of archived journals

Example file structure after multiple rotations:
```
my_store.jrn            # Active journal (current, uncompressed)
my_store.jrn.v1000.zz   # Archived at version 1000 (compressed)
my_store.jrn.v2500.zz   # Archived at version 2500 (compressed)
my_store.jrn.v4000.zz   # Archived at version 4000 (compressed)
```

## Thread Safety

Both `KVStore` and `VersionedKVStore` are **not** thread-safe by design for maximum performance. For concurrent access, wrap them in appropriate synchronization primitives:

```rust
use std::sync::{Arc, Mutex};
use bd_resilient_kv::KVStore;

let store = Arc::new(Mutex::new(
    KVStore::new("shared_store", 1024 * 1024, None, None)?
));

// Use store across threads with proper locking
```

## Advanced Usage

### Archived Journal Compression

**VersionedKVStore** automatically compresses archived journals to save disk space:

```rust
use bd_resilient_kv::VersionedKVStore;
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    let mut store = VersionedKVStore::new(
        "my_store.jrn",
        512 * 1024,  // 512KB rotation threshold
        None
    )?;

    // Write data that will trigger rotation
    for i in 0..10000 {
        store.insert(format!("key_{}", i), Value::Integer(i as i64))?;
    }

    // After rotation, archived journals are automatically compressed:
    // - my_store.jrn (active, uncompressed)
    // - my_store.jrn.v10000.zz (archived, compressed with zlib)

    Ok(())
}
```

**Compression Details**:
- **Format**: zlib (RFC 1950) with compression level 3
- **Performance**: Balanced speed/compression ratio
- **Transparency**: Recovery automatically detects and decompresses archived journals
- **Naming**: `.zz` extension indicates compressed archives
- **Typical Savings**: >50% size reduction for text-based data

**Active vs Archived**:
- Active journals remain **uncompressed** for maximum write performance
- Only archived journals are compressed during rotation
- No configuration needed - compression is automatic

### Custom Buffer Sizes

Choose buffer sizes based on your use case:

```rust
// Small applications
let store = KVStore::new("small_app", 64 * 1024, None, None)?; // 64KB

// Medium applications
let store = KVStore::new("medium_app", 1024 * 1024, None, None)?; // 1MB

// Large applications
let store = KVStore::new("large_app", 16 * 1024 * 1024, None, None)?; // 16MB
```

### Monitoring and Debugging

```rust
use bd_resilient_kv::KVStore;

let store = KVStore::new("debug_store", 1024 * 1024, None, None)?;

// Check store statistics
println!("Store size: {} items", store.len());
println!("Is empty: {}", store.is_empty());

// Get all data for debugging
let all_data = store.as_hashmap();
println!("All data: {:?}", all_data);

// Or iterate over keys and values
for (key, value) in store.as_hashmap() {
    println!("{}: {:?}", key, value);
}
```

## Contributing

This library is part of the bitdrift shared-core repository. See the main repository for contribution guidelines.

## License

This source code is governed by a source available license that can be found in the LICENSE file.
