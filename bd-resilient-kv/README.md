# bd-resilient-kv

A versioned, crash-resilient key-value store library for Rust with automatic persistence, compression, and point-in-time recovery.

## Overview

`bd-resilient-kv` provides a versioned persistent key-value store that automatically saves data to disk while maintaining excellent performance through in-memory caching and smart buffering strategies. The library is designed to be crash-resilient with version tracking, ensuring data integrity and enabling point-in-time recovery even in the event of unexpected shutdowns.

## Key Features

- **🛡️ Crash Resilient**: Automatic persistence with crash recovery capabilities
- **🕐 Version Tracking**: Every write returns a timestamp for precise state tracking
- **⏪ Point-in-Time Recovery**: Recover state at any historical timestamp
- **⚡ High Performance**: O(1) reads via in-memory caching, optimized writes
- **🗜️ Automatic Compression**: Archived journals automatically compressed using zlib
- **💾 Memory Mapped I/O**: Efficient file operations using memory-mapped files
- **🔄 Automatic Rotation**: Journal rotation with archived history preservation
- **🎯 Simple API**: HashMap-like interface that's easy to use
- **📦 Protobuf-based**: Efficient serialization for versioned entries

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
bd-resilient-kv = { path = "path/to/bd-resilient-kv" }
bd-proto = { path = "path/to/bd-proto" }
```

### Basic Usage

```rust
use bd_resilient_kv::{VersionedKVStore, PersistentStoreConfig, Scope};
use bd_proto::protos::state::payload::StateValue;
use bd_proto::protos::state::payload::state_value::Value_type;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a new versioned store with 1MB buffer size
    let config = PersistentStoreConfig {
        buffer_size: 1024 * 1024,
        high_water_mark_ratio: 0.8,
    };
    
    let mut store = VersionedKVStore::new(
        "my_store",
        config,
        None, // No retention registry
    ).await?;

    // Insert some values - each returns a timestamp
    let ts1 = store.insert(
        Scope::Permanent,
        "name",
        StateValue {
            value_type: Some(Value_type::StringValue("Alice".to_string())),
            ..Default::default()
        }
    ).await?;
    
    println!("Inserted at timestamp: {}", ts1);

    // Read values (synchronous)
    if let Some((value, timestamp)) = store.get(Scope::Permanent, "name") {
        println!("Name: {:?} (written at {})", value, timestamp);
    }

    // Check existence
    if store.contains(Scope::Permanent, "name") {
        println!("Name is stored");
    }

    // Remove a key (returns timestamp)
    let ts2 = store.remove(Scope::Permanent, "name").await?;
    println!("Removed at timestamp: {}", ts2);

    Ok(())
}
```

### Working with Scopes

The versioned store supports multiple scopes for different data lifecycles:

```rust
use bd_resilient_kv::{VersionedKVStore, Scope};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut store = VersionedKVStore::new("scoped_store", config, None).await?;

    // Permanent data - survives restarts
    store.insert(Scope::Permanent, "user_id", value).await?;

    // Ephemeral data - cleared on restart
    store.insert(Scope::Ephemeral, "session_token", value).await?;

    // Get all data for a scope
    let permanent_data = store.get_all(Scope::Permanent);
    for (key, (value, timestamp)) in permanent_data {
        println!("{}: {:?} at {}", key, value, timestamp);
    }

    Ok(())
}
```

### Persistence and Point-in-Time Recovery

The store automatically rotates journals and archives old versions:

```rust
use bd_resilient_kv::{VersionedKVStore, VersionedRecovery};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create store and add data
    let mut store = VersionedKVStore::new("history_store", config, None).await?;
    
    let ts1 = store.insert(Scope::Permanent, "key1", value1).await?;
    let ts2 = store.insert(Scope::Permanent, "key2", value2).await?;
    let ts3 = store.remove(Scope::Permanent, "key1").await?;
    
    // Recover state at a specific point in time
    let recovery = VersionedRecovery::new(/* journal bytes */)?;
    let state_at_ts2 = recovery.recover_at_timestamp(ts2)?;
    
    // state_at_ts2 will have key1 and key2, but not the removal

    Ok(())
}
```

## API Reference

### VersionedKVStore

The main interface for the versioned key-value store.

#### Constructor

```rust
pub async fn new<P: AsRef<Path>>(
    base_path: P,
    config: PersistentStoreConfig,
    retention_registry: Option<Arc<RetentionRegistry>>,
) -> anyhow::Result<Self>
```

- `base_path`: Base path for journal files
- `config`: Store configuration (buffer size, high water mark ratio)
- `retention_registry`: Optional registry for retention policies

#### Core Methods

```rust
// Read operations (synchronous, O(1) from cache)
pub fn get(&self, scope: Scope, key: &str) -> Option<(&StateValue, OffsetDateTime)>
pub fn contains(&self, scope: Scope, key: &str) -> bool
pub fn get_all(&self, scope: Scope) -> &AHashMap<String, (StateValue, OffsetDateTime)>
pub fn snapshot(&self) -> ScopedMaps

// Write operations (async, returns timestamp)
pub async fn insert(
    &mut self,
    scope: Scope,
    key: &str,
    value: StateValue
) -> anyhow::Result<OffsetDateTime>

pub async fn remove(
    &mut self,
    scope: Scope,
    key: &str
) -> anyhow::Result<OffsetDateTime>

// Journal management
pub async fn rotate_journal(&mut self) -> anyhow::Result<()>
```

## Architecture

### Versioned Journal with Rotation

The store uses a single journal with automatic rotation and archival:

1. **Active Journal**: Receives new writes with timestamps
2. **Rotation**: When buffer exceeds high water mark:
   - Current state is compacted into a new journal
   - Old journal is archived with `.t{timestamp}.zz` suffix
   - Archived journal is automatically compressed using zlib
3. **Point-in-Time Recovery**: Historical state can be reconstructed by replaying journals up to any timestamp

### Memory-Mapped I/O

- Uses `memmap2` for efficient file operations
- Changes are automatically synced to disk
- Crash recovery reads from journals in order
- Archived journals are decompressed transparently during recovery

### Caching Strategy

- Maintains in-memory maps per scope with timestamped values
- Cache is always kept in sync with the persistent state
- Provides O(1) read performance
- Write operations update both cache and journal asynchronously

### Async Architecture

- Write operations are async to enable efficient background compression
- Compression of archived journals uses streaming I/O
- Read operations remain synchronous for maximum performance
- Requires a Tokio runtime for async operations

## Performance Characteristics

| Operation        | Time Complexity | Notes                              |
|------------------|-----------------|-------------------------------------|
| `get()`          | O(1)            | Reads from in-memory cache         |
| `insert()`       | O(1) amortized  | Async journal write + cache update |
| `remove()`       | O(1) amortized  | Async journal write + cache update |
| `contains()`     | O(1)            | Cache lookup                       |
| `get_all()`      | O(1)            | Returns reference to scope map     |
| `snapshot()`     | O(n)            | Creates deep copy of all scopes    |
| `rotate_journal()`| O(n)           | Async compaction and compression   |

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

The library automatically manages journal files:

- **Creation**: Journal files are created on first write
- **Rotation**: Old journals are automatically archived with `.t{timestamp}.zz` suffix
- **Compression**: Archived journals are automatically compressed using zlib (RFC 1950, level 5)
- **Recovery**: All journals (active and archived) are loaded and validated on startup
- **Cleanup**: Archived journals can be deleted based on retention policies
- **Decompression**: Archived journals are transparently decompressed during recovery

Example file structure:
```
my_store.journal              # Active journal
my_store.journal.t1234567890  # Archived journal (uncompressed, during rotation)
my_store.journal.t1234567890.zz  # Archived journal (compressed)
```

## Thread Safety

`VersionedKVStore` is **not** thread-safe by design for maximum performance. For concurrent access, wrap it in appropriate synchronization primitives:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use bd_resilient_kv::VersionedKVStore;

let store = Arc::new(RwLock::new(
    VersionedKVStore::new("shared_store", config, None).await?
));

// Use store across tasks with async locking
```

## Advanced Usage

### Custom Buffer Sizes

Choose buffer sizes based on your use case:

```rust
use bd_resilient_kv::PersistentStoreConfig;

// Small applications
let config = PersistentStoreConfig {
    buffer_size: 64 * 1024,  // 64KB
    high_water_mark_ratio: 0.8,
};

// Medium applications  
let config = PersistentStoreConfig {
    buffer_size: 1024 * 1024,  // 1MB
    high_water_mark_ratio: 0.8,
};

// Large applications
let config = PersistentStoreConfig {
    buffer_size: 16 * 1024 * 1024,  // 16MB
    high_water_mark_ratio: 0.8,
};
```

### Monitoring and Debugging

```rust
use bd_resilient_kv::{VersionedKVStore, Scope};

let store = VersionedKVStore::new("debug_store", config, None).await?;

// Get all data for a scope
let permanent_data = store.get_all(Scope::Permanent);
println!("Permanent data count: {}", permanent_data.len());

// Get complete snapshot
let snapshot = store.snapshot();
for (scope_name, scope_map) in snapshot {
    println!("Scope {}: {} entries", scope_name, scope_map.len());
    for (key, (value, timestamp)) in scope_map {
        println!("  {}: {:?} at {}", key, value, timestamp);
    }
}
```

## Contributing

This library is part of the bitdrift shared-core repository. See the main repository for contribution guidelines.

## License

This source code is governed by a source available license that can be found in the LICENSE file.
