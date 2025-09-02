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

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
bd-resilient-kv = { path = "path/to/bd-resilient-kv" }
bd-bonjson = { path = "path/to/bd-bonjson" }
```

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
    if let Some(name) = store.get("name")? {
        println!("Name: {}", name);
    }
    
    // Check existence
    if store.contains_key("age")? {
        println!("Age is stored");
    }
    
    // Remove a key
    let old_value = store.remove("active")?;
    println!("Removed: {:?}", old_value);
    
    // Get all keys
    let keys = store.keys()?;
    println!("Keys: {:?}", keys);
    
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
        let mut store = KVStore::new("persistent_store", 1024 * 1024, None, None)?;
        let value = store.get("persistent_key")?;
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

## API Reference

### KVStore

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
pub fn get(&mut self, key: &str) -> anyhow::Result<Option<Value>>
pub fn contains_key(&mut self, key: &str) -> anyhow::Result<bool>
pub fn len(&mut self) -> anyhow::Result<usize>
pub fn is_empty(&mut self) -> anyhow::Result<bool>
pub fn keys(&mut self) -> anyhow::Result<Vec<String>>

// Write operations
pub fn insert(&mut self, key: String, value: Value) -> anyhow::Result<Option<Value>>
pub fn remove(&mut self, key: &str) -> anyhow::Result<Option<Value>>
pub fn clear(&mut self) -> anyhow::Result<()>

// Utility
pub fn values(&mut self) -> anyhow::Result<Vec<Value>>
pub fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>>
```

## Architecture

### Double-Buffered Journaling

The store uses a double-buffered approach with two journal files:

1. **Active Journal**: Receives new writes
2. **Inactive Journal**: Standby for compression
3. **Automatic Switching**: When the active journal reaches its high water mark, the system:
   - Compresses the current state into the inactive journal
   - Switches the inactive journal to become the new active journal
   - Resets the old active journal for future use

### Memory-Mapped I/O

- Uses `memmap2` for efficient file operations
- Changes are automatically synced to disk
- Crash recovery reads from the most recent valid journal

### Caching Strategy

- Maintains an in-memory `HashMap` cache of all key-value pairs
- Cache is always kept in sync with the persistent state
- Provides O(1) read performance
- Write operations update both cache and journal

## Performance Characteristics

| Operation        | Time Complexity | Notes                        |
|------------------|-----------------|------------------------------|
| `get()`          | O(1)            | Reads from in-memory cache   |
| `insert()`       | O(1) amortized  | Journal write + cache update |
| `remove()`       | O(1) amortized  | Journal write + cache update |
| `contains_key()` | O(1)            | Cache lookup                 |
| `len()`          | O(1)            | Cache size                   |
| `keys()`         | O(n)            | Clones keys from cache       |
| `clear()`        | O(1)            | Efficient journal clearing   |

## Error Handling

All operations return `anyhow::Result<T>` for comprehensive error handling:

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

## Thread Safety

`KVStore` is **not** thread-safe by design for maximum performance. For concurrent access, wrap it in appropriate synchronization primitives:

```rust
use std::sync::{Arc, Mutex};
use bd_resilient_kv::KVStore;

let store = Arc::new(Mutex::new(
    KVStore::new("shared_store", 1024 * 1024, None, None)?
));

// Use store across threads with proper locking
```

## Advanced Usage

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

let mut store = KVStore::new("debug_store", 1024 * 1024, None, None)?;

// Check store statistics
println!("Store size: {} items", store.len()?);
println!("Is empty: {}", store.is_empty()?);

// Get all data for debugging
let all_data = store.as_hashmap()?;
println!("All data: {:?}", all_data);
```

## Contributing

This library is part of the bitdrift shared-core repository. See the main repository for contribution guidelines.

## License

This source code is governed by a source available license that can be found in the LICENSE file.
