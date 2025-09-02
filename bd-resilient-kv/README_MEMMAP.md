# KVJournal with Memory-Mapped Files

The `KVJournal` now supports memory-mapped files through the use of `&mut [u8]` instead of `Vec<u8>`. This allows for efficient, crash-resilient key-value journaling that can work with both in-memory buffers and memory-mapped files.

## Key Changes

1. **Buffer Type**: Changed from `Vec<u8>` to `&mut [u8]` to support non-growable byte arrays
2. **Lifetime Parameter**: Added lifetime parameter `'a` to `KVJournal<'a>`
3. **Memory Mapping Support**: Can now work with `memmap2::MmapMut` for persistent storage

## Usage with Memory-Mapped Files

```rust
use memmap2::MmapMut;
use std::fs::OpenOptions;
use bd_resilient_kv::KVJournal;
use bd_bonjson::Value;

// Open/create a file for the journal
let file = OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open("journal.dat")?;

// Set the file size (e.g., 1MB)
file.set_len(1024 * 1024)?;

// Memory-map the file
let mut mmap = unsafe { MmapMut::map_mut(&file)? };

// Create the journal using the memory-mapped buffer
let mut kv = KVJournal::new(&mut mmap[..])?;

// Use the journal normally - all changes are automatically persisted
kv.set("user_name", &Value::String("Alice".to_string()))?;
kv.set("user_id", &Value::Signed(12345))?;

// Retrieve values
let map = kv.as_hashmap()?;
println!("User: {:?}", map.get("user_name"));
```

## Benefits

1. **Crash Resilience**: Data is immediately written to persistent storage
2. **Performance**: No need to explicitly sync data to disk
3. **Memory Efficiency**: Only maps the needed portion of the file into memory
4. **Compatibility**: Works with both memory-mapped files and regular byte arrays
5. **Zero-Copy**: Direct manipulation of the mapped memory region

## API Compatibility

The API remains largely the same, with the main difference being:
- Constructor takes `&mut [u8]` instead of `Vec<u8>`
- Struct has lifetime parameter: `KVJournal<'a>` instead of `KVJournal`

All existing functionality (set, get, delete, etc.) works identically.
