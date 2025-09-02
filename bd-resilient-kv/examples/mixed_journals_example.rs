// Example demonstrating the flexibility of the new DoubleBufferedKVJournal
// This example shows using two different journal types together

use bd_resilient_kv::{DoubleBufferedKVJournal, InMemoryKVJournal, MemMappedKVJournal, KVJournal};
use bd_bonjson::Value;
use std::fs;

fn main() -> anyhow::Result<()> {
    // Create a temporary file for the memory-mapped journal
    let temp_file = "/tmp/test_memmapped_journal.dat";
    let _ = fs::remove_file(temp_file); // Clean up if it exists
    
    // Create an in-memory journal
    let buffer_a = Box::leak(vec![0u8; 2048].into_boxed_slice());
    let in_memory_journal = InMemoryKVJournal::new(buffer_a, Some(0.7), None)?;
    
    // Create a memory-mapped journal
    let memmapped_journal = MemMappedKVJournal::new(temp_file, 2048, Some(0.7), None)?;
    
    // Create a double-buffered journal using different journal types
    let mut mixed_db_kv = DoubleBufferedKVJournal::new(in_memory_journal, memmapped_journal);
    
    println!("Created mixed DoubleBufferedKVJournal:");
    println!("  Journal A (in-memory): active = {}", mixed_db_kv.is_active_journal_a());
    
    // Add some data
    for i in 0..5 {
        let key = format!("mixed_key_{}", i);
        let value = format!("Value in mixed journal {}", i);
        mixed_db_kv.set(&key, &Value::String(value))?;
    }
    
    // Check state
    let map = mixed_db_kv.as_hashmap()?;
    println!("Data added to mixed journal:");
    for (key, value) in &map {
        println!("  {}: {:?}", key, value);
    }
    
    println!("High water mark: {} bytes", mixed_db_kv.high_water_mark());
    println!("Buffer usage ratio: {:.1}%", mixed_db_kv.buffer_usage_ratio() * 100.0);
    
    // Clean up
    let _ = fs::remove_file(temp_file);
    
    Ok(())
}
