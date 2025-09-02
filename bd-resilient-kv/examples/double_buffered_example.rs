// Example usage of DoubleBufferedKv
// 
// This example demonstrates how to use the DoubleBufferedKv type which automatically
// switches between two journals when one reaches its high water mark.

use bd_resilient_kv::{DoubleBufferedKVJournal, InMemoryKVJournal, KVJournal};
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    // Create two in-memory journals with 4KB buffers each
    let buffer_a = Box::leak(vec![0u8; 4096].into_boxed_slice());
    let buffer_b = Box::leak(vec![0u8; 4096].into_boxed_slice());
    let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.8), None)?;
    let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.8), None)?;
    
    // Create a double-buffered KV journal
    let mut db_kv = DoubleBufferedKVJournal::new(journal_a, journal_b);
    
    println!("Created DoubleBufferedKv with 4KB journals");
    println!("Starting with journal A: {}", db_kv.is_active_journal_a());
    
    // Add some data
    for i in 0..10 {
        let key = format!("user_{}", i);
        let value = format!("User data for person {}", i);
        db_kv.set(&key, &Value::String(value))?;
    }
    
    // Check the current state
    let map = db_kv.as_hashmap()?;
    println!("Added {} entries", map.len());
    println!("Current journal A active: {}", db_kv.is_active_journal_a());
    println!("Current buffer usage: {:.1}%", db_kv.buffer_usage_ratio() * 100.0);
    
    // Add more data to potentially trigger journal switching
    for i in 10..30 {
        let key = format!("data_{}", i);
        let value = format!("Some longer data content for entry {} that will help fill the buffer", i);
        db_kv.set(&key, &Value::String(value))?;
    }
    
    // Check state after adding more data
    let final_map = db_kv.as_hashmap()?;
    println!("Final state:");
    println!("  Total entries: {}", final_map.len());
    println!("  Active journal A: {}", db_kv.is_active_journal_a());
    println!("  Buffer usage: {:.1}%", db_kv.buffer_usage_ratio() * 100.0);
    
    // Demonstrate that all data is still accessible
    println!("  Sample data:");
    if let Some(value) = final_map.get("user_5") {
        println!("    user_5: {:?}", value);
    }
    if let Some(value) = final_map.get("data_25") {
        println!("    data_25: {:?}", value);
    }
    
    // Get initialization time
    let init_time = db_kv.get_init_time()?;
    println!("  Initialized at: {} ns since UNIX epoch", init_time);
    
    Ok(())
}
