// Example usage of DoubleBufferedKVJournal with MemMappedKVJournal
// 
// This example demonstrates how to use the DoubleBufferedKVJournal with MemMappedKVJournal 
// instances which provides persistent storage with automatic journal switching when one 
// journal reaches its high water mark.

use bd_resilient_kv::{DoubleBufferedKVJournal, MemMappedKVJournal, KVJournal};
use bd_bonjson::Value;
use tempfile::TempDir;

fn main() -> anyhow::Result<()> {
    // Create a temporary directory for demonstration
    let temp_dir = TempDir::new()?;
    let file_a = temp_dir.path().join("journal_a.db");
    let file_b = temp_dir.path().join("journal_b.db");
    
    println!("Creating double-buffered memory-mapped KV journal:");
    println!("  File A: {:?}", file_a);
    println!("  File B: {:?}", file_b);
    
    // Create two memory-mapped journals with 4KB files each
    let journal_a = MemMappedKVJournal::new(&file_a, 4096, Some(0.7), None)?;
    let journal_b = MemMappedKVJournal::new(&file_b, 4096, Some(0.7), None)?;
    
    // Create a double-buffered journal
    let mut db_kv = DoubleBufferedKVJournal::new(journal_a, journal_b);
    
    println!("Starting with journal A: {}", db_kv.is_active_journal_a());
    println!("High water mark: {} bytes", db_kv.high_water_mark());
    
    // Add some initial data
    for i in 0..5 {
        let key = format!("user_{}", i);
        let value = format!("User data for person {}", i);
        db_kv.set(&key, &Value::String(value))?;
    }
    
    println!("Added 5 entries");
    println!("Active journal A: {}", db_kv.is_active_journal_a());
    println!("Buffer usage: {:.1}%", db_kv.buffer_usage_ratio() * 100.0);
    
    // Add more data to potentially trigger journal switching
    for i in 5..25 {
        let key = format!("data_{}", i);
        let value = format!("Some longer data content for entry {} that will help demonstrate journal switching", i);
        db_kv.set(&key, &Value::String(value))?;
    }
    
    // Check final state
    let final_map = db_kv.as_hashmap()?;
    println!("Final state:");
    println!("  Total entries: {}", final_map.len());
    println!("  Active journal A: {}", db_kv.is_active_journal_a());
    println!("  Buffer usage: {:.1}%", db_kv.buffer_usage_ratio() * 100.0);
    
    // Demonstrate that all data is still accessible
    println!("  Sample data:");
    if let Some(value) = final_map.get("user_2") {
        println!("    user_2: {:?}", value);
    }
    if let Some(value) = final_map.get("data_20") {
        println!("    data_20: {:?}", value);
    }
    
    // Get initialization time
    let init_time = db_kv.get_init_time()?;
    println!("Initialized at: {} ns since UNIX epoch", init_time);
    
    // Demonstrate persistence by loading from existing files
    println!("\nTesting persistence...");
    
    // Drop the current instance to ensure files are written
    drop(db_kv);
    
    // Create new instances from the existing files
    let restored_journal_a = MemMappedKVJournal::from_file(&file_a, Some(0.7), None)?;
    let restored_journal_b = MemMappedKVJournal::from_file(&file_b, Some(0.7), None)?;
    let mut restored_kv = DoubleBufferedKVJournal::new(restored_journal_a, restored_journal_b);
    
    // Verify data persists
    let restored_map = restored_kv.as_hashmap()?;
    println!("Restored {} entries from disk", restored_map.len());
    
    // Verify specific data
    if let Some(value) = restored_map.get("user_2") {
        println!("  Restored user_2: {:?}", value);
    }
    
    // Add new data to the restored journal
    restored_kv.set("new_after_restore", &Value::String("This was added after restore".to_string()))?;
    
    let final_restored_map = restored_kv.as_hashmap()?;
    println!("Final restored map has {} entries", final_restored_map.len());
    
    println!("Memory-mapped double-buffered journal example completed successfully!");
    
    Ok(())
}
