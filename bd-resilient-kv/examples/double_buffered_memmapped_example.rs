// Example usage of DoubleBufferedMemMappedKv
// 
// This example demonstrates how to use the DoubleBufferedMemMappedKv type which provides
// persistent storage with automatic file switching when one file reaches its high water mark.

use bd_resilient_kv::{DoubleBufferedMemMappedKVJournal, KVJournal};
use bd_bonjson::Value;
use tempfile::TempDir;

fn main() -> anyhow::Result<()> {
    // Create a temporary directory for demonstration
    let temp_dir = TempDir::new()?;
    let file_a = temp_dir.path().join("kv_store_a.db");
    let file_b = temp_dir.path().join("kv_store_b.db");
    
    println!("Creating DoubleBufferedMemMappedKv with persistent files:");
    println!("  File A: {:?}", file_a);
    println!("  File B: {:?}", file_b);
    
    // Create a double-buffered memory-mapped KV store with 8KB files
    // High water mark is set to 70%
    let mut db_kv = DoubleBufferedMemMappedKVJournal::new(
        &file_a, 
        &file_b, 
        8192,     // 8KB file size
        Some(0.7), // 70% high water mark
        None
    )?;
    
    println!("Starting with file A: {}", db_kv.is_active_file_a());
    println!("File size: {} bytes", db_kv.file_size());
    println!("High water mark: {} bytes", db_kv.high_water_mark());
    
    // Add user data
    println!("\nAdding user data...");
    for i in 0..5 {
        let key = format!("user:{}", i);
        let value = format!(
            "{{\"id\":{},\"name\":\"User {}\",\"email\":\"user{}@example.com\",\"preferences\":{{\"theme\":\"dark\",\"language\":\"en\",\"notifications\":true}}}}",
            i, i, i
        );
        
        // Convert JSON string to BONJSON Value
        let bonjson_value = Value::String(value);
        db_kv.set(&key, &bonjson_value)?;
    }
    
    // Check current state
    let current_usage = db_kv.buffer_usage_ratio();
    println!("Current buffer usage: {:.1}%", current_usage * 100.0);
    println!("Active file: {}", if db_kv.is_active_file_a() { "A" } else { "B" });
    
    // Force sync to ensure data is written to disk
    db_kv.sync()?;
    println!("Data synced to disk");
    
    // Add more data to potentially trigger file switching
    println!("\nAdding more data to potentially trigger file switching...");
    for i in 5..25 {
        let key = format!("session:{}", i);
        let value = format!("Session data for user session {} with additional metadata and timestamps", i);
        
        let was_file_a = db_kv.is_active_file_a();
        db_kv.set(&key, &Value::String(value))?;
        let is_file_a = db_kv.is_active_file_a();
        
        if was_file_a != is_file_a {
            println!("File switched! {} -> {}", 
                     if was_file_a { "A" } else { "B" },
                     if is_file_a { "A" } else { "B" });
            break;
        }
    }
    
    // Get final state
    let final_map = db_kv.as_hashmap()?;
    let final_usage = db_kv.buffer_usage_ratio();
    
    println!("\nFinal state:");
    println!("  Total entries: {}", final_map.len());
    println!("  Active file: {}", if db_kv.is_active_file_a() { "A" } else { "B" });
    println!("  Buffer usage: {:.1}%", final_usage * 100.0);
    println!("  Active file path: {:?}", db_kv.active_file_path());
    
    // Show some sample data
    println!("\nSample data:");
    if let Some(user_data) = final_map.get("user:2") {
        println!("  user:2: {:?}", user_data);
    }
    if let Some(session_data) = final_map.get("session:10") {
        println!("  session:10: {:?}", session_data);
    }
    
    // Get initialization time
    let init_time = db_kv.get_init_time()?;
    println!("  Initialized at: {} ns since UNIX epoch", init_time);
    
    // Demonstrate persistence by recreating from file
    println!("\nDemonstrating persistence...");
    let active_file = db_kv.active_file_path().to_path_buf();
    let inactive_file = db_kv.inactive_file_path().to_path_buf();
    drop(db_kv); // Close the current instance
    
    // Recreate from the active file
    let mut restored_kv = DoubleBufferedMemMappedKVJournal::from_file(
        active_file,
        inactive_file,
        8192,
        Some(0.7),
        None
    )?;
    
    let restored_map = restored_kv.as_hashmap()?;
    println!("Restored {} entries from persistent file", restored_map.len());
    
    // Verify some data is still there
    if restored_map.contains_key("user:1") {
        println!("  Verified user:1 data is persistent: present");
    }
    
    println!("\nDouble-buffered memory-mapped KV demonstration complete!");
    
    Ok(())
}
