// Example: Optimized Double Buffered Memory-Mapped KV Store
// 
// This example demonstrates the optimized DoubleBufferedMemMappedKv which keeps both
// MemMappedResilientKv instances loaded in memory to avoid repeated disk I/O.

use bd_resilient_kv::{DoubleBufferedMemMappedKv, ResilientKv};
use bd_bonjson::Value;
use tempfile::TempDir;

fn main() -> anyhow::Result<()> {
    println!("=== Optimized Double Buffered Memory-Mapped KV Store Demo ===\n");

    // Create temporary directory for the demo
    let temp_dir = TempDir::new()?;
    let file_a = temp_dir.path().join("kv_a.data");
    let file_b = temp_dir.path().join("kv_b.data");

    println!("Creating double buffered memory-mapped KV store...");
    let mut kv = DoubleBufferedMemMappedKv::new(
        &file_a,
        &file_b,
        1024,       // 1KB file size (very small for demo)
        Some(0.4),  // Switch at 40% full (low threshold)
        None
    )?;

    println!("Starting with file: {:?}", kv.active_file_path());
    println!("Using file A: {}\n", kv.is_active_file_a());

    // Add data gradually to demonstrate switching behavior
    println!("Adding data to trigger buffer switching...");
    for i in 0..25 {
        let key = format!("key_{:02}", i);
        let value = format!("This is a very long value for key {} that should help fill up the small buffer quickly to trigger the switching mechanism", i);
        
        let before_switch = kv.is_active_file_a();
        kv.set(&key, &Value::String(value.clone()))?;
        let after_switch = kv.is_active_file_a();
        
        let usage = kv.buffer_usage_ratio();
        let hwm_triggered = kv.is_high_water_mark_triggered();
        
        print!("Entry {}: Usage {:.1}%, HWM: {}, ", i + 1, usage * 100.0, hwm_triggered);
        
        if before_switch != after_switch {
            println!("*** BUFFER SWITCHED: {} -> {} ***", 
                     if before_switch { "A" } else { "B" },
                     if after_switch { "A" } else { "B" });
            println!("  New active file: {:?}", kv.active_file_path());
        } else {
            println!("Active: {}", if after_switch { "A" } else { "B" });
        }
    }

    // Verify all data is preserved
    println!("\nVerifying data integrity after switching:");
    let map = kv.as_hashmap()?;
    println!("Total entries in store: {}", map.len());
    
    for i in 0..std::cmp::min(25, map.len()) {
        let key = format!("key_{:02}", i);
        if let Some(value) = map.get(&key) {
            println!("✓ {}: {}", key, 
                     if let Value::String(s) = value { 
                         if s.len() > 50 { 
                             format!("{}...", &s[..50]) 
                         } else { 
                             s.clone() 
                         }
                     } else { 
                         format!("{:?}", value) 
                     });
        } else {
            println!("✗ Missing key: {}", key);
        }
    }

    // Demonstrate persistence
    println!("\nForcing sync to disk...");
    kv.sync()?;
    println!("Active file: {:?}", kv.active_file_path());
    println!("Active file exists: {}", kv.active_file_path().exists());
    println!("Active file size: {} bytes", std::fs::metadata(kv.active_file_path())?.len());

    // Show the key optimization - both instances can be kept in memory
    println!("\n=== Optimization Details ===");
    println!("✓ Both MemMappedResilientKv instances can be kept loaded");
    println!("✓ No disk I/O required during buffer switches");
    println!("✓ Data copying happens in memory using as_hashmap()");
    println!("✓ Only inactive instance is recreated after switching");
    println!("✓ Persistent storage through memory-mapped files");

    Ok(())
}
