// Example: Double Buffered Memory-Mapped KV Journal with reinit_from optimization
// 
// This example demonstrates using DoubleBufferedKVJournal with MemMappedKVJournal instances,
// showcasing the efficient reinit_from method for journal switching.

use bd_resilient_kv::{DoubleBufferedKVJournal, MemMappedKVJournal, KVJournal};
use bd_bonjson::Value;
use tempfile::TempDir;

fn main() -> anyhow::Result<()> {
    println!("=== Double Buffered Memory-Mapped KV Journal with reinit_from Demo ===\n");

    // Create temporary directory for the demo
    let temp_dir = TempDir::new()?;
    let file_a = temp_dir.path().join("kv_a.data");
    let file_b = temp_dir.path().join("kv_b.data");

    println!("Creating memory-mapped journals:");
    println!("  File A: {:?}", file_a);
    println!("  File B: {:?}", file_b);

    // Create two smaller journals to demonstrate switching
    let journal_a = MemMappedKVJournal::new(&file_a, 1024, Some(0.6), None)?;
    let journal_b = MemMappedKVJournal::new(&file_b, 1024, Some(0.6), None)?;
    
    let mut db_kv = DoubleBufferedKVJournal::new(journal_a, journal_b);

    println!("High water mark: {} bytes (60% of 1024)", db_kv.high_water_mark());
    println!("Starting with journal A: {}\n", db_kv.is_active_journal_a());

    // Add data to fill up the journal and trigger switching
    println!("Adding data to demonstrate journal switching...");
    let mut entries_added = 0;
    let initial_journal = db_kv.is_active_journal_a();

    for i in 0..50 {
        let key = format!("demo_key_{:03}", i);
        let value = format!("Demo value with some content to fill up the journal {}", i);
        
        match db_kv.set(&key, &Value::String(value)) {
            Ok(()) => {
                entries_added += 1;
                let current_journal = db_kv.is_active_journal_a();
                
                // Check if we switched journals
                if current_journal != initial_journal {
                    println!("  ✓ Journal switched after {} entries!", entries_added);
                    println!("  ✓ Now using journal {}", if current_journal { "A" } else { "B" });
                    break;
                }
                
                if entries_added % 10 == 0 {
                    println!("  Added {} entries, usage: {:.1}%", 
                             entries_added, db_kv.buffer_usage_ratio() * 100.0);
                }
            }
            Err(e) => {
                println!("  Error adding entry {}: {}", i, e);
                break;
            }
        }
    }

    // Show final state
    let final_map = db_kv.as_hashmap()?;
    println!("\nFinal state:");
    println!("  Total entries: {}", final_map.len());
    println!("  Active journal A: {}", db_kv.is_active_journal_a());
    println!("  Buffer usage: {:.1}%", db_kv.buffer_usage_ratio() * 100.0);

    // Demonstrate reinit_from by creating another journal and copying data
    println!("\nDemonstrating reinit_from...");
    let temp_file_c = temp_dir.path().join("kv_c.data");
    let temp_file_d = temp_dir.path().join("kv_d.data");
    
    let journal_c = MemMappedKVJournal::new(&temp_file_c, 2048, Some(0.8), None)?;
    let journal_d = MemMappedKVJournal::new(&temp_file_d, 2048, Some(0.8), None)?;
    let mut target_kv = DoubleBufferedKVJournal::new(journal_c, journal_d);

    // Add some different data to target
    target_kv.set("original", &Value::String("This will be replaced".to_string()))?;
    
    println!("  Target journal before reinit: {} entries", target_kv.as_hashmap()?.len());
    
    // Use reinit_from to copy all data from source to target
    target_kv.reinit_from(&mut db_kv)?;
    
    let target_map = target_kv.as_hashmap()?;
    println!("  Target journal after reinit: {} entries", target_map.len());
    
    // Verify data was copied correctly
    if let Some(first_entry) = target_map.get("demo_key_000") {
        println!("  ✓ Sample copied data: demo_key_000 = {:?}", first_entry);
    }
    
    println!("\n=== Demo completed successfully! ===");
    println!("The reinit_from method efficiently transfers journal state without");
    println!("needing to recreate the entire buffer structure.");

    Ok(())
}
