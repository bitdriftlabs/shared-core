// Example demonstrating the reinit_from method
// This shows how to transfer data from one KV store to another

use bd_bonjson::Value;
use bd_resilient_kv::{InMemoryResilientKv, ResilientKv};

fn main() -> anyhow::Result<()> {
    // Create a source KV store with some data
    let mut source_buffer = vec![0; 256];
    let mut source_kv = InMemoryResilientKv::new(&mut source_buffer, None, None)?;
    
    // Add some data to the source
    source_kv.set("config", &Value::String("production".to_string()))?;
    source_kv.set("max_connections", &Value::Signed(100))?;
    source_kv.set("debug_enabled", &Value::Bool(false))?;
    
    println!("Source KV store contents:");
    let source_data = source_kv.as_hashmap()?;
    for (key, value) in &source_data {
        println!("  {}: {:?}", key, value);
    }
    
    // Create a target KV store with different data and high water mark settings
    let mut target_buffer = vec![0; 512]; // Larger buffer
    let mut target_kv = InMemoryResilientKv::new(
        &mut target_buffer, 
        Some(0.9), // Custom high water mark ratio
        None
    )?;
    
    // Add some initial data to target
    target_kv.set("old_setting", &Value::String("obsolete".to_string()))?;
    target_kv.set("version", &Value::Signed(1))?;
    
    println!("\nTarget KV store contents before reinit:");
    let target_data_before = target_kv.as_hashmap()?;
    for (key, value) in &target_data_before {
        println!("  {}: {:?}", key, value);
    }
    
    // Store the high water mark before reinit
    let high_water_mark_before = target_kv.high_water_mark();
    
    // Reinitialize target from source
    target_kv.reinit_from(&mut source_kv)?;
    
    println!("\nTarget KV store contents after reinit:");
    let target_data_after = target_kv.as_hashmap()?;
    for (key, value) in &target_data_after {
        println!("  {}: {:?}", key, value);
    }
    
    // Verify high water mark is preserved
    let high_water_mark_after = target_kv.high_water_mark();
    println!("\nHigh water mark preserved: {} -> {}", 
             high_water_mark_before, high_water_mark_after);
    assert_eq!(high_water_mark_before, high_water_mark_after);
    
    println!("\nreinit_from completed successfully!");
    println!("- Target KV now contains all data from source KV");
    println!("- Old target data was replaced");
    println!("- High water mark settings were preserved");
    
    Ok(())
}
