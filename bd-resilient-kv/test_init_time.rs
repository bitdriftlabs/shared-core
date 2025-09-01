use bd_resilient_kv::{InMemoryResilientKv, ResilientKv};
use bd_bonjson::Value;

fn main() -> anyhow::Result<()> {
    // Create a new KV store
    let mut buffer = vec![0; 1024];
    let mut kv = InMemoryResilientKv::new(&mut buffer, None, None)?;
    
    // Get and display the initialization time
    let init_time = kv.get_init_time()?;
    println!("KV store initialized at: {} nanoseconds since UNIX epoch", init_time);
    
    // Convert to seconds for readability
    let init_time_seconds = init_time / 1_000_000_000;
    println!("That's approximately: {} seconds since UNIX epoch", init_time_seconds);
    
    // Add some data
    kv.set("test_key", &Value::String("test_value".to_string()))?;
    
    // The initialization time should remain the same
    let init_time2 = kv.get_init_time()?;
    println!("After adding data, init time is still: {}", init_time2);
    assert_eq!(init_time, init_time2);
    
    println!("Success! The initialization time remains consistent.");
    Ok(())
}
