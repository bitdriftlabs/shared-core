use crate::writer::Writer;
use std::io::Cursor;
use std::io::Write;
use assert_no_alloc::*;

#[cfg(debug_assertions)]
#[global_allocator]
static A: AllocDisabler = AllocDisabler;

#[test]
fn writer_does_not_allocate() {
    writer_does_not_allocate_using_buff_size(1000);
    writer_does_not_allocate_using_buff_size(1);
    writer_does_not_allocate_using_buff_size(10);
}

fn writer_does_not_allocate_using_buff_size(buff_size: usize) {
    let mut buf = vec![0u8; buff_size];
    assert_no_alloc(|| {
        let mut cursor = Cursor::new(&mut buf[..]);
        let mut writer = Writer { writer: &mut cursor };

        // Test all writer methods
        writer.write_null().unwrap();
        writer.write_boolean(true).unwrap();
        writer.write_boolean(false).unwrap();
        writer.write_i64(-42).unwrap();
        writer.write_i64(0).unwrap();
        writer.write_u64(42).unwrap();
        writer.write_u64(0).unwrap();
        writer.write_f64(3.14159).unwrap();
        writer.write_f32(2.71f32).unwrap();
        writer.write_str("hello").unwrap();
        writer.write_str("").unwrap();
        writer.write_array_begin().unwrap();
        writer.write_map_begin().unwrap();
        writer.write_container_end().unwrap();

        // Also test the std::io::Write implementation
        writer.write(b"test").unwrap();
        writer.flush().unwrap();
    });
}


use std::fs::OpenOptions;

#[test]
fn writer_does_not_allocate_writing_to_file() {
    use std::io::BufWriter;
    use tempfile::NamedTempFile;
    
    // Create a temporary file
    let temp_file = NamedTempFile::new().unwrap();
    let file = OpenOptions::new()
        .write(true)
        .open(temp_file.path())
        .unwrap();
    
    // Use BufWriter to avoid frequent system calls
    let mut buf_writer = BufWriter::new(file);
    
    assert_no_alloc(|| {
        let mut writer = Writer { writer: &mut buf_writer };

        for _ in 0..100000 {
            // Test all writer methods
            writer.write_null().unwrap();
            writer.write_boolean(true).unwrap();
            writer.write_boolean(false).unwrap();
            writer.write_i64(-42).unwrap();
            writer.write_i64(0).unwrap();
            writer.write_u64(42).unwrap();
            writer.write_u64(0).unwrap();
            writer.write_f64(3.14159).unwrap();
            writer.write_f32(2.71f32).unwrap();
            writer.write_str("hello").unwrap();
            writer.write_str("").unwrap();
            writer.write_array_begin().unwrap();
            writer.write_map_begin().unwrap();
            writer.write_container_end().unwrap();

            // Also test the std::io::Write implementation
            writer.write(b"test").unwrap();
        }
        writer.flush().unwrap();
    });
}
