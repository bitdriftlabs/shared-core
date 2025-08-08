#![no_main]

use libfuzzer_sys::fuzz_target;
use bd_bonjson::decoder::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut decoder = Decoder::new(data);
    match decoder.decode() {
        Ok(_) => {},
        Err(_) => {}
    }
});
