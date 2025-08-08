#![no_main]

use bd_bonjson::decoder::Decoder;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
  let mut decoder = Decoder::new(data);
  match decoder.decode() {
    Ok(_) => {},
    Err(_) => {},
  }
});
