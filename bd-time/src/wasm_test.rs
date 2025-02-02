use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn pass() {
  println!("Hello, world!");
  eprintln!("Hello, world!");
  assert_eq!(1, 1);
}
