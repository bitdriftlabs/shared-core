mod uploader;

pub use uploader::{Client, MockClient, Uploader};

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
}
