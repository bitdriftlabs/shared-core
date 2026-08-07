// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use arbitrary::{Arbitrary, Unstructured};
use bd_log_matcher::matcher::{JsonPathToken, fuzz_json_path};

#[derive(Debug)]
pub struct JsonPathFuzzTestCase {
  json: Vec<u8>,
  path: Vec<JsonPathToken>,
}

impl<'a> Arbitrary<'a> for JsonPathFuzzTestCase {
  fn arbitrary(input: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let path_len = usize::from(input.arbitrary::<u8>()? % 8);
    let mut path = Vec::with_capacity(path_len);
    for _ in 0 .. path_len {
      if input.arbitrary()? {
        let key_len = usize::from(input.arbitrary::<u8>()? % 32);
        let key = String::from_utf8_lossy(input.bytes(key_len)?).into_owned();
        path.push(JsonPathToken::Key(key));
      } else {
        path.push(JsonPathToken::Index(input.arbitrary()?));
      }
    }
    Ok(Self {
      json: input.bytes(input.len())?.to_vec(),
      path,
    })
  }
}

pub fn run(test_case: &JsonPathFuzzTestCase) {
  fuzz_json_path(&String::from_utf8_lossy(&test_case.json), &test_case.path);
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus("corpus/json_path", |input| run(&input));
}
