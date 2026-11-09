// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_matcher::matcher::{JsonPathToken, resolve_json_path_for_testing};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

const JSON: &str = r#"{"metadata":{"request":{"items":[{"id":1},{"name":"target"}]}}}"#;

fn json_path(criterion: &mut Criterion) {
  let path = [
    JsonPathToken::Key("metadata".to_owned()),
    JsonPathToken::Key("request".to_owned()),
    JsonPathToken::Key("items".to_owned()),
    JsonPathToken::Index(1),
    JsonPathToken::Key("name".to_owned()),
  ];
  criterion.bench_function("json_path/string_positive", |bench| {
    bench.iter(|| {
      let result = resolve_json_path_for_testing(black_box(JSON), black_box(&path));
      assert_eq!(result.as_deref(), Some("target"));
      black_box(result);
    });
  });
}

criterion_group!(benches, json_path);
criterion_main!(benches);
