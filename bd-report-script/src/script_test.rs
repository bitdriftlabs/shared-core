// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Script;
use serde_json::json;

#[test]
fn compute_interpolated_key() {
  let script = Script::new(
    "
    app_id = .app_metrics.app_id
    error_name = .errors[0].name
    class_name = .errors[0].stacktrace[0].class_name
    .grouping_key = \"{{ app_id }}-{{ error_name }}-{{ class_name }}\"
    ",
  )
  .expect("is ok");
  let report = json!({"app_metrics": {"app_id": "com.example.myapp"},"errors": [{"name": "NullPointerException", "stacktrace": [{"class_name": "Builder", "symbol_name": "build()"}]}]});
  let output = script.run(&report).expect("can run");
  assert_eq!(
    Some("com.example.myapp-NullPointerException-Builder".to_string()),
    output.grouping_key
  );
}

#[test]
fn type_hints_are_working() {
  // report.type is a known string key, so we can call contains()
  Script::new(
    "
    is_web = contains(.type, \"web\")
    .grouping_key = \"{{ is_web }}\"
    ",
  )
  .expect("is ok");

  // unknown_field hasn't been declared anywhere, and need to be type-cast
  let script = Script::new(
    "
    has_unknown = contains(.unknown_field, \"thing\")
    .grouping_key = \"{{ has_unknown }}\"
    ",
  )
  .expect_err("is not ok");
  assert!(script.to_string().contains("exact type"));
}
