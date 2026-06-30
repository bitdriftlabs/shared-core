// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Script;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::Report;

#[test]
fn type_hints_are_working() {
  // report.type is a known string key, so we can call contains()
  Script::new::<Report<'_>>(
    "
    is_mine = contains(.type, \"47\")
    _x = string!(is_mine)
    ",
    vec![],
  )
  .expect("is ok");

  // unknown_field hasn't been declared anywhere, and needs to be type-cast
  let script = Script::new::<Report<'_>>(
    "
    has_unknown = contains(.unknown_field, \"thing\")
    _x = string!(has_unknown)
    ",
    vec![],
  )
  .expect_err("is not ok");
  assert!(script.to_string().contains("exact type"));
}

#[test]
fn scripts_with_differing_comments_or_whitespace_are_equal() {
  use vrl::parser::ast::SyntaxEq;
  let script1 = Script::new::<Report<'_>>(r#"abs(int!("-62"))"#, vec![]).expect("is ok");
  let script2 = Script::new::<Report<'_>>(
    r#"# coercing an integer from a string
abs(int!("-62"))"#,
    vec![],
  )
  .expect("is ok");
  assert!(script1.ast.syntax_eq(&script2.ast));
  assert_eq!(script1, script2);
}
