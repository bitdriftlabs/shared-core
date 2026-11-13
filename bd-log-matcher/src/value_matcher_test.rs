// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{StringMatch, ValueOrSavedFieldId};
use bd_log_primitives::tiny_set::TinyMap;
use bd_proto::protos::value_matcher::value_matcher::Operator;

#[test]
fn regex_operator_matches_like_the_regex() {
  let candidates = [
    "",
    "checkout",
    "pre checkout post",
    "check",
    "CHECKOUT",
    "a.b(c)",
    "ab(c)",
    "51.32.31",
    "51.32.31.3",
    "x51.32.31",
    "Dal xoquwo duyideseyiqok()",
    "Dal xoquwo duyideseyiqok",
    "dal xoquwo duyideseyiqok",
    "abccde",
    "abcde",
    "abcccde",
    "abde",
    "abxde",
    "ab)cde",
    "ab]cde",
    "abc1de",
    "abc12de",
    "abcef",
    "abdef",
    "a]b",
    "a}b",
  ];
  for pattern in [
    "checkout",
    r"a\.b\(c\)",
    "",
    "a.b",
    "^check",
    "out$",
    r"^51\.32\.31$",
    "Dal xoquwo duyideseyiqok()",
    "Dal xoquwo duyideseyiqok(x)?",
    "abc+de",
    "abc*de",
    "abc?de",
    "abc{2}de",
    "abc{1,}de",
    "abc+?de",
    r"abc\d+de",
    "ab[)(\\]]cde",
    "ab(c|x)de",
    "abcd?ef",
    "(?i)checkout",
    "check|post",
    "a]b",
    "a}b",
  ] {
    let Ok(regex) = regex_lite::Regex::new(pattern) else {
      continue;
    };
    let matcher = StringMatch::new(
      Operator::OPERATOR_REGEX.into(),
      ValueOrSavedFieldId::Value(pattern.to_string()),
    )
    .unwrap();
    for candidate in candidates {
      assert_eq!(
        matcher.evaluate(candidate, &TinyMap::default()),
        regex.is_match(candidate),
        "{pattern:?} against {candidate:?}"
      );
    }
  }
}
