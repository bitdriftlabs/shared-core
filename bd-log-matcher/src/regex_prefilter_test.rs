// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{LiteralFilter, PrefilteredRegex};

fn exact(literal: &str) -> LiteralFilter {
  LiteralFilter::Exact(literal.to_string())
}

fn required(literal: &str) -> LiteralFilter {
  LiteralFilter::Required(literal.to_string())
}

#[test]
fn literal_filter_recognizes_plain_and_escaped_text() {
  assert_eq!(LiteralFilter::new("checkout"), Some(exact("checkout")));
  assert_eq!(
    LiteralFilter::new("key=value pair"),
    Some(exact("key=value pair"))
  );
  assert_eq!(LiteralFilter::new(r"a\.b\(c\)"), Some(exact("a.b(c)")));
  // Characters that are special only inside a class are literal outside one.
  assert_eq!(LiteralFilter::new("a-b#c&d~e"), Some(exact("a-b#c&d~e")));
  assert_eq!(LiteralFilter::new(""), Some(exact("")));
}

#[test]
fn literal_filter_finds_the_longest_required_text() {
  assert_eq!(
    LiteralFilter::new("Dal xoquwo duyideseyiqok()"),
    Some(required("Dal xoquwo duyideseyiqok"))
  );
  assert_eq!(
    LiteralFilter::new(r"^51\.32\.31$"),
    Some(required("51.32.31"))
  );
  assert_eq!(LiteralFilter::new("ab.cdef"), Some(required("cdef")));
  assert_eq!(LiteralFilter::new(r"abc\d+de"), Some(required("abc")));
  // An optional or repeated character is left out; `+` keeps one copy.
  assert_eq!(LiteralFilter::new("abcd?ef"), Some(required("abc")));
  assert_eq!(LiteralFilter::new("ab*cdef"), Some(required("cdef")));
  assert_eq!(LiteralFilter::new("abc{2}de"), Some(required("ab")));
  assert_eq!(LiteralFilter::new("abc+de"), Some(required("abc")));
  assert_eq!(LiteralFilter::new("ab+?cdef"), Some(required("cdef")));
  // Groups and classes, including brackets inside them, end a run.
  assert_eq!(LiteralFilter::new("abc(x|y)de"), Some(required("abc")));
  assert_eq!(LiteralFilter::new(r"ab[)(\]]cde"), Some(required("cde")));
  assert_eq!(LiteralFilter::new("ab[]x]cde"), Some(required("cde")));
  assert_eq!(LiteralFilter::new("ab((c)[)])de"), Some(required("ab")));
}

#[test]
fn literal_filter_rejects_patterns_without_required_text() {
  for pattern in [
    ".*",
    ".+",
    "a|b",
    "abc|def",
    "(abc)",
    "[ab]",
    r"\d",
    "(?i)abc",
    "abc(?i)def",
    "(?:abc)",
    r"\x41bc",
    r"\p{L}bc",
    r"a\",
    "a(",
    "a[",
    "a{2",
  ] {
    assert_eq!(LiteralFilter::new(pattern), None, "{pattern}");
  }
}

#[test]
fn required_literal_is_in_every_match() {
  // Every candidate a regex matches must contain the literal the filter requires.
  let candidates = [
    "abcde", "abccde", "abde", "ab]cde", "abxde", "abcxde", "xyz", "abc12de", "abcdef",
  ];
  for pattern in [
    "abc+de",
    "ab[c\\]]cde",
    r"ab\d?cde",
    "ab(c)?de",
    "a(b|x)c?de",
    "ab.?de",
    r"abc\d*de",
    "ab]cde",
  ] {
    let Ok(regex) = regex_lite::Regex::new(pattern) else {
      continue;
    };
    let Some(LiteralFilter::Required(literal)) = LiteralFilter::new(pattern) else {
      continue;
    };
    for candidate in candidates {
      if regex.is_match(candidate) {
        assert!(
          candidate.contains(literal.as_str()),
          "{pattern:?} matched {candidate:?} without {literal:?}"
        );
      }
    }
  }
}

#[test]
fn prefiltered_regex_matches_like_the_regex_for_generated_patterns() {
  use rand::rngs::StdRng;
  use rand::{RngExt as _, SeedableRng as _};

  const TOKENS: &[&str] = &[
    "a",
    "b",
    "c",
    "ab",
    "bc",
    ".",
    r"\.",
    "(",
    ")",
    "[",
    "]",
    "[ab]",
    "[^a]",
    "^",
    "$",
    "*",
    "+",
    "?",
    "*?",
    "{2}",
    "{1,}",
    "{0,1}",
    "|",
    r"\d",
    r"\w",
    r"\b",
    "(?i)",
    "(?:",
    "{",
    "}",
    "1",
    r"\x41",
    r"\x{41}",
    r"\u{41}",
    r"\pL",
    r"\p{L}",
    r"\n",
    r"\b{start}",
  ];
  const CANDIDATE_CHARS: &[char] = &['a', 'b', 'c', 'A', '.', '1', ']', ' '];

  let mut rng = StdRng::seed_from_u64(0);
  let mut compiled = 0;
  for _ in 0 .. 20_000 {
    let pattern: String = (0 .. rng.random_range(1 ..= 8))
      .map(|_| TOKENS[rng.random_range(0 .. TOKENS.len())])
      .collect();
    let Ok(regex) = regex_lite::Regex::new(&pattern) else {
      continue;
    };
    compiled += 1;
    let matcher = PrefilteredRegex::new(&pattern).unwrap();
    for _ in 0 .. 20 {
      let candidate: String = (0 .. rng.random_range(0 ..= 10))
        .map(|_| CANDIDATE_CHARS[rng.random_range(0 .. CANDIDATE_CHARS.len())])
        .collect();
      assert_eq!(
        matcher.is_match(&candidate),
        regex.is_match(&candidate),
        "{pattern:?} against {candidate:?}"
      );
    }
  }
  assert!(
    compiled > 1_000,
    "only {compiled} generated patterns compiled"
  );
}
