// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Checks `PrefilteredRegex` against two independent oracles. `regex_lite` decides what matches.
//! `regex_syntax`, a full parser, generates strings that `regex_lite` confirms match the pattern;
//! each must contain the literal the prefilter requires, so an unsound literal is found even when
//! random candidates would rarely match the pattern.

use arbitrary::{Arbitrary, Unstructured};
use bd_log_matcher::regex_prefilter::{LiteralFilter, PrefilteredRegex};
use regex_syntax::hir::{Class, Hir, HirKind};

/// Pieces of regex syntax, so most generated patterns compile and exercise the scanner's rules.
const TOKENS: &[&str] = &[
  "a",
  "b",
  "c",
  "ab",
  "bc",
  "abc",
  " ",
  "-",
  "#",
  "]",
  "}",
  ".",
  r"\.",
  r"\\",
  r"\(",
  r"\[",
  r"\{",
  r"\]",
  "(",
  ")",
  "[",
  "[ab]",
  "[^a]",
  "[]a]",
  "[a-c]",
  "[[:alpha:]]",
  "^",
  "$",
  "*",
  "+",
  "?",
  "*?",
  "+?",
  "??",
  "{2}",
  "{1,}",
  "{0,2}",
  "{1,3}?",
  "|",
  r"\d",
  r"\w",
  r"\s",
  r"\D",
  r"\b",
  r"\B",
  r"\A",
  r"\z",
  r"\n",
  "\n",
  r"\t",
  r"\x41",
  r"\x{42}",
  r"\u{43}",
  r"\pL",
  r"\p{L}",
  r"\PL",
  "(?i)",
  "(?-i)",
  "(?s)",
  "(?m)",
  "(?x)",
  "(?:",
  "(?x:",
  "(?P<n>",
  "(?<n>",
  "{",
  "1",
];
const CANDIDATE_CHARS: &[char] = &[
  'a', 'b', 'c', 'A', 'B', '.', '-', '#', ' ', '\n', '1', ']', '}', '(', ')', '[', '\\', 'é',
];
const MAX_REPETITIONS: u32 = 3;
const MAX_SAMPLE_LEN: usize = 256;

#[derive(Debug)]
pub struct RegexPrefilterFuzzTestCase {
  pattern: String,
  candidates: Vec<String>,
  choices: Vec<u8>,
}

impl<'a> Arbitrary<'a> for RegexPrefilterFuzzTestCase {
  fn arbitrary(input: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let pattern = if input.ratio(1, 4)? {
      input.arbitrary()?
    } else {
      let mut pattern = String::new();
      for _ in 0 ..= input.int_in_range(0 ..= 9)? {
        pattern.push_str(input.choose(TOKENS)?);
      }
      pattern
    };
    let mut candidates = Vec::new();
    for _ in 0 .. input.int_in_range(0 ..= 6)? {
      let mut candidate = String::new();
      for _ in 0 .. input.int_in_range(0 ..= 12)? {
        candidate.push(*input.choose(CANDIDATE_CHARS)?);
      }
      candidates.push(candidate);
    }
    let choices = input.bytes(input.len().min(512))?.to_vec();
    Ok(Self {
      pattern,
      candidates,
      choices,
    })
  }
}

pub fn run(test_case: &RegexPrefilterFuzzTestCase) {
  let pattern = &test_case.pattern;
  let Ok(regex) = regex_lite::Regex::new(pattern) else {
    return;
  };
  let prefiltered = PrefilteredRegex::new(pattern).unwrap();
  let filter = prefiltered.literal_filter();
  let hir = regex_syntax::parse(pattern).ok();

  // An exact literal must be what a full parser reads the pattern as.
  if let (Some(LiteralFilter::Exact(literal)), Some(hir)) = (filter, &hir) {
    let parsed = match hir.kind() {
      HirKind::Empty => Some(String::new()),
      HirKind::Literal(parsed) => Some(String::from_utf8_lossy(&parsed.0).into_owned()),
      _ => None,
    };
    assert_eq!(
      parsed.as_deref(),
      Some(literal.as_str()),
      "{pattern:?} is not the literal {literal:?}: {hir:?}"
    );
  }

  let mut checked = test_case.candidates.clone();
  if let Some(hir) = &hir {
    for offset in 0 .. 8 {
      let mut choices = Choices {
        bytes: &test_case.choices,
        index: offset * 31,
      };
      let mut sample = String::new();
      if !generate(hir, &mut choices, &mut sample) || !regex.is_match(&sample) {
        continue;
      }
      if let Some(LiteralFilter::Exact(literal) | LiteralFilter::Required(literal)) = filter {
        assert!(
          sample.contains(literal.as_str()),
          "{pattern:?} matches {sample:?}, which lacks the required literal {literal:?}"
        );
      }
      // Near misses: a match with one character removed.
      for (index, _) in sample.char_indices() {
        let mut near = sample.clone();
        near.remove(index);
        checked.push(near);
      }
      checked.push(sample);
    }
  }

  for candidate in &checked {
    assert_eq!(
      prefiltered.is_match(candidate),
      regex.is_match(candidate),
      "{pattern:?} against {candidate:?} with filter {filter:?}"
    );
  }
}

/// A stream of fuzzer bytes that steers string generation.
struct Choices<'a> {
  bytes: &'a [u8],
  index: usize,
}

impl Choices<'_> {
  fn next(&mut self, bound: usize) -> usize {
    let byte = if self.bytes.is_empty() {
      0
    } else {
      self.bytes[self.index % self.bytes.len()]
    };
    self.index += 1;
    usize::from(byte) % bound.max(1)
  }
}

/// Appends to `out` a string the parsed pattern can match, ignoring assertions such as `^` and
/// `\b`, which the caller checks by running the regex. Returns false when no string fits.
fn generate(hir: &Hir, choices: &mut Choices<'_>, out: &mut String) -> bool {
  if out.len() > MAX_SAMPLE_LEN {
    return false;
  }
  match hir.kind() {
    HirKind::Empty | HirKind::Look(_) => true,
    HirKind::Literal(literal) => {
      out.push_str(&String::from_utf8_lossy(&literal.0));
      true
    },
    HirKind::Class(Class::Unicode(class)) => {
      let ranges = class.ranges();
      if ranges.is_empty() {
        return false;
      }
      let range = ranges[choices.next(ranges.len())];
      let span = u32::from(range.end()) - u32::from(range.start());
      let offset = u32::try_from(choices.next(256)).unwrap_or_default() % (span + 1);
      out.push(char::from_u32(u32::from(range.start()) + offset).unwrap_or_else(|| range.start()));
      true
    },
    HirKind::Class(Class::Bytes(class)) => {
      let ascii: Vec<_> = class
        .ranges()
        .iter()
        .filter(|range| range.start().is_ascii())
        .collect();
      if ascii.is_empty() {
        return false;
      }
      let range = ascii[choices.next(ascii.len())];
      let end = range.end().min(0x7f);
      let offset = u8::try_from(choices.next(256)).unwrap_or_default() % (end - range.start() + 1);
      out.push(char::from(range.start() + offset));
      true
    },
    HirKind::Repetition(repetition) => {
      if repetition.min > 64 {
        return false;
      }
      let max = repetition
        .max
        .unwrap_or(repetition.min + MAX_REPETITIONS)
        .min(repetition.min + MAX_REPETITIONS);
      let count = repetition.min
        + u32::try_from(choices.next(usize::try_from(max - repetition.min + 1).unwrap_or(1)))
          .unwrap_or_default();
      (0 .. count).all(|_| generate(&repetition.sub, choices, out))
    },
    HirKind::Capture(capture) => generate(&capture.sub, choices, out),
    HirKind::Concat(hirs) => hirs.iter().all(|hir| generate(hir, choices, out)),
    HirKind::Alternation(hirs) => generate(&hirs[choices.next(hirs.len())], choices, out),
  }
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus("corpus/regex_prefilter", |input| run(&input));
}

#[test]
fn generated_cases() {
  use rand::rngs::StdRng;
  use rand::{RngExt as _, SeedableRng as _};

  // A deterministic sample of fuzz inputs, so the oracles run in every test pass.
  let mut rng = StdRng::seed_from_u64(0);
  for _ in 0 .. 20_000 {
    let bytes: Vec<u8> = (0 .. 256).map(|_| rng.random()).collect();
    if let Ok(test_case) = RegexPrefilterFuzzTestCase::arbitrary(&mut Unstructured::new(&bytes)) {
      run(&test_case);
    }
  }
}
