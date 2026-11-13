// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Regex matching with a literal prefilter. `regex_lite` has no prefilter of its own, so it steps
//! its NFA over every byte of a candidate even when the pattern needs text the candidate lacks.

#[cfg(test)]
#[path = "./regex_prefilter_test.rs"]
mod tests;

use regex_lite::Regex;

//
// PrefilteredRegex
//

/// An unanchored regex whose match is decided, or ruled out, by a substring search when the
/// pattern allows it.
#[derive(Clone, Debug)]
pub struct PrefilteredRegex {
  regex: Regex,
  literal_filter: Option<LiteralFilter>,
}

impl PrefilteredRegex {
  pub fn new(pattern: &str) -> Result<Self, regex_lite::Error> {
    Ok(Self {
      regex: Regex::new(pattern)?,
      literal_filter: LiteralFilter::new(pattern),
    })
  }

  #[must_use]
  pub fn is_match(&self, candidate: &str) -> bool {
    match &self.literal_filter {
      Some(LiteralFilter::Exact(literal)) => candidate.contains(literal.as_str()),
      Some(LiteralFilter::Required(literal)) if !candidate.contains(literal.as_str()) => false,
      _ => self.regex.is_match(candidate),
    }
  }

  /// The pattern the regex was compiled from.
  #[must_use]
  pub fn as_str(&self) -> &str {
    self.regex.as_str()
  }

  #[must_use]
  pub const fn literal_filter(&self) -> Option<&LiteralFilter> {
    self.literal_filter.as_ref()
  }
}

//
// LiteralFilter
//

/// The deepest group nesting the scan follows before giving up.
const MAX_GROUP_DEPTH: usize = 8;

/// What a substring search can decide about an unanchored regex without running it.
///
/// The scan accepts only a small, fixed subset of the syntax, and returns no filter for a pattern
/// with anything else in it, so that reading the subset below is enough to check it:
///
/// - Text: any character other than `\ . + * ? ( ) | [ ] { } ^ $`, or a backslash before one of `\
///   . + * ? ( ) | [ ] { } ^ $ # & - ~`.
/// - Other single-character atoms: `.`, `^`, `$`, `\d \D \w \W \s \S`, `\n \r \t`, `\A \z`, and `\b
///   \B` when not followed by `{`.
/// - A flat class, such as `[a-z_]` or `[^\]x]`, of text, ranges and the escapes above.
/// - A group, `(...)`, of this same subset, which may contain `|`.
/// - A quantifier after an atom: `*`, `+`, `?`, `{n}`, `{n,}` or `{n,m}`, optionally lazy.
///
/// Flags (`(?...)`), alternation outside a group, nested or POSIX classes and every other escape
/// are outside the subset. Without flags, nothing can change how later characters read, so text
/// means the same everywhere it appears.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiteralFilter {
  /// The pattern is this literal, so a candidate matches exactly when it contains it.
  Exact(String),
  /// Every match contains this literal, so a candidate that does not contain it cannot match.
  Required(String),
}

impl LiteralFilter {
  /// Scans `pattern`, returning `None` when no substring search can decide or rule out a match.
  #[must_use]
  pub fn new(pattern: &str) -> Option<Self> {
    let mut scanner = Scanner {
      chars: pattern.chars().peekable(),
    };
    let sequence = scanner.sequence(0)?;
    // A `)` without a group is not a valid pattern.
    if scanner.chars.next().is_some() {
      return None;
    }
    if sequence.exact {
      return sequence.runs.into_iter().next().map(Self::Exact);
    }
    sequence
      .runs
      .into_iter()
      // The first of the longest runs, as `max_by_key` keeps the last.
      .rev()
      .max_by_key(String::len)
      .filter(|run| !run.is_empty())
      .map(Self::Required)
  }
}

/// The runs of consecutive text in a sequence, each of which every match contains.
struct Sequence {
  runs: Vec<String>,
  /// Whether the sequence is nothing but text, so its one run is all it matches.
  exact: bool,
}

enum Atom {
  Text(char),
  Other,
}

enum Quantifier {
  None,
  OneOrMore,
  Other,
}

struct Scanner<'a> {
  chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl Scanner<'_> {
  /// Reads quantified atoms up to `)` or the end. Inside a group a `|` is allowed, and the caller
  /// ignores the runs.
  fn sequence(&mut self, depth: usize) -> Option<Sequence> {
    let mut sequence = Sequence {
      runs: vec![String::new()],
      exact: true,
    };
    while let Some(&c) = self.chars.peek() {
      if c == ')' {
        break;
      }
      if c == '|' {
        // An alternative need not contain the other's text.
        if depth == 0 {
          return None;
        }
        self.chars.next();
        continue;
      }
      let atom = self.atom(depth)?;
      let quantifier = self.quantifier()?;
      let run = sequence.runs.last_mut()?;
      match (atom, quantifier) {
        (Atom::Text(c), Quantifier::None) => run.push(c),
        // `+` repeats the character at least once; the text after it is a new run.
        (Atom::Text(c), Quantifier::OneOrMore) => {
          run.push(c);
          sequence.exact = false;
          sequence.runs.push(String::new());
        },
        _ => {
          sequence.exact = false;
          sequence.runs.push(String::new());
        },
      }
    }
    Some(sequence)
  }

  fn atom(&mut self, depth: usize) -> Option<Atom> {
    match self.chars.next()? {
      '\\' => self.escape(),
      '(' => {
        if self.chars.peek() == Some(&'?') || depth >= MAX_GROUP_DEPTH {
          return None;
        }
        self.sequence(depth + 1)?;
        self.chars.next_if_eq(&')')?;
        Some(Atom::Other)
      },
      '[' => {
        self.class()?;
        Some(Atom::Other)
      },
      '.' | '^' | '$' => Some(Atom::Other),
      '+' | '*' | '?' | '|' | ')' | ']' | '{' | '}' => None,
      c => Some(Atom::Text(c)),
    }
  }

  /// Reads the rest of an escape whose `\` was consumed.
  fn escape(&mut self) -> Option<Atom> {
    match self.chars.next()? {
      c @ ('\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
      | '#' | '&' | '-' | '~') => Some(Atom::Text(c)),
      'd' | 'D' | 'w' | 'W' | 's' | 'S' | 'n' | 'r' | 't' | 'A' | 'z' => Some(Atom::Other),
      'b' | 'B' if self.chars.peek() != Some(&'{') => Some(Atom::Other),
      _ => None,
    }
  }

  /// Reads the rest of a flat class whose `[` was consumed. A `]` first in the class, after any
  /// `^`, is text.
  fn class(&mut self) -> Option<()> {
    self.chars.next_if_eq(&'^');
    self.chars.next_if_eq(&']');
    loop {
      match self.chars.next()? {
        ']' => return Some(()),
        '[' => return None,
        '\\' => {
          self.escape()?;
        },
        _ => {},
      }
    }
  }

  fn quantifier(&mut self) -> Option<Quantifier> {
    let quantifier = match self.chars.peek() {
      Some('+') => Quantifier::OneOrMore,
      Some('*' | '?') => Quantifier::Other,
      Some('{') => {
        self.chars.next();
        self.counted()?;
        self.chars.next_if_eq(&'?');
        return Some(Quantifier::Other);
      },
      _ => return Some(Quantifier::None),
    };
    self.chars.next();
    // Laziness, as in `*?`, does not change what can match.
    self.chars.next_if_eq(&'?');
    Some(quantifier)
  }

  /// Reads `n}`, `n,}` or `n,m}` after a `{`.
  fn counted(&mut self) -> Option<()> {
    self.digits()?;
    if self.chars.next_if_eq(&',').is_some() && self.chars.peek() != Some(&'}') {
      self.digits()?;
    }
    self.chars.next_if_eq(&'}').map(|_| ())
  }

  fn digits(&mut self) -> Option<()> {
    let mut any = false;
    while self.chars.next_if(char::is_ascii_digit).is_some() {
      any = true;
    }
    any.then_some(())
  }
}
