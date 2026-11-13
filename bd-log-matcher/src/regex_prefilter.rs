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

/// What a substring search can decide about an unanchored regex without running it. This is a
/// conservative scan of the pattern, not a parser: anything it does not understand ends the
/// current run of literal text, or, where it could change how the rest of the pattern reads, gives
/// up on a filter entirely.
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
    let mut chars = pattern.chars().peekable();
    // The run of consecutive text that every match contains, and the longest finished run.
    let mut run = String::new();
    let mut longest = String::new();
    let mut exact = true;
    let mut end_run = |run: &mut String| {
      if run.len() > longest.len() {
        longest = std::mem::take(run);
      }
      run.clear();
    };

    while let Some(c) = chars.next() {
      // The atom's text if it is a literal character, or `None` if it matches something else.
      let atom = match c {
        '\\' => match chars.next()? {
          escaped @ ('\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}'
          | '^' | '$' | '#' | '&' | '-' | '~') => Some(escaped),
          // These escapes take more than one character, such as `\x41` or `\p{L}`.
          'x' | 'u' | 'U' | 'p' | 'P' => return None,
          // Classes, such as `\d`, assertions, such as `\b`, and special characters, such as
          // `\n`.
          _ => None,
        },
        // Flags, such as `(?i)`, change how the rest of the pattern reads.
        '(' if chars.peek() == Some(&'?') => return None,
        '(' => {
          skip_group(&mut chars)?;
          None
        },
        '[' => {
          skip_class(&mut chars)?;
          None
        },
        '.' | '^' | '$' => None,
        // An alternative need not contain the other's text. A quantifier without an atom, or an
        // unbalanced bracket, is not a valid pattern.
        '|' | '*' | '+' | '?' | '{' | ')' => return None,
        c => Some(c),
      };

      // A quantifier repeats the atom: `+` at least once, the others possibly never.
      let quantifier = chars.next_if(|&c| matches!(c, '*' | '+' | '?' | '{'));
      if quantifier == Some('{') {
        while chars.next()? != '}' {}
      }
      if quantifier.is_some() {
        // Laziness, as in `*?`, does not change what can match.
        chars.next_if_eq(&'?');
      }

      match (atom, quantifier) {
        (Some(c), None) => run.push(c),
        (Some(c), Some('+')) => {
          run.push(c);
          exact = false;
          end_run(&mut run);
        },
        _ => {
          exact = false;
          end_run(&mut run);
        },
      }
    }

    if exact {
      return Some(Self::Exact(run));
    }
    end_run(&mut run);
    (!longest.is_empty()).then_some(Self::Required(longest))
  }
}

/// Skips the rest of a group whose `(` was consumed, including nested groups and classes.
fn skip_group(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<()> {
  let mut depth = 1;
  while depth > 0 {
    match chars.next()? {
      '\\' => {
        chars.next()?;
      },
      '[' => skip_class(chars)?,
      '(' => depth += 1,
      ')' => depth -= 1,
      _ => {},
    }
  }
  Some(())
}

/// Skips the rest of a character class whose `[` was consumed, including nested classes. A `]`
/// first in the class, after any `^`, is literal.
fn skip_class(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<()> {
  chars.next_if_eq(&'^');
  chars.next_if_eq(&']');
  let mut depth = 1;
  while depth > 0 {
    match chars.next()? {
      '\\' => {
        chars.next()?;
      },
      '[' => {
        depth += 1;
        chars.next_if_eq(&'^');
        chars.next_if_eq(&']');
      },
      ']' => depth -= 1,
      _ => {},
    }
  }
  Some(())
}
