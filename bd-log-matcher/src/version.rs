// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./version_test.rs"]
mod test;

use anyhow::{anyhow, Result};
use bd_proto::protos::log_matcher::log_matcher::log_matcher::base_log_matcher::Operator;
use itertools::Itertools;
use regex::Regex;
use std::cmp::Ordering;
use std::fmt::Display;

//
// VersionSegment
//

/// A single version segment. The version string "1.foo.3" is split into three segments: `1`,
/// `'foo'`, and `3`.
///
/// A segment will only be a `String` if the original string segment cannot be converted to
/// a `u32`.
///
/// When parsing a segmenet as an integer, leading zeros will be stripped. This means that `01` will
/// be parsed as `1` and treated as the string "1" during comparisons with other strings.
#[derive(Clone, Debug, PartialEq, Eq)]
enum VersionSegment {
  Int(u32),
  String(String),
}

impl VersionSegment {
  fn from_str(s: &str) -> Self {
    s.parse::<u32>()
      .map_or_else(|_| Self::String(s.to_string()), Self::Int)
  }
}

// When performing comparisons if the two segments are of the same type we can directly compare
// them. Otherwise we convert the integer representation into a string and perform a string
// comparison.
impl PartialOrd for VersionSegment {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for VersionSegment {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self, other) {
      (Self::Int(a), Self::Int(b)) => a.cmp(b),
      (Self::String(a), Self::String(b)) => a.cmp(b),
      (Self::String(a), Self::Int(b)) => a.cmp(&b.to_string()),
      (Self::Int(a), Self::String(b)) => a.to_string().cmp(b),
    }
  }
}

impl Display for VersionSegment {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        Self::Int(v) => v.to_string(),
        Self::String(s) => s.clone(),
      }
    )
  }
}

//
// Version
//

/// A `Version` is a sequence of `VersionSegment`s. A single version string `1.foo.3` is split into
/// a sequence of three segments: `1`, `'foo'`, and `3`. A hypen (`-`) represents the start of the
/// pre-release suffix of the version, and a plus (`+`) represents the start of the build metadata.
/// Build metadata may exist after the pre-release suffix, while a hyphen used as part of the build
/// metadata is considered part of the build metadata.
///
/// When comparing two versions with `lhs <op> rhs`, we compare each segment in order, filling
/// in zeros for any segments that are missing in either version. Pre-release suffixes and build
/// metadata are ignored for the purpose of ordering comparisons.
#[derive(Clone, Debug)]
pub struct Version {
  segments: Vec<VersionSegment>,
  revision: Option<String>,
  build_metadata: Option<String>,
}

impl Version {
  fn new(s: &str) -> Self {
    // If we see a - or a + in the version we treat this as the start of the version
    // suffix which we ignore for the purpose of ordering comparisons.
    //
    // According to semver, hyphen is used to denote pre-release versions and the plus sign is used
    // to denote build metadata. Pre-release versions are ordered before the normal versions, but we
    // ignore them for the sake of simplicity. The build metadata should always be ignored as part
    // of version comparisons, so we ignore that as well.
    //
    // See: https://semver.org/#spec-item-9
    // See: https://semver.org/#spec-item-10
    //
    // This is a tradeoff made to maintain some support for special suffixes used in semver without
    // having to enforce that the string is a true semver string. As we have no guarantee that the
    // version string under comparison is a semver string, we can't enforce much of the
    // syntax/semantics that comes with semver. For example, Android provides no guarantees on
    // version format, so a version string may look like `1.02.3-alpha` which looks like
    // semver but isn't (leading zeros are not allowed in semver). Stripping the suffix however
    // allows a suffixed string to still be comparable to a non-suffixed string, e.g. `1.2.10` and
    // `1.2.4-alpha` can be compared with > and < operators and will consider `10 > 4`.
    //
    // This also means that something like `1.2-alpha` will be considered equal to `1.2` which
    // clearly isn't valid semver, but seems like a reasonable approximation to how people might
    // use versions in practice.
    let (s, revision) = s
      .split_once(|c| c == '-')
      .map(|(s, suffix)| (s, Some(suffix.to_string())))
      .unwrap_or((s, None));

    let (s, build_metadata) = s
      .split_once(|c| c == '+')
      .map(|(s, suffix)| (s, Some(suffix.to_string())))
      .unwrap_or((s, None));

    Self {
      segments: s.split('.').map(VersionSegment::from_str).collect(),
      revision,
      build_metadata,
    }
  }

  fn cmp(&self, other: &Self) -> Ordering {
    const ZERO: VersionSegment = VersionSegment::Int(0);

    for pair in self.segments.iter().zip_longest(other.segments.iter()) {
      let (a, b) = match pair {
        itertools::EitherOrBoth::Both(a, b) => (a, b),
        itertools::EitherOrBoth::Left(a) => (a, &ZERO),
        itertools::EitherOrBoth::Right(b) => (&ZERO, b),
      };

      match a.cmp(b) {
        Ordering::Equal => continue,
        other => return other,
      }
    }

    Ordering::Equal
  }
}

impl Display for Version {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      self
        .segments
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join(".")
        + &self
          .revision
          .as_deref()
          .map(|s| "-".to_string() + s)
          .unwrap_or_default()
        + &self
          .build_metadata
          .as_deref()
          .map(|s| "+".to_string() + s)
          .unwrap_or_default()
    )
  }
}

//
// SimpleComparator
//

// TODO(snowp): Refactor string and int matching to use this enum.

/// Supported comparison operators for version comparisons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimpleComparator {
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  GreaterThanOrEqual,
  NotEquals,
  Equals,
}

impl SimpleComparator {
  fn from_operator(operator: Operator) -> Result<Self> {
    Ok(match operator {
      Operator::OPERATOR_LESS_THAN => Self::LessThan,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => Self::LessThanOrEqual,
      Operator::OPERATOR_GREATER_THAN => Self::GreaterThan,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => Self::GreaterThanOrEqual,
      Operator::OPERATOR_NOT_EQUALS => Self::NotEquals,
      Operator::OPERATOR_EQUALS => Self::Equals,
      _ => return Err(anyhow!("Invalid operator for version")),
    })
  }

  fn compare(&self, a: &Version, b: &Version) -> bool {
    let ord = a.cmp(b);

    match self {
      Self::LessThan => ord == Ordering::Less,
      Self::LessThanOrEqual => ord == Ordering::Less || ord == Ordering::Equal,
      Self::GreaterThan => ord == Ordering::Greater,
      Self::GreaterThanOrEqual => ord == Ordering::Greater || ord == Ordering::Equal,
      Self::NotEquals => ord != Ordering::Equal,
      Self::Equals => ord == Ordering::Equal,
    }
  }
}

/// Describes a comparison match criteria that compares two version strings.
#[derive(Clone, Debug)]
pub enum VersionMatch {
  Regex(Regex),
  Comparator(SimpleComparator, Version),
}


// We want PartialEq to describe whether the underlying configuration for the type is the same, not
// based on how they match against a candidate.
impl PartialEq for VersionMatch {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      // The regexes are the same if the underlying input string is the same.
      (Self::Regex(a), Self::Regex(b)) => a.as_str() == b.as_str(),
      // The comparators are the same if the operator and the version strings are the same.
      (Self::Comparator(a_op, a_val), Self::Comparator(b_op, b_val)) => {
        a_op == b_op && a_val.to_string() == b_val.to_string()
      },
      _ => false,
    }
  }
}

impl Eq for VersionMatch {}

impl VersionMatch {
  pub fn new(operator: Operator, value: &str) -> Result<Self> {
    if operator == Operator::OPERATOR_UNSPECIFIED {
      return Err(anyhow!("UNSPECIFIED operator"));
    }

    Ok(match operator {
      Operator::OPERATOR_REGEX => Self::Regex(Regex::new(value)?),
      operator => Self::Comparator(
        SimpleComparator::from_operator(operator)?,
        Version::new(value),
      ),
    })
  }

  pub fn evaluate(&self, candidate: &str) -> bool {
    match self {
      Self::Regex(r) => r.is_match(candidate),
      Self::Comparator(operator, version) => operator.compare(&Version::new(candidate), version),
    }
  }
}
