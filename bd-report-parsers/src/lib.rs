// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use nom::character::complete::one_of;
use nom::combinator::{map, recognize};
use nom::error::ParseError;
use nom::multi::many1;
use nom::{IResult, Parser};

pub mod android;

fn decimal<'a, T: std::str::FromStr + std::default::Default, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, T, E> {
  map(recognize(many1(one_of("0123456789"))), |out: &str| {
    out.parse::<T>().unwrap_or_default()
  })
  .parse(input)
}

fn hexadecimal<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, u64, E> {
  map(
    recognize(many1(one_of("0123456789abcdefABCDEF"))),
    |out: &str| u64::from_str_radix(out, 16).unwrap_or_default(),
  )
  .parse(input)
}
