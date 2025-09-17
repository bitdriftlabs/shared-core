// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub use crate::input::MemmapView;
use nom::character::complete::one_of;
use nom::combinator::{map, recognize};
use nom::error::ParseError;
use nom::multi::many1;
use nom::{IResult, Parser};

pub mod android;
mod input;

#[cfg(test)]
fn make_tempfile(contents: &[u8]) -> anyhow::Result<std::fs::File> {
  use std::io::Write;

  let mut file = tempfile::tempfile()?;
  let written = file.write(contents)?;
  assert_eq!(contents.len(), written);
  Ok(file)
}

fn decimal<
  I: ToString + nom::Input + nom::Offset,
  T: std::str::FromStr + std::default::Default,
  E: ParseError<I>,
>(
  input: I,
) -> IResult<I, T, E>
where
  <I as nom::Input>::Item: nom::AsChar,
{
  map(recognize(many1(one_of("0123456789"))), |out: I| {
    out.to_string().parse::<T>().unwrap_or_default()
  })
  .parse(input)
}

fn hexadecimal<I: ToString + nom::Input + nom::Offset, E: ParseError<I>>(
  input: I,
) -> IResult<I, u64, E>
where
  <I as nom::Input>::Item: nom::AsChar,
{
  map(
    recognize(many1(one_of("0123456789abcdefABCDEF"))),
    |out: I| u64::from_str_radix(&out.to_string(), 16).unwrap_or_default(),
  )
  .parse(input)
}
