use nom::character::complete::one_of;
use nom::combinator::{map, recognize};
use nom::error::{ContextError, ParseError};
use nom::multi::many1;
use nom::{IResult, Parser};

pub mod android;

fn decimal<
  'a,
  T: std::str::FromStr + std::default::Default,
  E: ParseError<&'a str> + ContextError<&'a str>,
>(
  input: &'a str,
) -> IResult<&'a str, T, E> {
  map(recognize(many1(one_of("0123456789"))), |out: &str| {
    out.parse::<T>().unwrap_or_default()
  })
  .parse(input)
}

fn hexadecimal<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, u64, E> {
  map(
    recognize(many1(one_of("0123456789abcdefABCDEF"))),
    |out: &str| u64::from_str_radix(out, 16).unwrap_or_default(),
  )
  .parse(input)
}
