// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{decimal, hexadecimal};
use nom::branch::alt;
use nom::bytes::complete::{take_till, take_until, take_until1, take_while1};
use nom::bytes::{tag, take};
use nom::character::complete::multispace1;
use nom::combinator::{map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated};
use nom::{IResult, Parser};
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "./android_tests.rs"]
mod tests;

#[derive(Debug)]
pub struct ANR<'a> {
  pub subject: Option<&'a str>,
  pub pid: u64,
  pub timestamp: &'a str,
  pub metrics: BTreeMap<&'a str, &'a str>,
  pub threads: Vec<Thread<'a>>,
  pub attached_thread_count: usize,
}

#[derive(Debug)]
pub struct Source<'a> {
  pub path: &'a str,
  pub lineno: Option<i64>,
}

#[derive(Debug)]
pub struct ThreadHeader<'a> {
  pub name: &'a str,
  pub is_daemon: bool,
  pub tid: Option<u32>,
  pub priority: Option<f32>,
  pub state: &'a str,
}

#[derive(Debug)]
pub struct Thread<'a> {
  pub header: ThreadHeader<'a>,
  pub props: BTreeMap<&'a str, &'a str>,
  pub frames: Vec<Frame<'a>>,
}

#[derive(Debug)]
pub enum Frame<'a> {
  Java {
    symbol: &'a str,
    source: Source<'a>,
    state: Vec<&'a str>,
  },
  Native {
    index: u64,
    address: u64,
    path: &'a str,
    symbol: Option<(&'a str, Option<u64>)>,
    build_id: Option<&'a str>,
  },
}

pub fn anr<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, ANR<'a>, E> {
  map(
    (
      subject_parser,
      process_start_parser,
      process_properties,
      thread_counter,
      threads,
    ),
    |(subject, (pid, timestamp), metrics, attached_thread_count, threads)| ANR {
      subject,
      pid,
      timestamp,
      metrics,
      threads,
      attached_thread_count,
    },
  )
  .parse(input)
}

fn thread_counter<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, usize, E> {
  delimited(
    (take_until("DALVIK THREADS ("), tag("DALVIK THREADS (")),
    decimal::<usize, _>,
    tag("):\n"),
  )
  .parse(input)
}

fn subject_parser<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, Option<&'a str>, E> {
  opt(preceded(tag("Subject: "), take_until("\n"))).parse(input)
}

fn process_start_parser<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, (u64, &'a str), E> {
  (
    delimited(
      (take_until("----- pid "), tag("----- pid ")),
      decimal::<u64, _>,
      tag(" at "),
    ),
    terminated(take_until(" -----"), tag(" -----\n")),
  )
    .parse(input)
}

fn process_properties<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, BTreeMap<&'a str, &'a str>, E> {
  map(many1(process_property), |pairs| {
    pairs.iter().map(|pair| (pair.0, pair.1)).collect()
  })
  .parse(input)
}

fn process_property<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, (&'a str, &'a str), E> {
  let line_end = input.find('\n').unwrap_or(input.len());
  let line = &input[..= line_end];
  if line_end == 0 || line.contains("DALVIK THREADS") {
    return Err(nom::Err::Error(E::from_error_kind(
      input,
      ErrorKind::TakeUntil,
    )));
  }
  map(
    terminated(
      alt((
        separated_pair(take_until(":\t"), tag(":\t"), take_until("\n")),
        separated_pair(take_until(": "), tag(": "), take_until("\n")),
        separated_pair(take_until("="), tag("="), take_until("\n")),
        pair(take_till(|c: char| c.is_ascii_digit()), take_until("\n")),
        map(take_until1("\n"), |s: &str| (s, "")),
      )),
      tag("\n"),
    ),
    |(key, value)| (key.trim(), value),
  )
  .parse(line)
  .map(|(_, values)| (&input[line.len() ..], values))
}

fn threads<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Vec<Thread<'a>>, E> {
  separated_list1(tag("\n"), any_thread).parse(input)
}

fn any_thread<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Thread<'a>, E> {
  map(
    terminated(
      (
        thread_header,
        map(many0(thread_props), |props| {
          props
            .iter()
            .flatten()
            .map(|pair| (pair.0, pair.1))
            .collect::<BTreeMap<&'a str, &'a str>>()
        }),
        many1(any_frame),
      ),
      opt(tag("  (no managed stack frames)\n")),
    ),
    |(header, props, frames)| Thread {
      header,
      props,
      frames,
    },
  )
  .parse(input)
}

fn thread_header<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, ThreadHeader<'a>, E> {
  let name_parser = delimited(tag("\""), take_till(|c| c == '"'), tag("\""));
  let int_transform = |num: &str| num.parse().unwrap_or_default();
  let float_transform = |num: &str| num.parse().unwrap_or_default();
  map(
    (
      terminated(name_parser, tag(" ")),
      opt(map(terminated(tag("daemon"), tag(" ")), |_| true)),
      opt(map(
        delimited(tag("prio="), take_till(|c| c == ' '), tag(" ")),
        float_transform,
      )),
      opt(map(
        delimited(tag("tid="), take_till(|c| c == ' '), tag(" ")),
        int_transform,
      )),
      terminated(take_till(|c| c == '\n'), tag("\n")),
    ),
    |(name, is_daemon, priority, tid, state)| ThreadHeader {
      name,
      is_daemon: is_daemon.unwrap_or_default(),
      tid,
      priority,
      state,
    },
  )
  .parse(input)
}

fn any_frame<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Frame<'a>, E> {
  preceded(multispace1, alt((java_frame, native_frame))).parse(input)
}

fn thread_props<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, Vec<(&'a str, &'a str)>, E> {
  terminated(
    preceded(
      (multispace1, tag("| ")),
      separated_list0(
        tag(" "),
        (
          terminated(take_till(|c| c == '='), tag("=")),
          thread_prop_value,
        ),
      ),
    ),
    tag("\n"),
  )
  .parse(input)
}

fn thread_prop_value<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E> {
  let line_end = input.find('\n').unwrap_or(input.len());
  let value_end = input[.. line_end]
    .find('=')
    .and_then(|index| input[.. index].rfind(' '))
    .unwrap_or(line_end);
  take(value_end).parse(input)
}

fn java_frame<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Frame<'a>, E> {
  let symbol_parser = take_till(|c| c == '(' || c == '\n');
  map(
    (
      delimited(tag("at "), symbol_parser, tag("(")),
      terminated(source_location, tag(")\n")),
      many0(preceded(
        (multispace1, tag("- ")),
        terminated(take_until("\n"), tag("\n")),
      )),
    ),
    |(symbol, source, state)| Frame::Java {
      symbol,
      source,
      state,
    },
  )
  .parse(input)
}

fn native_frame<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Frame<'a>, E> {
  let build_id_parser = delimited(
    tag(" (BuildId: "),
    take_till(|c| c == ')' || c == '\n'),
    tag(")"),
  );
  map(
    terminated(
      (
        delimited(tag("native: #"), decimal, tag(" pc ")),
        terminated(hexadecimal, multispace1),
        native_path_parser,
        opt(native_symbol_parser),
        opt(build_id_parser),
      ),
      tag("\n"),
    ),
    |(index, address, path, symbol, build_id)| Frame::Native {
      index,
      address,
      path,
      symbol,
      build_id,
    },
  )
  .parse(input)
}

fn native_symbol_parser<'a, E: ParseError<&'a str>>(
  input: &'a str,
) -> IResult<&'a str, (&'a str, Option<u64>), E> {
  delimited(
    tag(" ("),
    alt((
      map(
        (
          take_till(|c| c == '+' || c == '\n'),
          preceded(tag("+"), decimal),
        ),
        |(symbol, offset)| (symbol, Some(offset)),
      ),
      map(take_till(|c| c == ')' || c == '\n'), |symbol: &str| {
        (symbol, None)
      }),
    )),
    tag(")"),
  )
  .parse(input)
}

fn native_path_parser<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E> {
  let line_end = input.find('\n').unwrap_or(input.len());
  let line = &input[.. line_end];
  let path_end = line
    .find(" (BuildId:")
    .and_then(|build_index| line[.. build_index].rfind(" ("))
    .or_else(|| line.rfind(" ("))
    .unwrap_or(line_end);
  take(path_end).parse(input)
}

fn source_location<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Source<'a>, E> {
  let (remainder, path) = take_while1(|c| c != ')' && c != ':').parse(input)?;
  if remainder.is_empty() || remainder.starts_with(')') {
    Ok((remainder, Source { path, lineno: None }))
  } else {
    map(preceded(tag(":"), decimal::<i64, _>), |lineno| Source {
      path,
      lineno: Some(lineno),
    })
    .parse(remainder)
  }
}
