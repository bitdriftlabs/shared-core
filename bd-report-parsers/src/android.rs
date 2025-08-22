// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{decimal, hexadecimal};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1;
use nom::branch::alt;
use nom::bytes::complete::{take_till, take_until, take_until1, take_while1};
use nom::bytes::{tag, take};
use nom::character::complete::multispace1;
use nom::combinator::{map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated};
use nom::{IResult, Parser};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;

#[cfg(test)]
#[path = "./android_tests.rs"]
mod tests;

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

pub fn build_anr<'a, 'fbb, E: ParseError<&'a str>>(
  mut builder: &mut flatbuffers::FlatBufferBuilder<'fbb>,
  input: &'a str,
) -> IResult<&'a str, flatbuffers::WIPOffset<v_1::Report<'fbb>>, E> {
  let (remainder, (subject, (pid, timestamp), metrics, _attached_count, thread_offsets)) = (
    subject_parser,
    process_start_parser,
    process_properties,
    thread_counter,
    separated_list1(tag("\n"), |text| build_thread(&mut builder, text)),
  )
    .parse(input)?;

  let time = OffsetDateTime::parse(&timestamp.replace(" ", "T"), &Iso8601::DEFAULT).map_or(
    v_1::Timestamp::default(),
    |stamp| {
      v_1::Timestamp::new(
        u64::try_from(stamp.unix_timestamp()).unwrap_or_default(),
        stamp.nanosecond(),
      )
    },
  );
  let app_args = v_1::AppMetricsArgs {
    // TODO: app_id, etc
    process_id: u32::try_from(pid).unwrap_or_default(),
    ..Default::default()
  };
  let cpu_abis = metrics.get("ABI").map(|abi| {
    let abi = builder.create_string(abi);
    builder.create_vector(&[abi])
  });
  let device_args = v_1::DeviceMetricsArgs {
    time: Some(&time),
    platform: v_1::Platform::Android,
    // TODO: architecture, model, etc
    cpu_abis,
    ..Default::default()
  };
  // TODO: improved name parsing
  let error_args = v_1::ErrorArgs {
    name: Some(builder.create_string("ANR")),
    reason: subject.map(|sub| builder.create_string(sub)),
    ..Default::default()
  };
  let error = v_1::Error::create(&mut builder, &error_args);
  let threads = Some(builder.create_vector(thread_offsets.as_slice()));
  let thread_details = v_1::ThreadDetails::create(
    &mut builder,
    &v_1::ThreadDetailsArgs {
      count: u16::try_from(thread_offsets.len()).unwrap_or_default(),
      threads,
    },
  );
  let args = v_1::ReportArgs {
    errors: Some(builder.create_vector(&[error])),
    app_metrics: Some(v_1::AppMetrics::create(&mut builder, &app_args)),
    device_metrics: Some(v_1::DeviceMetrics::create(&mut builder, &device_args)),
    thread_details: Some(thread_details),
    ..Default::default()
  };
  Ok((remainder, v_1::Report::create(&mut builder, &args)))
}

fn build_thread<'a, 'fbb, E: ParseError<&'a str>>(
  mut builder: &mut flatbuffers::FlatBufferBuilder<'fbb>,
  input: &'a str,
) -> IResult<&'a str, flatbuffers::WIPOffset<v_1::Thread<'fbb>>, E> {
  let (remainder, (header, _props, frames)) = terminated(
    (
      thread_header,
      many0(thread_props),
      many1(|text| build_frame(&mut builder, text)),
    ),
    opt(tag("  (no managed stack frames)\n")),
  )
  .parse(input)?;

  let args = v_1::ThreadArgs {
    name: Some(builder.create_string(header.name)),
    index: header.tid.unwrap_or_default(),
    priority: header.priority.unwrap_or_default(),
    state: Some(builder.create_string(header.state)),
    stack_trace: Some(builder.create_vector(frames.as_slice())),
    ..Default::default()
  };
  Ok((remainder, v_1::Thread::create(&mut builder, &args)))
}

fn build_frame<'a, 'fbb, E: ParseError<&'a str>>(
  builder: &mut flatbuffers::FlatBufferBuilder<'fbb>,
  input: &'a str,
) -> IResult<&'a str, flatbuffers::WIPOffset<v_1::Frame<'fbb>>, E> {
  let builder_ref0 = Rc::new(RefCell::new(builder));
  let builder_ref1 = Rc::clone(&builder_ref0);
  preceded(
    multispace1,
    alt((
      |text| {
        let mut builder = builder_ref0.borrow_mut();
        build_java_frame(&mut builder, text)
      },
      |text| {
        let mut builder = builder_ref1.borrow_mut();
        build_native_frame(&mut builder, text)
      },
    )),
  )
  .parse(input)
}

fn build_native_frame<'a, 'fbb, E: ParseError<&'a str>>(
  mut builder: &mut flatbuffers::FlatBufferBuilder<'fbb>,
  input: &'a str,
) -> IResult<&'a str, flatbuffers::WIPOffset<v_1::Frame<'fbb>>, E> {
  map(
    native_frame_parser(),
    |(_, address, path, symbol, build_id)| {
      let (symbol_name, symbol_offset) = symbol.unzip();
      let source_file_args = v_1::SourceFileArgs {
        path: Some(builder.create_string(&path)),
        ..Default::default()
      };
      let source_file = Some(v_1::SourceFile::create(&mut builder, &source_file_args));
      let args = v_1::FrameArgs {
        type_: v_1::FrameType::AndroidNative,
        symbol_name: symbol_name.map(|name| builder.create_string(&name)),
        symbol_address: symbol_offset
          .flatten()
          .map(|off| address - off)
          .unwrap_or_default(),
        source_file,
        frame_address: address,
        image_id: build_id.map(|id| builder.create_string(&id)),
        ..Default::default()
      };
      v_1::Frame::create(&mut builder, &args)
    },
  )
  .parse(input)
}

fn build_java_frame<'a, 'fbb, E: ParseError<&'a str>>(
  mut builder: &mut flatbuffers::FlatBufferBuilder<'fbb>,
  input: &'a str,
) -> IResult<&'a str, flatbuffers::WIPOffset<v_1::Frame<'fbb>>, E> {
  map(java_frame_parser(), |(symbol, source, state)| {
    let source_file_args = v_1::SourceFileArgs {
      path: Some(builder.create_string(source.path)),
      line: source.lineno.unwrap_or_default(),
      column: 0,
    };
    let source_file = Some(v_1::SourceFile::create(&mut builder, &source_file_args));
    let state = state
      .iter()
      .map(|item| builder.create_string(item))
      .collect::<Vec<_>>();
    let state = Some(builder.create_vector(state.as_slice()));
    let args = v_1::FrameArgs {
      type_: v_1::FrameType::JVM,
      symbol_name: Some(builder.create_string(&symbol)),
      source_file,
      state,
      ..Default::default()
    };
    v_1::Frame::create(&mut builder, &args)
  })
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
        separated_pair(
          take_until(": "),
          tag(": "),
          delimited(tag("'"), take_until("'"), tag("'")),
        ),
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

fn thread_counter<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, usize, E> {
  delimited(
    (take_until("DALVIK THREADS ("), tag("DALVIK THREADS (")),
    decimal::<usize, _>,
    tag("):\n"),
  )
  .parse(input)
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

fn java_frame_parser<'a, E>()
-> impl Parser<&'a str, Output = (&'a str, Source<'a>, Vec<&'a str>), Error = E>
where
  E: ParseError<&'a str>,
{
  let symbol_parser = take_till(|c| c == '(' || c == '\n');
  (
    delimited(tag("at "), symbol_parser, tag("(")),
    terminated(source_location, tag(")\n")),
    many0(preceded(
      (multispace1, tag("- ")),
      terminated(take_until("\n"), tag("\n")),
    )),
  )
}

fn native_frame_parser<'a, E>() -> impl Parser<
  &'a str,
  Output = (
    u64,
    u64,
    &'a str,
    Option<(&'a str, Option<u64>)>,
    Option<&'a str>,
  ),
  Error = E,
>
where
  E: ParseError<&'a str>,
{
  let build_id_parser = delimited(
    tag(" (BuildId: "),
    take_till(|c| c == ')' || c == '\n'),
    tag(")"),
  );
  terminated(
    (
      delimited(tag("native: #"), decimal, tag(" pc ")),
      terminated(hexadecimal, multispace1),
      native_path_parser,
      opt(native_symbol_parser),
      opt(build_id_parser),
    ),
    tag("\n"),
  )
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
