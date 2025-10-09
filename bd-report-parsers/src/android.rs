// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::MemmapView;
use crate::{decimal, hexadecimal};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{take_till, take_until, take_until1, take_while1};
use nom::bytes::{tag, take};
use nom::combinator::{map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated};
use nom::{AsChar, FindSubstring, IResult, Input, Parser};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

#[cfg(test)]
#[path = "./android_tests.rs"]
mod tests;

#[derive(Debug)]
pub struct Source {
  pub path: String,
  pub lineno: Option<i64>,
}

#[derive(Debug)]
pub struct ThreadHeader {
  pub name: String,
  pub is_daemon: bool,
  pub tid: Option<u32>,
  pub priority: Option<f32>,
  pub state: String,
}

struct Stacks<'a> {
  args: v_1::ThreadDetailsArgs<'a>,
  binary_images: Option<WIPOffset<Vector<'a, ForwardsUOffset<v_1::BinaryImage<'a>>>>>,
  main_stack: Option<WIPOffset<Vector<'a, ForwardsUOffset<v_1::Frame<'a>>>>>,
}

type BinaryImagePath = String;
type BinaryImageKey = (BinaryImagePath, Option<u64>);

const MAIN_THREAD: &str = "main";

pub fn build_anr<'a, 'fbb>(
  builder: &mut FlatBufferBuilder<'fbb>,
  app_info: &mut v_1::AppMetricsArgs<'fbb>,
  device_info: &mut v_1::DeviceMetricsArgs<'fbb>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, WIPOffset<v_1::Report<'fbb>>, nom::error::Error<MemmapView<'a>>> {
  let (remainder, (subject, (pid, _timestamp), metrics, _attached_count, stacks)) = (
    subject_parser,
    process_start_parser,
    process_properties,
    thread_counter,
    |text| build_threads(builder, text),
  )
    .parse(input)?;

  if let Ok(pid) = u32::try_from(pid) {
    app_info.process_id = pid;
  }
  device_info.platform = v_1::Platform::Android;
  device_info.cpu_abis = metrics.get("ABI").map(|abi| {
    let abi = builder.create_string(abi);
    builder.create_vector(&[abi])
  });
  let error_args = v_1::ErrorArgs {
    name: Some(builder.create_string(anr_name(subject.as_deref()))),
    reason: subject.map(|sub| builder.create_string(&sub)),
    stack_trace: stacks.main_stack,
    ..Default::default()
  };
  let error = v_1::Error::create(builder, &error_args);
  let thread_details = v_1::ThreadDetails::create(builder, &stacks.args);
  let args = v_1::ReportArgs {
    type_: v_1::ReportType::AppNotResponding,
    errors: Some(builder.create_vector(&[error])),
    app_metrics: Some(v_1::AppMetrics::create(builder, app_info)),
    device_metrics: Some(v_1::DeviceMetrics::create(builder, device_info)),
    thread_details: Some(thread_details),
    binary_images: stacks.binary_images,
    ..Default::default()
  };
  Ok((remainder, v_1::Report::create(builder, &args)))
}

fn build_threads<'a, 'fbb, E: ParseError<MemmapView<'a>>>(
  builder: &mut FlatBufferBuilder<'fbb>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, Stacks<'fbb>, E> {
  let mut images = BTreeMap::new();
  let (remainder, thread_infos) =
    separated_list1(tag("\n"), |text| build_thread(builder, &mut images, text)).parse(input)?;
  let thread_offsets = thread_infos
    .iter()
    .map(|args| v_1::Thread::create(builder, args))
    .collect_vec();
  let threads =
    (!thread_offsets.is_empty()).then(|| builder.create_vector(thread_offsets.as_slice()));
  // use offset in key to support multiple mappings of the same file
  let binary_images = images
    .iter()
    .map(|((path, offset), id)| {
      let args = v_1::BinaryImageArgs {
        id: id.clone().map(|id| builder.create_string(&id)),
        path: Some(builder.create_string(path)),
        load_address: offset.unwrap_or_default(),
      };
      v_1::BinaryImage::create(builder, &args)
    })
    .collect_vec();
  Ok((
    remainder,
    Stacks {
      args: v_1::ThreadDetailsArgs {
        count: u16::try_from(thread_offsets.len()).unwrap_or_default(),
        threads,
      },
      binary_images: (!binary_images.is_empty())
        .then(|| builder.create_vector(binary_images.as_slice())),
      main_stack: thread_infos
        .iter()
        .find(|t| t.active)
        .and_then(|t| t.stack_trace),
    },
  ))
}

fn build_thread<'a, 'fbb, E: ParseError<MemmapView<'a>>>(
  builder: &mut FlatBufferBuilder<'fbb>,
  images: &mut BTreeMap<BinaryImageKey, Option<String>>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, v_1::ThreadArgs<'fbb>, E> {
  let (remainder, (header, _props, frames)) = terminated(
    terminated(
      (
        thread_header,
        many0(thread_props),
        many1(|text| build_frame(builder, images, text)),
      ),
      opt(tag("  (no managed stack frames)\n")),
    ),
    opt(process_properties),
  )
  .parse(input)?;

  let args = v_1::ThreadArgs {
    active: header.name == MAIN_THREAD,
    name: Some(builder.create_string(&header.name)),
    index: header.tid.unwrap_or_default(),
    priority: header.priority.unwrap_or_default(),
    state: Some(builder.create_string(&header.state)),
    stack_trace: Some(builder.create_vector(frames.as_slice())),
    ..Default::default()
  };
  Ok((remainder, args))
}

fn build_frame<'a, 'fbb, E: ParseError<MemmapView<'a>>>(
  builder: &mut FlatBufferBuilder<'fbb>,
  images: &mut BTreeMap<BinaryImageKey, Option<String>>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, WIPOffset<v_1::Frame<'fbb>>, E> {
  let builder_ref0 = Rc::new(RefCell::new(builder));
  let builder_ref1 = Rc::clone(&builder_ref0);
  preceded(
    take_while1(AsChar::is_space),
    alt((
      |text| {
        let mut builder = builder_ref0.borrow_mut();
        build_java_frame(&mut builder, text)
      },
      |text| {
        let mut builder = builder_ref1.borrow_mut();
        build_native_frame(&mut builder, images, text)
      },
    )),
  )
  .parse(input)
}

fn build_native_frame<'a, 'fbb, E: ParseError<MemmapView<'a>>>(
  builder: &mut FlatBufferBuilder<'fbb>,
  images: &mut BTreeMap<BinaryImageKey, Option<String>>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, WIPOffset<v_1::Frame<'fbb>>, E> {
  let build_id_parser = delimited(
    tag(" (BuildId: "),
    take_till(|c| c == &b')' || c == &b'\n'),
    tag(")"),
  );
  map(
    terminated(
      (
        delimited(tag("native: #"), decimal::<_, u64, E>, tag(" pc ")),
        terminated(hexadecimal, take_while1(AsChar::is_space)),
        native_path_parser,
        opt(delimited(tag(" (offset "), hexadecimal, tag(")"))),
        opt(native_symbol_parser),
        opt(map(build_id_parser, |build_id: MemmapView<'a>| {
          build_id.to_string()
        })),
      ),
      tag("\n"),
    ),
    |(_, address, path, offset, symbol, build_id)| {
      let (symbol_name, symbol_offset) = symbol.unzip();
      images.insert((path.clone(), offset), build_id.clone());
      let args = v_1::FrameArgs {
        type_: v_1::FrameType::AndroidNative,
        symbol_name: symbol_name.map(|name| builder.create_string(&name)),
        symbol_address: symbol_offset
          .flatten()
          .map(|off| address - off)
          .unwrap_or_default(),
        frame_address: address,
        image_id: Some(builder.create_string(&build_id.unwrap_or(path))),
        ..Default::default()
      };
      v_1::Frame::create(builder, &args)
    },
  )
  .parse(input)
}

fn build_java_frame<'a, 'fbb, E: ParseError<MemmapView<'a>>>(
  builder: &mut FlatBufferBuilder<'fbb>,
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, WIPOffset<v_1::Frame<'fbb>>, E> {
  let symbol_parser = take_till(|c| c == &b'(' || c == &b'\n');
  map(
    (
      delimited(tag("at "), symbol_parser, tag("(")),
      terminated(source_location, tag(")\n")),
      many0(preceded(
        (take_while1(AsChar::is_space), tag("- ")),
        terminated(take_until("\n"), tag("\n")),
      )),
    ),
    |(symbol, source, state)| {
      let source_file_args = v_1::SourceFileArgs {
        path: Some(builder.create_string(&source.path)),
        line: source.lineno.unwrap_or_default(),
        column: 0,
      };
      let source_file = Some(v_1::SourceFile::create(builder, &source_file_args));
      let state = state
        .iter()
        .map(|item| builder.create_string(item.as_str()))
        .collect_vec();
      let args = v_1::FrameArgs {
        type_: v_1::FrameType::JVM,
        symbol_name: Some(builder.create_string(symbol.as_str())),
        source_file,
        state: (!state.is_empty()).then(|| builder.create_vector(state.as_slice())),
        ..Default::default()
      };
      v_1::Frame::create(builder, &args)
    },
  )
  .parse(input)
}

fn subject_parser<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, Option<String>, E> {
  let (remainder, subject) = opt(preceded(tag("Subject: "), take_until("\n"))).parse(input)?;

  Ok((remainder, subject.map(|s| s.to_string())))
}

fn process_start_parser<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, (u64, String), E> {
  (
    delimited(
      (take_until("----- pid "), tag("----- pid ")),
      decimal::<_, u64, _>,
      tag(" at "),
    ),
    map(
      terminated(take_until(" -----"), tag(" -----\n")),
      |start: MemmapView<'a>| start.to_string(),
    ),
  )
    .parse(input)
}

fn process_properties<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, BTreeMap<String, String>, E> {
  map(many1(process_property), |pairs| {
    pairs.into_iter().map(|pair| (pair.0, pair.1)).collect()
  })
  .parse(input)
}

fn process_property<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, (String, String), E> {
  let line_end = input.find('\n').map_or(input.len(), |n| n + 1);
  if line_end == 0 {
    return Err(nom::Err::Error(E::from_error_kind(
      input,
      ErrorKind::TakeUntil,
    )));
  }
  let line = input.take(line_end);
  let line_len = line.len();
  if line.find_substring("DALVIK THREADS").is_some() {
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
        pair(take_till(|c: &u8| c.is_ascii_digit()), take_until("\n")),
        map(take_until1("\n"), |s: MemmapView<'a>| {
          (s.clone(), s.take(0))
        }),
      )),
      tag("\n"),
    ),
    |(key, value)| (key.to_string().trim().to_owned(), value.to_string()),
  )
  .parse(line)
  .map(|(_, values)| (input.take_from(line_len), values))
}

fn thread_header<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, ThreadHeader, E> {
  let name_parser = delimited(tag("\""), take_till(|c| c == &b'"'), tag("\""));
  let int_transform = |num: MemmapView<'a>| num.as_str().parse().unwrap_or_default();
  let float_transform = |num: MemmapView<'a>| num.as_str().parse().unwrap_or_default();
  let (remainder, (name, is_daemon, priority, tid, state)) = (
    map(terminated(name_parser, tag(" ")), |name: MemmapView<'a>| {
      name.to_string()
    }),
    opt(map(terminated(tag("daemon"), tag(" ")), |_| true)),
    opt(map(
      delimited(tag("prio="), take_till(|c| c == &b' '), tag(" ")),
      float_transform,
    )),
    opt(map(
      delimited(tag("tid="), take_till(|c| c == &b' '), tag(" ")),
      int_transform,
    )),
    map(
      terminated(take_till(|c| c == &b'\n'), tag("\n")),
      |name: MemmapView<'a>| name.to_string(),
    ),
  )
    .parse(input)?;

  Ok((
    remainder,
    ThreadHeader {
      name,
      is_daemon: is_daemon.unwrap_or_default(),
      tid,
      priority,
      state,
    },
  ))
}

fn thread_counter<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, usize, E> {
  delimited(
    (take_until("DALVIK THREADS ("), tag("DALVIK THREADS (")),
    decimal::<_, usize, _>,
    tag("):\n"),
  )
  .parse(input)
}

fn thread_props<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, Vec<(String, String)>, E> {
  terminated(
    preceded(
      (take_while1(AsChar::is_space), tag("| ")),
      separated_list0(
        tag(" "),
        (
          map(
            terminated(take_till(|c| c == &b'='), tag("=")),
            |key: MemmapView<'a>| key.to_string(),
          ),
          thread_prop_value,
        ),
      ),
    ),
    tag("\n"),
  )
  .parse(input)
}

fn thread_prop_value<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, String, E> {
  let line_end = input.find('\n').unwrap_or(input.len());
  let segment = input.take(line_end);
  let line = segment.as_str();
  let value_end = line
    .find('=')
    .and_then(|index| line[.. index].rfind(' '))
    .unwrap_or(line_end);
  let (remainder, value) = take(value_end).parse(input)?;
  Ok((remainder, value.to_string()))
}

fn native_symbol_parser<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, (String, Option<u64>), E> {
  delimited(
    tag(" ("),
    alt((
      map(
        (
          take_till(|c: &u8| c == &b'+' || c == &b'\n'),
          preceded(tag("+"), decimal),
        ),
        |(symbol, offset): (MemmapView<'a>, u64)| (symbol.to_string(), Some(offset)),
      ),
      map(
        take_till(|c: &u8| c == &b')' || c == &b'\n'),
        |symbol: MemmapView<'a>| (symbol.to_string(), None),
      ),
    )),
    tag(")"),
  )
  .parse(input)
}

fn native_path_parser<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, String, E> {
  let line_end = input.find('\n').unwrap_or(input.len());
  let segment = input.take(line_end);
  let line = segment.as_str();
  let path_end = line
    .find(" (offset")
    .or_else(|| {
      line
        .rfind(" (BuildId:")
        .and_then(|build_index| line[.. build_index].rfind(" ("))
    })
    .or_else(|| line.rfind(" ("))
    .unwrap_or(line_end);
  let (remainder, path) = take(path_end).parse(input)?;
  Ok((remainder, path.to_string()))
}

fn source_location<'a, E: ParseError<MemmapView<'a>>>(
  input: MemmapView<'a>,
) -> IResult<MemmapView<'a>, Source, E> {
  let (remainder, path) = take_while1(|c| c != &b')' && c != &b':').parse(input)?;
  if remainder.is_empty() || remainder.peek() == Some(b')') {
    Ok((
      remainder,
      Source {
        path: path.to_string(),
        lineno: None,
      },
    ))
  } else {
    map(preceded(tag(":"), decimal::<_, i64, _>), |lineno| Source {
      path: path.to_string(),
      lineno: Some(lineno),
    })
    .parse(remainder)
  }
}

fn anr_name(description: Option<&str>) -> &'static str {
  const DEFAULT_ANR_NAME: &str = "Undetermined ANR";
  const ANR_TYPES: &[(&str, &[&str])] = &[
    ("Background ANR", &["bg anr"]),
    (
      "User Perceived ANR",
      &["input dispatching timed out", "user request after error"],
    ),
    ("Broadcast Receiver ANR", &["broadcast of intent"]),
    ("Content Provider ANR", &["content provider timeout"]),
    ("App Registered ANR", &["app registered timeout"]),
    ("App Start ANR", &["app start timeout", "failed to complete startup"]),
    (
      "Service ANR",
      &[
        "executing service",
        "service.startforeground() not called",
        "short fgs timeout",
        "timed out while trying to bind",
        "job service timeout",
        "no response to onstopjob",
        "service start timeout",
      ],
    ),
  ];

  let Some(description) = description else {
    return DEFAULT_ANR_NAME;
  };

  let normalized = description.to_lowercase();
  for (name, snippets) in ANR_TYPES {
    for snippet in *snippets {
      if normalized.contains(snippet) {
        return name;
      }
    }
  }

  DEFAULT_ANR_NAME
}
