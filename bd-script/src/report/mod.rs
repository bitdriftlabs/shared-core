// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod functions;
mod generated;

use crate::ScriptOutput;
pub use functions::all_functions as report_functions;
use std::collections::BTreeMap;
use std::ops::Add;

/// Data type used in functions targeting issue/report data
#[derive(Clone, Debug, Default)]
pub struct ReportOutput {
  pub grouping_hints: GroupingHints,
  pub metrics: BTreeMap<String, String>,
  /// Message set when `abort()` was called
  pub abort_message: Option<String>,
}

type ErrorIndex = isize;
type StackFrameIndex = isize;
type FrameData = BTreeMap<ErrorIndex, BTreeMap<StackFrameIndex, bool>>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupingHints {
  pub grouping_key: Option<String>,
  /// mapping of error index to frame index to whether a frame is significant
  /// for grouping
  significant_frame_data: FrameData,
}

impl ScriptOutput for ReportOutput {
  fn did_abort(&self) -> bool {
    self.abort_message.is_some()
  }
}

impl GroupingHints {
  #[must_use]
  pub fn is_significant_frame(&self, error_index: isize, frame_index: isize) -> Option<bool> {
    self
      .significant_frame_data
      .get(&error_index)
      .and_then(|error| error.get(&frame_index))
      .copied()
  }

  pub fn set_significant_frame(
    &mut self,
    error_index: ErrorIndex,
    frame_index: StackFrameIndex,
    is_significant: Option<bool>,
  ) {
    let error_data = self.significant_frame_data.entry(error_index).or_default();
    if let Some(bool_value) = is_significant {
      error_data.insert(frame_index, bool_value);
    } else {
      error_data.remove(&frame_index);
    }
  }

  #[must_use]
  pub fn new(grouping_key: Option<String>, frame_data: FrameData) -> Self {
    Self {
      grouping_key,
      significant_frame_data: frame_data,
    }
  }
}

impl Add for ReportOutput {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    let mut metrics = self.metrics;
    metrics.append(&mut rhs.metrics.clone());
    Self {
      grouping_hints: self.grouping_hints + rhs.grouping_hints,
      metrics,
      abort_message: None,
    }
  }
}

impl Add for GroupingHints {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    let significant_frame_data = rhs
      .significant_frame_data
      .keys()
      .chain(self.significant_frame_data.keys())
      .map(|err_index| {
        let rhs_frame_data = rhs
          .significant_frame_data
          .get(err_index)
          .map(ToOwned::to_owned);
        let frame_data = self
          .significant_frame_data
          .get(err_index)
          .map(|frame_data| {
            let mut frame_data = frame_data.to_owned();
            frame_data.append(&mut rhs_frame_data.clone().unwrap_or_default());
            frame_data
          })
          .or(rhs_frame_data)
          .unwrap_or_default();
        (*err_index, frame_data)
      })
      .collect();
    Self {
      grouping_key: rhs.grouping_key.or(self.grouping_key),
      significant_frame_data,
    }
  }
}
