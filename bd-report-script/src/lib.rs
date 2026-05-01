// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod functions;
mod generated;
mod target;

#[cfg(test)]
#[path = "./script_test.rs"]
mod test;

use crate::generated::get_report_schema;
use crate::target::ReportTarget;
use anyhow::anyhow;
use bd_log_primitives::LogFieldValue;
use std::collections::BTreeMap;
use vrl::compiler::{CompileConfig, Program, compile_with_external};
use vrl::diagnostic::Formatter;
pub use vrl::prelude::Value as ScriptValue;
use vrl::prelude::state::{ExternalEnv, RuntimeState};
use vrl::prelude::{Collection, Context, TimeZone};
use vrl::value::Kind;

#[derive(Debug)]
pub struct Script {
  program: Program,
}

#[derive(Debug)]
pub struct FeatureFlag {
  pub name: String,
  pub value: Option<String>,
  pub last_updated: time::OffsetDateTime,
}

#[derive(Debug)]
pub struct Output {
  pub grouping_key: Option<String>,
  pub metrics: BTreeMap<String, LogFieldValue>,
  /// mapping of error index to frame index to whether a frame is significant
  /// for grouping
  significant_frame_data: BTreeMap<isize, BTreeMap<isize, bool>>,
}

impl Output {
  /// Check if a frame should be used for grouping, enhanced with script target
  /// state. An existing value indicates the significance state has been set
  /// explicitly in the script
  #[must_use]
  pub fn is_significant_frame(&self, error_index: isize, frame_index: isize) -> Option<bool> {
    self
      .significant_frame_data
      .get(&error_index)
      .and_then(|error| error.get(&frame_index))
      .copied()
  }
}

impl Script {
  /// Create a new script for querying report contents,
  pub fn new(program_source: &str) -> anyhow::Result<Self> {
    let mut functions = vrl::stdlib::all();
    functions.append(&mut crate::functions::all());
    let compile_config = CompileConfig::default();

    let external =
      ExternalEnv::new_with_kind(get_report_schema(), Kind::object(Collection::empty()));

    let result = compile_with_external(program_source, &functions, &external, compile_config)
      .map_err(|e| anyhow!("Compilation error: {}", Formatter::new(program_source, e)))?;
    if !result.warnings.is_empty() {
      anyhow::bail!(Formatter::new(program_source, result.warnings).to_string(),);
    }

    Ok(Self {
      program: result.program,
    })
  }

  pub fn run<T: Into<ScriptValue>>(
    &self,
    report: T,
    feature_flags: &[FeatureFlag],
  ) -> anyhow::Result<Output> {
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut target = ReportTarget::new(report, feature_flags);
    let mut ctx = Context::new(&mut target, &mut state, &timezone);
    self.program.resolve(&mut ctx)?;

    Ok(target.into())
  }
}
