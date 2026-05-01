// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./script_test.rs"]
mod test;

use crate::{get_json_value, get_report_schema};
use anyhow::anyhow;
use serde_json::Value as JSONValue;
use std::collections::BTreeMap;
use vrl::compiler::{
  CompileConfig,
  OwnedValueOrRef,
  Program,
  SecretTarget,
  Target,
  compile_with_external,
};
use vrl::diagnostic::Formatter;
use vrl::path::{OwnedSegment, OwnedTargetPath, PathPrefix};
use vrl::prelude::state::{ExternalEnv, RuntimeState};
use vrl::prelude::{Collection, Context, TimeZone, Value as ScriptValue};
use vrl::value::Kind;

const GROUPING_KEY: &str = "grouping_key";

#[derive(Debug)]
pub struct Script {
  program: Program,
}

#[derive(Debug)]
pub struct Output {
  pub grouping_key: Option<String>,
}

impl Script {
  /// Create a new script for querying report contents,
  pub fn new(program_source: &str) -> anyhow::Result<Self> {
    let functions = vrl::stdlib::all();
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

  pub fn run(&self, report: &JSONValue) -> anyhow::Result<Output> {
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut target = ReportTarget::new(report);
    let mut ctx = Context::new(&mut target, &mut state, &timezone);
    self.program.resolve(&mut ctx)?;

    Ok(Output {
      grouping_key: target.get_computed(GROUPING_KEY),
    })
  }
}

#[derive(Debug)]
struct ReportTarget<'a> {
  report: &'a JSONValue,
  variables: BTreeMap<String, ScriptValue>,
}

impl<'a> ReportTarget<'a> {
  fn new(report: &'a JSONValue) -> Self {
    Self {
      report,
      variables: BTreeMap::new(),
    }
  }

  fn get_computed(&self, key: &str) -> Option<String> {
    self.variables.get(key).and_then(|value| {
      if let ScriptValue::Bytes(value) = value
        && let Ok(value) = String::from_utf8(value.iter().as_slice().into())
      {
        Some(value)
      } else {
        None
      }
    })
  }
}

impl SecretTarget for ReportTarget<'_> {
  fn get_secret(&self, _key: &str) -> Option<&str> {
    None
  }

  fn insert_secret(&mut self, _key: &str, _value: &str) {}

  fn remove_secret(&mut self, _key: &str) {}
}

impl Target for ReportTarget<'_> {
  fn target_insert(&mut self, path: &OwnedTargetPath, value: ScriptValue) -> Result<(), String> {
    if path.prefix == PathPrefix::Metadata {
      return Ok(());
    }

    // In the future we can do something more sophisticated with known/allowed
    // keys but the current combo of type checks + only allowing single-level of
    // adding values should be sufficient for now
    let components = &path.path.segments;
    if components.len() == 1
      && let OwnedSegment::Field(name) = &components[0]
    {
      self.variables.insert(name.to_owned().to_string(), value);
    }

    Ok(())
  }

  fn target_get(
    &mut self,
    path: &vrl::path::OwnedTargetPath,
  ) -> Result<Option<vrl::compiler::OwnedValueOrRef<'_>>, String> {
    match get_json_value(self.report, path) {
      Ok(value) => Ok(value),
      Err(error) => {
        let components = &path.path.segments;
        if path.prefix == PathPrefix::Event
          && error.index == 0
          && components.len() == 1
          && let OwnedSegment::Field(name) = &components[0]
          && let Some(variable) = self.variables.get(name.as_str())
        {
          return Ok(Some(OwnedValueOrRef::Ref(variable)));
        }

        Err(error.to_string())
      },
    }
  }

  fn target_remove(
    &mut self,
    path: &vrl::path::OwnedTargetPath,
    _compact: bool,
  ) -> Result<Option<vrl::prelude::Value>, String> {
    let components = &path.path.segments;
    if components.len() == 1
      && let OwnedSegment::Field(name) = &components[0]
    {
      return Ok(self.variables.remove(&name.to_string()));
    }

    Ok(None)
  }
}
