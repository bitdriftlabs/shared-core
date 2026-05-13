// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod functions;
mod generated;
mod input;
mod output;
mod target;

#[cfg(test)]
#[path = "./script_test.rs"]
mod test;

pub use crate::input::{PathError, Scriptable};
pub use crate::output::{GroupingHints, Output};
use crate::target::ReportTarget;
use anyhow::anyhow;
use vrl::compiler::{CompileConfig, Program, compile_with_external};
use vrl::diagnostic::Formatter;
use vrl::prelude::state::{ExternalEnv, RuntimeState};
use vrl::prelude::{Collection, Context, TimeZone, Value};
use vrl::value::Kind;

#[derive(Clone, Debug)]
pub struct Script {
  source: String,
  program: Program,
}

#[derive(Debug)]
pub struct FeatureFlag {
  pub name: String,
  pub value: Option<String>,
  pub last_updated: Option<time::OffsetDateTime>,
}

#[derive(Debug)]
pub struct ScriptValue(Value);

impl From<Value> for ScriptValue {
  fn from(value: Value) -> Self {
    Self(value)
  }
}

impl From<ScriptValue> for Value {
  fn from(value: ScriptValue) -> Self {
    value.0
  }
}

impl From<&str> for ScriptValue {
  fn from(value: &str) -> Self {
    Value::Bytes(value.to_owned().into()).into()
  }
}


impl Script {
  /// Create a new script for querying report contents,
  pub fn new<T: Scriptable>(program_source: &str) -> anyhow::Result<Self> {
    let mut functions = vrl::stdlib::all();
    functions.append(&mut crate::functions::all());
    let compile_config = CompileConfig::default();

    let external = ExternalEnv::new_with_kind(T::schema(), Kind::object(Collection::empty()));

    let result = compile_with_external(program_source, &functions, &external, compile_config)
      .map_err(|e| anyhow!("Compilation error: {}", Formatter::new(program_source, e)))?;
    if !result.warnings.is_empty() {
      anyhow::bail!(Formatter::new(program_source, result.warnings).to_string(),);
    }

    Ok(Self {
      source: program_source.to_owned(),
      program: result.program,
    })
  }

  pub fn run<T: Scriptable>(
    &self,
    object: &T,
    feature_flags: &[FeatureFlag],
  ) -> anyhow::Result<Output> {
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut target = ReportTarget::new(object, feature_flags);
    let mut ctx = Context::new(&mut target, &mut state, &timezone);
    self.program.resolve(&mut ctx)?;

    Ok(target.into())
  }
}

impl PartialEq for Script {
  fn eq(&self, other: &Self) -> bool {
    self.source == other.source
  }
}
