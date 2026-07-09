// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod input;
pub mod report;
mod target;

#[cfg(test)]
#[path = "./script_test.rs"]
mod test;

pub use crate::input::{PathError, Scriptable};
use crate::target::ScriptableTarget;
use anyhow::anyhow;
use std::fmt::Debug;
use vrl::compiler::{CompileConfig, Program, compile_with_external};
use vrl::diagnostic::Formatter;
use vrl::parser::ast::{Program as AST, SyntaxEq};
use vrl::prelude::state::{ExternalEnv, RuntimeState};
use vrl::prelude::{Collection, Context, Function, TimeZone, Value};
use vrl::value::Kind;

#[derive(Clone, Debug)]
pub struct Script {
  program: Program,
  ast: AST,
}

#[derive(Debug)]
pub struct ScriptValue(pub Value);

pub trait ScriptOutput: Default + Debug {}

impl Script {
  /// Create a new script
  pub fn new<T: Scriptable>(
    program_source: &str,
    custom_functions: Vec<Box<dyn Function>>,
  ) -> anyhow::Result<Self> {
    let mut functions = vrl::stdlib::all();
    functions.extend(custom_functions);
    let compile_config = CompileConfig::default();

    let external = ExternalEnv::new_with_kind(T::schema(), Kind::object(Collection::empty()));

    let result = compile_with_external(program_source, &functions, &external, compile_config)
      .map_err(|e| anyhow!("Compilation error: {}", Formatter::new(program_source, e)))?;
    if !result.warnings.is_empty() {
      anyhow::bail!(Formatter::new(program_source, result.warnings).to_string(),);
    }

    Ok(Self {
      program: result.program,
      ast: result.ast,
    })
  }

  pub fn run<T: Scriptable, O: ScriptOutput + 'static>(&self, object: &T) -> anyhow::Result<O> {
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut target = ScriptableTarget::new(object);
    let mut dynamic_data = O::default();
    let mut ctx =
      Context::new(&mut target, &mut state, &timezone).with_dynamic_state(&mut dynamic_data);
    self.program.resolve(&mut ctx)?;
    Ok(dynamic_data)
  }
}

impl PartialEq for Script {
  fn eq(&self, other: &Self) -> bool {
    self.ast.syntax_eq(&other.ast)
  }
}

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
