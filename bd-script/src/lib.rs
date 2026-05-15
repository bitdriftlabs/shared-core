// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod feature_flag;
mod functions;
mod input;
pub mod report;
mod target;

#[cfg(test)]
#[path = "./script_test.rs"]
mod test;

pub use crate::input::Scriptable;
use crate::target::ScriptableTarget;
use anyhow::anyhow;
use std::fmt::Debug;
use vrl::compiler::{CompileConfig, Program, compile_with_external};
use vrl::diagnostic::Formatter;
use vrl::prelude::state::{ExternalEnv, RuntimeState};
use vrl::prelude::{Collection, Context, ExpressionError, Function, TimeZone, Value};
use vrl::value::Kind;

#[derive(Clone, Debug)]
pub struct Script {
  source: String,
  program: Program,
}

#[derive(Debug)]
pub struct ScriptValue(Value);

pub trait ScriptOutput: Default + Debug {
  /// true if `abort()` was intentionally called and output still desired
  fn did_abort(&self) -> bool;
}

impl Script {
  /// Create a new script
  pub fn new<T: Scriptable>(
    program_source: &str,
    custom_functions: Vec<Box<dyn Function>>,
  ) -> anyhow::Result<Self> {
    let mut functions = vrl::stdlib::all();
    functions.extend(crate::functions::stdlib());
    functions.extend(custom_functions);
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

  pub fn run<T: Scriptable, O: ScriptOutput + 'static>(&self, object: &T) -> anyhow::Result<O> {
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut target = ScriptableTarget::new(object);
    let mut dynamic_data = O::default();
    let mut ctx =
      Context::new(&mut target, &mut state, &timezone).with_dynamic_state(&mut dynamic_data);
    match self.program.resolve(&mut ctx) {
      Ok(_) => Ok(dynamic_data),
      Err(ExpressionError::Abort { span, message }) => {
        if dynamic_data.did_abort() {
          Ok(dynamic_data)
        } else {
          Err(ExpressionError::Abort { span, message }.into())
        }
      },
      Err(other) => Err(other.into()),
    }
  }
}

impl PartialEq for Script {
  fn eq(&self, other: &Self) -> bool {
    self.source == other.source
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

/// Downcast the dynamic state in a context to a particular type, if possible
pub fn get_dynamic_data<'a, 'b, T: 'b + 'static>(ctx: &'a mut Context<'b>) -> Option<&'a mut T> {
  ctx.dynamic_state().and_then(|d| d.downcast_mut::<T>())
}
