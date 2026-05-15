// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::get_dynamic_data;
use crate::report::ReportOutput;
use vrl::prelude::{
  Expression,
  ExpressionError,
  Function,
  FunctionExpression,
  Parameter,
  TypeDef,
  Value,
  kind,
};

#[derive(Debug)]
pub struct AddField;

impl Function for AddField {
  fn identifier(&self) -> &'static str {
    "add_field"
  }

  fn examples(&self) -> &'static [vrl::prelude::Example] {
    &[]
  }

  fn parameters(&self) -> &'static [Parameter] {
    &[
      Parameter {
        keyword: "name",
        kind: kind::BYTES,
        required: true,
      },
      Parameter {
        keyword: "value",
        kind: kind::BYTES,
        required: true,
      },
    ]
  }

  fn compile(
    &self,
    _state: &vrl::prelude::TypeState,
    _ctx: &mut vrl::prelude::FunctionCompileContext,
    arguments: vrl::prelude::ArgumentList,
  ) -> vrl::prelude::Compiled {
    Ok(
      FieldData {
        name: arguments.required("name"),
        value: arguments.required("value"),
      }
      .as_expr(),
    )
  }
}

#[derive(Clone, Debug)]
struct FieldData {
  name: Box<dyn Expression>,
  value: Box<dyn Expression>,
}

impl FunctionExpression for FieldData {
  fn resolve(&self, ctx: &mut vrl::prelude::Context<'_>) -> vrl::prelude::Resolved {
    let name_value = self.name.resolve(ctx)?;
    let Some(name) = name_value.as_str() else {
      return Err(ExpressionError::from("name must be a string value"));
    };
    let value = self.value.resolve(ctx)?;
    let Some(value) = value.as_str() else {
      return Err(ExpressionError::from("value must be a string value"));
    };
    let Some(data) = get_dynamic_data::<ReportOutput>(ctx) else {
      return Ok(Value::Null);
    };
    data.metrics.insert(name.to_string(), value.to_string());
    Ok(Value::Null)
  }

  fn type_def(&self, _state: &vrl::prelude::TypeState) -> TypeDef {
    TypeDef::null().infallible().impure()
  }
}
