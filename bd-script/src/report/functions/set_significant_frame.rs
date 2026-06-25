// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::get_dynamic_data;
use crate::report::ReportOutput;
use vrl::compiler::expression::Literal;
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

#[allow(dead_code)]
#[derive(Debug)]
pub struct SetSignificantFrame;

impl Function for SetSignificantFrame {
  fn identifier(&self) -> &'static str {
    "set_significant_frame"
  }

  fn examples(&self) -> &'static [vrl::prelude::Example] {
    &[]
  }

  fn usage(&self) -> &'static str {
    ""
  }

  fn category(&self) -> &'static str {
    ""
  }

  fn pure(&self) -> bool {
    false
  }

  fn return_kind(&self) -> u16 {
    kind::NULL
  }

  fn parameters(&self) -> &'static [Parameter] {
    &[
      Parameter {
        keyword: "error_index",
        kind: kind::INTEGER,
        required: true,
        description: "",
        default: None,
        enum_variants: None,
      },
      Parameter {
        keyword: "frame_index",
        kind: kind::INTEGER,
        required: true,
        description: "",
        default: None,
        enum_variants: None,
      },
      Parameter {
        keyword: "is_significant",
        kind: kind::BOOLEAN | kind::NULL,
        required: false,
        description: "",
        default: None,
        enum_variants: None,
      },
    ]
  }

  fn compile(
    &self,
    _state: &vrl::prelude::TypeState,
    _ctx: &mut vrl::prelude::FunctionCompileContext,
    arguments: vrl::prelude::ArgumentList,
  ) -> vrl::prelude::Compiled {
    let error_index = arguments.required("error_index");
    let frame_index = arguments.required("frame_index");
    let is_significant = arguments
      .optional("is_significant")
      .unwrap_or_else(|| Box::new(Literal::Boolean(true)));

    Ok(
      FrameData {
        error_index,
        frame_index,
        is_significant,
      }
      .as_expr(),
    )
  }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct FrameData {
  error_index: Box<dyn Expression>,
  frame_index: Box<dyn Expression>,
  is_significant: Box<dyn Expression>,
}

impl FunctionExpression for FrameData {
  fn resolve(&self, ctx: &mut vrl::prelude::Context<'_>) -> vrl::prelude::Resolved {
    let error_index = resolve_isize(ctx, &self.error_index, "error_index")?;
    let frame_index = resolve_isize(ctx, &self.frame_index, "frame_index")?;
    let is_significant = self.is_significant.resolve(ctx)?;
    if is_significant.is_boolean() || is_significant.is_null() {
      let Some(data) = get_dynamic_data::<ReportOutput>(ctx) else {
        return Ok(Value::Null);
      };
      data.grouping_hints.set_significant_frame(
        error_index,
        frame_index,
        is_significant.as_boolean(),
      );
      Ok(Value::Null)
    } else {
      Err(ExpressionError::from(
        "is_significant must be null or a boolean value",
      ))
    }
  }

  fn type_def(&self, _: &vrl::prelude::TypeState) -> vrl::prelude::TypeDef {
    TypeDef::null().infallible().impure()
  }
}

#[allow(clippy::borrowed_box)]
fn resolve_isize(
  ctx: &mut vrl::prelude::Context<'_>,
  value: &Box<dyn Expression>,
  name: &str,
) -> Result<isize, ExpressionError> {
  let value = value.resolve(ctx)?;
  let Some(int_value) = value.as_integer() else {
    return Err(ExpressionError::from(format!(
      "{name} must be a positive integer"
    )));
  };
  if let Ok(isize_value) = isize::try_from(int_value)
    && isize_value >= 0
  {
    return Ok(isize_value);
  }
  Err(ExpressionError::from(format!(
    "{name} {int_value} is out of range - must be a positive integer"
  )))
}
