// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::target::significant_frame_path;
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

#[derive(Debug)]
pub struct SetSignificantFrame;

impl Function for SetSignificantFrame {
  fn identifier(&self) -> &'static str {
    "set_significant_frame"
  }

  fn examples(&self) -> &'static [vrl::prelude::Example] {
    &[]
  }

  fn parameters(&self) -> &'static [Parameter] {
    &[
      Parameter {
        keyword: "error_index",
        kind: kind::INTEGER,
        required: true,
      },
      Parameter {
        keyword: "frame_index",
        kind: kind::INTEGER,
        required: true,
      },
      Parameter {
        keyword: "is_significant",
        kind: kind::BOOLEAN | kind::NULL,
        required: false,
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
      ctx.target_mut().target_insert(
        &significant_frame_path(error_index, frame_index),
        is_significant,
      )?;
    } else {
      return Err(ExpressionError::from(
        "is_significant must be null or a boolean value",
      ));
    }

    Ok(Value::Null)
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
