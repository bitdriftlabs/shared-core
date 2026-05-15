// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::get_dynamic_data;
use crate::report::ReportOutput;
use vrl::parser::Span;
use vrl::prelude::expression::Literal;
use vrl::prelude::{
  Expression,
  ExpressionError,
  Function,
  FunctionExpression,
  Parameter,
  TypeDef,
  kind,
};

#[derive(Debug)]
pub struct Abort;

#[derive(Clone, Debug)]
struct Aborted {
  message: Box<dyn Expression>,
}


impl Function for Abort {
  fn identifier(&self) -> &'static str {
    "abort"
  }

  fn examples(&self) -> &'static [vrl::prelude::Example] {
    &[]
  }

  fn parameters(&self) -> &'static [Parameter] {
    &[Parameter {
      keyword: "message",
      kind: kind::BYTES,
      required: false,
    }]
  }

  fn compile(
    &self,
    _state: &vrl::prelude::TypeState,
    _ctx: &mut vrl::prelude::FunctionCompileContext,
    arguments: vrl::prelude::ArgumentList,
  ) -> vrl::prelude::Compiled {
    Ok(
      Aborted {
        message: arguments
          .optional("message")
          .unwrap_or_else(|| Box::new(Literal::String("ended execution using abort()".into()))),
      }
      .as_expr(),
    )
  }
}

impl FunctionExpression for Aborted {
  fn resolve(&self, ctx: &mut vrl::prelude::Context<'_>) -> vrl::prelude::Resolved {
    let message = self.message.resolve(ctx)?;
    if let Some(data) = get_dynamic_data::<ReportOutput>(ctx) {
      data.abort_message = message.as_str().as_deref().map(ToString::to_string);
      let span = Span::new(0, 0);
      return Err(ExpressionError::Abort {
        span,
        message: Some("ended execution using abort()".to_string()),
      });
    }

    Ok(vrl::core::Value::Null)
  }

  fn type_def(&self, _state: &vrl::prelude::TypeState) -> TypeDef {
    TypeDef::bytes().infallible().impure()
  }
}
