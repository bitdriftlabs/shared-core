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
  kind,
};

#[derive(Debug)]
pub struct SetGroupingKey;

impl Function for SetGroupingKey {
  fn identifier(&self) -> &'static str {
    "set_grouping_key"
  }

  fn examples(&self) -> &'static [vrl::prelude::Example] {
    &[]
  }

  fn parameters(&self) -> &'static [Parameter] {
    &[Parameter {
      keyword: "key",
      kind: kind::BYTES | kind::NULL,
      required: true,
    }]
  }

  fn compile(
    &self,
    _state: &vrl::prelude::TypeState,
    _ctx: &mut vrl::prelude::FunctionCompileContext,
    arguments: vrl::prelude::ArgumentList,
  ) -> vrl::prelude::Compiled {
    Ok(
      GroupingKey {
        key: arguments.required("key"),
      }
      .as_expr(),
    )
  }
}

#[derive(Clone, Debug)]
struct GroupingKey {
  key: Box<dyn Expression>,
}

impl FunctionExpression for GroupingKey {
  fn resolve(&self, ctx: &mut vrl::prelude::Context<'_>) -> vrl::prelude::Resolved {
    let key = self.key.resolve(ctx)?;
    if !key.is_bytes() && !key.is_null() {
      return Err(ExpressionError::from(
        "grouping key must be a string value or null",
      ));
    }
    let Some(data) = get_dynamic_data::<ReportOutput>(ctx) else {
      return Ok(key);
    };

    data.grouping_hints.grouping_key = key.as_str().as_deref().map(ToString::to_string);
    Ok(key)
  }

  fn type_def(&self, _state: &vrl::prelude::TypeState) -> TypeDef {
    TypeDef::bytes().infallible().impure()
  }
}
