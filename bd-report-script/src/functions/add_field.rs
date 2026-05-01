use crate::ScriptValue;
use crate::target::ReportTarget;
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
        kind: kind::ANY,
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
    ctx
      .target_mut()
      .target_insert(&ReportTarget::metrics_path(&name), value)?;
    Ok(ScriptValue::Null)
  }

  fn type_def(&self, _state: &vrl::prelude::TypeState) -> TypeDef {
    TypeDef::null().infallible().impure()
  }
}
