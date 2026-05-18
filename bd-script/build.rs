// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::{anyhow, bail};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::{File, read_to_string};
use std::io::Write;
use std::process::{Command, Stdio};

const SCHEMA: &str = "../bd-proto/src/flatbuffers/report.schema.json";
const FBS: &str = "../api/src/bitdrift_public/fbs/issue-reporting/v1/report.fbs";

pub fn main() {
  if std::env::var("SKIP_FILE_GEN").is_ok() {
    return;
  }
  println!("cargo:rerun-if-changed={SCHEMA}");
  std::fs::create_dir_all("src/report/generated").unwrap();
  generate_module("src/report/generated/mod.rs").expect("files exist");
}

fn generate_module(dest: &str) -> anyhow::Result<()> {
  let builder = Typewriter::new()?;
  let mut module = File::create(dest)?;
  builder.generate_types(&mut module)?;
  format_file(dest)
}

fn format_file(path: &str) -> anyhow::Result<()> {
  let mut command = Command::new("rustfmt")
    .arg("+nightly")
    .arg(path)
    .stderr(Stdio::piped())
    .spawn()
    .expect("failed to run rustfmt");

  let status = command
    .wait()?
    .code()
    .expect("no status set for fmt process");

  let mut stderr = command.stderr.take().unwrap();
  let mut output = vec![];

  std::io::copy(&mut stderr, &mut output)?;

  if status != 0 {
    for line in String::from_utf8(output).unwrap().split('\n') {
      println!("{line}");
    }
  }

  Ok(())
}

struct Typewriter {
  flatbuffer_schema: String,
  definitions: Map<String, Value>,
}

impl Typewriter {
  fn new() -> anyhow::Result<Self> {
    let flatbuffer_schema = read_to_string(FBS)?;
    let json = read_to_string(SCHEMA)?;
    let json_schema: Value = serde_json::from_str(&json)?;
    let mut definitions = json_schema
      .as_object()
      .expect("well-formed json schema")
      .get("definitions")
      .expect("has definitions")
      .as_object()
      .expect("is an object")
      .clone();
    definitions.sort_keys();
    Ok(Self {
      flatbuffer_schema,
      definitions,
    })
  }

  fn generate_types<T: Write>(&self, output: &mut T) -> anyhow::Result<()> {
    write!(output, "{PRELUDE}")?;
    for (name, definition) in &self.definitions {
      let definition = definition.as_object().expect("definition is an object");
      let resolved_name = self.get_struct_name(name, false);
      if definition.contains_key("enum") {
        write!(output, "{}", ENUM.replace("NAME", &resolved_name))?;
        continue;
      }
      let resolved_top_level_name = self.get_struct_name(name, true);

      let raw_properties = definition
        .get("properties")
        .expect("definition has properties")
        .as_object()
        .expect("properties is an object");
      let properties = raw_properties
        .iter()
        .map(|(key, value)| {
          (
            key,
            self.resolve_object(value).expect("all refs are defined"),
          )
        })
        .filter(|(_, value)| !value.contains_key("anyOf"))
        .collect::<BTreeMap<_, _>>(); // ignore anyOf
      self.write_from_impl(
        &resolved_name,
        &resolved_top_level_name,
        &properties,
        output,
      )?;
      self.write_scriptable_impl(
        &resolved_name,
        &resolved_top_level_name,
        definition,
        raw_properties,
        &properties,
        output,
      )?;
    }
    Ok(())
  }

  /// Differentiate flatbuffer tables from structs; the former need lifetime
  /// annotations in type definitions
  fn is_struct(&self, name: &str) -> bool {
    self.flatbuffer_schema.contains(&format!("struct {name}")) || name == "ReportTimestamp"
  }

  fn get_struct_name(&self, name: &str, is_top_level_struct: bool) -> String {
    let mut base = name.split('_').next_back().unwrap().to_string();
    let mut add_lifetime = !self.is_struct(&base);
    if base == "Timestamp" {
      // special case to rename two otherwise identically named types (also
      // renamed in template imports)
      base = if name.contains("reporting") {
        "ReportTimestamp"
      } else {
        add_lifetime = true; // common Timestamp is a table not struct
        "CommonTimestamp"
      }
      .to_string();
    }
    if is_top_level_struct && add_lifetime {
      base += "<'_>";
    }

    base.clone()
  }

  fn resolve_object<'a>(&'a self, value: &'a Value) -> anyhow::Result<&'a Map<String, Value>> {
    let obj_value = if let Some(reference) = value.get("$ref") {
      let Some(reference) = reference.as_str() else {
        bail!("$ref is not a string: {reference:#?}");
      };
      let Some(ref_value) = get_path(&self.definitions, reference).and_then(|v| v.as_object())
      else {
        bail!("$ref did not correspond to definitions: '{reference}'");
      };
      ref_value
    } else {
      value
        .as_object()
        .ok_or_else(|| anyhow!("expected object type for {value:#?}"))?
    };
    Ok(obj_value)
  }

  fn resolve_kind(&self, value: &Value) -> Option<String> {
    let Ok(data) = self.resolve_object(value) else {
      return None;
    };
    let data_type = data.get("type")?.as_str()?;

    match data_type {
      "boolean" | "float" | "integer" | "null" => Some(format!("Kind::{data_type}()")),
      "number" => Some("Kind::float()".to_string()),
      "string" => Some("Kind::bytes()".to_string()),
      "object" => {
        let refname = data.get("$ref")?;
        let item_type = self.get_struct_name(refname.as_str().unwrap(), false);
        Some(format!("{item_type}::schema()"))
      },
      "array" => {
        let items = data.get("items").expect("array has items");
        let item_type = items.get("$ref").map_or_else(
          || self.resolve_kind(items).unwrap(),
          |refname| {
            let struct_name = self.get_struct_name(refname.as_str().unwrap(), false);
            format!("{struct_name}::schema()")
          },
        );
        Some(format!(
          "Kind::array(Collection::empty().with_unknown({item_type}))"
        ))
      },
      _ => None,
    }
  }

  fn write_from_impl<T: Write>(
    &self,
    resolved_name: &str,
    resolved_top_level_name: &str,
    properties: &BTreeMap<&String, &Map<String, Value>>,
    output: &mut T,
  ) -> anyhow::Result<()> {
    // impl From<{name}> for ScriptValue
    let mut obj_from = FROM_TOP.replace("NAME", resolved_top_level_name);
    for prop in properties.keys() {
      let method_name = get_method_for_field(prop);
      obj_from = format!("{obj_from}      (\"{prop}\", value.{method_name}().into()),\n");
    }
    obj_from = format!("{obj_from}{FROM_BOTTOM}");
    write!(output, "{obj_from}")?;
    // Add ref versions of type defs for structs
    if self.is_struct(resolved_name) {
      write!(
        output,
        "{}",
        obj_from.replace(resolved_name, &format!("&{resolved_name}"))
      )?;
    }
    Ok(())
  }

  fn write_scriptable_impl<T: Write>(
    &self,
    resolved_name: &str,
    resolved_top_level_name: &str,
    definition: &Map<String, Value>,
    raw_properties: &Map<String, Value>,
    properties: &BTreeMap<&String, &Map<String, Value>>,
    output: &mut T,
  ) -> anyhow::Result<()> {
    // impl Scriptable for {name}
    write!(
      output,
      "{}",
      OBJ_SCRIPTABLE.replace("NAME", resolved_top_level_name)
    )?;
    for (prop, data) in properties {
      let method_name = get_method_for_field(prop);
      let data_type = data
        .get("type")
        .expect("data has type")
        .as_str()
        .expect("type is str");
      match data_type {
        "object" | "integer" | "boolean" | "number" => {
          writeln!(
            output,
            "      \"{prop}\" => self.{method_name}().resolve(&path[1 ..]),"
          )?;
        },
        "string" | "array" if is_required_property(resolved_name, prop, definition, data) => {
          writeln!(
            output,
            "      \"{prop}\" => self.{method_name}().resolve(&path[1 ..]),"
          )?;
        },
        "array" => {
          writeln!(
            output,
            "      \"{prop}\" => {{
        let Some(values) = self.{method_name}() else {{
          return Ok(None);
        }};
        values.resolve(&path[1 ..])
      }},"
          )?;
        },
        "string" => {
          writeln!(
            output,
            "      \"{prop}\" => self.{method_name}().map_or(Ok(None), |value| \
             value.resolve(&path[1 ..])),"
          )?;
        },
        other => bail!("unknown type: {other}"),
      }
    }

    write!(output, "{OBJ_RESOLVE_END}")?;
    for prop in properties.keys() {
      let raw_data = raw_properties
        .get(prop.as_str())
        .expect("we know it exists");
      if let Some(kind) = self.resolve_kind(raw_data) {
        write!(output, "\n        .with_known(\"{prop}\", {kind})")?;
      }
    }
    write!(
      output,
      "{}",
      OBJ_BOTTOM.replace("NAME", resolved_top_level_name)
    )?;

    Ok(())
  }
}

fn get_method_for_field(name: &str) -> String {
  if name == "type" {
    format!("{name}_")
  } else {
    name.to_string()
  }
}

fn get_path<'a>(obj: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
  let mut root = obj;
  let mut value = None;
  for component in path.split('/') {
    if component == "#" {
      continue;
    }
    value = root.get(component);
    if let Some(partial_obj) = value
      && partial_obj.is_object()
    {
      root = partial_obj.as_object().unwrap();
    }
  }
  value
}

fn is_required_property(
  object_name: &str,
  name: &str,
  definition: &Map<String, Value>,
  resolved_object: &Map<String, Value>,
) -> bool {
  definition.get("required").map_or_else(
    || resolved_object.contains_key("enum"),
    |required| {
      let required = required.as_array().expect("required is an array");
      required.contains(&Value::String(name.to_string()))
    },
    // value_type not being marked as required in Field is probably a bug in the
    // flatbuffers json schema generator around how unions are handled
  ) || (object_name == "Field" && name == "value_type")
}

// Templates

const PRELUDE: &str = r"// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ScriptValue;
use crate::input::{Scriptable, PathError};
use bd_proto::flatbuffers::common::bitdrift_public::fbs::common::v_1::{
  StringData, Field, Data, BinaryData, Timestamp as CommonTimestamp
};
#[allow(clippy::wildcard_imports)]
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  *,
  Timestamp as ReportTimestamp
};
use std::collections::BTreeMap;
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedValuePath};
use vrl::prelude::Collection;
use vrl::value::{KeyString, Kind};

";

const FROM_TOP: &str = r"
impl From<NAME> for ScriptValue {
  fn from(value: NAME) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
";

const FROM_BOTTOM: &str = "    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}
";

const OBJ_SCRIPTABLE: &str = r"
impl Scriptable for NAME {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
";
const OBJ_RESOLVE_END: &str = r"      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()";

const OBJ_BOTTOM: &str = r",
    )
  }
}
";

const ENUM: &str = r"
impl From<NAME> for ScriptValue {
  fn from(value: NAME) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for NAME {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}
";
