// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::{anyhow, bail};
use serde_json::{Map, Value};
use std::fs::{File, read_to_string};
use std::io::Write;
use std::process::{Command, Stdio};

const SCHEMA: &str = "../bd-proto/src/flatbuffers/report.schema.json";
const PRELUDE: &str = r"// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use vrl::prelude::Collection;
use vrl::value::Kind;

#[must_use]
pub fn get_report_schema() -> Kind {
";
const CONCLUSION: &str = r"}";

pub fn main() {
  if std::env::var("SKIP_FILE_GEN").is_ok() {
    return;
  }
  println!("cargo:rerun-if-changed={SCHEMA}");
  std::fs::create_dir_all("src/generated").unwrap();

  generate_module(SCHEMA, "src/generated/mod.rs").unwrap();
}

fn generate_module(src: &str, dest: &str) -> anyhow::Result<()> {
  let json = read_to_string(src)?;
  let parsed: Value = serde_json::from_str(&json)?;
  let definitions = parsed
    .as_object()
    .ok_or_else(|| anyhow!("failed to convert Value to object"))?;
  let mut module = File::create(dest)?;
  write!(module, "{PRELUDE}")?;
  write_kind(&parsed, definitions, &mut module)?;
  write!(module, "{CONCLUSION}")?;
  module.flush()?;

  let mut command = Command::new("cargo")
    .arg("+nightly")
    .arg("fmt")
    .arg("--package")
    .arg("bd-report-script")
    .stderr(Stdio::piped())
    .spawn()
    .expect("failed to run cargo fmt");

  let status = command
    .wait()?
    .code()
    .expect("no status set for fmt process");

  let mut stderr = command.stderr.take().unwrap();
  let mut output = vec![];

  std::io::copy(&mut stderr, &mut output)?;

  if status != 0 {
    println!("cargo:error=cargo fmt failed with status {status:#?}");
    for line in String::from_utf8(output).unwrap().split('\n') {
      println!("cargo:warning={line:#?}");
    }
  }

  Ok(())
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

fn resolve_object<'a>(
  value: &'a Value,
  definitions: &'a Map<String, Value>,
) -> anyhow::Result<&'a Map<String, Value>> {
  let obj_value = if let Some(reference) = value.get("$ref") {
    let Some(reference) = reference.as_str() else {
      bail!("$ref is not a string: {reference:#?}");
    };
    let Some(ref_value) = get_path(definitions, reference).and_then(|v| v.as_object()) else {
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

fn write_kind<T: Write>(
  obj: &Value,
  definitions: &Map<String, Value>,
  output: &mut T,
) -> anyhow::Result<()> {
  write!(output, "Kind::")?;
  let obj = resolve_object(obj, definitions)?;
  if let Some(type_) = obj.get("type")
    && type_.is_string()
  {
    match type_.as_str() {
      Some("boolean") => write!(output, "boolean()")?,
      Some("integer") => write!(output, "integer()")?,
      Some("number") => write!(output, "float()")?,
      Some("string") => write!(output, "bytes()")?,
      Some("object") => {
        write!(output, "object(Collection::empty()")?;
        if let Some(fields) = obj.get("properties") {
          let fields = fields
            .as_object()
            .ok_or_else(|| anyhow!("properties not an object: {obj:#?}"))?;
          for (name, field) in fields {
            write!(output, ".with_known(\"{name}\", ")?;
            write_kind(field, definitions, output)?;
            write!(output, ")")?;
          }
        }
        write!(output, ")")?;
      },
      Some("array") => {
        write!(output, "array(Collection::empty()")?;
        if let Some(items) = obj.get("items") {
          write!(output, ".with_unknown(")?;
          write_kind(items, definitions, output)?;
          write!(output, ")")?;
        }
        write!(output, ")")?;
      },
      _ => bail!("object type '{type_:#?}' could not be converted to string"),
    }
  } else {
    // skip types where not fully known, e.g. `anyOf` type permutations
    write!(output, "undefined()")?;
  }

  Ok(())
}
