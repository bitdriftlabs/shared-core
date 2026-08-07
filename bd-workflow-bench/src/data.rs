// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use ahash::AHashMap;
use anyhow::anyhow;
use base64::Engine as _;
use bd_log_primitives::{DataValue, LogArrayData, LogBinaryData, LogMapData};
use ordered_float::NotNan;
use serde_json::{Map, Value};

pub fn data_value(value: Value, location: &str) -> anyhow::Result<DataValue> {
  let Value::Object(data) = value else {
    return Err(anyhow!("{location}: data value must be an object"));
  };

  if data.len() != 1 {
    return Err(anyhow!(
      "{location}: data value must have exactly one variant"
    ));
  }

  let Some((variant, value)) = data.into_iter().next() else {
    return Err(anyhow!("{location}: data value is empty"));
  };
  match variant.as_str() {
    "string_data" | "stringData" => Ok(string(&value, location)?.into()),
    "binary_data" | "binaryData" => binary(value, location),
    "int_data" | "intData" => Ok(DataValue::U64(unsigned(value, location)?)),
    "sint_data" | "sintData" => Ok(DataValue::I64(signed(value, location)?)),
    "double_data" | "doubleData" => Ok(DataValue::Double(double(value, location)?)),
    "bool_data" | "boolData" => Ok(DataValue::Boolean(boolean(&value, location)?)),
    "map_data" | "mapData" => map(value, location),
    "array_data" | "arrayData" => array(value, location),
    _ => Err(anyhow!("{location}: unsupported data variant {variant:?}")),
  }
}

fn string(value: &Value, location: &str) -> anyhow::Result<String> {
  value
    .as_str()
    .map(ToOwned::to_owned)
    .ok_or_else(|| anyhow!("{location}: expected a string"))
}

fn boolean(value: &Value, location: &str) -> anyhow::Result<bool> {
  value
    .as_bool()
    .ok_or_else(|| anyhow!("{location}: expected a boolean"))
}

fn unsigned(value: Value, location: &str) -> anyhow::Result<u64> {
  match value {
    Value::Number(value) => value
      .as_u64()
      .ok_or_else(|| anyhow!("{location}: expected an unsigned integer")),
    Value::String(value) => value
      .parse()
      .map_err(|e| anyhow!("{location}: invalid unsigned integer: {e}")),
    _ => Err(anyhow!("{location}: expected an unsigned integer")),
  }
}

fn signed(value: Value, location: &str) -> anyhow::Result<i64> {
  match value {
    Value::Number(value) => value
      .as_i64()
      .ok_or_else(|| anyhow!("{location}: expected a signed integer")),
    Value::String(value) => value
      .parse()
      .map_err(|e| anyhow!("{location}: invalid signed integer: {e}")),
    _ => Err(anyhow!("{location}: expected a signed integer")),
  }
}

fn double(value: Value, location: &str) -> anyhow::Result<NotNan<f64>> {
  let value = match value {
    Value::Number(value) => value
      .as_f64()
      .ok_or_else(|| anyhow!("{location}: expected a double"))?,
    Value::String(value) => value
      .parse()
      .map_err(|e| anyhow!("{location}: invalid double: {e}"))?,
    _ => return Err(anyhow!("{location}: expected a double")),
  };
  NotNan::new(value).map_err(|_| anyhow!("{location}: double cannot be NaN"))
}

fn binary(value: Value, location: &str) -> anyhow::Result<DataValue> {
  let data = object(value, location)?;
  let payload = data
    .get("payload")
    .and_then(Value::as_str)
    .ok_or_else(|| anyhow!("{location}: binary data is missing payload"))?;
  let bytes = base64::engine::general_purpose::STANDARD
    .decode(payload)
    .map_err(|e| anyhow!("{location}: invalid base64 binary payload: {e}"))?;
  Ok(DataValue::Bytes(LogBinaryData::new(bytes)))
}

fn map(value: Value, location: &str) -> anyhow::Result<DataValue> {
  let data = object(value, location)?;
  let entries = data
    .get("entries")
    .and_then(Value::as_object)
    .ok_or_else(|| anyhow!("{location}: map data is missing entries"))?;
  let entries = entries
    .iter()
    .map(|(key, value)| {
      data_value(value.clone(), &format!("{location}.{key}")).map(|value| (key.clone(), value))
    })
    .collect::<anyhow::Result<AHashMap<_, _>>>()?;
  Ok(DataValue::Map(LogMapData::new(entries)))
}

fn array(value: Value, location: &str) -> anyhow::Result<DataValue> {
  let data = object(value, location)?;
  let items = data
    .get("items")
    .and_then(Value::as_array)
    .ok_or_else(|| anyhow!("{location}: array data is missing items"))?;
  let items = items
    .iter()
    .enumerate()
    .map(|(index, value)| data_value(value.clone(), &format!("{location}[{index}]")))
    .collect::<anyhow::Result<Vec<_>>>()?;
  Ok(DataValue::Array(LogArrayData::new(items)))
}

fn object(value: Value, location: &str) -> anyhow::Result<Map<String, Value>> {
  match value {
    Value::Object(value) => Ok(value),
    _ => Err(anyhow!("{location}: expected an object")),
  }
}
